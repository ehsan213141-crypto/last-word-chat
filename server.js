const express = require('express');
const http = require('http');
const path = require('path');
const bcrypt = require('bcrypt');
const { Server } = require('socket.io');
const db = require('./db');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const PORT = 3000;
const SALT_ROUNDS = 10;
const SELF_DESTRUCT_MS = 10000; // 10 seconds after the receiver opens a message

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ---------------------------------------------------------------------------
// Auth routes
// ---------------------------------------------------------------------------

app.post('/api/signup', (req, res) => {
  const { username, password } = req.body || {};
  if (!username || !password || username.trim().length < 3 || password.length < 4) {
    return res.status(400).json({ error: 'Username needs 3+ chars, password needs 4+ chars.' });
  }
  const cleanUsername = username.trim();

  db.get('SELECT id FROM users WHERE username = ?', [cleanUsername], (err, row) => {
    if (err) return res.status(500).json({ error: 'Server error.' });
    if (row) return res.status(409).json({ error: 'That username is already taken.' });

    bcrypt.hash(password, SALT_ROUNDS, (hashErr, hash) => {
      if (hashErr) return res.status(500).json({ error: 'Server error.' });
      db.run(
        'INSERT INTO users (username, password, created_at) VALUES (?, ?, ?)',
        [cleanUsername, hash, Date.now()],
        function (insertErr) {
          if (insertErr) return res.status(500).json({ error: 'Server error.' });
          res.json({ username: cleanUsername });
        }
      );
    });
  });
});

app.post('/api/login', (req, res) => {
  const { username, password } = req.body || {};
  if (!username || !password) {
    return res.status(400).json({ error: 'Username and password required.' });
  }
  const cleanUsername = username.trim();

  db.get('SELECT * FROM users WHERE username = ?', [cleanUsername], (err, row) => {
    if (err) return res.status(500).json({ error: 'Server error.' });
    if (!row) return res.status(401).json({ error: 'Invalid username or password.' });

    bcrypt.compare(password, row.password, (cmpErr, match) => {
      if (cmpErr) return res.status(500).json({ error: 'Server error.' });
      if (!match) return res.status(401).json({ error: 'Invalid username or password.' });
      res.json({ username: row.username });
    });
  });
});

// ---------------------------------------------------------------------------
// Realtime chat
// ---------------------------------------------------------------------------

const onlineUsers = new Map();   // username -> socket.id
const expiryTimers = new Map();  // messageId -> Timeout handle

function broadcastUserList() {
  io.emit('user_list', Array.from(onlineUsers.keys()));
}

function notify(users, event, payload) {
  users.forEach((u) => {
    const sid = onlineUsers.get(u);
    if (sid) io.to(sid).emit(event, payload);
  });
}

function scheduleExpiry(msg) {
  if (expiryTimers.has(msg.id)) return;
  const remaining = Math.max(0, SELF_DESTRUCT_MS - (Date.now() - msg.opened_at));
  const timer = setTimeout(() => {
    db.run('DELETE FROM messages WHERE id = ?', [msg.id], (err) => {
      expiryTimers.delete(msg.id);
      if (err) return;
      notify([msg.sender, msg.receiver], 'message_expired', { id: msg.id });
    });
  }, remaining);
  expiryTimers.set(msg.id, timer);
}

io.on('connection', (socket) => {
  const username = socket.handshake.auth && socket.handshake.auth.username;
  if (!username) {
    socket.disconnect(true);
    return;
  }

  socket.data.username = username;
  onlineUsers.set(username, socket.id);
  broadcastUserList();

  // Load conversation history with another user. Any message where I'm the
  // receiver and it hasn't been opened yet gets marked opened right now,
  // which kicks off its 10-second self-destruct timer.
  socket.on('get_history', ({ with: otherUser } = {}) => {
    if (!otherUser) return;

    db.all(
      `SELECT * FROM messages WHERE
        (sender = ? AND receiver = ?) OR (sender = ? AND receiver = ?)
        ORDER BY created_at ASC`,
      [username, otherUser, otherUser, username],
      (err, rows) => {
        if (err) return;
        const now = Date.now();
        const results = [];
        const toMarkOpened = [];

        rows.forEach((row) => {
          // Edge case: server restarted mid-countdown, so the in-memory timer
          // never fired even though the 10s already elapsed. Clean it up.
          if (row.opened_at && now - row.opened_at >= SELF_DESTRUCT_MS) {
            db.run('DELETE FROM messages WHERE id = ?', [row.id]);
            return;
          }
          if (row.receiver === username && !row.opened_at) {
            row.opened_at = now;
            toMarkOpened.push(row.id);
          }
          results.push(row);
        });

        const sendHistory = () => socket.emit('history', { with: otherUser, messages: results });

        if (!toMarkOpened.length) return sendHistory();

        const placeholders = toMarkOpened.map(() => '?').join(',');
        db.run(
          `UPDATE messages SET opened_at = ? WHERE id IN (${placeholders})`,
          [now, ...toMarkOpened],
          () => {
            results
              .filter((r) => toMarkOpened.includes(r.id))
              .forEach((r) => {
                scheduleExpiry(r);
                notify([r.sender, r.receiver], 'message_opened', { id: r.id, opened_at: now });
              });
            sendHistory();
          }
        );
      }
    );
  });

  // Fired by the client the instant a fresh incoming message renders inside
  // an already-open chat window (rather than waiting for the next history fetch).
  socket.on('open_message', ({ id } = {}) => {
    if (!id) return;
    db.get('SELECT * FROM messages WHERE id = ?', [id], (err, row) => {
      if (err || !row) return;
      if (row.receiver !== username || row.opened_at) return;

      const now = Date.now();
      db.run('UPDATE messages SET opened_at = ? WHERE id = ?', [now, id], (updateErr) => {
        if (updateErr) return;
        row.opened_at = now;
        scheduleExpiry(row);
        notify([row.sender, row.receiver], 'message_opened', { id, opened_at: now });
      });
    });
  });

  socket.on('send_message', ({ to, content } = {}) => {
    if (!to || !content || !content.trim()) return;
    const now = Date.now();

    db.run(
      'INSERT INTO messages (sender, receiver, content, created_at) VALUES (?, ?, ?, ?)',
      [username, to, content.trim(), now],
      function (err) {
        if (err) return;
        const msg = {
          id: this.lastID,
          sender: username,
          receiver: to,
          content: content.trim(),
          created_at: now,
          opened_at: null,
        };
        socket.emit('message_sent', msg);
        const sid = onlineUsers.get(to);
        if (sid) io.to(sid).emit('receive_message', msg);
      }
    );
  });

  socket.on('disconnect', () => {
    onlineUsers.delete(username);
    broadcastUserList();
  });
});

server.listen(PORT, () => {
  console.log(`💀 LAST WORD is running at http://localhost:${PORT}`);
});
