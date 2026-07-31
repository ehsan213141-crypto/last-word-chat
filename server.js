const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const PORT = process.env.PORT || 3000;

// Static files serve karo
app.use(express.static(path.join(__dirname)));

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

// Socket.io logic
io.on('connection', (socket) => {
  console.log('A user connected');
  
  // Jab message aaye to sab ko bhejo
  socket.on('chat message', (data) => {
    io.emit('chat message', data);
  });
  
  // Jab user leave kare
  socket.on('disconnect', () => {
    console.log('User disconnected');
  });
});

server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
