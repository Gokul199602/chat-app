const { createServer } = require("http");
const { Server } = require("socket.io");
const { createClient } = require("redis");
const { createAdapter } = require("@socket.io/redis-adapter");
const fs = require("fs");
const path = require("path");

const portIndex = process.argv.indexOf("--port");
const PORT = portIndex !== -1 ? Number(process.argv[portIndex + 1]) : 3000;

async function main() {
  const pubClient = createClient({ url: "redis://localhost:6379" });
  const subClient = pubClient.duplicate();

  await Promise.all([pubClient.connect(), subClient.connect()]);
  console.log("Redis connected");

  // Subscribe to "room-messages" channel
  // Any message published here will be picked up by ALL socket server instances
  await subClient.subscribe("room-messages", (raw) => {
    // raw is a string, parse it back to object
    const { roomId, text } = JSON.parse(raw);
    console.log("Received from Redis channel:", roomId, text);

    // Emit to the room
    io.to(roomId).emit("message", { id: "REST-API", text });
  });

  console.log("Subscribed to Redis channel: room-messages");

  const httpServer = createServer((req, res) => {
    const htmlPath = path.join(__dirname, "static", "index.html");
    const fileStream = fs.createReadStream(htmlPath);
    res.writeHead(200, { "Content-Type": "text/html" });
    fileStream.on("error", () => {
      res.writeHead(404, { "Content-Type": "text/plain" });
      res.end("File not found");
    });
    fileStream.pipe(res);
  });

  const io = new Server(httpServer);
  io.adapter(createAdapter(pubClient, subClient));

  io.on("connection", (socket) => {
    console.log("connected:", socket.id);

    socket.on("join", (roomId) => {
      socket.join(roomId);
      console.log(socket.id, "joined room:", roomId);
    });

    socket.on("message", ({ roomId, text }) => {
      io.to(roomId).emit("message", { id: socket.id, text });
    });

    socket.on("disconnect", () => {
      console.log("disconnected:", socket.id);
    });
  });

  httpServer.listen(PORT, () => {
    console.log(`Server running at http://localhost:${PORT}`);
  });
}

main();
