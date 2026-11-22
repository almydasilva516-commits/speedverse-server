
const http = require("http");
const crypto = require("crypto");
const WebSocket = require("ws");

const PORT = process.env.PORT || 3000;
const MAX_PLAYERS = 50;

const players = new Map();

const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({
      status: "online",
      game: "SPEEDVERSE",
      players: players.size,
      maxPlayers: MAX_PLAYERS
    }));
  }

  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("SPEEDVERSE Multiplayer Server Online");
});

const wss = new WebSocket.Server({
  server,
  maxPayload: 4096
});

function send(ws, data) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function broadcast(data, excluded = null) {
  const message = JSON.stringify(data);

  for (const player of players.values()) {
    if (
      player.ws !== excluded &&
      player.ws.readyState === WebSocket.OPEN
    ) {
      player.ws.send(message);
    }
  }
}

wss.on("connection", (ws) => {
  if (players.size >= MAX_PLAYERS) {
    send(ws, { type: "server_full" });
    return ws.close();
  }

  const id = crypto.randomUUID();

  const player = {
    id,
    ws,
    x: 0,
    y: 0,
    z: 0,
    rotation: 0,
    animation: "idle"
  };

  players.set(id, player);

  send(ws, {
    type: "welcome",
    id,
    maxPlayers: MAX_PLAYERS,
    players: Array.from(players.values())
      .filter(p => p.id !== id)
      .map(({ id, x, y, z, rotation, animation }) => ({
        id, x, y, z, rotation, animation
      }))
  });

  broadcast({
    type: "player_join",
    player: {
      id,
      x: 0,
      y: 0,
      z: 0,
      rotation: 0,
      animation: "idle"
    }
  }, ws);

  ws.on("message", (raw) => {
    let data;

    try {
      data = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (!data || typeof data !== "object") return;

    if (data.type === "update") {
      const values = ["x", "y", "z", "rotation"];

      for (const key of values) {
        if (
          typeof data[key] !== "number" ||
          !Number.isFinite(data[key]) ||
          Math.abs(data[key]) > 100000
        ) return;
      }

      player.x = data.x;
      player.y = data.y;
      player.z = data.z;
      player.rotation = data.rotation;

      const allowedAnimations = [
        "idle", "walk", "run", "jump", "fall", "punch"
      ];

      if (allowedAnimations.includes(data.animation)) {
        player.animation = data.animation;
      }

      broadcast({
        type: "player_update",
        player: {
          id,
          x: player.x,
          y: player.y,
          z: player.z,
          rotation: player.rotation,
          animation: player.animation
        }
      }, ws);
    }
  });

  ws.on("close", () => {
    players.delete(id);
    broadcast({ type: "player_leave", id });
  });

  ws.on("error", () => {
    players.delete(id);
  });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`SPEEDVERSE ONLINE na porta ${PORT}`);
});
