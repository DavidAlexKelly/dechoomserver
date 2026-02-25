#!/usr/bin/env node
/**
 * DECHOOM Multiplayer Relay Server
 * 
 * Holds players in a waiting room until at least 2 are connected,
 * then sends UIDs to all waiting players simultaneously so Doom's
 * handshake window starts at the same time for everyone.
 */

const { WebSocketServer } = require("ws");
const http = require("http");

const PORT = process.env.PORT || 2342;
const MIN_PLAYERS = 2;
const MAX_PLAYERS = 4;
const PLAYER_TIMEOUT_MS = 5 * 60 * 1000; // 5 min max wait

// Attach to an HTTP server so Render's proxy can forward WebSocket upgrades.
// A bare WebSocketServer({ port }) listens raw TCP which Render can't reach.
const httpServer = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("DECHOOM relay OK\n");
});

const wss = new WebSocketServer({ server: httpServer });

// All currently connected clients waiting or in-game
// { ws, uid, ready, joinedAt }
let waiting = [];
let nextUid = 1;
let gameStarted = false;

function allocateUid() {
  return nextUid++;
}

function log(...args) {
  console.log(new Date().toISOString(), ...args);
}

function broadcastPlayerCount() {
  const msg = JSON.stringify({ type: "waiting", count: waiting.length, need: MIN_PLAYERS });
  for (const p of waiting) {
    if (p.ws.readyState === 1) {
      // Send as a text message so we can display it in the UI
      // (Doom ignores text frames, only processes binary)
      try { p.ws.send(msg); } catch(_) {}
    }
  }
}

function startGame() {
  gameStarted = true;
  log(`Starting game with ${waiting.length} players`);

  // Send all UIDs simultaneously so all clients start their handshake together
  for (const player of waiting) {
    if (player.ws.readyState === 1) {
      const hello = Buffer.alloc(4);
      hello.writeUInt32LE(player.uid, 0);
      player.ws.send(hello);
      player.ready = true;
      log(`Sent UID ${player.uid} to player`);
    }
  }
}

wss.on("connection", (ws, req) => {
  const uid = allocateUid();
  const player = { ws, uid, ready: false, joinedAt: Date.now() };
  waiting.push(player);

  const addr = req.socket.remoteAddress;
  log(`[+] Player connected uid=${uid} addr=${addr} waiting=${waiting.length}`);

  broadcastPlayerCount();

  // If we have enough players, start immediately
  if (waiting.length >= MIN_PLAYERS && !gameStarted) {
    startGame();
  }

  // If game already started and a new player joins mid-session, send their UID immediately
  if (gameStarted && !player.ready) {
    const hello = Buffer.alloc(4);
    hello.writeUInt32LE(uid, 0);
    ws.send(hello);
    player.ready = true;
    log(`Late join uid=${uid}`);
  }

  ws.on("message", (data) => {
    if (!(data instanceof Buffer)) data = Buffer.from(data);
    if (data.length < 8) return;

    // Stamp the real sender UID into bytes [4..7]
    data.writeUInt32LE(uid, 4);

    // Relay to all other ready players
    for (const other of waiting) {
      if (other.uid !== uid && other.ready && other.ws.readyState === 1) {
        try { other.ws.send(data); } catch(_) {}
      }
    }
  });

  ws.on("close", () => {
    waiting = waiting.filter(p => p.uid !== uid);
    log(`[-] Player disconnected uid=${uid} remaining=${waiting.length}`);

    // Reset game state if everyone left
    if (waiting.length === 0) {
      gameStarted = false;
      nextUid = 1;
      log("All players left, resetting server");
    }

    broadcastPlayerCount();
  });

  ws.on("error", (err) => {
    log(`[!] Error uid=${uid}:`, err.message);
    waiting = waiting.filter(p => p.uid !== uid);
    if (waiting.length === 0) {
      gameStarted = false;
      nextUid = 1;
    }
  });

  // Timeout player if they wait too long alone
  setTimeout(() => {
    if (!player.ready && player.ws.readyState === 1) {
      log(`Timeout waiting for players, starting solo uid=${uid}`);
      // Send UID anyway so Doom doesn't hang forever
      const hello = Buffer.alloc(4);
      hello.writeUInt32LE(uid, 0);
      ws.send(hello);
      player.ready = true;
      gameStarted = true;
    }
  }, PLAYER_TIMEOUT_MS);
});

httpServer.listen(PORT, () => {
  log(`DECHOOM relay listening on http://0.0.0.0:${PORT}`);
  log(`WebSocket upgrade available at ws://0.0.0.0:${PORT}`);
  log(`Waiting for ${MIN_PLAYERS} players before starting`);
});

process.on("SIGTERM", () => {
  log("Shutting down...");
  wss.close();
});
