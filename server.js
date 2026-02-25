#!/usr/bin/env node
/**
 * DECHOOM Multiplayer Relay Server
 *
 * Protocol (from net_websockets.c):
 *   Every binary message is: [to: uint32LE][from: uint32LE][payload...]
 *
 *   - On connect: server sends the client their UID as 4 bytes (uint32LE)
 *     Doom stores this as instanceUID and uses it as the "from" address.
 *   - Client sends a registration packet: to=0, from=instanceUID, payload empty (8 bytes)
 *     This tells the server "I exist, my UID is X".
 *   - Subsequent packets: server reads bytes[0..3] as destination UID,
 *     stamps bytes[4..7] with the real sender UID, and routes to that recipient.
 *   - If to=0 (broadcast), relay to all other ready clients.
 *
 * Lobby:
 *   Hold clients until MIN_PLAYERS connected, then send UIDs simultaneously
 *   so Doom's handshake window opens at the same time for all players.
 */

const { WebSocketServer } = require("ws");
const http = require("http");

const PORT = process.env.PORT || 2342;
const MIN_PLAYERS = 2;
const MAX_PLAYERS = 4;
const PLAYER_TIMEOUT_MS = 5 * 60 * 1000; // 5 min max wait

// clients: Map<uid, { ws, uid, ready, joinedAt }>
let clients = new Map();
let nextUid = 1;
let gameStarted = false;

function log(...args) {
  console.log(new Date().toISOString(), ...args);
}

function allocateUid() {
  return nextUid++;
}

function resetState() {
  for (const p of clients.values()) {
    try { p.ws.terminate(); } catch (_) {}
  }
  clients = new Map();
  gameStarted = false;
  nextUid = 1;
  log("State reset.");
}

// HTTP server for health checks and admin
const httpServer = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);

  if (url.pathname === "/reset" && req.method === "POST") {
    resetState();
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("Reset OK\n");
    return;
  }

  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify({
    status: "DECHOOM relay OK",
    clients: clients.size,
    gameStarted,
    nextUid,
  }) + "\n");
});

const wss = new WebSocketServer({ server: httpServer });

function sendUid(ws, uid) {
  // Send the UID to the client as 4-byte little-endian uint32
  // Doom reads this as instanceUID in net_websockets.c
  const buf = Buffer.alloc(4);
  buf.writeUInt32LE(uid, 0);
  ws.send(buf);
}

function broadcastPlayerCount() {
  const msg = JSON.stringify({ type: "waiting", count: clients.size, need: MIN_PLAYERS });
  for (const p of clients.values()) {
    if (p.ws.readyState === 1) {
      try { p.ws.send(msg); } catch (_) {}
    }
  }
}

function startGame() {
  gameStarted = true;
  log(`Starting game with ${clients.size} players`);
  // Send all UIDs simultaneously so all clients start their handshake together
  for (const player of clients.values()) {
    if (player.ws.readyState === 1) {
      sendUid(player.ws, player.uid);
      player.ready = true;
      log(`Sent UID ${player.uid} to player`);
    }
  }
}

wss.on("connection", (ws, req) => {
  const uid = allocateUid();
  const player = { ws, uid, ready: false, joinedAt: Date.now() };
  clients.set(uid, player);

  const addr = req.socket.remoteAddress;
  log(`[+] Player connected uid=${uid} addr=${addr} total=${clients.size}`);

  broadcastPlayerCount();

  // If we already have enough players, start immediately
  if (clients.size >= MIN_PLAYERS && !gameStarted) {
    startGame();
  }

  // Late join: game already in progress, give them their UID right away
  if (gameStarted && !player.ready) {
    sendUid(ws, uid);
    player.ready = true;
    log(`Late join uid=${uid}`);
  }

  ws.on("message", (data) => {
    if (!(data instanceof Buffer)) data = Buffer.from(data);

    // Must be at least 8 bytes (to + from header)
    if (data.length < 8) return;

    // Read destination UID from bytes [0..3]
    const toUid = data.readUInt32LE(0);

    // Stamp the real sender UID into bytes [4..7]
    data.writeUInt32LE(uid, 4);

    if (toUid === 0) {
      // Broadcast to all other ready clients
      for (const other of clients.values()) {
        if (other.uid !== uid && other.ready && other.ws.readyState === 1) {
          try { other.ws.send(data); } catch (_) {}
        }
      }
    } else {
      // Unicast to specific client
      const target = clients.get(toUid);
      if (target && target.ready && target.ws.readyState === 1) {
        try { target.ws.send(data); } catch (_) {}
      }
    }
  });

  ws.on("close", () => {
    clients.delete(uid);
    log(`[-] Player disconnected uid=${uid} remaining=${clients.size}`);

    if (clients.size < MIN_PLAYERS) {
      gameStarted = false;
      if (clients.size === 0) nextUid = 1;
      log(`Below min players, reset gameStarted (${clients.size} remaining)`);
    }

    broadcastPlayerCount();
  });

  ws.on("error", (err) => {
    log(`[!] Error uid=${uid}:`, err.message);
    clients.delete(uid);
    if (clients.size < MIN_PLAYERS) {
      gameStarted = false;
      if (clients.size === 0) nextUid = 1;
    }
  });

  // Timeout: if still waiting alone after PLAYER_TIMEOUT_MS, send UID anyway
  setTimeout(() => {
    if (!player.ready && ws.readyState === 1) {
      log(`Timeout uid=${uid}, starting solo`);
      sendUid(ws, uid);
      player.ready = true;
      gameStarted = true;
    }
  }, PLAYER_TIMEOUT_MS);
});

httpServer.listen(PORT, () => {
  log(`DECHOOM relay listening on http://0.0.0.0:${PORT}`);
  log(`Waiting for ${MIN_PLAYERS} players before starting`);
});

process.on("SIGTERM", () => {
  log("Shutting down...");
  wss.close();
});
