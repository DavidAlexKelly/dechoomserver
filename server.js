#!/usr/bin/env node
/**
 * DECHOOM Multiplayer Relay Server
 *
 * Protocol (from net_websockets.c / d_loop.c):
 *
 * LOBBY PHASE (text frames):
 *   Server → Client: { type: "waiting", count: N, need: M }
 *   Server → Client: { type: "start", role: "server"|"client" }
 *     - First player gets role "server" (Doom -server flag)
 *     - All others get role "client" (Doom -connect 1 flag)
 *
 * GAME PHASE (binary frames, 8-byte header):
 *   [to: uint32LE][from: uint32LE][payload...]
 *   - to=0 means broadcast to all other players
 *   - to=N means unicast to player with uid=N
 *   Server stamps bytes[4..7] with real sender uid before forwarding.
 */

const { WebSocketServer } = require("ws");
const http = require("http");

const PORT = process.env.PORT || 2342;
const MIN_PLAYERS = 2;
const PLAYER_TIMEOUT_MS = 5 * 60 * 1000;

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

const httpServer = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  if (url.pathname === "/reset" && req.method === "POST") {
    resetState();
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("Reset OK\n");
    return;
  }
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ status: "DECHOOM relay OK", clients: clients.size, gameStarted, nextUid }) + "\n");
});

const wss = new WebSocketServer({ server: httpServer });

function sendText(ws, obj) {
  try { ws.send(JSON.stringify(obj)); } catch (_) {}
}

function broadcastWaiting() {
  const msg = { type: "waiting", count: clients.size, need: MIN_PLAYERS };
  for (const p of clients.values()) {
    if (p.ws.readyState === 1) sendText(p.ws, msg);
  }
}

function startGame() {
  gameStarted = true;
  log(`Starting game with ${clients.size} players`);
  let position = 1;
  for (const player of clients.values()) {
    if (player.ws.readyState === 1) {
      const role = position === 1 ? "server" : "client";
      player.role = role;
      player.ready = true;
      sendText(player.ws, { type: "start", role });
      log(`Sent role=${role} to uid=${player.uid}`);
    }
    position++;
  }
}

wss.on("connection", (ws, req) => {
  const uid = allocateUid();
  const player = { ws, uid, role: null, ready: false, joinedAt: Date.now() };
  clients.set(uid, player);
  log(`[+] Player connected uid=${uid} addr=${req.socket.remoteAddress} total=${clients.size}`);

  broadcastWaiting();

  if (clients.size >= MIN_PLAYERS && !gameStarted) {
    startGame();
  }

  if (gameStarted && !player.ready) {
    player.role = "client";
    player.ready = true;
    sendText(ws, { type: "start", role: "client" });
    log(`Late join uid=${uid} as client`);
  }

  ws.on("message", (data) => {
    if (typeof data === "string") return;
    if (!(data instanceof Buffer)) data = Buffer.from(data);
    if (data.length < 8) return;

    const toUid = data.readUInt32LE(0);
    data.writeUInt32LE(uid, 4);

    if (toUid === 0) {
      for (const other of clients.values()) {
        if (other.uid !== uid && other.ready && other.ws.readyState === 1) {
          try { other.ws.send(data); } catch (_) {}
        }
      }
    } else {
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
    broadcastWaiting();
  });

  ws.on("error", (err) => {
    log(`[!] Error uid=${uid}:`, err.message);
    clients.delete(uid);
    if (clients.size < MIN_PLAYERS) {
      gameStarted = false;
      if (clients.size === 0) nextUid = 1;
    }
  });

  setTimeout(() => {
    if (!player.ready && ws.readyState === 1) {
      log(`Timeout uid=${uid}, starting solo as server`);
      player.role = "server";
      player.ready = true;
      gameStarted = true;
      sendText(ws, { type: "start", role: "server" });
    }
  }, PLAYER_TIMEOUT_MS);
});

httpServer.listen(PORT, () => {
  log(`DECHOOM relay listening on http://0.0.0.0:${PORT}`);
  log(`Waiting for ${MIN_PLAYERS} players before starting`);
});

process.on("SIGTERM", () => { log("Shutting down..."); wss.close(); });
