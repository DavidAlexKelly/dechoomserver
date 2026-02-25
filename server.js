#!/usr/bin/env node
/**
 * DECHOOM Multiplayer Relay Server
 *
 * LOBBY PHASE (JS WebSocket, text frames):
 *   Client → Server: { type: "lobby" }   ← JS sends this on connect to identify itself
 *   Server → Client: { type: "waiting", count: N, need: M }
 *   Server → Client: { type: "start", role: "server"|"client" }
 *
 * GAME PHASE (Doom WebSocket, binary frames):
 *   [to: uint32LE][from: uint32LE][payload...]
 *
 * The relay distinguishes lobby vs game connections by the first message:
 *   - Text JSON with type="lobby" → lobby connection
 *   - Binary 8+ bytes → Doom game connection
 *
 * UID TRANSLATION:
 *   Client hardcodes -connect 1, so server must always appear as uid=1.
 *   Relay rewrites from=serverActualUid → from=1 on all packets to clients.
 *   Relay routes to=1 from clients → actual server connection.
 */

const { WebSocketServer } = require("ws");
const http = require("http");

const PORT = process.env.PORT || 2342;
const MIN_PLAYERS = 2;
const PLAYER_TIMEOUT_MS = 5 * 60 * 1000;

let lobbyClients = new Map();
let nextLobbyUid = 1;
let gameStarted = false;

let serverConn = null;
let serverDoomUid = null;
let gameConns = new Map();

function log(...args) {
  console.log(new Date().toISOString(), ...args);
}

function resetState() {
  for (const p of lobbyClients.values()) {
    try { p.ws.terminate(); } catch (_) {}
  }
  for (const conn of gameConns.values()) {
    try { conn.terminate(); } catch (_) {}
  }
  lobbyClients = new Map();
  gameStarted = false;
  nextLobbyUid = 1;
  serverConn = null;
  serverDoomUid = null;
  gameConns = new Map();
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
  res.end(JSON.stringify({
    status: "DECHOOM relay OK",
    lobbyClients: lobbyClients.size,
    gameStarted,
    serverDoomUid,
    gameConns: gameConns.size,
  }) + "\n");
});

const wss = new WebSocketServer({ server: httpServer });

function sendText(ws, obj) {
  try { ws.send(JSON.stringify(obj)); } catch (_) {}
}

function broadcastWaiting() {
  const msg = { type: "waiting", count: lobbyClients.size, need: MIN_PLAYERS };
  for (const p of lobbyClients.values()) {
    if (p.ws.readyState === 1) sendText(p.ws, msg);
  }
}

function startLobby() {
  gameStarted = true;
  log(`Starting lobby with ${lobbyClients.size} players`);
  let position = 1;
  for (const player of lobbyClients.values()) {
    if (player.ws.readyState === 1) {
      const role = position === 1 ? "server" : "client";
      player.role = role;
      player.ready = true;
      sendText(player.ws, { type: "start", role });
      log(`Sent role=${role} to lobby uid=${player.uid}`);
    }
    position++;
  }
}

function routeGamePacket(ws, data) {
  const toUid = data.readUInt32LE(0);

  const rewritten = Buffer.from(data);
  if (ws.isServer) {
    rewritten.writeUInt32LE(1, 4); // server always appears as uid=1
  }

  if (toUid === 0) {
    for (const conn of gameConns.values()) {
      if (conn !== ws && conn.readyState === 1) {
        try { conn.send(rewritten); } catch (_) {}
      }
    }
  } else if (toUid === 1) {
    // Client → server
    if (serverConn && serverConn.readyState === 1) {
      try { serverConn.send(data); } catch (_) {}
    }
  } else {
    const target = gameConns.get(toUid);
    if (target && target.readyState === 1) {
      try { target.send(rewritten); } catch (_) {}
    }
  }
}

function setupLobbyConnection(ws, addr) {
  const uid = nextLobbyUid++;
  const player = { ws, uid, role: null, ready: false };
  lobbyClients.set(uid, player);
  log(`[+] Lobby connection uid=${uid} addr=${addr} total=${lobbyClients.size}`);

  broadcastWaiting();

  if (lobbyClients.size >= MIN_PLAYERS && !gameStarted) {
    startLobby();
  } else if (gameStarted && !player.ready) {
    player.role = "client";
    player.ready = true;
    sendText(ws, { type: "start", role: "client" });
    log(`Late join uid=${uid} as client`);
  }

  ws.on("close", () => {
    lobbyClients.delete(uid);
    log(`[-] Lobby disconnected uid=${uid} remaining=${lobbyClients.size}`);
    if (lobbyClients.size < MIN_PLAYERS) {
      gameStarted = false;
      if (lobbyClients.size === 0) nextLobbyUid = 1;
    }
    broadcastWaiting();
  });

  setTimeout(() => {
    if (!player.ready && ws.readyState === 1) {
      log(`Timeout lobby uid=${uid}, starting solo`);
      player.ready = true;
      gameStarted = true;
      sendText(ws, { type: "start", role: "server" });
    }
  }, PLAYER_TIMEOUT_MS);
}

function setupGameConnection(ws, firstPacket, addr) {
  const fromUid = firstPacket.readUInt32LE(4);
  const toUid = firstPacket.readUInt32LE(0);

  ws.doomUid = fromUid;

  if (serverConn === null) {
    // First game connection is the server
    serverConn = ws;
    serverDoomUid = fromUid;
    ws.isServer = true;
    gameConns.set(fromUid, ws);
    log(`[+] Server game connection doomUid=${fromUid} addr=${addr}`);

    // Forward server's registration broadcast with from=1
    if (toUid === 0) {
      const rewritten = Buffer.from(firstPacket);
      rewritten.writeUInt32LE(1, 4);
      for (const conn of gameConns.values()) {
        if (conn !== ws && conn.readyState === 1) {
          try { conn.send(rewritten); } catch (_) {}
        }
      }
    }
  } else {
    ws.isServer = false;
    gameConns.set(fromUid, ws);
    log(`[+] Client game connection doomUid=${fromUid} addr=${addr}`);
    routeGamePacket(ws, firstPacket);
  }

  ws.on("message", (msg) => {
    if (typeof msg === "string") return;
    if (!(msg instanceof Buffer)) msg = Buffer.from(msg);
    if (msg.length < 8) return;
    routeGamePacket(ws, msg);
  });

  ws.on("close", () => {
    gameConns.delete(fromUid);
    if (ws.isServer) {
      serverConn = null;
      serverDoomUid = null;
      log(`[-] Server game disconnected`);
    } else {
      log(`[-] Client game disconnected doomUid=${fromUid}`);
    }
  });
}

wss.on("connection", (ws, req) => {
  const addr = req.socket.remoteAddress;
  ws.isServer = false;
  ws.doomUid = null;

  // Wait for first message to classify connection type
  ws.once("message", (data) => {
    if (typeof data === "string") {
      // Lobby connection — JS sent { type: "lobby" }
      try {
        const msg = JSON.parse(data);
        if (msg.type === "lobby") {
          setupLobbyConnection(ws, addr);
        }
      } catch (_) {
        log(`[!] Unrecognised text message from ${addr}`);
        ws.close();
      }
    } else {
      // Game connection — Doom sent first binary packet
      if (!(data instanceof Buffer)) data = Buffer.from(data);
      if (data.length >= 8) {
        setupGameConnection(ws, data, addr);
      } else {
        log(`[!] Short binary packet from ${addr}, ignoring`);
        ws.close();
      }
    }
  });

  ws.on("error", (err) => {
    log(`[!] WS error ${addr}:`, err.message);
  });
});

httpServer.listen(PORT, () => {
  log(`DECHOOM relay listening on http://0.0.0.0:${PORT}`);
  log(`Waiting for ${MIN_PLAYERS} players before starting`);
});

process.on("SIGTERM", () => { log("Shutting down..."); wss.close(); });
