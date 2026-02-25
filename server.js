#!/usr/bin/env node
/**
 * DECHOOM Multiplayer Relay Server
 *
 * Two WebSocket paths:
 *   /lobby  — JS lobby handshake (text JSON)
 *   /       — Doom game connection (binary, via -wss flag)
 *
 * LOBBY PHASE:
 *   Client → Server (on connect to /lobby): automatic
 *   Server → Client: { type: "waiting", count: N, need: M }
 *   Server → Client: { type: "start", role: "server"|"client" }
 *
 * GAME PHASE binary protocol:
 *   [to: uint32LE][from: uint32LE][payload...]
 *   Server always appears as uid=1 to clients (relay rewrites from field).
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
  for (const p of lobbyClients.values()) { try { p.ws.terminate(); } catch (_) {} }
  for (const c of gameConns.values()) { try { c.terminate(); } catch (_) {} }
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
    res.writeHead(200); res.end("Reset OK\n"); return;
  }
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify({
    status: "DECHOOM relay OK",
    lobbyClients: lobbyClients.size,
    gameStarted, serverDoomUid,
    gameConns: gameConns.size,
  }) + "\n");
});

// Two separate WebSocket servers on the same HTTP server, routed by path
const lobbyWss = new WebSocketServer({ noServer: true });
const gameWss = new WebSocketServer({ noServer: true });

httpServer.on("upgrade", (req, socket, head) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  if (url.pathname === "/lobby") {
    lobbyWss.handleUpgrade(req, socket, head, (ws) => {
      lobbyWss.emit("connection", ws, req);
    });
  } else {
    gameWss.handleUpgrade(req, socket, head, (ws) => {
      gameWss.emit("connection", ws, req);
    });
  }
});

function sendText(ws, obj) { try { ws.send(JSON.stringify(obj)); } catch (_) {} }

function broadcastWaiting() {
  const msg = { type: "waiting", count: lobbyClients.size, need: MIN_PLAYERS };
  for (const p of lobbyClients.values()) {
    if (p.ws.readyState === 1) sendText(p.ws, msg);
  }
}

function startLobby() {
  gameStarted = true;
  log(`Starting game with ${lobbyClients.size} lobby players`);
  let pos = 1;
  for (const p of lobbyClients.values()) {
    if (p.ws.readyState === 1) {
      const role = pos === 1 ? "server" : "client";
      p.role = role; p.ready = true;
      sendText(p.ws, { type: "start", role });
      log(`role=${role} → lobby uid=${p.uid}`);
    }
    pos++;
  }
}

// ── LOBBY connections (/lobby) ────────────────────────────────────────────────
lobbyWss.on("connection", (ws, req) => {
  const uid = nextLobbyUid++;
  const player = { ws, uid, role: null, ready: false };
  lobbyClients.set(uid, player);
  log(`[+] Lobby uid=${uid} total=${lobbyClients.size}`);

  broadcastWaiting();
  if (lobbyClients.size >= MIN_PLAYERS && !gameStarted) startLobby();
  else if (gameStarted && !player.ready) {
    player.role = "client"; player.ready = true;
    sendText(ws, { type: "start", role: "client" });
  }

  ws.on("close", () => {
    lobbyClients.delete(uid);
    log(`[-] Lobby uid=${uid} remaining=${lobbyClients.size}`);
    if (lobbyClients.size < MIN_PLAYERS) {
      gameStarted = false;
      if (lobbyClients.size === 0) nextLobbyUid = 1;
    }
    broadcastWaiting();
  });

  ws.on("error", (err) => log(`[!] Lobby error uid=${uid}:`, err.message));

  setTimeout(() => {
    if (!player.ready && ws.readyState === 1) {
      log(`Lobby timeout uid=${uid}, solo start`);
      player.ready = true; gameStarted = true;
      sendText(ws, { type: "start", role: "server" });
    }
  }, PLAYER_TIMEOUT_MS);
});

// ── GAME connections (/) ──────────────────────────────────────────────────────
function routeGamePacket(ws, data) {
  const toUid = data.readUInt32LE(0);
  const rewritten = Buffer.from(data);
  if (ws.isServer) rewritten.writeUInt32LE(1, 4); // server always uid=1 to clients

  if (toUid === 0) {
    for (const c of gameConns.values()) {
      if (c !== ws && c.readyState === 1) { try { c.send(rewritten); } catch (_) {} }
    }
  } else if (toUid === 1) {
    if (serverConn && serverConn.readyState === 1) { try { serverConn.send(data); } catch (_) {} }
  } else {
    const t = gameConns.get(toUid);
    if (t && t.readyState === 1) { try { t.send(rewritten); } catch (_) {} }
  }
}

gameWss.on("connection", (ws, req) => {
  const addr = req.socket.remoteAddress;
  ws.isServer = false;
  ws.doomUid = null;

  ws.once("message", (data) => {
    if (!(data instanceof Buffer)) data = Buffer.from(data);
    if (data.length < 8) { ws.close(); return; }

    const fromUid = data.readUInt32LE(4);
    const toUid = data.readUInt32LE(0);
    ws.doomUid = fromUid;

    if (serverConn === null) {
      serverConn = ws;
      serverDoomUid = fromUid;
      ws.isServer = true;
      gameConns.set(fromUid, ws);
      log(`[+] Server game doomUid=${fromUid} addr=${addr}`);
      // Broadcast server registration with from=1
      if (toUid === 0) {
        const r = Buffer.from(data); r.writeUInt32LE(1, 4);
        for (const c of gameConns.values()) {
          if (c !== ws && c.readyState === 1) { try { c.send(r); } catch (_) {} }
        }
      }
    } else {
      ws.isServer = false;
      gameConns.set(fromUid, ws);
      log(`[+] Client game doomUid=${fromUid} addr=${addr}`);
      routeGamePacket(ws, data);
    }

    ws.on("message", (msg) => {
      if (!(msg instanceof Buffer)) msg = Buffer.from(msg);
      if (msg.length >= 8) routeGamePacket(ws, msg);
    });

    ws.on("close", () => {
      gameConns.delete(fromUid);
      if (ws.isServer) { serverConn = null; serverDoomUid = null; log(`[-] Server game disconnected`); }
      else log(`[-] Client game disconnected doomUid=${fromUid}`);
    });
  });

  ws.on("error", (err) => log(`[!] Game error ${addr}:`, err.message));
});

httpServer.listen(PORT, () => {
  log(`DECHOOM relay listening on http://0.0.0.0:${PORT}`);
  log(`Lobby path: wss://host/lobby`);
  log(`Game path:  wss://host/`);
});

process.on("SIGTERM", () => { log("Shutting down..."); lobbyWss.close(); gameWss.close(); });
