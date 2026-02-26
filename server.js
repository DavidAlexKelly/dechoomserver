#!/usr/bin/env node
/**
 * DECHOOM Multiplayer Relay Server
 *
 * Single WebSocket endpoint. Connection type distinguished by query param:
 *   wss://host/?type=lobby  — JS lobby handshake
 *   wss://host/             — Doom game connection (via -wss flag)
 *
 * LOBBY PHASE (text JSON):
 *   Server → Client: { type: "waiting", count: N, need: M }
 *   Server → Client: { type: "start", role: "server"|"client" }
 *
 * GAME PHASE (binary, 8-byte header):
 *   [to: uint32LE][from: uint32LE][payload...]
 *   Server always appears as uid=1 to clients.
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
let pendingClientPackets = [];
let serverLobbyWs = null; // server's lobby WS, kept open until client game connects // packets from clients that arrived before server

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
  pendingClientPackets = [];
  serverLobbyWs = null;
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

const wss = new WebSocketServer({ server: httpServer });

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
      if (role === "server") {
        serverLobbyWs = p.ws; // hold open - send "go" when client game connects
        log(`Holding server lobby ws open for go signal`);
      }
    }
    pos++;
  }
}

function routeGamePacket(ws, data) {
  const toUid = data.readUInt32LE(0);

  if (toUid === 0) {
    // Broadcast to all other game connections
    for (const c of gameConns.values()) {
      if (c !== ws && c.readyState === 1) { try { c.send(data); } catch (_) {} }
    }
  } else if (toUid === 1) {
    // Client → server
    if (serverConn && serverConn.readyState === 1) {
      try { serverConn.send(data); } catch (_) {}
    }
    // If server not yet connected, drop - client will retry SYNs naturally
  } else {
    // Unicast to specific UID
    const t = gameConns.get(toUid);
    if (t && t.readyState === 1) { try { t.send(data); } catch (_) {} }
  }
}

wss.on("connection", (ws, req) => {
  const addr = req.socket.remoteAddress;
  const url = new URL(req.url, `http://${req.headers.host}`);
  const isLobby = url.searchParams.get("type") === "lobby";

  log(`[+] Connection addr=${addr} type=${isLobby ? "lobby" : "game"} url=${req.url}`);

  if (isLobby) {
    // ── LOBBY CONNECTION ──────────────────────────────────────────────────────
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

    ws.on("message", () => {}); // ignore any messages from lobby clients
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

  } else {
    // ── GAME CONNECTION (Doom -wss) ───────────────────────────────────────────
    ws.isServer = false;
    ws.doomUid = null;

    ws.once("message", (data) => {
      if (!(data instanceof Buffer)) data = Buffer.from(data);
      if (data.length < 8) { ws.close(); return; }

      const fromUid = data.readUInt32LE(4);
      const toUid = data.readUInt32LE(0);
      ws.doomUid = fromUid;

      // Identify server by instanceUID=1 (hardcoded in d_loop.c for -server)
      // Any other UID is a client running -connect
      const isServerConn = (fromUid === 1);

      ws.isServer = isServerConn;
      gameConns.set(fromUid, ws);

      if (isServerConn) {
        serverConn = ws;
        serverDoomUid = 1;
        log(`[+] Server game doomUid=${fromUid} addr=${addr}`);
        // Forward server's broadcast registration to any already-connected clients
        if (toUid === 0) {
          for (const c of gameConns.values()) {
            if (c !== ws && c.readyState === 1) { try { c.send(data); } catch (_) {} }
          }
        }

      } else {
        log(`[+] Client game doomUid=${fromUid} addr=${addr}`);
        routeGamePacket(ws, data);
        // Signal server lobby to launch Doom now that client is connected
        if (serverLobbyWs && serverLobbyWs.readyState === 1) {
          log(`Sending go signal to server lobby`);
          sendText(serverLobbyWs, { type: "go" });
          serverLobbyWs = null;
        }
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
  }
});

httpServer.listen(PORT, () => {
  log(`DECHOOM relay listening on http://0.0.0.0:${PORT}`);
  log(`Lobby: wss://host/?type=lobby`);
  log(`Game:  wss://host/`);
});

process.on("SIGTERM", () => { log("Shutting down..."); wss.close(); });
