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
 *   Server → Client: { type: "start", role: "server"|"client", playerCount: N }
 *   Server → Client: { type: "go" }  (server player only, once all clients connected)
 *
 * GAME PHASE (binary, 8-byte header):
 *   [to: uint32LE][from: uint32LE][payload...]
 *   Server always appears as uid=1 to clients.
 *
 * UID COLLISION HANDLING:
 *   When two clients (e.g. same browser, same tabs) produce the same instanceUID,
 *   the relay detects the collision on the first packet and assigns the later
 *   connection a fresh remapped UID >= 100000. All packets are rewritten
 *   transparently so Doom never sees the duplicate.
 */

const { WebSocketServer } = require("ws");
const http = require("http");

const PORT = process.env.PORT || 2342;
const MIN_PLAYERS = 2;
const MAX_PLAYERS = 8; // matches MAXPLAYERS in doomdef.h
const PLAYER_TIMEOUT_MS = 5 * 60 * 1000;

// UIDs >= 100000 are used for remapped collision UIDs, well clear of
// Doom's normal small-integer range and the server's hardcoded uid=1.
let nextRemappedUid = 100000;

let lobbyClients = new Map();
let nextLobbyUid = 1;
let gameStarted = false;

let serverConn = null;
let serverDoomUid = null;
let gameConns = new Map(); // effectiveUid → ws

// Per-ws metadata: { claimedUid, effectiveUid, isServer }
const connMeta = new WeakMap();

let expectedGameClients = 0;
let connectedGameClients = 0;
let serverLobbyWs = null;

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
  serverLobbyWs = null;
  expectedGameClients = 0;
  connectedGameClients = 0;
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
    gameStarted,
    serverDoomUid,
    gameConns: gameConns.size,
    expectedGameClients,
    connectedGameClients,
  }) + "\n");
});

const wss = new WebSocketServer({ server: httpServer });

function sendText(ws, obj) { try { ws.send(JSON.stringify(obj)); } catch (_) {} }

function broadcastWaiting() {
  const players = Array.from(lobbyClients.values()).map(p => p.username ?? "Operator");
  const hostUid = lobbyClients.size > 0 ? lobbyClients.keys().next().value : null;
  for (const p of lobbyClients.values()) {
    const msg = {
      type: "waiting",
      count: lobbyClients.size,
      need: MIN_PLAYERS,
      players,
      isHost: p.uid === hostUid,
      canStart: lobbyClients.size >= MIN_PLAYERS,
    };
    if (p.ws.readyState === 1) sendText(p.ws, msg);
  }
}

function startLobby() {
  gameStarted = true;
  const totalPlayers = lobbyClients.size;
  log(`Starting game with ${totalPlayers} lobby players`);

  expectedGameClients = totalPlayers - 1;
  connectedGameClients = 0;
  log(`Waiting for ${expectedGameClients} Doom game client(s) to connect before sending go`);

  let pos = 1;
  for (const p of lobbyClients.values()) {
    if (p.ws.readyState === 1) {
      const role = pos === 1 ? "server" : "client";
      p.role = role; p.ready = true;
      sendText(p.ws, { type: "start", role, playerCount: totalPlayers });
      log(`role=${role} → lobby uid=${p.uid}`);
      if (role === "server") {
        serverLobbyWs = p.ws;
        log(`Holding server lobby ws open until all ${expectedGameClients} client(s) connect`);
      }
    }
    pos++;
  }
}

// Return a copy of data with the from field (bytes 4-7) rewritten.
function rewriteFrom(data, newFromUid) {
  const out = Buffer.from(data);
  out.writeUInt32LE(newFromUid, 4);
  return out;
}

function routeGamePacket(senderWs, data) {
  const meta = connMeta.get(senderWs);
  const effectiveFrom = meta ? meta.effectiveUid : data.readUInt32LE(4);

  // Rewrite from field so all downstream recipients see the effective UID
  const patchedData = rewriteFrom(data, effectiveFrom);
  const toUid = patchedData.readUInt32LE(0);

  const label = toUid === 0 ? "broadcast" : `uid${effectiveFrom}→uid${toUid}`;
  log(`pkt ${label} len=${data.length} serverReady=${!!serverConn}`);

  if (toUid === 0) {
    for (const [, c] of gameConns.entries()) {
      if (c !== senderWs && c.readyState === 1) {
        try { c.send(patchedData); } catch (_) {}
      }
    }
  } else if (toUid === 1) {
    if (serverConn && serverConn.readyState === 1) {
      try { serverConn.send(patchedData); } catch (_) {}
    } else {
      log(`DROP pkt to server (not connected)`);
    }
  } else {
    const t = gameConns.get(toUid);
    if (t && t.readyState === 1) { try { t.send(patchedData); } catch (_) {} }
    else log(`DROP pkt to uid=${toUid} (not found)`);
  }
}

function maybeGoSignal() {
  log(`maybeGoSignal: ${connectedGameClients}/${expectedGameClients} clients connected`);
  if (!serverLobbyWs) return;
  if (expectedGameClients < 1) return;
  if (connectedGameClients < expectedGameClients) return;

  const savedLobbyWs = serverLobbyWs;
  serverLobbyWs = null;

  setTimeout(() => {
    if (savedLobbyWs.readyState === 1) {
      log(`All ${connectedGameClients} client(s) connected — sending go to server`);
      sendText(savedLobbyWs, { type: "go" });
    } else {
      log(`Server lobby WS closed before go could be sent`);
    }
  }, 500);
}

wss.on("connection", (ws, req) => {
  const addr = req.socket.remoteAddress;
  const url = new URL(req.url, `http://${req.headers.host}`);
  const isLobby = url.searchParams.get("type") === "lobby";

  log(`[+] Connection addr=${addr} type=${isLobby ? "lobby" : "game"} url=${req.url}`);

  if (isLobby) {
    // ── LOBBY CONNECTION ──────────────────────────────────────────────────────
    // Reject if lobby is full
    if (lobbyClients.size >= MAX_PLAYERS) {
      log(`[!] Lobby full (${lobbyClients.size}/${MAX_PLAYERS}), rejecting`);
      sendText(ws, { type: 'rejected', reason: `Lobby is full (max ${MAX_PLAYERS} players)` });
      ws.close();
      return;
    }

    const uid = nextLobbyUid++;
    const player = { ws, uid, role: null, ready: false };
    lobbyClients.set(uid, player);
    log(`[+] Lobby uid=${uid} total=${lobbyClients.size}`);

    broadcastWaiting();
    if (gameStarted && !player.ready) {
      player.role = "client"; player.ready = true;
      sendText(ws, { type: "start", role: "client", playerCount: lobbyClients.size });
    }

    ws.on("message", (data) => {
      try {
        const msg = JSON.parse(data.toString());
        if (msg.type === "lobby" && msg.username) {
          player.username = msg.username;
          broadcastWaiting();
        } else if (msg.type === "start_game") {
          const hostUid = lobbyClients.keys().next().value;
          if (player.uid === hostUid && lobbyClients.size >= MIN_PLAYERS && !gameStarted) {
            log(`Host uid=${player.uid} started the game`);
            startLobby();
          }
        } else if (msg.type === "kill_event") {
          const killMsg = JSON.stringify({
            type: "kill_event",
            killer: msg.killer,
            victim: msg.victim,
          });
          log(`kill_event: ${msg.killer} eliminated ${msg.victim}`);
          for (const p of lobbyClients.values()) {
            if (p.ws.readyState === 1) {
              try { p.ws.send(killMsg); } catch (_) {}
            }
          }
        }
      } catch (_) {}
    });

    ws.on("close", () => {
      lobbyClients.delete(uid);
      log(`[-] Lobby uid=${uid} remaining=${lobbyClients.size}`);
      if (lobbyClients.size < MIN_PLAYERS) {
        gameStarted = false;
        expectedGameClients = 0;
        connectedGameClients = 0;
        if (lobbyClients.size === 0) nextLobbyUid = 1;
      }
      broadcastWaiting();
    });

    ws.on("error", (err) => log(`[!] Lobby error uid=${uid}:`, err.message));

    setTimeout(() => {
      if (!player.ready && ws.readyState === 1) {
        log(`Lobby timeout uid=${uid}, solo start`);
        player.ready = true; gameStarted = true;
        expectedGameClients = 0;
        connectedGameClients = 0;
        sendText(ws, { type: "start", role: "server", playerCount: 1 });
      }
    }, PLAYER_TIMEOUT_MS);

  } else {
    // ── GAME CONNECTION (Doom -wss) ───────────────────────────────────────────
    ws.once("message", (data) => {
      if (!(data instanceof Buffer)) data = Buffer.from(data);
      if (data.length < 8) { ws.close(); return; }

      const claimedFromUid = data.readUInt32LE(4);
      const toUid = data.readUInt32LE(0);
      const isServerConn = (claimedFromUid === 1);

      if (isServerConn) {
        // ── SERVER GAME CONNECTION ────────────────────────────────────────────
        connMeta.set(ws, { claimedUid: 1, effectiveUid: 1, isServer: true });
        gameConns.set(1, ws);
        serverConn = ws;
        serverDoomUid = 1;
        log(`[+] Server game claimedUid=1 effectiveUid=1 addr=${addr}`);

        if (toUid === 0) {
          for (const [, c] of gameConns.entries()) {
            if (c !== ws && c.readyState === 1) { try { c.send(data); } catch (_) {} }
          }
        }

        ws.on("message", (msg) => {
          if (!(msg instanceof Buffer)) msg = Buffer.from(msg);
          if (msg.length >= 8) routeGamePacket(ws, msg);
        });

        ws.on("close", () => {
          gameConns.delete(1);
          serverConn = null;
          serverDoomUid = null;
          log(`[-] Server game disconnected`);
        });

      } else {
        // ── CLIENT GAME CONNECTION ────────────────────────────────────────────
        let effectiveUid = claimedFromUid;

        if (gameConns.has(claimedFromUid)) {
          // UID collision — two clients in the same browser produced the same
          // instanceUID. Assign a unique remapped UID for this connection.
          effectiveUid = nextRemappedUid++;
          log(`[!] UID collision: claimedUid=${claimedFromUid} already in use — remapping to effectiveUid=${effectiveUid}`);
        }

        connMeta.set(ws, { claimedUid: claimedFromUid, effectiveUid, isServer: false });
        gameConns.set(effectiveUid, ws);

        log(`[+] Client game claimedUid=${claimedFromUid} effectiveUid=${effectiveUid} addr=${addr}`);
        connectedGameClients++;
        log(`Client game connections: ${connectedGameClients}/${expectedGameClients}`);

        // Route initial packet with effective UID rewritten into from field
        routeGamePacket(ws, data);

        maybeGoSignal();

        ws.on("message", (msg) => {
          if (!(msg instanceof Buffer)) msg = Buffer.from(msg);
          if (msg.length >= 8) routeGamePacket(ws, msg);
        });

        ws.on("close", () => {
          gameConns.delete(effectiveUid);
          log(`[-] Client game disconnected claimedUid=${claimedFromUid} effectiveUid=${effectiveUid}`);
        });
      }
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
