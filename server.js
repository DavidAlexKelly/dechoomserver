#!/usr/bin/env node
/**
 * DECHOOM Multiplayer Relay Server
 *
 * Protocol (net_websockets.c / d_loop.c):
 *
 * LOBBY PHASE (JS WebSocket, text frames):
 *   Server → Client: { type: "waiting", count: N, need: M }
 *   Server → Client: { type: "start", role: "server"|"client" }
 *
 * GAME PHASE (Doom WebSocket, binary frames):
 *   [to: uint32LE][from: uint32LE][payload...]
 *
 * KEY INSIGHT:
 *   The client always does -connect 1, which resolves to "server uid = 1".
 *   But Doom generates instanceUID randomly from srand(time()) — it won't be 1.
 *   So the relay must translate:
 *     - Packets FROM the server → rewrite from field to 1 before forwarding to clients
 *     - Packets TO uid=1 from clients → route to whoever the server connection is
 *
 *   On first connect the server sends an 8-byte registration packet (to=0, from=serverUID).
 *   The relay captures serverUID from this, then uses 1 as the server's "public" uid.
 */

const { WebSocketServer } = require("ws");
const http = require("http");

const PORT = process.env.PORT || 2342;
const MIN_PLAYERS = 2;
const PLAYER_TIMEOUT_MS = 5 * 60 * 1000;

// Lobby state
let lobbyClients = new Map(); // uid -> { ws, uid, role, ready }
let nextLobbyUid = 1;
let gameStarted = false;

// Game state - track server and clients separately
let serverConn = null;      // the Doom WebSocket of the server player
let serverDoomUid = null;   // the server's actual Doom instanceUID (from registration packet)
let gameConns = new Map();  // doomUid -> ws (for all game connections including server)

function log(...args) {
  console.log(new Date().toISOString(), ...args);
}

function resetState() {
  for (const p of lobbyClients.values()) {
    try { p.ws.terminate(); } catch (_) {}
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

wss.on("connection", (ws, req) => {
  // Determine if this is a lobby connection (text) or game connection (binary)
  // We track state on the socket itself
  ws.isGame = false;
  ws.doomUid = null;
  ws.isServer = false;

  const addr = req.socket.remoteAddress;

  // First message determines type:
  // - Text JSON = lobby connection
  // - Binary 8+ bytes = game connection (Doom opened -wss)

  let lobbyUid = null;

  const onFirstMessage = (data) => {
    ws.removeListener("message", onFirstMessage);

    if (typeof data === "string" || (data instanceof Buffer && data.length < 8)) {
      // Lobby connection
      ws.isGame = false;
      lobbyUid = nextLobbyUid++;
      const player = { ws, uid: lobbyUid, role: null, ready: false };
      lobbyClients.set(lobbyUid, player);
      log(`[+] Lobby connection uid=${lobbyUid} addr=${addr} total=${lobbyClients.size}`);

      broadcastWaiting();

      if (lobbyClients.size >= MIN_PLAYERS && !gameStarted) {
        startLobby();
      }
      if (gameStarted && !player.ready) {
        player.role = "client";
        player.ready = true;
        sendText(ws, { type: "start", role: "client" });
        log(`Late join uid=${lobbyUid} as client`);
      }

      ws.on("message", (msg) => {
        if (typeof msg === "string") return; // ignore further text
      });

    } else {
      // Game connection from Doom (-wss opened)
      ws.isGame = true;
      if (!(data instanceof Buffer)) data = Buffer.from(data);

      // Registration packet: to=0, from=instanceUID, no payload (exactly 8 bytes)
      // or normal packet with payload
      const toUid = data.readUInt32LE(0);
      const fromUid = data.readUInt32LE(4);

      // First binary connection with to=0 is the server registering
      if (serverConn === null) {
        serverConn = ws;
        serverDoomUid = fromUid;
        ws.isServer = true;
        ws.doomUid = fromUid;
        gameConns.set(fromUid, ws);
        log(`[+] Server game connection doomUid=${fromUid}`);

        // Forward the registration broadcast to all other game connections
        // (rewriting from=serverDoomUid to from=1 so clients think server is uid=1)
        const rewritten = Buffer.from(data);
        rewritten.writeUInt32LE(1, 4); // server always appears as uid=1 to clients
        for (const [uid, conn] of gameConns) {
          if (conn !== ws && conn.readyState === 1) {
            try { conn.send(rewritten); } catch (_) {}
          }
        }
      } else {
        // Client game connection
        ws.doomUid = fromUid;
        ws.isServer = false;
        gameConns.set(fromUid, ws);
        log(`[+] Client game connection doomUid=${fromUid}`);

        // Process this first packet normally
        routeGamePacket(ws, data);
      }

      // Handle subsequent game packets
      ws.on("message", (msg) => {
        if (typeof msg === "string") return;
        if (!(msg instanceof Buffer)) msg = Buffer.from(msg);
        if (msg.length < 8) return;
        routeGamePacket(ws, msg);
      });
    }
  };

  ws.on("message", onFirstMessage);

  ws.on("close", () => {
    if (!ws.isGame) {
      if (lobbyUid) {
        lobbyClients.delete(lobbyUid);
        log(`[-] Lobby disconnected uid=${lobbyUid} remaining=${lobbyClients.size}`);
        if (lobbyClients.size < MIN_PLAYERS) {
          gameStarted = false;
          if (lobbyClients.size === 0) nextLobbyUid = 1;
        }
        broadcastWaiting();
      }
    } else {
      if (ws.doomUid !== null) gameConns.delete(ws.doomUid);
      if (ws.isServer) {
        serverConn = null;
        serverDoomUid = null;
        log(`[-] Server game disconnected`);
      } else {
        log(`[-] Client game disconnected doomUid=${ws.doomUid}`);
      }
    }
  });

  ws.on("error", (err) => {
    log(`[!] Error:`, err.message);
  });

  // Lobby timeout
  setTimeout(() => {
    if (!ws.isGame && lobbyUid) {
      const player = lobbyClients.get(lobbyUid);
      if (player && !player.ready && ws.readyState === 1) {
        log(`Timeout lobby uid=${lobbyUid}, starting solo`);
        player.ready = true;
        gameStarted = true;
        sendText(ws, { type: "start", role: "server" });
      }
    }
  }, PLAYER_TIMEOUT_MS);
});

function routeGamePacket(ws, data) {
  const toUid = data.readUInt32LE(0);
  const fromUid = data.readUInt32LE(4);

  // Rewrite from field:
  // - If from server: always appear as uid=1 to clients
  // - If from client: use their actual doomUid
  const rewritten = Buffer.from(data);
  if (ws.isServer) {
    rewritten.writeUInt32LE(1, 4);
  }
  // else keep fromUid as-is

  if (toUid === 0) {
    // Broadcast to all other game connections
    for (const [, conn] of gameConns) {
      if (conn !== ws && conn.readyState === 1) {
        try { conn.send(rewritten); } catch (_) {}
      }
    }
  } else if (toUid === 1) {
    // Client addressing the server — route to serverConn
    if (serverConn && serverConn.readyState === 1) {
      try { serverConn.send(data); } catch (_) {} // send original, don't rewrite
    }
  } else {
    // Unicast to specific doomUid
    const target = gameConns.get(toUid);
    if (target && target.readyState === 1) {
      try { target.send(rewritten); } catch (_) {}
    }
  }
}

httpServer.listen(PORT, () => {
  log(`DECHOOM relay listening on http://0.0.0.0:${PORT}`);
  log(`Waiting for ${MIN_PLAYERS} players before starting`);
});

process.on("SIGTERM", () => { log("Shutting down..."); wss.close(); });
