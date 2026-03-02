#!/usr/bin/env node
/**
 * DECHOOM Multiplayer Relay Server
 *
 * Two WebSocket endpoints on the same port:
 *   ws://<host>/          — binary Doom net packets (unchanged)
 *   ws://<host>/?type=lobby — JSON lobby/match control messages
 *
 * Lobby message types (client → server):
 *   { type: "lobby", username }                      — join lobby
 *   { type: "start_game" }                           — host starts match
 *   { type: "map_select", map: n }                   — host picks map
 *   { type: "time_limit", minutes: n }               — host sets time limit (0 = no limit)
 *   { type: "kill_event", killer, victim }            — in-game frag broadcast
 *   { type: "frag_update", username, frags, deaths } — live score sync
 *
 * Lobby message types (server → client):
 *   { type: "waiting", players, count, need, isHost, canStart, selectedMap, timeLimitMinutes }
 *   { type: "start", role, playerCount, players, map, timeLimitMinutes }
 *   { type: "go" }          — server player launches
 *   { type: "client_go" }   — client player launches
 *   { type: "lobby_reset", reason }
 *   { type: "rejected", reason }
 *   { type: "kill_event", killer, victim }
 *   { type: "frag_update", username, frags, deaths }
 *   { type: "match_end", scores: [{username, frags, deaths}], reason }
 */

const { WebSocketServer, WebSocket } = require("ws");

const PORT = process.env.PORT || 2342;
const DEV_MODE = process.env.NODE_ENV !== "production";
const MIN_PLAYERS = DEV_MODE ? 1 : 2;  // In dev, allow starting with 1 player
const MAX_PLAYERS = 8;

const wss = new WebSocketServer({ port: PORT });

// ── Binary relay state ────────────────────────────────────────────────────────
const binaryClients = new Map(); // uid → ws
let nextUid = 1;

function allocateUid() {
  while (binaryClients.has(nextUid)) {
    nextUid = (nextUid % 0xfffe) + 1;
  }
  return nextUid++;
}

// ── Lobby state ───────────────────────────────────────────────────────────────
// Single lobby; resets when everyone leaves or match ends
let peekClients = new Set();  // WebSockets watching lobby without joining
let lobbyClients = [];        // [{ws, username, frags, deaths}]
let matchInProgress = false;
let selectedMap = 1;
let timeLimitMinutes = 10;    // default 10 min; 0 = no limit
let matchTimer = null;        // setTimeout handle
let matchStartTime = null;
let clientsReady = 0;
let expectedClients = 0;

function lobbyBroadcast(msg) {
  const str = JSON.stringify(msg);
  for (const c of lobbyClients) {
    if (c.ws.readyState === WebSocket.OPEN) c.ws.send(str);
  }
}

function broadcastWaiting() {
  const players = lobbyClients.map(c => c.username);
  const count   = players.length;
  const isEnough = count >= MIN_PLAYERS;

  // Send to joined lobby clients (with isHost / canStart)
  for (let i = 0; i < lobbyClients.length; i++) {
    const c = lobbyClients[i];
    if (c.ws.readyState !== WebSocket.OPEN) continue;
    c.ws.send(JSON.stringify({
      type: "waiting",
      players,
      count,
      need: MIN_PLAYERS,
      isHost: i === 0,
      canStart: isEnough,
      selectedMap,
      timeLimitMinutes,
    }));
  }

  // Send to peek (spectator) connections — read-only view
  const peekMsg = JSON.stringify({
    type: "lobby_status",
    players,
    count,
    need: MIN_PLAYERS,
    selectedMap,
    timeLimitMinutes,
    matchInProgress,
  });
  for (const ws of peekClients) {
    if (ws.readyState === WebSocket.OPEN) ws.send(peekMsg);
  }
}

function endMatch(reason = "Time limit reached") {
  if (matchTimer) { clearTimeout(matchTimer); matchTimer = null; }
  matchInProgress = false;
  matchStartTime  = null;
  clientsReady = 0;
  expectedClients = 0;

  const scores = lobbyClients.map(c => ({
    username: c.username,
    frags:    c.frags,
    deaths:   c.deaths,
  })).sort((a, b) => b.frags - a.frags);

  lobbyBroadcast({ type: "match_end", scores, reason });
  console.log(`[LOBBY] Match ended: ${reason}`);

  // Notify peek clients that the match is over
  broadcastWaiting();
}

function startMatchTimer() {
  if (matchTimer) clearTimeout(matchTimer);
  if (!timeLimitMinutes) return; // 0 = no limit
  matchStartTime = Date.now();
  const ms = timeLimitMinutes * 60 * 1000;
  matchTimer = setTimeout(() => endMatch("Time limit reached"), ms);
  console.log(`[LOBBY] Match timer started: ${timeLimitMinutes} min`);
}

// ── WebSocket handler ─────────────────────────────────────────────────────────
wss.on("connection", (ws, req) => {
  const url   = new URL(req.url, `http://localhost`);
  const isLobby = url.searchParams.get("type") === "lobby";

  if (!isLobby) {
    // ── Binary relay ──────────────────────────────────────────────────────────
    const uid = allocateUid();
    binaryClients.set(uid, ws);
    console.log(`[+] Binary client uid=${uid} total=${binaryClients.size}`);

    const hello = Buffer.alloc(4);
    hello.writeUInt32LE(uid, 0);
    ws.send(hello);

    ws.on("message", (data) => {
      if (!(data instanceof Buffer)) data = Buffer.from(data);
      if (data.length < 8) return;
      data.writeUInt32LE(uid, 4);
      for (const [otherUid, otherWs] of binaryClients) {
        if (otherUid !== uid && otherWs.readyState === WebSocket.OPEN) {
          otherWs.send(data);
        }
      }
    });

    ws.on("close", () => {
      binaryClients.delete(uid);
      console.log(`[-] Binary client uid=${uid} total=${binaryClients.size}`);
    });

    ws.on("error", (err) => {
      console.error(`[!] Binary error uid=${uid}:`, err.message);
      binaryClients.delete(uid);
    });

    return;
  }

  // ── Peek (spectator) client ────────────────────────────────────────────────
  const isPeek = url.searchParams.get("peek") === "1";
  if (isPeek) {
    peekClients.add(ws);
    console.log(`[PEEK] Spectator connected (${peekClients.size} watching)`);

    // Send current lobby state immediately
    const players = lobbyClients.map(c => c.username);
    ws.send(JSON.stringify({
      type: "lobby_status",
      players,
      count: players.length,
      need: MIN_PLAYERS,
      selectedMap,
      timeLimitMinutes,
      matchInProgress,
    }));

    ws.on("close", () => {
      peekClients.delete(ws);
      console.log(`[PEEK] Spectator disconnected (${peekClients.size} watching)`);
    });
    ws.on("error", () => { peekClients.delete(ws); });
    return;
  }

  // ── Lobby client ─────────────────────────────────────────────────────────────
  let entry = null; // filled on "lobby" message

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    if (msg.type === "lobby") {
      if (lobbyClients.length >= MAX_PLAYERS) {
        ws.send(JSON.stringify({ type: "rejected", reason: "Lobby is full" }));
        return;
      }
      let username = msg.username || "Operative";

      // In dev mode, auto-suffix duplicate usernames so the same person
      // can test multiple clients from one machine.
      if (DEV_MODE) {
        const existing = lobbyClients.map(c => c.username);
        if (existing.includes(username)) {
          let suffix = 2;
          while (existing.includes(`${username} (${suffix})`)) suffix++;
          const newName = `${username} (${suffix})`;
          console.log(`[LOBBY][DEV] Duplicate username "${username}" → renamed to "${newName}"`);
          username = newName;
        }
      }

      entry = { ws, username, frags: 0, deaths: 0 };
      lobbyClients.push(entry);
      console.log(`[LOBBY] ${entry.username} joined (${lobbyClients.length} players)`);
      broadcastWaiting();
      return;
    }

    if (!entry) return; // must have joined first

    const isHost = lobbyClients[0] === entry;

    if (msg.type === "map_select" && isHost && !matchInProgress) {
      selectedMap = msg.map ?? 1;
      broadcastWaiting();

    } else if (msg.type === "time_limit" && isHost && !matchInProgress) {
      timeLimitMinutes = Math.max(0, Math.min(60, parseInt(msg.minutes) || 0));
      broadcastWaiting();

    } else if (msg.type === "start_game" && isHost && !matchInProgress) {
      if (lobbyClients.length < MIN_PLAYERS) return;
      matchInProgress = true;
      clientsReady = 0; // reset ready counter
      expectedClients = lobbyClients.length - 1; // everyone except host

      const players    = lobbyClients.map(c => c.username);
      const playerCount = lobbyClients.length;

      console.log(`[LOBBY] Starting match: ${playerCount} players, map ${selectedMap}, ${timeLimitMinutes} min`);

      // Step 1: Tell all clients to launch Doom first
      lobbyClients.slice(1).forEach((c, i) => {
        setTimeout(() => {
          if (c.ws.readyState !== WebSocket.OPEN) return;
          c.ws.send(JSON.stringify({
            type: "start", role: "client", playerCount, players,
            map: selectedMap, timeLimitMinutes,
          }));
          // client_go immediately — they need to connect before host starts listening
          setTimeout(() => {
            if (c.ws.readyState === WebSocket.OPEN) {
              c.ws.send(JSON.stringify({ type: "client_go" }));
            }
          }, 800);
        }, i * 300);
      });

      // Step 2: Tell host to launch — but hold the "go" until clients are ready
      // Give host the start message now so it can download/init in parallel
      setTimeout(() => {
        if (lobbyClients[0]?.ws.readyState === WebSocket.OPEN) {
          lobbyClients[0].ws.send(JSON.stringify({
            type: "start", role: "server", playerCount, players,
            map: selectedMap, timeLimitMinutes,
          }));
        }
      }, 200);
      // host "go" is sent by the client_ready handler below once all clients check in

    } else if (msg.type === "client_ready") {
      // Client's Doom is initialised and connected to the binary relay
      clientsReady++;
      console.log(`[LOBBY] Client ready: ${clientsReady}/${expectedClients}`);
      if (clientsReady >= expectedClients && lobbyClients[0]?.ws.readyState === WebSocket.OPEN) {
        // All clients ready — now tell the host to launch Doom
        // It will immediately start listening for the already-connected clients
        console.log(`[LOBBY] All clients ready, sending go to host`);
        lobbyClients[0].ws.send(JSON.stringify({ type: "go" }));
        startMatchTimer();
      }

    } else if (msg.type === "kill_event") {
      // Relay to all other lobby clients
      const str = JSON.stringify({ type: "kill_event", killer: msg.killer, victim: msg.victim });
      for (const c of lobbyClients) {
        if (c !== entry && c.ws.readyState === WebSocket.OPEN) c.ws.send(str);
      }

    } else if (msg.type === "frag_update") {
      // Update score for this player and broadcast to everyone
      entry.frags  = msg.frags  ?? entry.frags;
      entry.deaths = msg.deaths ?? entry.deaths;
      const str = JSON.stringify({
        type: "frag_update",
        username: entry.username,
        frags:    entry.frags,
        deaths:   entry.deaths,
      });
      for (const c of lobbyClients) {
        if (c.ws.readyState === WebSocket.OPEN) c.ws.send(str);
      }
    }
  });

  ws.on("close", () => {
    if (!entry) return;
    const wasHost = lobbyClients[0] === entry;
    lobbyClients = lobbyClients.filter(c => c !== entry);
    console.log(`[LOBBY] ${entry.username} left (${lobbyClients.length} remaining)`);

    if (matchInProgress) {
      if (lobbyClients.length < 1) {
        // Everyone gone — just clean up
        endMatch("All players disconnected");
        lobbyClients = [];
        selectedMap = 1;
        timeLimitMinutes = 10;
      } else {
        // Notify remaining players
        lobbyBroadcast({ type: "lobby_reset", reason: `${entry.username} disconnected` });
      }
    } else {
      if (lobbyClients.length === 0) {
        // Empty lobby — full reset
        selectedMap = 1;
        timeLimitMinutes = 10;
      } else {
        if (wasHost) {
          console.log(`[LOBBY] Host left, ${lobbyClients[0].username} is new host`);
        }
        broadcastWaiting();
      }
    }
  });

  ws.on("error", (err) => {
    console.error(`[LOBBY] Error:`, err.message);
  });
});

wss.on("listening", () => {
  console.log(`DECHOOM relay listening on ws://0.0.0.0:${PORT}`);
  console.log(`Lobby: ws://0.0.0.0:${PORT}/?type=lobby`);
  if (DEV_MODE) console.log(`[DEV] Dev mode ON — duplicate usernames will be auto-suffixed`);
});

process.on("SIGTERM", () => {
  console.log("Shutting down...");
  if (matchTimer) clearTimeout(matchTimer);
  wss.close();
});
