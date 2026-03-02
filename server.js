#!/usr/bin/env node
/**
 * DECHOOM Multiplayer Relay Server
 *
 * Two WebSocket endpoints on the same port:
 *   ws://<host>/            — binary Doom net packets
 *   ws://<host>/?type=lobby — JSON lobby/match control messages
 *
 * Binary protocol (websockets-doom):
 *   - On connect: server sends 4 bytes (LE uint32) = assigned UID
 *   - Packets from client: first 4 bytes = destination UID, bytes 4-7 = source UID
 *   - Destination UID 0xFFFFFFFF = broadcast to all other clients
 *   - Server overwrites bytes 4-7 with the real sender UID before forwarding
 */

const { WebSocketServer, WebSocket } = require("ws");

const PORT = process.env.PORT || 2342;
const DEV_MODE = process.env.NODE_ENV !== "production";
const MIN_PLAYERS = DEV_MODE ? 1 : 2;
const MAX_PLAYERS = 8;

// How long the host Doom gets to start and open its listening socket
// before clients are told to connect.
const HOST_HEADSTART_MS = 4000;

const wss = new WebSocketServer({ port: PORT });

// ── Binary relay state ────────────────────────────────────────────────────────
const binaryClients = new Map();
let nextUid = 1;

function allocateUid() {
  while (binaryClients.has(nextUid)) nextUid = (nextUid % 0xfffe) + 1;
  return nextUid++;
}

// ── Lobby state ───────────────────────────────────────────────────────────────
let peekClients      = new Set();
let lobbyClients     = [];
let matchInProgress  = false;
let selectedMap      = 1;
let timeLimitMinutes = 10;
let matchTimer       = null;
let matchStartTime   = null;
let clientGoTimer    = null; // pending setTimeout for sending client_go messages

function lobbyBroadcast(msg) {
  const str = JSON.stringify(msg);
  for (const c of lobbyClients)
    if (c.ws.readyState === WebSocket.OPEN) c.ws.send(str);
}

function broadcastWaiting() {
  const players  = lobbyClients.map(c => c.username);
  const count    = players.length;
  const isEnough = count >= MIN_PLAYERS;

  for (let i = 0; i < lobbyClients.length; i++) {
    const c = lobbyClients[i];
    if (c.ws.readyState !== WebSocket.OPEN) continue;
    c.ws.send(JSON.stringify({
      type: "waiting", players, count, need: MIN_PLAYERS,
      isHost: i === 0, canStart: isEnough, selectedMap, timeLimitMinutes,
    }));
  }

  const peekMsg = JSON.stringify({
    type: "lobby_status", players, count, need: MIN_PLAYERS,
    selectedMap, timeLimitMinutes, matchInProgress,
  });
  for (const ws of peekClients)
    if (ws.readyState === WebSocket.OPEN) ws.send(peekMsg);
}

function endMatch(reason = "Time limit reached") {
  if (matchTimer)   { clearTimeout(matchTimer);   matchTimer   = null; }
  if (clientGoTimer){ clearTimeout(clientGoTimer); clientGoTimer = null; }
  matchInProgress = false;
  matchStartTime  = null;

  const scores = lobbyClients
    .map(c => ({ username: c.username, frags: c.frags, deaths: c.deaths }))
    .sort((a, b) => b.frags - a.frags);

  lobbyBroadcast({ type: "match_end", scores, reason });
  console.log(`[LOBBY] Match ended: ${reason}`);
  broadcastWaiting();
}

function startMatchTimer() {
  if (matchTimer) clearTimeout(matchTimer);
  if (!timeLimitMinutes) return;
  matchStartTime = Date.now();
  matchTimer = setTimeout(() => endMatch("Time limit reached"), timeLimitMinutes * 60 * 1000);
  console.log(`[LOBBY] Match timer started: ${timeLimitMinutes} min`);
}

// ── WebSocket handler ─────────────────────────────────────────────────────────
wss.on("connection", (ws, req) => {
  const url     = new URL(req.url, `http://localhost`);
  const isLobby = url.searchParams.get("type") === "lobby";

  if (!isLobby) {
    // ── Binary relay ──────────────────────────────────────────────────────────
    const uid = allocateUid();
    binaryClients.set(uid, ws);
    console.log(`[+] Binary client uid=${uid} total=${binaryClients.size}`);

    // Send 4-byte hello with assigned UID
    const hello = Buffer.alloc(4);
    hello.writeUInt32LE(uid, 0);
    ws.send(hello);

    ws.on("message", (data) => {
      if (!(data instanceof Buffer)) data = Buffer.from(data);

      // Doom packets: bytes 0-3 = destination UID, bytes 4-7 = source UID
      // Minimum valid packet is 1 byte (but realistically needs at least a few
      // bytes for the Doom net layer). We only need 4 bytes to read the
      // destination; if the packet has ≥ 8 bytes we stamp the source UID.
      if (data.length < 4) return;

      const destUid = data.readUInt32LE(0);

      // Stamp the real sender UID into bytes 4-7 (if room)
      if (data.length >= 8) {
      data.writeUInt32LE(uid, 4);
      }

      const BROADCAST = 0xFFFFFFFF;

      if (destUid === BROADCAST || destUid === 0) {
        // Broadcast: send to all other binary clients
        for (const [otherUid, otherWs] of binaryClients) {
          if (otherUid !== uid && otherWs.readyState === WebSocket.OPEN) {
          otherWs.send(data);
          }
        }
      } else {
        // Unicast: route to the specific destination UID
        const destWs = binaryClients.get(destUid);
        if (destWs && destWs.readyState === WebSocket.OPEN) {
          destWs.send(data);
    } else {
          // Destination not found — try broadcast as fallback so the Doom
          // engine's own timeout / retry logic can handle it
          for (const [otherUid, otherWs] of binaryClients) {
            if (otherUid !== uid && otherWs.readyState === WebSocket.OPEN) {
              otherWs.send(data);
            }
          }
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

  // ── Peek (spectator) client ───────────────────────────────────────────────
  const isPeek = url.searchParams.get("peek") === "1";
  if (isPeek) {
    peekClients.add(ws);
    console.log(`[PEEK] Spectator connected (${peekClients.size} watching)`);
    const players = lobbyClients.map(c => c.username);
    ws.send(JSON.stringify({
      type: "lobby_status", players, count: players.length,
      need: MIN_PLAYERS, selectedMap, timeLimitMinutes, matchInProgress,
    }));
    ws.on("close", () => { peekClients.delete(ws); console.log(`[PEEK] Spectator disconnected (${peekClients.size} watching)`); });
    ws.on("error", () => peekClients.delete(ws));
    return;
  }

  // ── Lobby client ──────────────────────────────────────────────────────────
  let entry = null;

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    if (msg.type === "lobby") {
      if (lobbyClients.length >= MAX_PLAYERS) {
        ws.send(JSON.stringify({ type: "rejected", reason: "Lobby is full" }));
        return;
      }
      let username = msg.username || "Operative";
      if (DEV_MODE) {
        const existing = lobbyClients.map(c => c.username);
        if (existing.includes(username)) {
          let suffix = 2;
          while (existing.includes(`${username} (${suffix})`)) suffix++;
          username = `${username} (${suffix})`;
          console.log(`[LOBBY][DEV] Renamed duplicate to "${username}"`);
        }
      }
      entry = { ws, username, frags: 0, deaths: 0 };
      lobbyClients.push(entry);
      console.log(`[LOBBY] ${entry.username} joined (${lobbyClients.length} players)`);
      broadcastWaiting();
      return;
    }

    if (!entry) return;
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

      // Clear any stale binary connections from previous matches so the
      // new Doom WASM instances get a clean relay environment.
      for (const [staleUid, staleWs] of binaryClients) {
        console.log(`[BINARY] Clearing stale binary client uid=${staleUid} before match start`);
        try { staleWs.close(1000, "match starting"); } catch {}
        binaryClients.delete(staleUid);
      }

      const players     = lobbyClients.map(c => c.username);
      const playerCount = lobbyClients.length;

      console.log(`[LOBBY] Starting match: ${playerCount} players, map ${selectedMap}, ${timeLimitMinutes} min`);

      // ── LAUNCH SEQUENCE ───────────────────────────────────────────────────
      // Step 1: Send all players their "start" packet (triggers asset download)
      //         but DON'T send go/client_go yet.
      lobbyClients.forEach((c, i) => {
        const role = i === 0 ? "server" : "client";
        setTimeout(() => {
          if (c.ws.readyState !== WebSocket.OPEN) return;
          c.ws.send(JSON.stringify({
            type: "start", role, playerCount, players,
            map: selectedMap, timeLimitMinutes,
          }));
        }, i * 200);
      });

      // Step 2: Send "go" to host after a short delay so it starts downloading.
      // Host Doom will start and sit waiting for client connections.
      setTimeout(() => {
        if (lobbyClients[0]?.ws.readyState === WebSocket.OPEN) {
          console.log(`[LOBBY] Sending go to host`);
          lobbyClients[0].ws.send(JSON.stringify({ type: "go" }));
        }
      }, 500);

      // Step 3: After giving the host a head-start to get its Doom listening,
      // tell clients to connect. HOST_HEADSTART_MS after the host go signal.
      clientGoTimer = setTimeout(() => {
        clientGoTimer = null;
        lobbyClients.slice(1).forEach((c, i) => {
          setTimeout(() => {
            if (c.ws.readyState !== WebSocket.OPEN) return;
            console.log(`[LOBBY] Sending client_go to ${c.username}`);
            c.ws.send(JSON.stringify({ type: "client_go" }));
          }, i * 300);
        });
        startMatchTimer();
      }, 500 + HOST_HEADSTART_MS);
      // ─────────────────────────────────────────────────────────────────────

    } else if (msg.type === "client_ready") {
      // No longer used to gate the host launch — kept for compatibility
      console.log(`[LOBBY] client_ready received from ${entry.username} (informational)`);

    } else if (msg.type === "kill_event") {
      const str = JSON.stringify({ type: "kill_event", killer: msg.killer, victim: msg.victim });
      for (const c of lobbyClients)
        if (c !== entry && c.ws.readyState === WebSocket.OPEN) c.ws.send(str);

    } else if (msg.type === "frag_update") {
      entry.frags  = msg.frags  ?? entry.frags;
      entry.deaths = msg.deaths ?? entry.deaths;
      const str = JSON.stringify({
        type: "frag_update", username: entry.username,
        frags: entry.frags, deaths: entry.deaths,
      });
      for (const c of lobbyClients)
        if (c.ws.readyState === WebSocket.OPEN) c.ws.send(str);
    }
  });

  ws.on("close", () => {
    if (!entry) return;
    const wasHost = lobbyClients[0] === entry;
    lobbyClients = lobbyClients.filter(c => c !== entry);
    console.log(`[LOBBY] ${entry.username} left (${lobbyClients.length} remaining)`);

    if (matchInProgress) {
      if (lobbyClients.length < 1) {
        endMatch("All players disconnected");
        lobbyClients     = [];
        selectedMap      = 1;
        timeLimitMinutes = 10;
      } else {
        lobbyBroadcast({ type: "lobby_reset", reason: `${entry.username} disconnected` });
      }
    } else {
      if (lobbyClients.length === 0) {
        selectedMap = 1; timeLimitMinutes = 10;
      } else {
        if (wasHost) console.log(`[LOBBY] Host left, ${lobbyClients[0].username} is new host`);
        broadcastWaiting();
      }
    }
  });

  ws.on("error", (err) => console.error(`[LOBBY] Error:`, err.message));
});

wss.on("listening", () => {
  console.log(`DECHOOM relay listening on ws://0.0.0.0:${PORT}`);
  console.log(`Lobby: ws://0.0.0.0:${PORT}/?type=lobby`);
  if (DEV_MODE) console.log(`[DEV] Dev mode ON`);
});

process.on("SIGTERM", () => {
  console.log("Shutting down...");
  if (matchTimer)    clearTimeout(matchTimer);
  if (clientGoTimer) clearTimeout(clientGoTimer);
  wss.close();
});

