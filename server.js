#!/usr/bin/env node
/**
 * DECHOOM Multiplayer Relay Server
 * 
 * Implements the Cloudflare doom-wasm WebSocket routing protocol.
 * Each client connects, is assigned a uint32 UID, and all packets
 * are broadcast to every other connected client with routing headers
 * so Doom's net_websockets.c can reconstruct the fake IP table.
 * 
 * Protocol (binary, little-endian):
 *   Incoming from client: [4 bytes to_uid][4 bytes from_uid][N bytes payload]
 *   Server broadcasts to all others as-is, replacing from_uid with sender's actual UID
 *   On connect: server sends [4 bytes UID assignment] (special hello packet, from=0)
 */

const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 2342;

const wss = new WebSocketServer({ port: PORT });

// Map of uid -> ws
const clients = new Map();
let nextUid = 1;

function allocateUid() {
  // UIDs are uint32, avoid 0 (reserved) and wrap safely
  while (clients.has(nextUid)) {
    nextUid = (nextUid % 0xfffe) + 1;
  }
  return nextUid++;
}

wss.on("connection", (ws, req) => {
  const uid = allocateUid();
  clients.set(uid, ws);

  const addr = req.socket.remoteAddress;
  console.log(`[+] Client connected: uid=${uid} addr=${addr} total=${clients.size}`);

  // Send UID assignment to new client
  // Format: 4-byte UID in little-endian (this is the "hello" the C code expects)
  const hello = Buffer.alloc(4);
  hello.writeUInt32LE(uid, 0);
  ws.send(hello);

  ws.on("message", (data) => {
    if (!(data instanceof Buffer)) data = Buffer.from(data);

    // Minimum packet is 8 bytes (to + from headers)
    if (data.length < 8) return;

    // Rewrite the from-uid in bytes [4..7] with this client's actual uid
    // (prevents spoofing)
    data.writeUInt32LE(uid, 4);

    // Broadcast to all other connected clients
    for (const [otherUid, otherWs] of clients) {
      if (otherUid !== uid && otherWs.readyState === 1 /* OPEN */) {
        otherWs.send(data);
      }
    }
  });

  ws.on("close", () => {
    clients.delete(uid);
    console.log(`[-] Client disconnected: uid=${uid} total=${clients.size}`);
  });

  ws.on("error", (err) => {
    console.error(`[!] Error uid=${uid}:`, err.message);
    clients.delete(uid);
  });
});

wss.on("listening", () => {
  console.log(`DECHOOM relay listening on ws://0.0.0.0:${PORT}`);
  console.log(`Max players: 4 (Doom limit)`);
});

process.on("SIGTERM", () => {
  console.log("Shutting down...");
  wss.close();
});