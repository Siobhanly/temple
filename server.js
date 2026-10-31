'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number.parseInt(process.env.PORT || '8080', 10);
const CAP = positiveInt(process.env.CAP, 50);
const MAXI = positiveInt(process.env.MAXI, 8);
const ROOMS = [
  { id: 'lotus', name: 'Lotus Hall' },
  { id: 'pine', name: 'Pine Hall' },
  { id: 'cloud', name: 'Cloud Hall' },
];
const COLORS = ['e8a87c', 'c38d9e', '85dcb0', '41b3a3', 'e27d60', 'f6c85f', '9fd3c7', 'b8a9c9'];
const NAME_A = ['Quiet', 'Calm', 'Gentle', 'Warm', 'Still', 'Soft', 'Slow', 'Clear', 'Bright', 'Deep'];
const NAME_B = ['Wind', 'Wave', 'Pine', 'Bird', 'River', 'Cloud', 'Moon', 'Stone', 'Leaf', 'Rain'];
const ALLOWED_ACTS = new Set(['greet', 'bow', 'stand', 'beads', 'incense', 'bell', 'release']);
const roomInstances = new Map(ROOMS.map(r => [r.id, new Map()]));
const merits = new Map(ROOMS.map(r => [r.id, 0]));
let nextVisitorId = 1;

function positiveInt(value, fallback) {
  const n = Number.parseInt(value, 10);
  return Number.isInteger(n) && n > 0 ? n : fallback;
}

function send(ws, message) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(message));
}

function visitorList(instance, except) {
  return [...instance].filter(peer => peer !== except && peer.readyState === WebSocket.OPEN).map(peer => peer.visitor);
}

function roomPublicState() {
  return ROOMS.map(room => {
    const instances = roomInstances.get(room.id);
    return {
      id: room.id,
      name: room.name,
      cap: CAP,
      instances: [...instances.entries()]
        .filter(([, sockets]) => sockets.size > 0)
        .sort(([a], [b]) => a - b)
        .map(([inst, sockets]) => ({ inst, count: sockets.size, cap: CAP })),
    };
  });
}

function chooseInstance(roomId, requestedInst) {
  const instances = roomInstances.get(roomId);
  if (Number.isInteger(requestedInst) && requestedInst >= 0 && requestedInst < MAXI) {
    const existing = instances.get(requestedInst);
    if (!existing || existing.size < CAP) {
      if (!existing) instances.set(requestedInst, new Set());
      return requestedInst;
    }
  }

  const open = [...instances.entries()]
    .filter(([, sockets]) => sockets.size > 0 && sockets.size < CAP)
    .sort((a, b) => b[1].size - a[1].size || a[0] - b[0]);
  if (open.length) return open[0][0];

  for (let i = 0; i < MAXI; i += 1) {
    if (!instances.has(i)) {
      instances.set(i, new Set());
      return i;
    }
  }
  return null;
}

function makeVisitor(instance) {
  let name;
  do {
    name = `${NAME_A[Math.floor(Math.random() * NAME_A.length)]} ${NAME_B[Math.floor(Math.random() * NAME_B.length)]} ${String(Math.floor(Math.random() * 100)).padStart(2, '0')}`;
  } while ([...instance].some(ws => ws.visitor && ws.visitor.name === name));
  const visitor = {
    id: `v${nextVisitorId++}`,
    name,
    color: `#${COLORS[Math.floor(Math.random() * COLORS.length)]}`,
    x: 0.15 + Math.random() * 0.70,
    y: 0.45 + Math.random() * 0.45,
    pose: 'stand',
  };
  return visitor;
}

function addMerit(roomId) {
  const next = merits.get(roomId) + 1;
  merits.set(roomId, next);
  for (const sockets of roomInstances.get(roomId).values()) {
    for (const peer of sockets) send(peer, { t: 'merit', merit: next });
  }
}

function parseRequestUrl(request) {
  try { return new URL(request.url, 'http://localhost'); } catch { return null; }
}

function safeJson(data) {
  try { return JSON.parse(data.toString()); } catch { return null; }
}

const publicDir = path.join(__dirname, 'public');
const server = http.createServer((req, res) => {
  const url = parseRequestUrl(req);
  if (!url) {
    res.writeHead(400).end('Bad request');
    return;
  }
  if (req.method === 'GET' && url.pathname === '/') {
    fs.readFile(path.join(publicDir, 'index.html'), (error, body) => {
      if (error) { res.writeHead(500).end('Internal server error'); return; }
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' });
      res.end(body);
    });
    return;
  }
  if (req.method === 'GET' && url.pathname === '/og.png') {
    res.writeHead(200, { 'content-type': 'image/png', 'content-length': ogPng.length, 'cache-control': 'public, max-age=3600' });
    res.end(ogPng);
    return;
  }
  if (req.method === 'GET' && url.pathname === '/api/rooms') {
    const body = JSON.stringify(roomPublicState());
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'public, max-age=5' });
    res.end(body);
    return;
  }
  res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' }).end('Not found');
});

const wss = new WebSocketServer({ noServer: true, maxPayload: 512 });
server.on('upgrade', (req, socket, head) => {
  const url = parseRequestUrl(req);
  if (!url || url.pathname !== '/ws') {
    socket.write('HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n');
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, req, url));
});

wss.on('connection', (ws, req, url) => {
  let roomId = url.searchParams.get('room') || ROOMS[0].id;
  if (!roomInstances.has(roomId)) {
    ws.close(4004, 'Unknown room');
    return;
  }
  const instParam = url.searchParams.get('inst');
  const requestedInst = instParam !== null && /^\d+$/.test(instParam) ? Number(instParam) : null;
  const inst = chooseInstance(roomId, requestedInst);
  if (inst === null) {
    send(ws, { t: 'full' });
    ws.close(4009, 'Every hall is full');
    return;
  }

  const instance = roomInstances.get(roomId).get(inst);
  const visitor = makeVisitor(instance);
  ws.visitor = visitor;
  ws.roomId = roomId;
  ws.inst = inst;
  ws.alive = true;
  ws.rateWindow = Date.now();
  ws.rateCount = 0;
  ws.lastMoveAt = 0;
  instance.add(ws);

  send(ws, { t: 'welcome', self: visitor, peers: visitorList(instance, ws), room: roomId, inst, cap: CAP, merit: merits.get(roomId) });
  for (const peer of instance) {
    if (peer !== ws) send(peer, { t: 'join', peer: visitor });
  }

  ws.on('pong', () => { ws.alive = true; });
  ws.on('message', (data, isBinary) => {
    const now = Date.now();
    if (now - ws.rateWindow >= 1000) {
      ws.rateWindow = now;
      ws.rateCount = 0;
    }
    ws.rateCount += 1;
    if (ws.rateCount > 20 || isBinary) return;

    const msg = safeJson(data);
    if (!msg || typeof msg !== 'object' || Array.isArray(msg) || typeof msg.t !== 'string') return;
    if (msg.t === 'ping') {
      if (Object.keys(msg).length === 1) send(ws, { t: 'pong' });
      return;
    }
    if (msg.t === 'bead') {
      if (Object.keys(msg).length === 1) addMerit(roomId);
      return;
    }
    if (msg.t === 'move') {
      if (!Number.isFinite(msg.x) || !Number.isFinite(msg.y) || now - ws.lastMoveAt < 100) return;
      const x = Math.max(0, Math.min(1, msg.x));
      const y = Math.max(0, Math.min(1, msg.y));
      ws.lastMoveAt = now;
      visitor.x = x;
      visitor.y = y;
      visitor.pose = 'stand';
      for (const peer of instance) if (peer !== ws) send(peer, { t: 'move', id: visitor.id, x, y });
      return;
    }
    if (msg.t === 'act' && ALLOWED_ACTS.has(msg.a)) {
      if (msg.a === 'bow') visitor.pose = 'bow';
      else if (msg.a === 'stand') visitor.pose = 'stand';
      else if (msg.a === 'beads') visitor.pose = 'beads';
      const outgoing = { t: 'act', id: visitor.id, a: msg.a };
      if (msg.a === 'incense') {
        if (typeof msg.to === 'string' && msg.to !== visitor.id && [...instance].some(peer => peer.visitor && peer.visitor.id === msg.to)) outgoing.to = msg.to;
        addMerit(roomId);
      } else if (msg.a === 'bell') {
        addMerit(roomId);
      }
      for (const peer of instance) if (peer !== ws) send(peer, outgoing);
    }
  });

  ws.on('close', () => {
    const current = roomInstances.get(roomId).get(inst);
    if (!current) return;
    current.delete(ws);
    for (const peer of current) send(peer, { t: 'leave', id: visitor.id });
    if (current.size === 0) roomInstances.get(roomId).delete(inst);
  });
});

const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.alive) { ws.terminate(); continue; }
    ws.alive = false;
    ws.ping();
  }
}, 30_000);
heartbeat.unref();

function crc32(buffer) {
  let crc = 0xffffffff;
  for (const byte of buffer) crc = (crc >>> 8) ^ CRC_TABLE[(crc ^ byte) & 0xff];
  return (crc ^ 0xffffffff) >>> 0;
}
const CRC_TABLE = (() => {
  const table = new Uint32Array(256);
  for (let n = 0; n < 256; n += 1) {
    let c = n;
    for (let k = 0; k < 8; k += 1) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    table[n] = c >>> 0;
  }
  return table;
})();
const ogPng = makeOgPng();
function pngChunk(type, data) {
  const name = Buffer.from(type);
  const len = Buffer.alloc(4); len.writeUInt32BE(data.length);
  const check = Buffer.concat([name, data]);
  const crc = Buffer.alloc(4); crc.writeUInt32BE(crc32(check));
  return Buffer.concat([len, check, crc]);
}
function makeOgPng() {
  const width = 1200; const height = 630;
  const raw = Buffer.alloc((width * 4 + 1) * height);
  for (let y = 0; y < height; y += 1) {
    const row = y * (width * 4 + 1);
    raw[row] = 0;
    for (let x = 0; x < width; x += 1) {
      const dx = x - 600; const dy = y - 315;
      const disc = dx * dx + dy * dy <= 140 * 140;
      const p = row + 1 + x * 4;
      raw[p] = disc ? 0xe0 : 0x1a;
      raw[p + 1] = disc ? 0xa8 : 0x14;
      raw[p + 2] = disc ? 0x4a : 0x10;
      raw[p + 3] = 255;
    }
  }
  const ihdr = Buffer.alloc(13);
  ihdr.writeUInt32BE(width, 0); ihdr.writeUInt32BE(height, 4); ihdr[8] = 8; ihdr[9] = 6; ihdr[10] = 0; ihdr[11] = 0; ihdr[12] = 0;
  return Buffer.concat([
    Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]),
    pngChunk('IHDR', ihdr),
    pngChunk('IDAT', zlib.deflateSync(raw)),
    pngChunk('IEND', Buffer.alloc(0)),
  ]);
}

server.listen(PORT, '0.0.0.0', () => console.log(`listening on ${PORT}`));

function shutdown() {
  clearInterval(heartbeat);
  server.close(() => process.exit(0));
  for (const ws of wss.clients) ws.close(1001, 'Server shutting down');
  setTimeout(() => process.exit(0), 1000).unref();
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
