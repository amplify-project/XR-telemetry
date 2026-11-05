'use strict';
// XR-telemetry gateway (spec §8).
//
//   WebSocket producers ──/ingest──▶ PUBLISH + XADD  device:{src}:{signal}  ──▶ Redis
//   Redis  PSUBSCRIBE device:*:*  ──▶ /consume ──▶ WebSocket consumers
//
// Consumers are fed from Redis, not from the producers directly, so they see
// every conforming producer the same way: a headset behind this gateway, a
// Python wearable publisher writing to Redis itself, another gateway.
//
// State lives in Redis. The gateway only keeps per-connection clocks and a
// short cache of the latest sample per (source, signal) for snapshots.

const http = require('http');
const crypto = require('crypto');
const Redis = require('ioredis');
const { WebSocketServer } = require('ws');
const { ClockSync } = require('./clock');
const { NAME_RE, signalKey, toStreamFields, fromWire } = require('./sample');
const { isLegacyBatch, legacyToPose } = require('./legacy');

// Keys under device:{src}: that are not signals (spec §3.2).
const RESERVED = new Set(['meta', 'alive', 'participant']);

/**
 * @param {object}  opt
 * @param {string}  [opt.redisUrl='redis://127.0.0.1:6379']
 * @param {string}  [opt.namespace='']       key prefix, e.g. 'amplify' -> amplify:device:…
 * @param {?http.Server} [opt.server=null]   attach to this server instead of listening
 * @param {number}  [opt.port=8090]
 * @param {string}  [opt.basePath='']        serve under a prefix a proxy does not strip
 * @param {?string} [opt.ingestToken=null]
 * @param {?string} [opt.consumeToken=null]
 * @param {number}  [opt.maxlen=20000]       approximate stream cap (MAXLEN ~)
 * @param {number}  [opt.aliveTtlS=5]        device:{src}:alive expiry
 * @param {number}  [opt.pingMs=2000]        clock probe period per producer
 * @param {number}  [opt.keepAliveMs=30000]  WebSocket ping to consumers; 0 = off
 * @param {number}  [opt.snapshotTtlMs=10000] samples older than this are left out of snapshots
 * @param {boolean} [opt.log=true]
 */
function createGateway({
  redisUrl = 'redis://127.0.0.1:6379', namespace = '', server: hostServer = null, port = 8090,
  basePath = '', ingestToken = null, consumeToken = null, maxlen = 20000, aliveTtlS = 5,
  pingMs = 2000, keepAliveMs = 30000, snapshotTtlMs = 10000, log = true,
} = {}) {
  const ns = namespace ? `${namespace.replace(/:+$/, '')}:` : '';
  const base = (basePath || '').replace(/\/+$/, '');
  const tokens = { '/ingest': ingestToken || null, '/consume': consumeToken || null };
  const say = (...a) => { if (log) console.log('[xrtel]', ...a); };

  // Two connections: a subscribed ioredis connection cannot issue commands.
  const redis = new Redis(redisUrl, { lazyConnect: false, maxRetriesPerRequest: 2 });
  const sub = new Redis(redisUrl, { lazyConnect: false });
  for (const c of [redis, sub]) c.on('error', (e) => say('redis:', e.message));

  const consumers = new Set();              // { ws, session, signals:Set|null }
  const latest = new Map();                 // `${src}\0${signal}` -> { at, msg }
  let producers = 0;

  // ── Redis -> consumers ────────────────────────────────────────────────
  const chanRe = new RegExp(`^${escapeRe(ns)}device:([^:]+):([^:]+)$`);
  sub.psubscribe(`${ns}device:*:*`).catch((e) => say('psubscribe failed:', e.message));
  sub.on('pmessage', (_pattern, channel, payload) => {
    const m = chanRe.exec(channel);
    if (!m || RESERVED.has(m[2])) return;
    const [, src, signal] = m;
    let fields;
    try { fields = JSON.parse(payload); } catch { fields = { value: payload }; }
    if (fields === null || typeof fields !== 'object' || Array.isArray(fields)) fields = { value: fields };
    const msg = { type: 'sample', signal, device: src, ...fields };
    latest.set(`${src}\0${signal}`, { at: Date.now(), msg });
    const data = JSON.stringify(msg);
    for (const c of consumers) if (wants(c, msg)) send(c.ws, data);
  });

  // Session filtering applies only to samples that carry a session: producers
  // that write to Redis without one (current wearable publishers) reach every
  // consumer of the namespace. Spec §12 tracks giving them one.
  function wants(c, msg) {
    if (c.signals && !c.signals.has(msg.signal)) return false;
    if (c.session && msg.session && msg.session !== c.session) return false;
    return true;
  }

  // ── Session ───────────────────────────────────────────────────────────
  // A producer that does not name one gets the current session (spec §3.2),
  // read through a short cache so a batch does not cost a round trip.
  let sessionCache = { at: 0, value: null };
  async function currentSession() {
    if (Date.now() - sessionCache.at < 5000) return sessionCache.value;
    let v = null;
    try { v = (await redis.get(`${ns}session:current`)) || (await redis.get(`${ns}engagement:session_id`)); }
    catch { /* keep the old value on a transient error */ v = sessionCache.value; }
    sessionCache = { at: Date.now(), value: v };
    return v;
  }

  // ── HTTP + upgrade ────────────────────────────────────────────────────
  const health = () => ({
    ok: redis.status === 'ready' && sub.status === 'ready',
    redis: redis.status, producers, consumers: consumers.size, namespace: ns.replace(/:$/, '') || null,
    auth: { ingest: !!tokens['/ingest'], consume: !!tokens['/consume'] },
  });
  const handler = (req, res) => {
    if ((req.url || '').split('?')[0] === base + '/health') {
      const h = health();
      res.writeHead(h.ok ? 200 : 503, { 'content-type': 'application/json' });
      return res.end(JSON.stringify(h));
    }
    res.writeHead(404); res.end();
  };
  const attached = !!hostServer;
  const server = hostServer || http.createServer(handler);

  const wss = new WebSocketServer({ noServer: true });
  const onUpgrade = (req, socket, head) => {
    const url = new URL(req.url || '/', 'http://gateway');
    if (base && !url.pathname.startsWith(base + '/')) { if (!attached) socket.destroy(); return; }
    const path = url.pathname.slice(base.length);
    if (path !== '/ingest' && path !== '/consume') { if (!attached) socket.destroy(); return; }
    const want = tokens[path];
    if (want && !sameSecret(url.searchParams.get('token'), want)) return refuse(socket, 401, 'Unauthorized');
    const session = url.searchParams.get('session');
    if (session && !NAME_RE.test(session)) return refuse(socket, 400, 'Bad Request');
    const signals = url.searchParams.get('signals');
    wss.handleUpgrade(req, socket, head, (ws) => {
      if (path === '/consume') onConsumer(ws, { session, signals });
      else onProducer(ws, { session });
    });
  };
  server.on('upgrade', onUpgrade);

  // ── Producers ─────────────────────────────────────────────────────────
  function onProducer(ws, { session: qSession }) {
    producers++;
    const clock = new ClockSync();
    let device = null, meta = {}, seq = 0, pinger = null;
    const probe = () => send(ws, JSON.stringify({ ping: Date.now() }));

    ws.on('message', async (raw) => {
      let msg; try { msg = JSON.parse(raw); } catch { return; }

      // Stamp the arrival before anything else: time spent here widens the estimate.
      if (msg.pong !== undefined) return clock.observe(msg.pong, msg.c, Date.now());

      if (msg.hello !== undefined) {
        const id = String(msg.hello);
        if (!NAME_RE.test(id)) return ws.close(1008, 'bad device id');
        device = id;
        meta = { ...meta, ...(msg.meta || {}) };
        await register(device, meta).catch((e) => say('register:', e.message));
        if (!pinger) { probe(); pinger = setInterval(probe, pingMs); }
        return;
      }
      if (!device) return;                                 // data needs a prior hello

      // Spec batch {s, b}, or the Livestreamed player's original {b} of poses.
      let signal, batch;
      if (typeof msg.s === 'string' && Array.isArray(msg.b)) { signal = msg.s; batch = msg.b; }
      else if (isLegacyBatch(msg)) { signal = 'pose'; batch = msg.b.map((s) => legacyToPose(s, meta)); }
      else return;
      if (!NAME_RE.test(signal) || RESERVED.has(signal)) return;

      const session = qSession || meta.session || await currentSession();
      const key = signalKey(ns, device, signal);
      const p = redis.pipeline();
      for (const w of batch) {
        if (!w || typeof w !== 'object') continue;
        const sample = fromWire(w, { device, session, participant: meta.participant, frame: meta.frame, clock, seq: seq++ });
        const fields = toStreamFields(sample);
        p.publish(key, JSON.stringify(dropNull(sample)));
        p.xadd(key, 'MAXLEN', '~', maxlen, '*', ...fields);
      }
      p.set(`${ns}device:${device}:alive`, '1', 'EX', aliveTtlS);
      p.exec().catch((e) => say('write:', e.message));
    });

    ws.on('close', () => {
      producers--;
      clearInterval(pinger);
      if (!device) return;
      redis.del(`${ns}device:${device}:alive`).catch(() => {});
      for (const k of latest.keys()) if (k.startsWith(device + '\0')) latest.delete(k);
      const data = JSON.stringify({ type: 'leave', device });
      for (const c of consumers) send(c.ws, data);
    });
    ws.on('error', () => { /* close follows */ });
  }

  // device:{src}:meta and the participant mapping (spec §3.2, §7).
  async function register(device, meta) {
    const flat = {};
    for (const k of ['kind', 'platform', 'frame', 'sdk', 'version', 'input', 'mode'])
      if (meta[k] != null) flat[k] = String(meta[k]);
    const p = redis.pipeline();
    if (Object.keys(flat).length) p.hset(`${ns}device:${device}:meta`, flat);
    if (meta.participant && NAME_RE.test(String(meta.participant))) {
      p.set(`${ns}device:${device}:participant`, String(meta.participant));
      p.hset(`${ns}participant:${meta.participant}`, String(meta.role || meta.kind || 'device'), device);
    }
    await p.exec();
  }

  // ── Consumers ─────────────────────────────────────────────────────────
  function onConsumer(ws, { session, signals }) {
    const c = { ws, session: session || null, signals: signals ? new Set(signals.split(',').filter(Boolean)) : null };
    consumers.add(c);
    const cutoff = Date.now() - snapshotTtlMs;
    const samples = [];
    for (const { at, msg } of latest.values()) if (at >= cutoff && wants(c, msg)) samples.push(msg);
    send(ws, JSON.stringify({ type: 'snapshot', samples }));
    ws.on('close', () => consumers.delete(c));
    ws.on('error', () => consumers.delete(c));
  }

  // Proxies (CloudFront, nginx) drop a socket that stays silent; a consumer is
  // silent whenever no producer is. Browsers and ws answer pings on their own.
  const keepAlive = keepAliveMs > 0 && setInterval(() => {
    for (const c of consumers) if (c.ws.readyState === 1) c.ws.ping();
  }, keepAliveMs);

  const ready = new Promise((resolve) => {
    const done = () => resolve();
    if (attached) return done();
    server.listen(port, () => {
      const auth = ['ingest', 'consume'].map((k) => `${k}=${tokens['/' + k] ? 'token' : 'open'}`).join(' ');
      say(`listening on :${server.address().port}${base}  /ingest /consume /health  redis=${redisUrl}  ns=${ns || '(none)'}  auth: ${auth}`);
      done();
    });
  });

  return {
    server, wss, redis, health, ready,
    async close() {
      clearInterval(keepAlive);
      server.removeListener('upgrade', onUpgrade);
      for (const ws of wss.clients) ws.terminate();
      if (!attached) await new Promise((r) => server.close(() => r()));
      await Promise.allSettled([redis.quit(), sub.quit()]);
    },
  };
}

// ── helpers ─────────────────────────────────────────────────────────────
function send(ws, data) { if (ws.readyState === 1) ws.send(data); }
function dropNull(o) { const r = {}; for (const k in o) if (o[k] !== null && o[k] !== undefined) r[k] = o[k]; return r; }
function escapeRe(s) { return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); }
function refuse(socket, code, text) {
  socket.end(`HTTP/1.1 ${code} ${text}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`);
}
// Constant-time compare; hashing first evens out the lengths timingSafeEqual needs.
function sameSecret(got, want) {
  if (typeof got !== 'string') return false;
  const h = (v) => crypto.createHash('sha256').update(v).digest();
  return crypto.timingSafeEqual(h(got), h(want));
}

module.exports = { createGateway };
