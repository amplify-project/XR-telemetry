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

const fs = require('fs');
const path = require('path');
const http = require('http');
const https = require('https');
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
 * @param {?{key, cert}} [opt.tls=null]  serve https/wss itself (LAN with a self-signed cert;
 *                                       behind a proxy or CloudFront leave it null)
 * @param {?string} [opt.archiverUrl=null] where /sessions… is proxied to (the archiver's HTTP API)
 * @param {boolean} [opt.log=true]
 */
function createGateway({
  redisUrl = 'redis://127.0.0.1:6379', namespace = '', server: hostServer = null, port = 8090,
  basePath = '', ingestToken = null, consumeToken = null, maxlen = 20000, aliveTtlS = 5,
  pingMs = 2000, keepAliveMs = 30000, snapshotTtlMs = 10000, tls = null, archiverUrl = null, log = true,
} = {}) {
  const ns = namespace ? `${namespace.replace(/:+$/, '')}:` : '';
  const base = (basePath || '').replace(/\/+$/, '');
  const tokens = { '/ingest': ingestToken || null, '/consume': consumeToken || null };
  const say = (...a) => { if (log) console.log('[xrtel]', ...a); };
  // Operator console: one static page, read once. It holds no secret (the
  // token is typed into it), so it is served without auth.
  let uiPage = null;
  try { uiPage = fs.readFileSync(path.join(__dirname, '..', 'ui', 'index.html')); } catch { /* no ui/ next to src/ */ }

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

  // Operator control (spec §3.2): the session is named by whoever runs the
  // test, at the moment they choose, and everything that arrives without a
  // session of its own — headsets, wearables writing to Redis — is filed under it.
  async function startSession(id, extra = {}) {
    const now = new Date().toISOString();
    const meta = {};
    for (const [k, v] of Object.entries(extra || {})) if (v != null && typeof v !== 'object') meta[k] = String(v);
    const p = redis.pipeline()
      .set(`${ns}session:current`, id).set(`${ns}engagement:session_id`, id)
      .hsetnx(`${ns}session:${id}:meta`, 'started', now)
      .hdel(`${ns}session:${id}:meta`, 'ended');
    if (Object.keys(meta).length) p.hset(`${ns}session:${id}:meta`, meta);
    await p.exec();
    sessionCache = { at: Date.now(), value: id };
    await operatorEvent(id, 'session_start');
    return { current: id, meta: await redis.hgetall(`${ns}session:${id}:meta`) };
  }
  async function stopSession() {
    const id = await currentSession();
    if (!id) return { current: null, stopped: null };
    await operatorEvent(id, 'session_stop');
    await redis.pipeline()
      .del(`${ns}session:current`).del(`${ns}engagement:session_id`)
      .hset(`${ns}session:${id}:meta`, 'ended', new Date().toISOString()).exec();
    sessionCache = { at: Date.now(), value: null };
    return { current: null, stopped: id, meta: await redis.hgetall(`${ns}session:${id}:meta`) };
  }
  // Marks and session boundaries are ordinary `event` samples from a source
  // called `operator`: live consumers see them and the archiver files them.
  async function operatorEvent(session, name, value) {
    const now = Date.now() / 1000;
    const sample = { device: 'operator', timestamp: now, srv_ts: now, srv_err_ms: 0, session, name };
    if (value !== undefined && value !== null) sample.value = value;
    const key = signalKey(ns, 'operator', 'event');
    await redis.pipeline().publish(key, JSON.stringify(sample))
      .xadd(key, 'MAXLEN', '~', maxlen, '*', ...toStreamFields(sample)).exec();
    return sample;
  }

  // ── HTTP + upgrade ────────────────────────────────────────────────────
  const health = () => ({
    ok: redis.status === 'ready' && sub.status === 'ready',
    redis: redis.status, producers, consumers: consumers.size, namespace: ns.replace(/:$/, '') || null,
    auth: { ingest: !!tokens['/ingest'], consume: !!tokens['/consume'] },
    archive: !!archiverUrl,
  });
  const handler = (req, res) => {
    route(req, res).catch((e) => {
      say('http:', e.message);
      if (!res.headersSent) reply(res, 500, { error: e.message }); else res.end();
    });
  };
  async function route(req, res) {
    const url = new URL(req.url || '/', 'http://gateway');
    // Dashboards in a browser call the API from another origin; auth is a bearer token, not a cookie.
    res.setHeader('access-control-allow-origin', '*');
    res.setHeader('access-control-allow-headers', 'authorization, content-type');
    res.setHeader('access-control-allow-methods', 'GET, POST, OPTIONS');
    const inBase = !base || url.pathname === base || url.pathname.startsWith(base + '/');
    const p = inBase ? url.pathname.slice(base.length) : null;
    if (p === '/health') { const h = health(); return reply(res, h.ok ? 200 : 503, h); }
    if (uiPage && (p === '' || p === '/' || p === '/ui')) { res.writeHead(302, { location: `${base}/ui/` }); return res.end(); }
    if (uiPage && (p === '/ui/' || p === '/ui/index.html')) {
      res.writeHead(200, {
        'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache',
        'content-security-policy': "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self' ws: wss:; img-src 'self' data:; base-uri 'none'; form-action 'none'",
        'x-content-type-options': 'nosniff', 'referrer-policy': 'no-referrer',
      });
      return res.end(uiPage);
    }
    const api = p === '/session' || p === '/session/start' || p === '/session/stop' || p === '/mark' ||
      p === '/sessions' || (p && p.startsWith('/sessions/'));
    if (!api) {
      // Say where we are: an empty 404 behind a proxy looks like "nothing there".
      res.writeHead(404, { 'content-type': 'text/plain' });
      return res.end(`xr-telemetry gateway: not found. Serving ${base || '/'} — try ${base}/health\n`);
    }
    if (req.method === 'OPTIONS') { res.writeHead(204); return res.end(); }
    const want = tokens['/consume'];
    const bearer = /^Bearer\s+(.+)$/i.exec(req.headers.authorization || '')?.[1];
    if (want && !sameSecret(bearer || url.searchParams.get('token'), want)) return reply(res, 401, { error: 'consume token required' });

    if (p === '/session' && req.method === 'GET') {
      const id = await currentSession();
      return reply(res, 200, { current: id, meta: id ? await redis.hgetall(`${ns}session:${id}:meta`) : null });
    }
    if (p === '/session/start' && req.method === 'POST') {
      const body = await readJson(req);
      const id = String(body.id ?? body.session ?? '');
      if (!NAME_RE.test(id)) return reply(res, 400, { error: 'id must match [A-Za-z0-9_.-]{1,64}' });
      return reply(res, 200, await startSession(id, body.meta));
    }
    if (p === '/session/stop' && req.method === 'POST') return reply(res, 200, await stopSession());
    if (p === '/mark' && req.method === 'POST') {
      const body = await readJson(req);
      const name = String(body.name ?? '');
      if (!name || name.length > 128) return reply(res, 400, { error: 'name required' });
      const session = body.session ? String(body.session) : await currentSession();
      if (!session || !NAME_RE.test(session)) return reply(res, 409, { error: 'no session running: start one first' });
      return reply(res, 200, await operatorEvent(session, name, body.value));
    }
    if (p.startsWith('/sessions') && req.method === 'GET') return proxyToArchiver(req, res, p, url.searchParams);
    return reply(res, 405, { error: 'method not allowed' });
  }

  function proxyToArchiver(req, res, path, params) {
    if (!archiverUrl) return reply(res, 503, { error: 'no archiver configured (ARCHIVER_URL)' });
    const q = new URLSearchParams();
    for (const k of ['format', 'signal', 'device']) if (params.get(k)) q.set(k, params.get(k));   // never the token
    const target = new URL(path + (q.size ? '?' + q : ''), archiverUrl);
    const up = http.request(target, { method: 'GET' }, (r) => {
      const h = {};
      for (const k of ['content-type', 'content-disposition']) if (r.headers[k]) h[k] = r.headers[k];
      res.writeHead(r.statusCode, h);
      r.pipe(res);
    });
    up.on('error', () => { if (!res.headersSent) reply(res, 502, { error: 'archiver unreachable' }); else res.end(); });
    up.end();
  }
  const attached = !!hostServer;
  const server = hostServer || (tls ? https.createServer(tls, handler) : http.createServer(handler));
  const scheme = hostServer ? (typeof server.setSecureContext === 'function' ? 'wss' : 'ws') : (tls ? 'wss' : 'ws');

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
      say(`listening on ${scheme}://0.0.0.0:${server.address().port}${base}  /ingest /consume /health /session /mark /sessions  archive=${archiverUrl || 'none'}  redis=${redisUrl}  ns=${ns || '(none)'}  auth: ${auth}`);
      done();
    });
  });

  return {
    server, wss, redis, health, ready, scheme,
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
function reply(res, code, body) {
  res.writeHead(code, { 'content-type': 'application/json' });
  res.end(JSON.stringify(body));
}
function readJson(req, limit = 16384) {
  return new Promise((resolve, reject) => {
    let size = 0; const chunks = [];
    req.on('data', (c) => { size += c.length; if (size > limit) { reject(new Error('body too large')); req.destroy(); } else chunks.push(c); });
    req.on('end', () => {
      if (!size) return resolve({});
      try { const v = JSON.parse(Buffer.concat(chunks).toString('utf8')); resolve(v && typeof v === 'object' ? v : {}); }
      catch { resolve({}); }
    });
    req.on('error', reject);
  });
}
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
