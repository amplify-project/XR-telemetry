'use strict';
// End-to-end: gateway + a real Redis, no headset.
//
//   npm test                         # starts a throwaway Redis in Docker (port 16379)
//   REDIS_URL=redis://host:6379 npm test   # or use one you have (keys under xrtest:*)
//
// Checks the spec, not the implementation: what lands in Redis, and what a
// WebSocket consumer and a plain Redis subscriber see — including a producer
// that writes to Redis itself the way Physiological-Signals does.

const { execSync } = require('child_process');
const Redis = require('ioredis');
const { WebSocket } = require('ws');
const { createGateway } = require('../src/gateway');

const NS = 'xrtest';
const IN = 'in-secret', OUT = 'out-secret';
let failed = false, container = null, gw, redis;

const wait = (ms) => new Promise((r) => setTimeout(r, ms));
const ok = (m) => console.log('  ✓ ' + m);
const fail = (m) => { console.error('  ✗ ' + m); failed = true; };
const check = (cond, m, detail) => (cond ? ok(m) : fail(m + (detail !== undefined ? ': ' + JSON.stringify(detail) : '')));

async function startRedis() {
  if (process.env.REDIS_URL) return process.env.REDIS_URL;
  container = 'xrtel-test-redis';
  execSync(`docker rm -f ${container} >/dev/null 2>&1 || true; docker run -d --rm --name ${container} -p 16379:6379 redis:7-alpine >/dev/null`);
  const url = 'redis://127.0.0.1:16379';
  const r = new Redis(url, { lazyConnect: true, retryStrategy: () => 200 });
  for (let i = 0; i < 50; i++) { try { await r.connect(); await r.ping(); break; } catch { await wait(200); } }
  r.disconnect();
  return url;
}

function status(url) {
  return new Promise((res) => {
    const ws = new WebSocket(url);
    ws.on('open', () => { ws.close(); res(101); });
    ws.on('unexpected-response', (_q, r) => res(r.statusCode));
    ws.on('error', () => res(-1));
  });
}

// A WebSocket client that answers clock probes with a deliberately skewed clock.
function client(url, skewMs = 0) {
  return new Promise((res, rej) => {
    const ws = new WebSocket(url);
    ws.events = [];
    ws.now = () => Date.now() - skewMs;
    ws.on('message', (raw) => {
      const m = JSON.parse(raw);
      if (m.ping !== undefined) return ws.send(JSON.stringify({ pong: m.ping, c: ws.now() }));
      ws.events.push(m);
    });
    ws.on('open', () => res(ws));
    ws.on('error', rej);
  });
}

(async () => {
  const redisUrl = await startRedis();
  redis = new Redis(redisUrl);
  const stale = await redis.keys(`${NS}:*`);
  if (stale.length) await redis.del(...stale);

  gw = createGateway({ redisUrl, namespace: NS, port: 0, ingestToken: IN, consumeToken: OUT, log: false });
  await gw.ready;
  await wait(200);                                       // psubscribe settles
  const B = `ws://127.0.0.1:${gw.server.address().port}`;

  // ── auth ──
  check(await status(`${B}/ingest`) === 401, 'ingest without token → 401');
  check(await status(`${B}/consume?token=${IN}`) === 401, 'consume with the ingest token → 401');
  check(await status(`${B}/ingest?token=${IN}&session=a%2Fb`) === 400, 'bad session name → 400');

  // A plain Redis subscriber, standing in for a Python or Unity consumer.
  const rawSub = new Redis(redisUrl);
  const rawSeen = [];
  await rawSub.psubscribe(`${NS}:device:*:*`);
  rawSub.on('pmessage', (_p, ch, msg) => rawSeen.push({ ch, msg: JSON.parse(msg) }));

  const consAll = await client(`${B}/consume?token=${OUT}`);
  const consA = await client(`${B}/consume?token=${OUT}&session=rehearsal-a`);
  const consPose = await client(`${B}/consume?token=${OUT}&signals=pose`);
  await wait(100);

  // ── spec producer, clock 5 h off ──
  const SKEW = 5 * 3600 * 1000;
  const quest = await client(`${B}/ingest?token=${IN}&session=rehearsal-a`, SKEW);
  quest.send(JSON.stringify({ hello: 'quest-07', meta: { kind: 'xr', platform: 'quest3', frame: 'room', participant: 'p12', role: 'xr' } }));
  await wait(150);                                       // first clock exchange
  const sentAt = Date.now();
  quest.send(JSON.stringify({ s: 'pose', b: [
    { t: quest.now(), w: quest.now() + SKEW, px: 1, py: 1.6, pz: -2, qx: 0, qy: 0, qz: 0, qw: 1, gx: 0, gy: 0, gz: -1, gaze_src: 'head' },
  ] }));
  quest.send(JSON.stringify({ s: 'event', b: [{ t: quest.now(), w: Date.now(), name: 'pinch', value: 'SAX' }] }));
  await wait(200);

  const [[, f]] = await redis.xrange(`${NS}:device:quest-07:pose`, '-', '+');
  const row = {}; for (let i = 0; i < f.length; i += 2) row[f[i]] = f[i + 1];
  check(row.device === 'quest-07' && row.px === '1' && row.qw === '1' && row.frame === 'room', 'pose landed in the Redis stream, flat', row);
  check(row.session === 'rehearsal-a' && row.participant === 'p12', 'stream row carries session and participant', row);
  const srvMs = +row.srv_ts * 1000;
  check(Math.abs(srvMs - sentAt) < 50, `srv_ts undoes a 5 h clock skew (off by ${Math.round(srvMs - sentAt)} ms, ±${row.srv_err_ms} ms)`);
  check(!!rawSeen.find((e) => e.ch === `${NS}:device:quest-07:pose` && e.msg.px === 1), 'a plain Redis subscriber got the pose (PUBLISH)');
  check((await redis.hget(`${NS}:device:quest-07:meta`, 'platform')) === 'quest3', 'device meta registered');
  check((await redis.hget(`${NS}:participant:p12`, 'xr')) === 'quest-07' &&
        (await redis.get(`${NS}:device:quest-07:participant`)) === 'p12', 'participant mapping written both ways');
  check((await redis.ttl(`${NS}:device:quest-07:alive`)) > 0, 'presence key alive with a TTL');

  // ── a producer that writes to Redis itself, as Physiological-Signals does ──
  const physio = { device: 'MD-V5-0000334', timestamp: Date.now() / 1000, hr: 72.5, eda_z: 0.4, calibrating: false, quality: { hr: 'ok' } };
  await redis.publish(`${NS}:device:MD-V5-0000334:physio_metrics`, JSON.stringify(physio));
  await redis.xadd(`${NS}:device:MD-V5-0000334:physio_metrics`, 'MAXLEN', '~', 20000, '*', 'device', physio.device, 'timestamp', physio.timestamp, 'hr', 72.5);
  await wait(150);

  const got = (c, sig, dev) => c.events.find((e) => e.type === 'sample' && e.signal === sig && e.device === dev);
  check(!!got(consAll, 'pose', 'quest-07'), 'WS consumer got the headset pose');
  check(got(consAll, 'physio_metrics', 'MD-V5-0000334')?.hr === 72.5, 'WS consumer got the direct-to-Redis physio sample');
  check(!!got(consAll, 'event', 'quest-07'), 'WS consumer got the event signal');
  check(!!got(consPose, 'pose', 'quest-07') && !got(consPose, 'physio_metrics', 'MD-V5-0000334') && !got(consPose, 'event', 'quest-07'),
        'signals=pose filters to poses only');

  // ── sessions ──
  const other = await client(`${B}/ingest?token=${IN}&session=rehearsal-b`);
  other.send(JSON.stringify({ hello: 'quest-99', meta: { kind: 'xr' } }));
  other.send(JSON.stringify({ s: 'pose', b: [{ t: 1, w: Date.now(), px: 0, py: 1.6, pz: 0, qx: 0, qy: 0, qz: 0, qw: 1 }] }));
  await wait(150);
  check(!got(consA, 'pose', 'quest-99') && !!got(consA, 'pose', 'quest-07'), 'session A consumer does not see session B');
  check(!!got(consA, 'physio_metrics', 'MD-V5-0000334'), 'samples without a session reach every session (spec §8)');

  // ── the Livestreamed player's original format, unchanged ──
  const player = await client(`${B}/ingest?token=${IN}`);
  player.send(JSON.stringify({ hello: 'lip-player', meta: { mode: 'vr', frame: 'local-floor', rec: 'take-1' } }));
  player.send(JSON.stringify({ b: [{ t: 1, w: Date.now(), mt: 12.5, p: [0, 1.6, 0], q: [0, 0, 0, 1], z: 0.3, f: 2 }] }));
  await wait(150);
  const lp = got(consAll, 'pose', 'lip-player');
  check(lp && lp.py === 1.6 && lp.qw === 1 && lp.gz === -1 && lp.gaze_src === 'head' && lp.media_t === 12.5 && lp.z === 0.3 && lp.f === 2 && lp.rec === 'take-1',
        'legacy player batch mapped to a spec pose (gaze derived, z/f/rec kept)', lp);

  // ── snapshot for a late consumer ──
  const late = await client(`${B}/consume?token=${OUT}`);
  await wait(100);
  const snap = late.events.find((e) => e.type === 'snapshot');
  const ids = (snap?.samples || []).map((s) => `${s.device}/${s.signal}`).sort();
  check(ids.includes('quest-07/pose') && ids.includes('MD-V5-0000334/physio_metrics'), 'late consumer gets a snapshot of the latest samples', ids);

  // ── leave ──
  quest.close();
  await wait(150);
  check(consAll.events.some((e) => e.type === 'leave' && e.device === 'quest-07'), 'consumers told when a producer leaves');
  check((await redis.exists(`${NS}:device:quest-07:alive`)) === 0, 'presence key removed on leave');

  const h = gw.health();
  check(h.ok && h.namespace === NS, 'health reports Redis ready', h);

  for (const ws of [consAll, consA, consPose, other, player, late]) ws.close();
  rawSub.disconnect();
  await finish();
})().catch(async (e) => { console.error(e); failed = true; await finish(); });

async function finish() {
  try { const k = await redis.keys(`${NS}:*`); if (k.length) await redis.del(...k); } catch {}
  try { redis.disconnect(); } catch {}
  try { await gw?.close(); } catch {}
  if (container) execSync(`docker rm -f ${container} >/dev/null 2>&1 || true`);
  console.log(failed ? '\nE2E FAILED' : '\nE2E PASSED');
  process.exit(failed ? 1 : 0);
}
