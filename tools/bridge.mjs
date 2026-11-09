// Bridge: a local Redis (e.g. the Physiological-Signals hub on a venue LAN)
// → a remote gateway (e.g. the cloud deployment), with no change to the
// producers writing to that Redis.
//
//   node tools/bridge.mjs --redis redis://hub.local:6379 \
//        --url wss://<host>/telemetry/ingest --token <INGEST_TOKEN>
//
//   --signals physio_metrics,engagement   only these signals (default: all)
//   --namespace amplify                   key prefix on the local Redis
//   --no-streams                          Pub/Sub only, do not read streams
//   --crowd-device engagement-pc          device name for the crowd score (below)
//   --no-crowd                            do not forward the crowd score
//
// Crowd engagement. Physiological-Signals' engagement GUI publishes the average
// over everyone it scores on the plain channel `engagement_score`, as a bare
// number ("0.6123"), outside device:*:*. The bridge forwards it as signal
// `engagement` of one device (--crowd-device), field `engagement`, with
// `crowd: true`, so it sits next to the per-person device:{serial}:engagement.
// It counts as the `engagement` signal for --signals.
//
// Pub/Sub and streams. Physiological-Signals PUBLISHes and XADDs most signals
// to the same key, but some only to the stream (raw_sensors). The bridge
// forwards Pub/Sub, which keeps the producer's types (booleans, objects), and
// reads a stream only for keys it has never seen on Pub/Sub, so nothing is sent
// twice. A sample whose `timestamp` is not newer than the last one forwarded for
// its key is dropped as well, which covers the moment a key is first seen.
// Streams are read from the moment the bridge starts; new keys are found by a
// scan every few seconds.
//
// Every local device becomes one producer on the gateway, so it gets the
// reference clock: srv_ts is the moment the bridge read the sample, mapped onto
// the gateway's clock (± the LAN hop and the bridge's own error). The device's
// own `timestamp` is kept as is.
//
// Samples that already carry srv_ts came through a gateway (headsets) and are
// not forwarded: that keeps a bridge pointed at its own Redis from looping, and
// headsets on the LAN should talk to the gateway directly anyway.
//
// Needs `npm install` in gateway/ (ioredis) — like tools/simulate.mjs.
import { createRequire } from 'module';
import { XRTelemetry } from '../sdk/js/xr-telemetry.js';

const args = {};
const argv = process.argv.slice(2);
for (let i = 0; i < argv.length; i++) if (argv[i].startsWith('--')) args[argv[i].slice(2)] = argv[i + 1]?.startsWith('--') ? true : argv[++i];
if (!args.url) {
  console.error('usage: node tools/bridge.mjs --redis redis://host:6379 --url wss://host/telemetry/ingest --token <INGEST_TOKEN> [--signals a,b] [--namespace ns] [--no-streams] [--crowd-device name | --no-crowd]');
  process.exit(1);
}
const require = createRequire(new URL('../gateway/', import.meta.url));
const Redis = require('ioredis');

const ns = args.namespace ? `${String(args.namespace).replace(/:+$/, '')}:` : '';
const only = typeof args.signals === 'string' ? new Set(args.signals.split(',')) : null;
const RESERVED = new Set(['meta', 'alive', 'participant']);
const target = args.url + (args.token ? (args.url.includes('?') ? '&' : '?') + 'token=' + encodeURIComponent(args.token) : '');
const chanRe = new RegExp(`^${ns.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}device:([^:]+):([^:]+)$`);

const redisUrl = args.redis || 'redis://127.0.0.1:6379';
const useStreams = !args['no-streams'];
const CROWD_CHANNEL = `${ns}engagement_score`;
const crowdDevice = typeof args['crowd-device'] === 'string' ? args['crowd-device'] : 'engagement-pc';
const useCrowd = !args['no-crowd'] && (!only || only.has('engagement'));
const redis = new Redis(redisUrl);
const sub = new Redis(redisUrl);
const xr = useStreams ? new Redis(redisUrl) : null;   // blocking XREAD gets its own connection
for (const c of [redis, sub, xr]) c?.on('error', (e) => console.error('[bridge] redis:', e.message));

const producers = new Map();       // device -> XRTelemetry
const pending = new Map();         // device -> Promise<XRTelemetry>, while its meta is read
const pubsubKeys = new Set();      // keys seen on Pub/Sub: their streams are not read
const lastTs = new Map();          // key -> last forwarded `timestamp` (s)
const streamIds = new Map();       // stream key -> last id read (keys never seen on Pub/Sub)
let forwarded = 0, fromStreams = 0, skipped = 0, dupes = 0;

// Producers publish several channels per device at once (Physiological-Signals:
// physio_metrics, hr_filtered, eda_filtered in the same tick). The pending map
// makes them share one producer instead of each opening its own connection.
function producerFor(device) {
  const p = producers.get(device);
  if (p) return p;
  let q = pending.get(device);
  if (!q) {
    q = (async () => {
      // Carry the device's registry entry and participant over, so the remote
      // side knows what it is and who wears it.
      const [meta, participant] = await Promise.all([
        redis.hgetall(`${ns}device:${device}:meta`).catch(() => ({})),
        redis.get(`${ns}device:${device}:participant`).catch(() => null),
      ]);
      const np = new XRTelemetry({ url: target, device, rateHz: 0, flushMs: 200, maxQueue: 2000,
        meta: { kind: device === crowdDevice && useCrowd ? 'engagement' : 'wearable', ...meta, ...(participant ? { participant } : {}), bridged: true } });
      np.start();
      producers.set(device, np);
      pending.delete(device);
      console.log(`[bridge] ${device} → ${args.url}`);
      return np;
    })();
    pending.set(device, q);
  }
  return q;
}

/** device:{device}:{signal} → [device, signal], or null when not forwarded. */
function parseKey(key) {
  const m = chanRe.exec(key);
  if (!m || RESERVED.has(m[2]) || (only && !only.has(m[2]))) return null;
  return [m[1], m[2]];
}

// The producer's capture time in seconds. Physiological-Signals' real publisher
// sends ISO text (datetime.now().isoformat(), local time, no zone), its
// simulator epoch seconds; the engagement GUI ISO with +00:00. ISO without a
// zone is read as local time on the bridge's machine (the venue's).
const ISO_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}/;
function captureSeconds(ts) {
  if (typeof ts === 'number') return Number.isFinite(ts) ? ts : null;
  if (typeof ts === 'string' && ISO_RE.test(ts)) {
    const ms = Date.parse(ts);
    return Number.isFinite(ms) ? ms / 1000 : null;
  }
  return null;
}

async function forward(key, device, signal, f) {
  if (f.srv_ts != null) { skipped++; return false; }
  const { timestamp: rawTs, device: _d, ...fields } = f;
  const timestamp = captureSeconds(rawTs);
  if (timestamp !== null) {
    if (timestamp <= (lastTs.get(key) ?? -Infinity)) { dupes++; return false; }
    lastTs.set(key, timestamp);
  }
  const p = await producerFor(device);
  // w = the device's own capture time, so the gateway keeps it as `timestamp`.
  p.send(signal, { ...fields, ...(timestamp !== null ? { w: timestamp * 1000 } : {}) });
  forwarded++;
  return true;
}

await sub.psubscribe(`${ns}device:*:*`);
sub.on('pmessage', async (_pat, channel, payload) => {
  const k = parseKey(channel);
  if (!k) return;
  let f; try { f = JSON.parse(payload); } catch { return; }
  if (!f || typeof f !== 'object' || Array.isArray(f)) return;
  pubsubKeys.add(channel);
  streamIds.delete(channel);
  await forward(channel, k[0], k[1], f);
});

// ── crowd engagement: `engagement_score`, a bare number ─────────────────
let crowdForwarded = 0;
if (useCrowd) {
  await sub.subscribe(CROWD_CHANNEL);
  sub.on('message', async (channel, payload) => {
    if (channel !== CROWD_CHANNEL) return;
    const v = Number(payload);
    if (!Number.isFinite(v)) return;
    if (await forward(CROWD_CHANNEL, crowdDevice, 'engagement', { engagement: v, crowd: true })) crowdForwarded++;
  });
}

// ── streams: keys that are never PUBLISHed ──────────────────────────────
let running = true, startId = '0-0';

// Stream values are strings: numbers back to numbers, the rest as is (objects
// stay JSON text, as the archiver keeps them).
const NUM_RE = /^-?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$/;
function fromStream(flat) {
  const f = {};
  for (let i = 0; i < flat.length; i += 2) {
    const k = flat[i], v = flat[i + 1];
    f[k] = k !== 'device' && NUM_RE.test(v) ? Number(v) : v;
  }
  return f;
}

// SCAN … TYPE needs Redis 6; the Physiological-Signals Windows bundle ships
// 5.0, so the type of each new key is asked once instead.
const notStreams = new Set();
async function discover() {
  let cursor = '0';
  do {
    const [next, keys] = await redis.scan(cursor, 'MATCH', `${ns}device:*:*`, 'COUNT', 500);
    cursor = next;
    const fresh = keys.filter((k) => !streamIds.has(k) && !pubsubKeys.has(k) && !notStreams.has(k) && parseKey(k));
    if (!fresh.length) continue;
    const types = await redis.pipeline(fresh.map((k) => ['type', k])).exec();
    fresh.forEach((k, i) => {
      if (types[i][1] !== 'stream') notStreams.add(k);
      else if (!pubsubKeys.has(k)) streamIds.set(k, startId);
    });
  } while (cursor !== '0');
}

async function readStreams() {
  while (running) {
    if (!streamIds.size) { await new Promise((r) => setTimeout(r, 1000)); continue; }
    const keys = [...streamIds.keys()];
    let res;
    try {
      res = await xr.xread('COUNT', 500, 'BLOCK', 1000, 'STREAMS', ...keys, ...keys.map((k) => streamIds.get(k)));
    } catch (e) {
      if (running) { console.error('[bridge] xread:', e.message); await new Promise((r) => setTimeout(r, 1000)); }
      continue;
    }
    for (const [key, entries] of res || []) {
      const [device, signal] = parseKey(key);
      for (const [id, flat] of entries) {
        if (!streamIds.has(key)) break;            // seen on Pub/Sub meanwhile: stop reading it
        streamIds.set(key, id);
        if (await forward(key, device, signal, fromStream(flat))) fromStreams++;
      }
    }
  }
}

if (useStreams) {
  // Start at the Redis server's "now": the stream's past is not replayed.
  const [s, us] = await redis.time();
  startId = `${Number(s) * 1000 + Math.floor(Number(us) / 1000)}-0`;
  const scan = () => discover().catch((e) => console.error('[bridge] scan:', e.message));
  await scan();
  setInterval(scan, 5000).unref();
  readStreams();
}

console.log(`[bridge] ${redisUrl}${ns ? ' ns=' + ns : ''} device:*:* (Pub/Sub${useStreams ? ' + streams' : ''})${useCrowd ? ` + ${CROWD_CHANNEL} as ${crowdDevice}:engagement` : ''} → ${args.url}${only ? ' signals=' + [...only] : ''}`);
setInterval(() => {
  const up = [...producers.values()].filter((p) => p.connected).length;
  const streamSignals = [...new Set([...streamIds.keys()].map((k) => parseKey(k)?.[1]))].join(',') || '-';
  console.log(`[bridge] ${forwarded} forwarded (${fromStreams} from streams: ${streamSignals}${useCrowd ? `; ${crowdForwarded} crowd` : ''}), ` +
    `${dupes} duplicates dropped, ${skipped} skipped (already through a gateway), ${up}/${producers.size} devices connected`);
}, 30000).unref();

const stop = () => {
  running = false;
  for (const p of producers.values()) p.stop();
  sub.disconnect(); redis.disconnect(); xr?.disconnect();
  setTimeout(() => process.exit(0), 300);
};
process.on('SIGINT', stop);
process.on('SIGTERM', stop);
