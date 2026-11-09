// Bridge: a local Redis (e.g. the Physiological-Signals hub on a venue LAN)
// → a remote gateway (e.g. the cloud deployment), with no change to the
// producers writing to that Redis.
//
//   node tools/bridge.mjs --redis redis://hub.local:6379 \
//        --url wss://<host>/telemetry/ingest --token <INGEST_TOKEN>
//
//   --signals physio_metrics,engagement   only these signals (default: all)
//   --namespace amplify                   key prefix on the local Redis
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
  console.error('usage: node tools/bridge.mjs --redis redis://host:6379 --url wss://host/telemetry/ingest --token <INGEST_TOKEN> [--signals a,b] [--namespace ns]');
  process.exit(1);
}
const require = createRequire(new URL('../gateway/', import.meta.url));
const Redis = require('ioredis');

const ns = args.namespace ? `${String(args.namespace).replace(/:+$/, '')}:` : '';
const only = typeof args.signals === 'string' ? new Set(args.signals.split(',')) : null;
const RESERVED = new Set(['meta', 'alive', 'participant']);
const target = args.url + (args.token ? (args.url.includes('?') ? '&' : '?') + 'token=' + encodeURIComponent(args.token) : '');
const chanRe = new RegExp(`^${ns.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}device:([^:]+):([^:]+)$`);

const redis = new Redis(args.redis || 'redis://127.0.0.1:6379');
const sub = new Redis(args.redis || 'redis://127.0.0.1:6379');
for (const c of [redis, sub]) c.on('error', (e) => console.error('[bridge] redis:', e.message));

const producers = new Map();       // device -> XRTelemetry
const pending = new Map();         // device -> Promise<XRTelemetry>, while its meta is read
let forwarded = 0, skipped = 0;

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
        meta: { kind: 'wearable', ...meta, ...(participant ? { participant } : {}), bridged: true } });
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

await sub.psubscribe(`${ns}device:*:*`);
sub.on('pmessage', async (_pat, channel, payload) => {
  const m = chanRe.exec(channel);
  if (!m || RESERVED.has(m[2]) || (only && !only.has(m[2]))) return;
  const [, device, signal] = m;
  let f; try { f = JSON.parse(payload); } catch { return; }
  if (!f || typeof f !== 'object' || Array.isArray(f) || f.srv_ts != null) { skipped++; return; }
  const { timestamp, device: _d, ...fields } = f;
  const p = await producerFor(device);
  // w = the device's own capture time, so the gateway keeps it as `timestamp`.
  p.send(signal, { ...fields, ...(typeof timestamp === 'number' ? { w: timestamp * 1000 } : {}) });
  forwarded++;
});

console.log(`[bridge] ${args.redis || 'redis://127.0.0.1:6379'}${ns ? ' ns=' + ns : ''} device:*:* → ${args.url}${only ? ' signals=' + [...only] : ''}`);
setInterval(() => {
  const up = [...producers.values()].filter((p) => p.connected).length;
  console.log(`[bridge] ${forwarded} forwarded, ${skipped} skipped (already through a gateway), ${up}/${producers.size} devices connected`);
}, 30000).unref();

const stop = () => { for (const p of producers.values()) p.stop(); sub.disconnect(); redis.disconnect(); setTimeout(() => process.exit(0), 300); };
process.on('SIGINT', stop);
process.on('SIGTERM', stop);
