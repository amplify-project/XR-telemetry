// Fake producers, to try the whole chain without a headset or a wristband.
//
//   node tools/simulate.mjs                         # 3 headsets → ws://127.0.0.1:8090/ingest
//   node tools/simulate.mjs --headsets 5 --physio 2               # into the session started with `xrtel start`
//   node tools/simulate.mjs --session pilot-3                       # or a session of their own
//   node tools/simulate.mjs --url wss://host/ingest --token … --headsets 2
//
// Headsets go through the gateway with the JS SDK (pose at 20 Hz, an event now
// and then). --physio N writes fake wearables straight to Redis the way
// Physiological-Signals does (PUBLISH + XADD device:{serial}:physio_metrics),
// so both paths meet in the same consumers. Needs `npm install` in gateway/.
import { createRequire } from 'module';
import { XRTelemetry } from '../sdk/js/xr-telemetry.js';

const args = Object.fromEntries(process.argv.slice(2).join(' ').split('--').filter(Boolean)
  .map((a) => { const [k, ...v] = a.trim().split(/\s+/); return [k, v.join(' ') || true]; }));
const N = +(args.headsets ?? 3), P = +(args.physio ?? 0);
const session = typeof args.session === 'string' ? args.session : null;   // none: the operator's session
let url = args.url || 'ws://127.0.0.1:8090/ingest';
const q = new URLSearchParams({ ...(session ? { session } : {}), ...(args.token ? { token: args.token } : {}) });
url += (url.includes('?') ? '&' : '?') + q;

const headsets = [];
for (let i = 0; i < N; i++) {
  const t = new XRTelemetry({ url, device: `sim-quest-${i + 1}`,
    meta: { kind: 'xr', platform: 'simulator', frame: 'room', participant: `p${i + 1}`, role: 'xr' } });
  t.start();
  headsets.push({ t, seat: [(i - (N - 1) / 2) * 1.2, 1.6, 2], phase: Math.random() * 6 });
}
const t0 = Date.now();
setInterval(() => {                                   // 72 Hz "render loop"
  const s = (Date.now() - t0) / 1000;
  for (const h of headsets) {
    const yaw = 0.6 * Math.sin(s * 0.4 + h.phase);    // looking around
    h.t.pose({ x: h.seat[0], y: h.seat[1] + 0.02 * Math.sin(s * 2 + h.phase), z: h.seat[2] },
             { x: 0, y: Math.sin(yaw / 2), z: 0, w: Math.cos(yaw / 2) }, { media_t: Math.round(s * 1000) / 1000 });
  }
}, 1000 / 72);
setInterval(() => { const h = headsets[Math.floor(Math.random() * N)]; h?.t.event('pinch', ['SAX', 'KEY', 'DR'][Math.floor(Math.random() * 3)]); }, 4000);

if (P > 0) {
  const Redis = createRequire(import.meta.url)('../gateway/node_modules/ioredis');
  const redis = new Redis(args.redis || process.env.REDIS_URL || 'redis://127.0.0.1:6379');
  const ns = args.namespace ? args.namespace + ':' : '';
  setInterval(() => {
    for (let i = 0; i < P; i++) {
      const serial = `MD-SIM-${String(i + 1).padStart(7, '0')}`;
      const m = { device: serial, timestamp: Date.now() / 1000, hr: 70 + 8 * Math.sin(Date.now() / 7000 + i), eda_z: Math.sin(Date.now() / 5000 + i), calibrating: 0 };
      const key = `${ns}device:${serial}:physio_metrics`;
      redis.publish(key, JSON.stringify(m));
      redis.xadd(key, 'MAXLEN', '~', 20000, '*', ...Object.entries(m).flat().map(String));
    }
  }, 1000);
}
console.log(`[sim] ${N} headsets → ${url.replace(/token=[^&]*/, 'token=…')}${P ? `, ${P} wearables → Redis` : ''}. Ctrl-C to stop.`);
