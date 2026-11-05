// XR-telemetry JavaScript SDK — browsers, WebXR, and Node >= 22 (global WebSocket).
//
// No dependencies, no knowledge of three.js or WebXR: pose() takes duck-typed
// {x,y,z} and {x,y,z,w} objects, so it works with three.js vectors, WebXR
// DOMPointReadOnly or plain objects.
//
//   import { XRTelemetry } from './xr-telemetry.js';
//   const tel = new XRTelemetry({ url: 'wss://host/ingest?session=pilot-3&token=…',
//                                 device: 'quest-07', meta: { kind: 'xr', frame: 'room' } });
//   tel.start();
//   // every render frame — decimated to rateHz, allocation-free until a sample is taken:
//   tel.pose(camera.position, camera.quaternion, { media_t: video.currentTime });
//   // whenever something happens:
//   tel.event('pinch', 'SAX');
//   // any other signal, any flat fields:
//   tel.send('controller', { trigger: 0.8, hand: 'right' });
//
// Wire protocol (spec §8): {hello, meta} once per connection, then batches
// {s: signal, b: [samples]}; every sample carries t (monotonic ms) and w (wall
// ms). The gateway answers with clock probes {ping}; we reply {pong, c} at once.
//
// Ported from Livestreamed_Immersive_Player/src/telemetry/Telemetry.js, where
// the fixed sampling grid and the prompt probe reply were measured on Quest 3.

export class XRTelemetry {
  /**
   * @param {object}  opt
   * @param {string}  opt.url                  ws(s)://…/ingest, with ?session= and ?token= as needed
   * @param {string} [opt.device]              stable source id (random if omitted)
   * @param {object} [opt.meta={}]             hello metadata: kind, platform, frame, participant, role…
   * @param {number} [opt.rateHz=20]           pose() rate, held on a fixed grid; 0 = every call
   * @param {number} [opt.flushMs=100]         batch window; <= 0 sends every sample at once
   * @param {number} [opt.maxQueue=240]        samples buffered while offline (oldest dropped)
   * @param {boolean}[opt.autoReconnect=true]
   */
  constructor({ url, device, meta = {}, rateHz = 20, flushMs = 100, maxQueue = 240, autoReconnect = true } = {}) {
    if (!url) throw new Error('XRTelemetry: `url` is required');
    this.url = url;
    this.device = device || ('dev-' + Math.random().toString(36).slice(2, 10));
    this.meta = meta;
    this._interval = rateHz > 0 ? 1000 / rateHz : 0;
    this._flushMs = flushMs;
    this._maxQueue = maxQueue;
    this._autoReconnect = autoReconnect;
    this._ws = null;
    this._queue = [];                 // [signal, sample]
    this._nextPose = 0;
    this._flushTimer = null;
    this._reconnectTimer = null;
    this._backoff = 500;
    this._stopped = true;
    this._onHide = () => this._flush();
  }

  /** Open the connection and start batching. Idempotent. */
  start() {
    if (!this._stopped) return;
    this._stopped = false;
    this._nextPose = 0;
    this._connect();
    if (this._flushMs > 0 && !this._flushTimer) this._flushTimer = setInterval(() => this._flush(), this._flushMs);
    if (typeof addEventListener === 'function') addEventListener('pagehide', this._onHide);
  }

  /** Flush, close, stop reconnecting. Idempotent. */
  stop() {
    if (this._stopped) return;
    this._stopped = true;
    if (typeof removeEventListener === 'function') removeEventListener('pagehide', this._onHide);
    clearInterval(this._flushTimer); this._flushTimer = null;
    clearTimeout(this._reconnectTimer); this._reconnectTimer = null;
    this._flush();
    if (this._ws) { try { this._ws.close(); } catch (_) { /* gone */ } this._ws = null; }
    this._queue.length = 0;
  }

  /**
   * Head pose, decimated to rateHz. Safe to call every frame.
   * @param {{x,y,z}}   position     metres
   * @param {{x,y,z,w}} orientation  unit quaternion
   * @param {object}   [extra]       more flat fields: media_t, gx/gy/gz + gaze_src:'eye', project fields…
   * @returns {boolean} whether a sample was taken
   */
  pose(position, orientation, extra) {
    const now = nowMs();
    if (now < this._nextPose - GRID_EPS_MS) return false;       // decimation gate (hot path)
    // Deadlines advance on a fixed grid, not from the sample taken: from a render
    // loop "last + interval" only fires on a frame boundary, and at 72 Hz a 20 Hz
    // target became a steady 18 Hz. On the grid the lateness does not accumulate.
    this._nextPose += this._interval;
    if (this._nextPose <= now) this._nextPose = now + this._interval;
    const s = {
      t: r(now, 1), w: Date.now(),
      px: r(position.x, 4), py: r(position.y, 4), pz: r(position.z, 4),
      qx: r(orientation.x, 4), qy: r(orientation.y, 4), qz: r(orientation.z, 4), qw: r(orientation.w, 4),
    };
    if (extra) Object.assign(s, extra);
    if (s.gx === undefined) {                                     // head-forward gaze: q applied to (0,0,-1)
      const { x, y, z, w } = orientation;
      s.gx = r(-2 * (x * z + w * y), 4); s.gy = r(2 * (w * x - y * z), 4); s.gz = r(2 * (x * x + y * y) - 1, 4);
      s.gaze_src = 'head';
    }
    this._push('pose', s);
    return true;
  }

  /** A discrete occurrence (spec §5.2). */
  event(name, value = null, extra) {
    this._push('event', { t: r(nowMs(), 1), w: Date.now(), name, ...(value !== null ? { value } : {}), ...extra });
  }

  /** Any signal with any flat fields; t and w are added. Not decimated. */
  send(signal, fields) {
    this._push(signal, { t: r(nowMs(), 1), w: Date.now(), ...fields });
  }

  /** Merge into the metadata and re-send the hello now (the gateway merges on device id). */
  setMeta(patch) {
    Object.assign(this.meta, patch);
    if (!this._stopped) this._sendRaw({ hello: this.device, meta: this.meta });
  }

  get connected() { return this._isOpen(); }

  // ── internals ──────────────────────────────────────────────────────────
  _push(signal, s) {
    this._queue.push([signal, s]);
    if (this._queue.length > this._maxQueue) this._queue.shift();   // keep the newest
    if (this._flushMs <= 0) this._flush();
  }

  _connect() {
    if (this._stopped) return;
    let ws;
    try { ws = new WebSocket(this.url); } catch (_) { return this._scheduleReconnect(); }
    this._ws = ws;
    ws.onopen = () => { this._backoff = 500; this._sendRaw({ hello: this.device, meta: this.meta }); };
    // Answer clock probes in the handler itself: anything done before replying
    // goes straight into the gateway's error bound.
    ws.onmessage = (ev) => {
      let m; try { m = JSON.parse(ev.data); } catch (_) { return; }
      if (m.ping !== undefined) this._sendRaw({ pong: m.ping, c: nowMs() });
    };
    ws.onclose = () => { if (this._ws === ws) { this._ws = null; this._scheduleReconnect(); } };
    ws.onerror = () => { try { ws.close(); } catch (_) { /* noop */ } };
  }

  _scheduleReconnect() {
    if (this._stopped || !this._autoReconnect || this._reconnectTimer) return;
    this._reconnectTimer = setTimeout(() => { this._reconnectTimer = null; this._connect(); }, this._backoff);
    this._backoff = Math.min(this._backoff * 2, 5000);
  }

  _flush() {
    if (!this._queue.length || !this._isOpen()) return;          // stay buffered until reconnect
    const bySignal = new Map();
    for (const [sig, s] of this._queue) {
      let b = bySignal.get(sig);
      if (!b) bySignal.set(sig, b = []);
      b.push(s);
    }
    this._queue = [];
    for (const [s, b] of bySignal) this._sendRaw({ s, b });
  }

  _sendRaw(obj) {
    if (!this._isOpen()) return false;
    try { this._ws.send(JSON.stringify(obj)); return true; } catch (_) { return false; }
  }

  _isOpen() { return !!this._ws && this._ws.readyState === 1; }
}

// Slack on the grid deadline: when the render rate divides rateHz exactly
// (90 Hz / 20 Hz) a frame can land a float hair early and lose a whole frame.
const GRID_EPS_MS = 1;

// Monotonic where it exists. `t` and the pong share it on purpose: the gateway
// maps this timeline onto its clock, and a wall clock stepped by NTP mid-session
// would move samples under an offset measured before the step.
function nowMs() { return typeof performance !== 'undefined' ? performance.now() : Date.now(); }
function r(n, d) { const f = 10 ** d; return Math.round(n * f) / f; }
