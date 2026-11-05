'use strict';
// Sample format helpers (spec §3-§4).

const NAME_RE = /^[A-Za-z0-9_.-]{1,64}$/;

/** Redis key / channel of one signal of one source, with the optional namespace. */
function signalKey(ns, src, signal) { return `${ns}device:${src}:${signal}`; }

/**
 * Flat field map for XADD, the way Physiological-Signals' _safe_xadd does it:
 * null/undefined dropped, booleans as 0/1, objects/arrays JSON-encoded,
 * everything else as is. Returns the flat argument list ioredis wants.
 */
function toStreamFields(sample) {
  const out = [];
  for (const [k, v] of Object.entries(sample)) {
    if (v === null || v === undefined) continue;
    if (typeof v === 'boolean') out.push(k, v ? '1' : '0');
    else if (typeof v === 'object') out.push(k, JSON.stringify(v));
    else out.push(k, String(v));
  }
  return out;
}

/**
 * Wire sample from a WebSocket producer -> spec sample.
 *   t  monotonic ms (source)   -> t_mono, and srv_ts through the clock
 *   w  wall ms (source)        -> timestamp (s)
 * Every other field passes through untouched (spec §4.2: unknown fields are
 * preserved), so a project adds its own without touching the gateway.
 */
function fromWire(raw, { device, session, participant, frame, clock, seq }) {
  const { t, w, ...fields } = raw;
  const srv = clock.toServer(t);
  const s = {
    device,
    timestamp: typeof w === 'number' ? round(w / 1000, 4) : round(Date.now() / 1000, 4),
    srv_ts: srv != null ? round(srv / 1000, 4) : null,
    srv_err_ms: srv != null ? round(clock.errorMs, 2) : null,
    t_mono: typeof t === 'number' ? t : null,
    seq,
    session: session || null,
    participant: participant || null,
    ...fields,
  };
  if (frame && s.frame === undefined && 'qw' in fields) s.frame = frame;   // poses inherit the hello's frame
  return s;
}

function round(n, d) { const f = 10 ** d; return Math.round(n * f) / f; }

module.exports = { NAME_RE, signalKey, toStreamFields, fromWire };
