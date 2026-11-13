'use strict';
// The Livestreamed Immersive Player's original wire format, accepted as is so
// the player can point at this gateway with no change (spec §11):
//
//   { b: [ { t, w, mt, p:[x,y,z], q:[x,y,z,w], g?:[x,y,z], z, f, r? } ] }   (no `s`)
//
// mapped to a spec `pose` sample. `z` (zoom), `f` (focused source), `r` (the
// emoji reaction chosen in the XR menu, an id such as "clap", or null) and the
// hello's `rec` (voice recording id) are player fields and pass through.

function isLegacyBatch(msg) {
  return msg.s === undefined && Array.isArray(msg.b) && msg.b.length > 0 && Array.isArray(msg.b[0].q);
}

function legacyToPose(s, meta = {}) {
  const out = { t: s.t, w: s.w };
  if (Array.isArray(s.p)) [out.px, out.py, out.pz] = s.p;
  if (Array.isArray(s.q)) [out.qx, out.qy, out.qz, out.qw] = s.q;
  const g = Array.isArray(s.g) ? s.g : forwardFromQuat(s.q);
  if (g) { [out.gx, out.gy, out.gz] = g; out.gaze_src = Array.isArray(s.g) ? 'eye' : 'head'; }
  if (s.mt !== undefined) out.media_t = s.mt;
  if (s.z !== undefined) out.z = s.z;
  if (s.f !== undefined) out.f = s.f;
  if (s.r != null) out.r = String(s.r);
  out.frame = meta.frame || 'local-floor';
  if (meta.input) out.input = meta.input;
  if (meta.rec) out.rec = meta.rec;
  return out;
}

// Head-forward = q applied to (0,0,-1): gaze when there is no eye tracking.
function forwardFromQuat(q) {
  if (!Array.isArray(q)) return null;
  const [x, y, z, w] = q;
  const r = (n) => Math.round(n * 1e4) / 1e4;
  return [r(-2 * (x * z + w * y)), r(2 * (w * x - y * z)), r(2 * (x * x + y * y) - 1)];
}

module.exports = { isLegacyBatch, legacyToPose, forwardFromQuat };
