'use strict';
// Reference clock (spec §6): one source's monotonic clock expressed on ours,
// Cristian's algorithm with the minimum-round-trip filter NTP uses.
//
// We send `ping` at t1 (our clock), the source answers its own reading `c`, the
// answer lands at t2. Assuming a symmetric trip, it read `c` at t1 + rtt/2, so
// offset = t1 + rtt/2 - c. That symmetry is the whole error, which is why the
// smallest round trip of a sliding window wins: a fast exchange had little
// queueing to be lopsided about, and the window follows oscillator drift
// instead of clinging to one lucky early packet.
//
// Ported from Livestreamed_Immersive_Player/telemetry/relay.js (ClockSync).

class ClockSync {
  constructor(window = 16) { this._w = window; this._obs = []; this._best = null; }

  observe(t1, c, t2) {
    if (typeof c !== 'number' || !isFinite(c)) return;
    const rtt = t2 - t1;
    if (rtt < 0) return;                         // our clock stepped; drop it
    this._obs.push({ rtt, offset: (t1 + rtt / 2) - c });
    if (this._obs.length > this._w) this._obs.shift();
    this._best = this._obs.reduce((a, b) => (b.rtt < a.rtt ? b : a));
  }

  /** Source monotonic reading (ms) -> our wall clock (ms). Null until the first exchange. */
  toServer(t) {
    if (this._best == null || typeof t !== 'number' || !isFinite(t)) return null;
    return t + this._best.offset;
  }

  /** Half the best round trip (ms): the bound on how wrong toServer() can be. */
  get errorMs() { return this._best ? this._best.rtt / 2 : null; }
}

module.exports = { ClockSync };
