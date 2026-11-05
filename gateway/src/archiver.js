'use strict';
// XR-telemetry archiver (spec §9).
//
//   Redis streams device:*:*  ──XREADGROUP xrtel-archiver──▶  SQLite (one long table)
//                                                              └─▶ HTTP: /sessions, /sessions/{id}, /export
//
// Redis keeps only the last ~20 000 samples of each stream (about 16 min of
// pose at 20 Hz). The archiver is what makes a test durable: it reads every
// stream through a consumer group, so a restart resumes where it stopped and a
// sample is acknowledged only once it is committed.
//
// Its HTTP API has no auth of its own: it is meant to sit on a private network
// behind the gateway, which checks the consume token and proxies to it.

const http = require('http');
const Redis = require('ioredis');
const { DatabaseSync } = require('node:sqlite');

const RESERVED = new Set(['meta', 'alive', 'participant']);
// Fields kept as text even when they look numeric (ids, names).
const TEXT_FIELDS = new Set(['device', 'session', 'participant', 'name', 'frame', 'gaze_src', 'input', 'rec']);
// Leading CSV columns, in this order when present; the rest follow alphabetically.
const LEAD = ['ts', 'signal', 'device', 'session', 'participant', 'srv_ts', 'srv_err_ms', 'timestamp',
  't_mono', 'seq', 'media_t', 'name', 'value'];
const NO_SESSION = '_none';
const NAME_RE = /^[A-Za-z0-9_.-]{1,64}$/;

/**
 * @param {object} opt
 * @param {string} [opt.redisUrl='redis://127.0.0.1:6379']
 * @param {string} [opt.namespace='']
 * @param {string} [opt.dbPath='xrtel.sqlite']   ':memory:' for tests
 * @param {number} [opt.port=8091]               HTTP API; null = no server
 * @param {string} [opt.group='xrtel-archiver']
 * @param {string} [opt.consumer='archiver-1']
 * @param {number} [opt.scanMs=3000]             how often new streams are looked for
 * @param {number} [opt.liveWindowMs=60000]      a sample without a session that is newer than
 *                                               this is filed under session:current; older ones
 *                                               (history read at first start) under '_none'
 * @param {number} [opt.retentionDays=30]       delete sessions whose last sample is older; 0 = keep forever
 * @param {number} [opt.maxMb=0]                 also delete the oldest finished sessions while the
 *                                               database is larger than this; 0 = no size limit
 * @param {number} [opt.purgeEveryMs=3600000]    how often both limits are applied
 * @param {boolean}[opt.log=true]
 */
function createArchiver({
  redisUrl = 'redis://127.0.0.1:6379', namespace = '', dbPath = 'xrtel.sqlite', port = 8091,
  group = 'xrtel-archiver', consumer = 'archiver-1', scanMs = 3000, liveWindowMs = 60000,
  retentionDays = 30, maxMb = 0, purgeEveryMs = 3600000, log = true,
} = {}) {
  const ns = namespace ? `${namespace.replace(/:+$/, '')}:` : '';
  const say = (...a) => { if (log) console.log('[xrtel-archiver]', ...a); };
  const keyRe = new RegExp(`^${ns.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}device:([^:]+):([^:]+)$`);

  // ── storage ───────────────────────────────────────────────────────────
  const db = new DatabaseSync(dbPath);
  db.exec(`
    PRAGMA journal_mode = WAL;
    PRAGMA synchronous = NORMAL;
    CREATE TABLE IF NOT EXISTS samples (
      session   TEXT NOT NULL,
      device    TEXT NOT NULL,
      signal    TEXT NOT NULL,
      ts        REAL NOT NULL,      -- srv_ts, else timestamp, else the Redis stream id time (s)
      srv_ts    REAL,
      timestamp REAL,
      stream_id TEXT NOT NULL,
      fields    TEXT NOT NULL,      -- the whole sample, JSON
      UNIQUE (device, signal, stream_id)
    );
    CREATE INDEX IF NOT EXISTS samples_by_session ON samples (session, signal, device, ts);
    CREATE TABLE IF NOT EXISTS session_streams (
      session TEXT NOT NULL, device TEXT NOT NULL, signal TEXT NOT NULL,
      n INTEGER NOT NULL, first_ts REAL NOT NULL, last_ts REAL NOT NULL,
      PRIMARY KEY (session, device, signal)
    );
  `);
  const insert = db.prepare(`INSERT OR IGNORE INTO samples (session, device, signal, ts, srv_ts, timestamp, stream_id, fields)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?)`);
  const bump = db.prepare(`INSERT INTO session_streams (session, device, signal, n, first_ts, last_ts) VALUES (?, ?, ?, 1, ?, ?)
                           ON CONFLICT (session, device, signal) DO UPDATE SET n = n + 1,
                             first_ts = min(first_ts, excluded.first_ts), last_ts = max(last_ts, excluded.last_ts)`);

  // ── Redis ─────────────────────────────────────────────────────────────
  const redis = new Redis(redisUrl, { maxRetriesPerRequest: 2 });
  const reader = new Redis(redisUrl);            // blocking reads get their own connection
  for (const c of [redis, reader]) c.on('error', (e) => say('redis:', e.message));

  const streams = new Set();
  let stopped = false, archived = 0;

  // Read once per batch, not cached: a sample that lands right after `xrtel start`
  // must not be filed under the previous state. One GET per batch costs nothing.
  async function currentSession() {
    try { return (await redis.get(`${ns}session:current`)) || (await redis.get(`${ns}engagement:session_id`)) || null; }
    catch { return null; }
  }

  async function discover() {
    let cursor = '0';
    do {
      const [next, keys] = await redis.scan(cursor, 'MATCH', `${ns}device:*:*`, 'COUNT', 1000, 'TYPE', 'stream');
      cursor = next;
      for (const k of keys) {
        const m = keyRe.exec(k);
        if (!m || RESERVED.has(m[2]) || streams.has(k)) continue;
        // From 0: whatever the stream still holds when the archiver first sees it is kept too.
        try { await redis.xgroup('CREATE', k, group, '0'); }
        catch (e) { if (!String(e.message).includes('BUSYGROUP')) { say('xgroup', k, e.message); continue; } }
        streams.add(k);
        say('archiving', k);
      }
    } while (cursor !== '0');
  }

  async function store(entries) {
    const now = Date.now();
    const live = await currentSession();
    db.exec('BEGIN');
    try {
      for (const { key, id, flat } of entries) {
        const [, device, signal] = keyRe.exec(key);
        const f = parseFields(flat);
        const idMs = +id.split('-')[0];
        const session = (typeof f.session === 'string' && f.session) ||
          (live && now - idMs < liveWindowMs ? live : NO_SESSION);
        // Trust a capture time only if it is within an hour of the arrival in Redis:
        // an unsynced or skewed producer clock must not throw a sample into 1970.
        const near = (v) => (v != null && Math.abs(v - idMs / 1000) < 3600 ? v : null);
        const ts = near(num(f.srv_ts)) ?? near(num(f.timestamp)) ?? idMs / 1000;
        const r = insert.run(session, device, signal, ts, num(f.srv_ts), num(f.timestamp), id, JSON.stringify(f));
        if (r.changes) { bump.run(session, device, signal, ts, ts); archived++; }
      }
      db.exec('COMMIT');
    } catch (e) { db.exec('ROLLBACK'); throw e; }
  }

  async function readOnce(ids) {
    const keys = [...streams];
    if (!keys.length) { await sleep(500); return 0; }
    // New entries ('>') block briefly; the pending list ('0') answers at once.
    const block = ids === '>' ? ['BLOCK', 1000] : [];
    const res = await reader.xreadgroup('GROUP', group, consumer, 'COUNT', 1000, ...block,
      'STREAMS', ...keys, ...keys.map(() => ids));
    if (!res) return 0;
    const entries = [], gone = [];
    for (const [key, list] of res) for (const [id, flat] of list) (flat ? entries : gone).push({ key, id, flat });
    if (!entries.length && !gone.length) return 0;
    if (entries.length) await store(entries);
    const p = redis.pipeline();
    for (const { key, id } of [...entries, ...gone]) p.xack(key, group, id);   // gone = trimmed before we read it
    await p.exec();
    return entries.length + gone.length;
  }

  async function run() {
    await discover();
    // Pending first: samples delivered before a crash but never acknowledged.
    while (!stopped && await readOnce('0') > 0) { /* drain */ }
    let lastScan = Date.now();
    while (!stopped) {
      try {
        if (Date.now() - lastScan > scanMs) { await discover(); lastScan = Date.now(); }
        await readOnce('>');
      } catch (e) {
        if (stopped) break;
        say('read:', e.message);
        await sleep(1000);
      }
    }
  }

  const ready = new Promise((resolve) => redis.once('ready', resolve));
  const loop = ready.then(run).catch((e) => say('stopped:', e.message));

  // ── retention ─────────────────────────────────────────────────────────
  // Whole sessions go, never half of one, and never the running one. `_none`
  // (samples outside any session) never ends, so it is trimmed row by row.
  const dbBytes = () => {
    const page = db.prepare('PRAGMA page_size').get().page_size;
    const used = db.prepare('PRAGMA page_count').get().page_count - db.prepare('PRAGMA freelist_count').get().freelist_count;
    return used * page;
  };
  function dropSession(id) {
    db.prepare('DELETE FROM samples WHERE session = ?').run(id);
    db.prepare('DELETE FROM session_streams WHERE session = ?').run(id);
    redis.del(`${ns}session:${id}:meta`).catch(() => {});
  }
  function trimNone(beforeTs) {
    const r = db.prepare('DELETE FROM samples WHERE session = ? AND ts < ?').run(NO_SESSION, beforeTs);
    if (!r.changes) return 0;
    db.prepare('DELETE FROM session_streams WHERE session = ?').run(NO_SESSION);
    db.prepare(`INSERT INTO session_streams (session, device, signal, n, first_ts, last_ts)
                SELECT session, device, signal, count(*), min(ts), max(ts) FROM samples WHERE session = ?
                GROUP BY session, device, signal`).run(NO_SESSION);
    return r.changes;
  }
  async function purge(now = Date.now() / 1000) {
    const current = await currentSession();
    const dropped = [];
    let trimmed = 0;
    const finished = () => db.prepare(`SELECT session, max(last_ts) AS last FROM session_streams
                                      WHERE session NOT IN (?, ?) GROUP BY session ORDER BY last`).all(NO_SESSION, current || '');
    if (retentionDays > 0) {
      const cutoff = now - retentionDays * 86400;
      for (const r of finished()) if (r.last < cutoff) { dropSession(r.session); dropped.push(r.session); }
      trimmed += trimNone(cutoff);
    }
    if (maxMb > 0) {
      const limit = maxMb * 1024 * 1024;
      // Oldest first, until under the limit; `_none` counts as the oldest material it holds.
      while (dbBytes() > limit) {
        const oldest = finished()[0];
        const noneFirst = db.prepare('SELECT min(ts) AS t FROM samples WHERE session = ?').get(NO_SESSION).t;
        if (noneFirst != null && (!oldest || noneFirst < oldest.last)) { trimmed += trimNone(Math.min(noneFirst + 3600, oldest ? oldest.last : now)); continue; }
        if (!oldest) { say(`archive over ${maxMb} MB but only the running session is left`); break; }
        dropSession(oldest.session); dropped.push(oldest.session);
      }
    }
    // SQLite reuses freed pages but does not give them back: VACUUM shrinks the file.
    if (dropped.length || trimmed) {
      db.exec('VACUUM');
      say(`retention: deleted ${dropped.length ? 'sessions ' + dropped.join(', ') : 'no sessions'}` +
          `${trimmed ? `, ${trimmed} samples outside sessions` : ''}; now ${(dbBytes() / 1048576).toFixed(1)} MB`);
    }
    return { dropped, trimmed };
  }
  const purger = (retentionDays > 0 || maxMb > 0) && setInterval(() => purge().catch((e) => say('retention:', e.message)), purgeEveryMs);
  let firstPurge = null;                     // a minute after start: a restarted archiver catches up on retention
  if (purger) ready.then(() => { if (!stopped) firstPurge = setTimeout(() => purge().catch((e) => say('retention:', e.message)), 60000); });

  // ── queries ───────────────────────────────────────────────────────────
  async function sessions() {
    const rows = db.prepare(`SELECT session, sum(n) AS samples, min(first_ts) AS first_ts, max(last_ts) AS last_ts,
                               group_concat(DISTINCT device) AS devices, group_concat(DISTINCT signal) AS signals
                             FROM session_streams GROUP BY session ORDER BY last_ts DESC`).all();
    const current = await currentSession();
    const out = [];
    for (const r of rows) {
      const expires = retentionDays > 0 && r.session !== current && r.session !== NO_SESSION ? r.last_ts + retentionDays * 86400 : null;
      out.push({ ...(await sessionMeta(r.session)), ...summary(r), current: r.session === current, expires_ts: expires });
    }
    return { current, retention_days: retentionDays || null, max_mb: maxMb || null, size_mb: round(dbBytes() / 1048576, 1), sessions: out };
  }

  async function session(id) {
    const streamsRows = db.prepare(`SELECT device, signal, n, first_ts, last_ts FROM session_streams
                                    WHERE session = ? ORDER BY device, signal`).all(id);
    if (!streamsRows.length) return null;
    const marks = db.prepare(`SELECT ts, fields FROM samples WHERE session = ? AND signal = 'event' AND device = 'operator'
                              ORDER BY ts`).all(id).map((r) => ({ ts: r.ts, ...pick(JSON.parse(r.fields), ['name', 'value']) }));
    const first = Math.min(...streamsRows.map((r) => r.first_ts)), last = Math.max(...streamsRows.map((r) => r.last_ts));
    const running = id === await currentSession();
    return {
      ...(await sessionMeta(id)), session: id, first_ts: first, last_ts: last, duration_s: round(last - first, 1),
      expires_ts: retentionDays > 0 && !running && id !== NO_SESSION ? last + retentionDays * 86400 : null,
      samples: streamsRows.reduce((a, r) => a + r.n, 0),
      streams: streamsRows.map((r) => ({ ...r, rate_hz: r.last_ts > r.first_ts ? round((r.n - 1) / (r.last_ts - r.first_ts), 1) : null })),
      marks,
    };
  }

  async function sessionMeta(id) {
    try { const h = await redis.hgetall(`${ns}session:${id}:meta`); return h && Object.keys(h).length ? { meta: h } : {}; }
    catch { return {}; }
  }

  /** Writes the export to `out` (a writable) row by row; returns false if the session has nothing. */
  function exportTo(out, id, { format = 'csv', signal = null, device = null } = {}) {
    let sql = 'SELECT ts, signal, fields FROM samples WHERE session = ?';
    const args = [id];
    if (signal) { sql += ' AND signal = ?'; args.push(signal); }
    if (device) { sql += ' AND device = ?'; args.push(device); }
    sql += ' ORDER BY ts';
    const stmt = db.prepare(sql);
    if (format === 'ndjson') {
      let n = 0;
      for (const r of stmt.iterate(...args)) { out.write(JSON.stringify({ ts: r.ts, signal: r.signal, ...JSON.parse(r.fields) }) + '\n'); n++; }
      return n > 0;
    }
    // CSV: one pass for the union of columns (signals differ), one to write.
    const cols = new Set();
    for (const r of stmt.iterate(...args)) for (const k of Object.keys(JSON.parse(r.fields))) cols.add(k);
    if (!cols.size) return false;
    const header = [...LEAD.filter((c) => c === 'ts' || c === 'signal' || cols.has(c)),
      ...[...cols].filter((c) => !LEAD.includes(c)).sort()];
    out.write(header.join(',') + '\n');
    for (const r of stmt.iterate(...args)) {
      const f = { ts: r.ts, signal: r.signal, ...JSON.parse(r.fields) };
      out.write(header.map((c) => csvCell(f[c])).join(',') + '\n');
    }
    return true;
  }

  // ── HTTP ──────────────────────────────────────────────────────────────
  const server = port == null ? null : http.createServer(async (req, res) => {
    const url = new URL(req.url || '/', 'http://archiver');
    const parts = url.pathname.split('/').filter(Boolean);
    const json = (code, body) => { res.writeHead(code, { 'content-type': 'application/json' }); res.end(JSON.stringify(body)); };
    try {
      if (req.method !== 'GET' && req.method !== 'HEAD') return json(405, { error: 'method not allowed' });
      if (parts.length === 1 && parts[0] === 'health') return json(200, { ok: redis.status === 'ready', streams: streams.size, archived,
        size_mb: round(dbBytes() / 1048576, 1), retention_days: retentionDays || null, max_mb: maxMb || null });
      if (parts[0] !== 'sessions') return json(404, { error: 'not found' });
      if (parts.length === 1) return json(200, await sessions());
      const id = decodeURIComponent(parts[1]);
      if (!NAME_RE.test(id)) return json(400, { error: 'bad session id' });
      if (parts.length === 2) { const s = await session(id); return s ? json(200, s) : json(404, { error: 'no such session' }); }
      if (parts.length === 3 && parts[2] === 'export') {
        const format = url.searchParams.get('format') === 'ndjson' ? 'ndjson' : 'csv';
        const signal = url.searchParams.get('signal'), device = url.searchParams.get('device');
        const name = [id, signal, device].filter(Boolean).join('_') + (format === 'csv' ? '.csv' : '.ndjson');
        // Headers go out with the first row; an empty export becomes a 404 instead.
        let started = false;
        const out = { write: (s) => {
          if (!started) {
            res.writeHead(200, { 'content-type': format === 'csv' ? 'text/csv; charset=utf-8' : 'application/x-ndjson',
              'content-disposition': `attachment; filename="${name}"` });
            started = true;
          }
          res.write(s);
        } };
        const any = exportTo(out, id, { format, signal, device });
        return any ? res.end() : json(404, { error: 'nothing to export' });
      }
      return json(404, { error: 'not found' });
    } catch (e) {
      say('http:', e.message);
      if (!res.headersSent) json(500, { error: e.message }); else res.end();
    }
  });
  const listening = server ? new Promise((r) => server.listen(port, () => {
    say(`listening on http://0.0.0.0:${server.address().port}  db=${dbPath}  redis=${redisUrl}  ns=${ns || '(none)'}  ` +
        `retention=${retentionDays > 0 ? retentionDays + ' days' : 'forever'}  max=${maxMb > 0 ? maxMb + ' MB' : 'none'}`);
    r();
  })) : Promise.resolve();

  return {
    server, db, ready: Promise.all([ready, listening]), sessions, session, exportTo, purge,
    get archived() { return archived; },
    async close() {
      stopped = true;
      clearInterval(purger); clearTimeout(firstPurge);
      reader.disconnect();                       // unblocks a pending XREADGROUP
      await loop;
      if (server) await new Promise((r) => server.close(() => r()));
      await redis.quit().catch(() => {});
      db.close();
    },
  };
}

// ── helpers ─────────────────────────────────────────────────────────────
function parseFields(flat) {
  const f = {};
  for (let i = 0; i < flat.length; i += 2) {
    const k = flat[i], v = flat[i + 1];
    f[k] = !TEXT_FIELDS.has(k) && /^-?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$/.test(v) ? Number(v) : v;
  }
  return f;
}
function num(v) { const n = typeof v === 'number' ? v : parseFloat(v); return Number.isFinite(n) ? n : null; }
function round(n, d) { const f = 10 ** d; return Math.round(n * f) / f; }
function pick(o, keys) { const r = {}; for (const k of keys) if (o[k] !== undefined) r[k] = o[k]; return r; }
function summary(r) {
  return { session: r.session, samples: r.samples, first_ts: r.first_ts, last_ts: r.last_ts,
    duration_s: round(r.last_ts - r.first_ts, 1), devices: r.devices ? r.devices.split(',') : [],
    signals: r.signals ? r.signals.split(',') : [] };
}
function csvCell(v) {
  if (v === undefined || v === null) return '';
  const s = typeof v === 'object' ? JSON.stringify(v) : String(v);
  return /[",\n\r]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

module.exports = { createArchiver, NO_SESSION };
