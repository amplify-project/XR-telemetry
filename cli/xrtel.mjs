#!/usr/bin/env node
// xrtel — run tests and get the data out of an XR-telemetry deployment.
//
//   xrtel login https://<host>/telemetry <CONSUME_TOKEN>   # once; saved in ~/.config/xrtel/config.json
//   xrtel start pilot-03 --note "Quest x2 + EmotiBit"      # from now on, everything goes to pilot-03
//   xrtel mark song-2                                      # a marker in the timeline
//   xrtel watch                                            # live, readable
//   xrtel stop
//   xrtel sessions                                         # what has been recorded
//   xrtel show pilot-03                                    # devices, rates, marks
//   xrtel export pilot-03 --signal pose                    # -> pilot-03_pose.csv
//
// Talks only to the gateway (HTTP + WebSocket), with the consume token: no
// Redis access needed. XRTEL_URL / XRTEL_TOKEN or --url / --token override
// the saved login.

import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';

const UA = 'xrtel-cli/0.2';          // CloudFront's managed rules refuse requests without one
const CONFIG = path.join(process.env.XDG_CONFIG_HOME || path.join(os.homedir(), '.config'), 'xrtel', 'config.json');

const HELP = `xrtel — XR-telemetry command line

  login <url> <token>        save the gateway URL (…/telemetry) and the CONSUME token
  status                     gateway health and the running session
  start <name> [--note txt]  start a session now: everything without its own session goes to <name>
  stop                       end the running session
  mark <name> [value]        put a marker in the running session
  watch [--session s] [--signals pose,event] [--raw]
                             live samples, one line each (--raw: the JSON)
  sessions                   recorded sessions, newest first
  show <name>                one session: duration, devices, rates, marks
  export <name> [--signal s] [--device d] [--format csv|ndjson] [-o file]
                             download a session (default <name>[_signal][_device].csv)

  Options for every command: --url, --token (or XRTEL_URL, XRTEL_TOKEN).`;

// ── args & config ─────────────────────────────────────────────────────────
const argv = process.argv.slice(2);
const opts = {}, pos = [];
for (let i = 0; i < argv.length; i++) {
  const a = argv[i];
  if (a === '-o') opts.o = argv[++i];
  else if (a.startsWith('--')) {
    const k = a.slice(2);
    if (k === 'raw' || k === 'help') opts[k] = true; else opts[k] = argv[++i];
  } else pos.push(a);
}
const cmd = pos.shift();

function loadConfig() { try { return JSON.parse(fs.readFileSync(CONFIG, 'utf8')); } catch { return {}; } }
const saved = loadConfig();
const BASE = (opts.url || process.env.XRTEL_URL || saved.url || '').replace(/\/+$/, '');
const TOKEN = opts.token || process.env.XRTEL_TOKEN || saved.token || '';

const die = (msg, code = 1) => { console.error(msg); process.exit(code); };
const need = () => { if (!BASE) die('No gateway configured. Run:  xrtel login https://<host>/telemetry <CONSUME_TOKEN>'); };

async function api(method, p, body) {
  need();
  const res = await fetch(BASE + p, {
    method,
    headers: { 'user-agent': UA, ...(TOKEN ? { authorization: `Bearer ${TOKEN}` } : {}), ...(body ? { 'content-type': 'application/json' } : {}) },
    body: body ? JSON.stringify(body) : undefined,
  }).catch((e) => die(`Cannot reach ${BASE}: ${e.cause?.code || e.message}`));
  if (res.status === 401) die('401: wrong or missing consume token (xrtel login …)');
  if (res.status === 403) die('403 from the CDN: if POST commands fail, allow all HTTP methods on the CloudFront behavior');
  return res;
}
async function json(method, p, body) {
  const res = await api(method, p, body);
  const text = await res.text();
  let data; try { data = JSON.parse(text); } catch { data = { error: text.trim().slice(0, 200) }; }
  if (!res.ok) die(`${res.status}: ${data.error || text}`);
  return data;
}

// ── formatting ────────────────────────────────────────────────────────────
const pad = (n, w = 2) => String(n).padStart(w, '0');
function clock(ts, ms = false) {
  if (ts == null) return '—';
  const d = new Date(ts * 1000);
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}${ms ? '.' + pad(d.getMilliseconds(), 3) : ''}`;
}
function day(ts) { const d = new Date(ts * 1000); return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`; }
function dur(s) {
  if (s == null) return '—';
  s = Math.round(s);
  return s >= 3600 ? `${Math.floor(s / 3600)}h${pad(Math.floor(s / 60) % 60)}m` : s >= 60 ? `${Math.floor(s / 60)}m${pad(s % 60)}s` : `${s}s`;
}
const f2 = (v) => (typeof v === 'number' ? v.toFixed(2) : v ?? '—');
function yawDeg({ qx, qy, qz, qw }) {
  if ([qx, qy, qz, qw].some((v) => typeof v !== 'number')) return null;
  return Math.round(Math.atan2(2 * (qw * qy + qx * qz), 1 - 2 * (qx * qx + qy * qy)) * 180 / Math.PI);
}
const SKIP = new Set(['type', 'signal', 'device', 'timestamp', 'srv_ts', 'srv_err_ms', 't_mono', 'seq', 'session', 'participant']);
function line(m) {
  const t = clock(m.srv_ts ?? m.timestamp ?? Date.now() / 1000, true);
  const who = `${m.device}`.padEnd(16);
  if (m.signal === 'pose') {
    const extra = ['media_t', 'z', 'f'].filter((k) => m[k] !== undefined).map((k) => `${k}=${f2(m[k])}`).join(' ');
    return `${t} ${who} pose   p=(${f2(m.px)}, ${f2(m.py)}, ${f2(m.pz)}) yaw=${yawDeg(m) ?? '—'}° ${extra}`;
  }
  if (m.signal === 'event') return `${t} ${who} EVENT  ${m.name}${m.value !== undefined ? ' = ' + m.value : ''}`;
  const kv = Object.entries(m).filter(([k]) => !SKIP.has(k)).slice(0, 8)
    .map(([k, v]) => `${k}=${typeof v === 'number' ? +v.toFixed(3) : typeof v === 'object' ? JSON.stringify(v) : v}`).join(' ');
  return `${t} ${who} ${m.signal.padEnd(6)} ${kv}`;
}

// ── commands ──────────────────────────────────────────────────────────────
const commands = {
  async login() {
    const [url, token] = pos;
    if (!url) die('usage: xrtel login https://<host>/telemetry <CONSUME_TOKEN>');
    const base = url.replace(/\/+$/, '');
    const h = await fetch(base + '/health', { headers: { 'user-agent': UA } }).then((r) => r.json())
      .catch((e) => die(`No gateway at ${base}/health (${e.cause?.code || e.message})`));
    fs.mkdirSync(path.dirname(CONFIG), { recursive: true });
    fs.writeFileSync(CONFIG, JSON.stringify({ url: base, token: token || '' }, null, 2) + '\n', { mode: 0o600 });
    console.log(`Saved ${CONFIG}\nGateway ok=${h.ok}, archive=${h.archive ? 'yes' : 'no'}, auth consume=${h.auth?.consume ? 'token' : 'open'}`);
  },

  async status() {
    need();
    const h = await (await api('GET', '/health')).json();
    const s = await json('GET', '/session');
    console.log(`gateway   ${BASE}  ${h.ok ? 'ok' : 'NOT OK'} (redis ${h.redis})`);
    console.log(`archive   ${h.archive ? 'on' : 'OFF — sessions are not being saved'}`);
    console.log(`live      ${h.producers} producer(s) on this gateway, ${h.consumers} consumer(s)`);
    console.log(`session   ${s.current ? `${s.current} (since ${s.meta?.started ? clock(Date.parse(s.meta.started) / 1000) : '?'})` : 'none — xrtel start <name>'}`);
  },

  async start() {
    const [name] = pos;
    if (!name) die('usage: xrtel start <name> [--note "…"]');
    const before = await json('GET', '/session');
    const r = await json('POST', '/session/start', { id: name, meta: opts.note ? { note: opts.note } : undefined });
    if (before.current && before.current !== name) console.log(`(stopped following ${before.current})`);
    console.log(`▶ session ${r.current} started ${clock(Date.parse(r.meta.started) / 1000)}`);
    console.log('  Everything without its own session is now recorded under it. End with: xrtel stop');
  },

  async stop() {
    const r = await json('POST', '/session/stop');
    console.log(r.stopped ? `■ session ${r.stopped} ended. Export with: xrtel export ${r.stopped}` : 'No session was running.');
  },

  async mark() {
    const [name, value] = pos;
    if (!name) die('usage: xrtel mark <name> [value]');
    const r = await json('POST', '/mark', { name, value, session: opts.session });
    console.log(`⚑ ${r.name}${r.value !== undefined ? ' = ' + r.value : ''} at ${clock(r.srv_ts, true)} in ${r.session}`);
  },

  async watch() {
    need();
    const { WebSocket } = await import('ws').catch(() => die('watch needs the ws package: cd cli && npm install'));
    const q = new URLSearchParams();
    if (opts.session) q.set('session', opts.session);
    if (opts.signals) q.set('signals', opts.signals);
    if (TOKEN) q.set('token', TOKEN);
    const url = BASE.replace(/^http/, 'ws') + '/consume?' + q;
    const ws = new WebSocket(url, { headers: { 'user-agent': UA } });
    ws.on('unexpected-response', (_q, r) => die(`WebSocket refused: HTTP ${r.statusCode}${r.statusCode === 401 ? ' (consume token)' : ''}`));
    ws.on('error', (e) => die(`WebSocket error: ${e.message}`));
    ws.on('open', () => console.error(`watching ${BASE}${opts.session ? ' session=' + opts.session : ''}${opts.signals ? ' signals=' + opts.signals : ''} — Ctrl+C to stop`));
    ws.on('close', () => die('connection closed', 0));
    ws.on('message', (raw) => {
      const m = JSON.parse(raw);
      if (opts.raw) return console.log(JSON.stringify(m));
      if (m.type === 'snapshot') { console.error(`(snapshot: ${m.samples.length} latest sample(s))`); m.samples.forEach((s) => console.log(line(s))); }
      else if (m.type === 'sample') console.log(line(m));
      else if (m.type === 'leave') console.log(`${clock(Date.now() / 1000, true)} ${String(m.device).padEnd(16)} left`);
    });
  },

  async sessions() {
    const { current, sessions } = await json('GET', '/sessions');
    if (!sessions.length) return console.log('No sessions recorded yet.' + (current ? '' : ' Start one: xrtel start <name>'));
    const rows = sessions.map((s) => [
      (s.current ? '▶ ' : '  ') + s.session, `${day(s.first_ts)} ${clock(s.first_ts)}`, dur(s.duration_s),
      String(s.samples), s.devices.filter((d) => d !== 'operator').join(', '),
    ]);
    const head = ['  session', 'started', 'length', 'samples', 'devices'];
    const w = head.map((h, i) => Math.max(h.length, ...rows.map((r) => r[i].length)));
    const fmt = (r) => r.map((c, i) => (i === 3 ? c.padStart(w[i]) : c.padEnd(w[i]))).join('  ');
    console.log(fmt(head)); rows.forEach((r) => console.log(fmt(r)));
    if (sessions.some((s) => s.session === '_none')) console.log('\n_none = samples that arrived while no session was running.');
  },

  async show() {
    const [name] = pos;
    if (!name) die('usage: xrtel show <name>');
    const s = await json('GET', `/sessions/${encodeURIComponent(name)}`);
    console.log(`${s.session}  ${day(s.first_ts)} ${clock(s.first_ts)} → ${clock(s.last_ts)}  (${dur(s.duration_s)}, ${s.samples} samples)`);
    if (s.meta?.note) console.log(`note: ${s.meta.note}`);
    console.log('\n  device            signal            samples   rate');
    for (const r of s.streams.filter((r) => r.device !== 'operator'))
      console.log(`  ${r.device.padEnd(17)} ${r.signal.padEnd(17)} ${String(r.n).padStart(7)}   ${r.rate_hz != null ? r.rate_hz + ' Hz' : '—'}`);
    if (s.marks.length) {
      console.log('\n  marks');
      for (const m of s.marks) console.log(`  ${clock(m.ts, true)}  +${dur(m.ts - s.first_ts).padEnd(6)} ${m.name}${m.value !== undefined ? ' = ' + m.value : ''}`);
    }
  },

  async export() {
    const [name] = pos;
    if (!name) die('usage: xrtel export <name> [--signal pose] [--device id] [--format csv|ndjson] [-o file]');
    const format = opts.format === 'ndjson' ? 'ndjson' : 'csv';
    const q = new URLSearchParams({ format });
    if (opts.signal) q.set('signal', opts.signal);
    if (opts.device) q.set('device', opts.device);
    const res = await api('GET', `/sessions/${encodeURIComponent(name)}/export?${q}`);
    if (res.status === 404) die(`Nothing to export: ${name} has no ${opts.signal ? `'${opts.signal}' ` : ''}samples${opts.device ? ` from ${opts.device}` : ''}. See: xrtel show ${name}`);
    if (!res.ok) { const t = await res.text(); let e; try { e = JSON.parse(t).error; } catch { e = t; } die(`${res.status}: ${e}`); }
    const file = opts.o || [name, opts.signal, opts.device].filter(Boolean).join('_') + '.' + format;
    await pipeline(Readable.fromWeb(res.body), fs.createWriteStream(file));
    const lines = fs.readFileSync(file, 'utf8').split('\n').filter(Boolean).length - (format === 'csv' ? 1 : 0);
    console.log(`${file}: ${lines} rows`);
  },
};

if (!cmd || opts.help || cmd === 'help') { console.log(HELP); process.exit(0); }
if (!commands[cmd]) die(`Unknown command "${cmd}".\n\n${HELP}`);
await commands[cmd]();
