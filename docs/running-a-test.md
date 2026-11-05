# Running a test

From "headsets on" to a CSV, without touching Redis or WebSockets. Everything
goes through the `xrtel` command and a deployment's gateway URL.

## Once

```bash
cd cli && npm install && npm link          # puts `xrtel` on your PATH (or: node cli/xrtel.mjs …)
xrtel login https://<host>/telemetry <CONSUME_TOKEN>
xrtel status
```

`login` saves the URL and token in `~/.config/xrtel/config.json` (mode 600).
`XRTEL_URL` / `XRTEL_TOKEN` or `--url` / `--token` override it, e.g. for
scripts.

The consume token is the deployment's secret: whoever has it can start and
stop sessions and download data. The ingest token, which ends up inside the
web player, cannot.

## A test

```bash
xrtel start pilot-03 --note "2 Quest + EmotiBit, room B"
#   … participants put the headsets on; the experience runs …
xrtel mark song-2                 # a marker on the timeline, whenever something happens
xrtel mark incident "Q2 lost tracking"
xrtel stop
```

From `start` to `stop`, every sample that arrives **without a session of its
own** is filed under `pilot-03`: headsets through the gateway, and wearables
that write to Redis directly (Physiological-Signals). The headsets need no
change, and no new URL to load per test.

A producer that names a session itself (`?session=…` on its ingest URL) keeps
it: that is how a fixed installation or a second experiment can run in
parallel on the same deployment. Samples that arrive while no session is
running are kept too, under `_none`.

While it runs:

```bash
xrtel watch                       # one line per sample, every device
xrtel watch --signals pose,event  # less noise
xrtel watch --raw > live.ndjson   # the JSON, to pipe somewhere
```

## Afterwards

```bash
xrtel sessions                    # everything recorded, newest first
xrtel show pilot-03               # duration, devices, sample rates, marks
xrtel export pilot-03                         # everything        -> pilot-03.csv
xrtel export pilot-03 --signal pose           # one signal        -> pilot-03_pose.csv
xrtel export pilot-03 --signal physio_metrics --device MD-V5-0000334
xrtel export pilot-03 --format ndjson         # one JSON object per line, nested fields intact
```

`xrtel show` is the first thing to look at after a test: a headset missing
from the list, or a rate well below 20 Hz for `pose`, is visible there before
anyone opens a spreadsheet.

### The CSV

One row per sample, sorted by `ts`, columns:

| Column | Meaning |
|---|---|
| `ts` | Best capture time available, Unix seconds: `srv_ts`, else `timestamp`, else arrival in Redis |
| `signal`, `device`, `session`, `participant` | What and who |
| `srv_ts`, `srv_err_ms` | Reference-clock time and its error bound (spec §6) |
| `timestamp` | The producer's own wall clock |
| `media_t` | Position in the content being played |
| `name`, `value` | For `event` rows, including the `operator` marks |
| … | Every other field of the signal (`px…qw`, `gx…gz`, `hr`, `eda_z`, `z`, `f`…) |

Exporting everything at once gives the union of all signals' columns, mostly
empty per row. Export one signal at a time for analysis; use the full export
or NDJSON to keep markers and signals on one timeline.

In Python:

```python
import pandas as pd
pose = pd.read_csv("pilot-03_pose.csv")
marks = pd.read_csv("pilot-03.csv").query("device == 'operator'")[["ts", "name", "value"]]
pose.to_parquet("pilot-03_pose.parquet")       # for datasets
```

## The same over HTTP

`xrtel` is a thin client of the gateway's API. Anything else (a dashboard, a
notebook, the experience itself) can call it with
`Authorization: Bearer <CONSUME_TOKEN>`:

| Method and path | |
|---|---|
| `GET  /session` | `{current, meta}` |
| `POST /session/start` | body `{"id": "pilot-03", "meta": {"note": "…"}}` |
| `POST /session/stop` | |
| `POST /mark` | body `{"name": "song-2", "value": "SAX"}`, optional `"session"` |
| `GET  /sessions` | all sessions with sample counts, devices and signals |
| `GET  /sessions/{id}` | one session: streams with rates, marks |
| `GET  /sessions/{id}/export?format=csv\|ndjson&signal=&device=` | the data |

Behind CloudFront, the behavior must allow `POST` (*Allowed HTTP methods:
GET, HEAD, OPTIONS, PUT, POST, PATCH, DELETE*), or `start`/`stop`/`mark` get a
403 from the CDN.

## Where the data lives

```
headsets ─▶ gateway ─▶ Redis ─┬─▶ live consumers (xrtel watch, dashboards, Physiological-Signals)
wearables ───────────▶ Redis ─┘
                              └─▶ archiver ─▶ SQLite (Docker volume archive_data) ─▶ /sessions, export
```

Redis keeps only the last ~20 000 samples per stream (about 16 minutes of
pose at 20 Hz). The archiver reads every stream through a consumer group and
acknowledges a sample only once it is committed, so it loses nothing while it
runs and catches up after a restart, as long as it was down for less than
that window.

Back up the archive with:

```bash
docker compose cp archiver:/data/xrtel.sqlite ./xrtel-$(date +%F).sqlite
```
