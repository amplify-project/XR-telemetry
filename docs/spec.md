# XR-telemetry specification — draft v0.1

Status: **draft for discussion**. Nothing here is frozen until v1.0; field names
marked *(open)* are expected to change.

## 1. Purpose

A shared way for heterogeneous producers — WebXR headsets, Unity apps,
wearables, computer-vision pipelines — to publish time-stamped samples about
the people taking part in an experience, and for any number of consumers to
read them **live** and **afterwards**, on one time axis.

It is a **convention over Redis** plus a few small components, not a new
broker:

- **Key space and sample format** (§3–§5): what a producer writes to Redis.
  Anything that follows it interoperates, whatever language it is written in.
- **Time** (§6): how samples from different machines end up comparable.
- **Gateway** (§8): a WebSocket front for clients that cannot speak Redis
  (browsers, WebXR) and for deployments where Redis must not be exposed.
- **Archiver** (§9): durable storage in a database or files for analysis.

The convention is deliberately the one the AMPLIFY
[Physiological-Signals](https://github.com/amplify-project/Physiological-Signals)
stack already uses (`device:{src}:{signal}`, Pub/Sub + Streams), so that stack
conforms with no change (§11).

### Non-goals

- Not a general message bus or RPC layer. Commands (e.g. "reset baseline")
  may share the key space but are out of scope of this spec.
- No media transport (audio/video). Samples may *reference* media by time.
- No analysis or modelling. Those are consumers.

## 2. Terms

| Term | Meaning |
|---|---|
| **source** (`src`) | One device that produces samples: a headset, a wristband, a camera pipeline. Stable id, e.g. an EmotiBit serial `MD-V5-0000334` or `quest-07`. |
| **participant** | A person. One participant may wear several sources (§7). |
| **signal** | A named kind of sample from a source: `pose`, `physio_metrics`, `engagement`… |
| **sample** | One flat record of one signal at one instant. |
| **session** | One run of an experience (a rehearsal, a concert). Every sample belongs to at most one. |
| **namespace** | Optional key prefix isolating several projects on one Redis (§3.3). |

## 3. Key space

### 3.1 Signals

For every sample a producer does **both**:

```
PUBLISH  device:{src}:{signal}  <JSON object>        # live, fire-and-forget
XADD     device:{src}:{signal}  MAXLEN ~ N  * <fields>   # history, replayable
```

Same name for the channel and the stream (Redis keeps the two namespaces
apart). `{src}` and `{signal}` match `[A-Za-z0-9_.-]{1,64}`. `N` is the
producer's choice; 20 000 is the current practice. Approximate trimming (`~`)
is recommended.

Consumers that only want *now* use `PSUBSCRIBE device:*:{signal}`. Consumers
that must not lose samples (recorders, the archiver) read the stream with
`XREADGROUP`.

### 3.2 Registry, presence, sessions, participants

| Key | Type | Content |
|---|---|---|
| `device:{src}:meta` | hash | `kind` (`xr`, `wearable`, `camera`…), `platform`, `signals` (comma list), `frame` (§5.1), `sdk`, `version` |
| `device:{src}:alive` | string, TTL | Set with `EX 5` on every batch; absence = source gone |
| `session:current` | string | Current session id |
| `engagement:session_id` | string | Legacy name of the same thing; the gateway keeps both in step *(open: retire?)* |
| `session:{id}:meta` | hash | `started` (ISO 8601), `project`, `venue`, free fields |
| `participant:{pid}` | hash | `{role} → {src}`, e.g. `xr → quest-07`, `wearable → MD-V5-0000334` |
| `device:{src}:participant` | string | Reverse lookup, `{pid}` |

### 3.3 Namespaces

A deployment that hosts several projects on one Redis sets a prefix
`{ns}:` in front of every key above (`amplify:device:quest-07:pose`). Default:
no prefix, which is what existing producers do. Using a separate Redis
database number is an equivalent alternative.

## 4. Sample format

A sample is a **flat** map of field → scalar, so that it can be written to a
Stream as is and loaded into a table without parsing. Nested values, when
unavoidable, are JSON-encoded strings.

### 4.1 Common fields

| Field | Type | Req. | Meaning |
|---|---|---|---|
| `device` | string | yes | `{src}`, repeated so a sample is self-contained |
| `timestamp` | float, s | yes | Capture time, Unix epoch, **producer's wall clock** |
| `srv_ts` | float, s | no | Capture time on the **reference clock** (§6) |
| `srv_err_ms` | float | no | Bound on the error of `srv_ts` (half the best round trip) |
| `t_mono` | float, ms | no | Producer's monotonic clock, for intervals within one source |
| `seq` | int | no | Per-source sequence number, to detect loss |
| `session` | string | no | Session id, if known to the producer |
| `participant` | string | no | Participant id, if known to the producer |
| `media_t` | float, s | no | Presentation time of the content being experienced |

`timestamp` keeps compatibility with every current producer. `srv_ts` is what
makes two sources comparable (§6).

### 4.2 Signal-specific fields

Everything else in the sample is defined by the signal (§5). Unknown fields
must be preserved by every component that forwards samples (gateway, archiver),
so that a project can add its own without touching XR-telemetry.

## 5. Standard signals

### 5.1 `pose` — head pose and gaze

| Field | Meaning |
|---|---|
| `px py pz` | Head position, metres |
| `qx qy qz qw` | Head orientation, unit quaternion |
| `gx gy gz` | Gaze direction, unit vector. Eye-tracked if `gaze_src = eye`, head-forward otherwise |
| `gaze_src` | `eye` or `head` |
| `frame` | Coordinate frame of `p`/`q`, below |
| `input` | How the view is driven: `xr`, `mouse`, `touch`, `gyro` |

All frames are right-handed, **Y up, −Z forward**, metres.

| `frame` | Origin and meaning |
|---|---|
| `local-floor` | Per-headset floor origin set at session start. **Not** comparable across headsets. |
| `room` | A physical room shared by all participants, after alignment. Comparable across headsets. |
| `screen` | Flat-screen view: `p` is always 0, `q` is the camera orientation, not a head. |

Consumers must read `frame` rather than assume it. Mixing frames is a
deliberate analysis choice, never a default.

### 5.2 `event` — discrete occurrences

| Field | Meaning |
|---|---|
| `name` | Event name, e.g. `focus`, `zoom`, `pinch`, `annotation` |
| `value` | Scalar or JSON string |

### 5.3 Registered profiles

Signals defined by member projects, listed here so consumers can rely on them.
Each project owns its field list; this table only names them.

| Signal | Owner | Rate |
|---|---|---|
| `physio_metrics` | Physiological-Signals | ~1 Hz |
| `raw_sensors` | Physiological-Signals | per batch |
| `valence_cont`, `arousal_cont` | Physiological-Signals | ~1 Hz |
| `engagement` | Physiological-Signals | ~1 Hz |
| `hr_filtered`, `eda_filtered` | Physiological-Signals | stream rate |
| `pose`, `event` | XR-telemetry | 20 Hz / on change |

*(open)* A JSON Schema per signal, served from the repo, that the gateway can
validate against.

## 6. Time

Three clocks, because no single one does the job:

| Clock | Field | Good for | Not good for |
|---|---|---|---|
| Producer monotonic | `t_mono` | intervals within one source | anything across sources |
| Producer wall | `timestamp` | compatibility, coarse alignment | sub-second alignment: only as good as each machine's NTP |
| Reference | `srv_ts` | aligning sources | — |

**Reference clock.** The gateway's clock. For each connected source it
estimates the offset between the source's monotonic clock and its own with
Cristian's algorithm: send `ping` at `t1`, the source answers its own reading
`c`, the answer lands at `t2`; then `offset = t1 + (t2 − t1)/2 − c`. It keeps
the exchange with the **smallest round trip in a sliding window** (16 by
default) — the NTP filter: a fast exchange had little queueing to be
asymmetric about, and the window lets the estimate follow oscillator drift.
`srv_ts = (t_mono + offset)/1000`, and `srv_err_ms` is half that round trip.

**Redis stream ids.** The id `XADD *` returns is the Redis server's time in
milliseconds. It is a common clock for free, but it stamps *arrival*, not
capture (batched samples share it). Useful as a fallback and as a check.

*(open)* Producers that write to Redis directly (no gateway) cannot get
`srv_ts` today. Options: a small timing endpoint they probe, or estimating
their offset from stream-id − `timestamp` over a window.

## 7. Identity

A participant wearing a headset and a wristband is two sources. The mapping
lives in Redis (§3.2), set by the operator or declared by a source. Producers
should put `participant` on samples when they know it; consumers resolve it
through `device:{src}:participant` when they do not.

## 8. Gateway (WebSocket)

For browsers and WebXR (which cannot open a Redis connection), and for any
deployment where Redis is not reachable or must not be exposed.

**Endpoints.** `/ingest` (producers), `/consume` (consumers), `/health`,
optionally under a base path. Query: `session=`, `token=`.

**Auth.** One token for ingest and one for consume, per namespace. Refused
before the WebSocket opens (HTTP 401). The ingest token may end up in client
code and is a deterrent; the consume token is the secret.

**Producer messages** (JSON):

```json
{ "hello": "quest-07", "meta": { "kind": "xr", "frame": "room", "participant": "p12" } }
{ "s": "pose", "b": [ { "t": 1234.5, "w": 1730800000123, "px": 0, "py": 1.6, "pz": 0,
                        "qx": 0, "qy": 0, "qz": 0, "qw": 1 } ] }
{ "pong": 1730800000100, "c": 1234.4 }
```

The gateway writes each sample to Redis per §3–§4, adding `srv_ts`,
`srv_err_ms` and `session`, and answers `ping` probes every 2 s.

**Consumer messages.** On connect a `snapshot` (latest sample of each live
source, per signal), then `sample` and `leave` messages. *(open)* A `signals=`
filter on the query string.

**Keepalive.** WebSocket ping to consumers every 30 s, so that proxies
(CloudFront, nginx) do not close an idle connection.

The reference implementation starts from the relay in
`Livestreamed_Immersive_Player/telemetry/relay.js`, which already implements
tokens, sessions, the reference clock and a base path.

## 9. Persistence

Redis holds the recent past (bounded streams). Durable storage is a consumer:

**Archiver.** Reads `device:*:*` streams with consumer group `xrtel-archiver`,
so a restart resumes where it stopped, and writes to:

- **PostgreSQL / TimescaleDB**, one long table:
  `(srv_ts, timestamp, session, participant, device, signal, fields jsonb)`,
  hypertable on `srv_ts` *(open: wide tables per signal for heavy ones)*.
- **Parquet** files, partitioned `session=/signal=/device=`, for datasets.

## 10. Security and privacy

- Pose, gaze and physiological signals tied to a person are **personal data**
  (and physiology is health-related). Pseudonymous ids only; the mapping to
  identities stays outside XR-telemetry.
- Redis is never exposed to the Internet. A Redis in a trusted LAN may run
  without auth (current practice); anything else goes through the gateway or
  uses Redis ACLs and TLS.
- Host cloud components in the EU.
- The gateway does not log query strings (tokens).

## 11. Conformance of existing projects

**Physiological-Signals** — conforms as is: `device:{serial}:{signal}`,
PUBLISH + XADD, flat payload with `device` and `timestamp`, `MAXLEN` 20 000.
Gains, without changing anything: XR poses under the same pattern, durable
storage via the archiver. Optional additions: `seq`, `participant`, and
`srv_ts` once direct producers can get a reference clock (§6).

**Livestreamed Immersive Player** — today publishes to its own relay with a
compact wire format (`p`, `q`, `z`, `f`, `mt`, `rec`). Mapping: `p`→`px py pz`,
`q`→`qx qy qz qw`, `gaze`→`gx gy gz`, `mt`→`media_t`, `srv`→`srv_ts`;
`z` (zoom) and `f` (focused source) and `rec` (voice recording id) stay as
project fields on `pose` or become `event`s *(open)*.

## 12. Open questions

1. Retire `engagement:session_id` in favour of `session:current`, or keep both?
2. Reference clock for producers that write to Redis directly (§6).
3. Per-signal JSON Schemas and whether the gateway validates.
4. Archiver table shape for high-rate signals.
5. Namespace by key prefix or by Redis database number.
6. Binary encoding (MessagePack) on the WebSocket for high rates.
