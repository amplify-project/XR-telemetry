# For partner projects: what changes

Short answer: **no code changes** for any existing AMPLIFY component. What
changes is where things point, and which token each one carries.

```mermaid
flowchart LR
  subgraph venue[Venue or lab LAN]
    phys["Physiological-Signals<br/>wearables + engagement PC"]
    hub[("their Redis hub<br/>unchanged")]
    bridge["tools/bridge.mjs<br/>new, runs next to the hub"]
  end
  subgraph cloud[Cloud deployment]
    gw[gateway] --> redis[(Redis)] --> ar[archiver]
  end
  quest["Quest / browser<br/>Livestreamed player"]
  unity["Unity render<br/>relay consumer"]
  ana["Analysis<br/>notebooks, R, Python"]

  phys -- "PUBLISH + XADD, as today" --> hub
  hub --> bridge -- "wss /ingest + ingest token" --> gw
  quest -- "wss /ingest + ingest token" --> gw
  gw -- "wss /consume?format=legacy" --> unity
  gw -- "console · xrtel export · /sessions API" --> ana
```

| Partner / component | Code change | What to do |
|---|---|---|
| **Livestreamed Immersive Player** (Quest, browsers) | none | Publish it with the gateway's ingest URL and token (`TELEMETRY_URL` / `TELEMETRY_TOKEN` in `deploy-demo.sh`). Its own wire format is accepted as is. |
| **Physiological-Signals**, on its LAN | none | Keep publishing to the local hub. To get the data into a cloud deployment, run the [bridge](#physiological-signals-bridge) next to the hub. |
| **Physiological-Signals**, sharing the gateway's Redis | none | Point it at that Redis. Its key space already conforms. |
| **Consumers of the old relay** (Unity render, scripts reading `/consume`) | none | Point them at `wss://<host>/telemetry/consume?format=legacy&token=<CONSUME_TOKEN>`: same `snapshot` / `update` / `leave` messages, same field names. |
| **New consumers** | — | Spec format (`/consume`, one `sample` per message, flat fields), the console, `xrtel export`, or Redis directly. |
| **Analysis** | — | CSV/NDJSON per session from the console or `xrtel export`. See [Running a test](running-a-test.md). |
| **New producers** (Unity, Python, other headsets) | write to the gateway or to Redis | [Spec §8](spec.md#8-gateway-websocket) (WebSocket) or §3–§4 (Redis). |

## Physiological-Signals bridge

The wearables and the engagement PC talk to a Redis hub on the venue LAN, found
by mDNS. That stays exactly as it is. The cloud Redis is not reachable from
outside, and must not be.

The bridge reads the hub and forwards every sample to the cloud gateway over
WebSocket with the ingest token, one producer per wearable:

```bash
git clone https://github.com/amplify-project/XR-telemetry.git
cd XR-telemetry/gateway && npm install && cd ..
node tools/bridge.mjs --redis redis://<hub>:6379 \
     --url wss://<host>/telemetry/ingest --token <INGEST_TOKEN> \
     --signals physio_metrics,valence_cont,arousal_cont,engagement,raw_sensors
```

**On the venue's Windows PCs** it is one more launcher next to theirs:
[`examples/physiological-signals/6_START_TELEMETRY_BRIDGE.bat`](../examples/physiological-signals/6_START_TELEMETRY_BRIDGE.bat).
Set the clone path, ingest URL and token at its top, copy it into
`Handover_Oct-Dec_Concerts\`, and start it after `1_START_REDIS.bat`. It checks
Node.js (22 or newer), installs the dependencies the first time and restarts
the bridge if it stops. It is optional: the venue works the same without it.

`--signals` keeps the cloud to what is used there. Without it every
`device:*:*` signal is forwarded, including the GUI feeds `hr_filtered` and
`eda_filtered`. `raw_sensors` is a once-a-second snapshot of the latest value
of each sensor: worth it for battery, temperature and humidity, not for motion
or PPG analysis (the full-rate data stays in the venue's `raw_motion_*.csv`).

**Pub/Sub and streams.** The bridge forwards what is `PUBLISH`ed and, for keys
that are only `XADD`ed (`raw_sensors`), reads the stream. A key that has both is
sent once, from Pub/Sub. Streams are read from the moment the bridge starts; a
new key is picked up within 5 s. Works with Redis 5 (the Physiological-Signals
Windows bundle); `--no-streams` turns the stream side off.

**Crowd engagement.** The engagement GUI publishes the crowd average on the
plain channel `engagement_score` as a bare number. The bridge forwards it as
signal `engagement` of device `engagement-pc` (`--crowd-device` renames it),
with `crowd: true`; it is part of `engagement` for `--signals`. Per-person
engagement (`device:{serial}:engagement`) needs face ID: with `--no-face-id`
only the crowd value exists.

**Timestamps.** The EmotiBit publisher stamps samples with local time as ISO
text (`2026-11-09T13:35:57.632660`, no zone); the bridge reads it as local time
on its own PC, so run it in the venue's time zone (the hub PC is the natural
place). Epoch seconds and ISO with a zone are taken as they are.

What the bridge adds:

- **A reference clock** for the wearables. `srv_ts` is the moment the bridge
  read the sample, on the gateway's clock. The wearable's own `timestamp` is
  kept as is. Today they have no common clock with the headsets.
- **Registry and identity.** `device:{serial}:meta` and the participant mapping
  are carried over.
- **Sessions.** Wearable samples land in the session the operator started,
  next to the headsets.

It does not forward samples that already went through a gateway (they carry
`srv_ts`), so it cannot loop. Headsets on the LAN should use the gateway
directly anyway.

Running it on the engagement PC or the hub machine is enough: it only needs to
reach the hub and the Internet. If the Internet drops, each wearable keeps up
to 2000 samples queued, all its signals together, and sends them on reconnect:
with the five signals above at 1 Hz, about 6 minutes. The venue keeps working
meanwhile; only the cloud copy has a gap after that.

### Testing without wearables: `simulate_emotibits.py`

Physiological-Signals ships `physio/simulate_emotibits.py`, which fakes N
EmotiBits on the same channels as the real publisher. It only `PUBLISH`es; the
real publisher (`multiemotibit_UDP_SD_RFv2.py`) also `XADD`s to the same key.
The archiver reads streams, so:

| Simulator pointed at | Live (`/consume`, console) | Archived in the session |
|---|---|---|
| the gateway's Redis directly | yes | **no** (Pub/Sub only, no stream) |
| its own hub, with the bridge | yes | yes (the bridge forwards Pub/Sub and stream-only keys) |

Test through the bridge, as a venue would run it:

```bash
docker run -d --rm --name physio-hub -p 127.0.0.1:6381:6379 redis:7-alpine
node cli/xrtel.mjs start physio-test
node tools/bridge.mjs --redis redis://127.0.0.1:6381 --url ws://localhost:8090/ingest &
python physio/simulate_emotibits.py --count 3 --port 6381   # in Physiological-Signals; Ctrl+C
node cli/xrtel.mjs stop && node cli/xrtel.mjs show physio-test
```

Expect `physio_metrics`, `hr_filtered` and `eda_filtered` at 1 Hz per device,
on the reference clock. The real publisher, pointed straight at the gateway's
Redis, is archived without the bridge.

## Session names: `engagement:session_id`

The engagement PC sets `engagement:session_id` to its recording folder, and the
wearable writers stamp it into their CSVs as a join key. XR-telemetry
**reads** it and **never writes it**:

- While the operator runs a session (`xrtel start`, console), that name wins.
- Otherwise samples without a session of their own follow
  `engagement:session_id`. A concert started on the engagement PC is then
  archived under the same folder name as its CSVs.
- The console and `xrtel status` show which of the two is in effect.

## Through CloudFront: send a User-Agent

The reference cloud deployment sits behind CloudFront, whose default protection
refuses requests without a `User-Agent` header (HTTP 403). Browsers, the Quest,
`xrtel`, the bridge and Node's built-in WebSocket send one. Some WebSocket
clients do not, for example .NET's `ClientWebSocket` used by Unity libraries:

```csharp
var ws = new ClientWebSocket();
ws.Options.SetRequestHeader("User-Agent", "amplify-unity/1.0");
```

A 403 that the gateway did not produce (`X-Cache: Error from cloudfront`) is
usually this.

## Tokens

| Token | Who gets it | What it allows |
|---|---|---|
| ingest | every producer: the web player (where anyone can read it), the bridge, Unity producers | publish samples |
| consume | operators, consumers, analysts | read live, start/stop sessions, marks, download |

Share the consume token like a password, never in client code.
