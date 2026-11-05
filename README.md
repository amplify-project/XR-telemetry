# XR-telemetry

Interoperable telemetry for extended-reality experiences: head pose and gaze
from headsets, physiological signals from wearables, and events from any
platform, published to one place, read **live** and **afterwards**, on one time
axis.

Developed in the [AMPLIFY](https://github.com/amplify-project) project.

> **Status: v0.1, working prototype.** Gateway, JavaScript SDK and examples run
> end to end against Redis; the specification is still a draft.

## The idea

XR-telemetry is a **convention over Redis**, plus a few small pieces:

```
 WebXR / browser ──WS──▶ gateway ──┐
 Unity / Python ───────────────────┼──▶ Redis  device:{src}:{signal}   Pub/Sub (live) + Streams (history)
 wearables, CV pipelines ──────────┘                 │
                                    ┌────────────────┼──────────────────┐
                              live consumers     archiver            any service
                              (render, AR)    → PostgreSQL / Parquet   in any language
```

- **Key space and sample format** — anything that follows it interoperates,
  whatever language or platform it runs on.
- **One time axis** — a reference clock with a measured error bound, so a
  headset and a wristband on different machines can be aligned.
- **Explicit coordinate frames** on every pose sample (`local-floor`, `room`,
  `screen`).
- **Gateway** — WebSocket front for browsers/WebXR and for deployments where
  Redis must not be exposed; tokens and sessions.
- **Archiver** — durable storage for analysis and datasets.

The AMPLIFY Physiological-Signals stack already follows the key space, so it
interoperates with no change.

## Try it

Needs Docker and Node ≥ 22.

```bash
cd deploy && cp .env.example .env && docker compose up -d --build   # Redis (:6380) + gateway (:8090)
curl localhost:8090/health
cd ../gateway && npm install && cd ..
node tools/simulate.mjs --headsets 3 --physio 2 --redis redis://127.0.0.1:6380
```

Three simulated headsets publish poses and events through the gateway with the
JS SDK; two simulated wearables write straight to Redis the way
Physiological-Signals does. Then read everything, from any language:

```bash
python examples/python/consume.py --redis redis://127.0.0.1:6380                 # live
python examples/python/consume.py --redis redis://127.0.0.1:6380 --replay pose   # history
```

or over WebSocket at `ws://localhost:8090/consume` (`?session=`, `?signals=pose,event`).

**Existing Livestreamed Immersive Player**: point it at the gateway with
`?telemetry=ws://<host>:8090/ingest` — its current wire format is accepted as is
and mapped to spec `pose` samples.

**Tests**: `cd gateway && npm test` (starts a throwaway Redis in Docker).

## Layout

| Path | What |
|---|---|
| `docs/spec.md` | The specification |
| `gateway/` | WebSocket ↔ Redis gateway (Node): tokens, sessions, reference clock, snapshot, presence |
| `sdk/js/` | Browser / WebXR / Node SDK: `pose()`, `event()`, `send()` |
| `tools/simulate.mjs` | Fake headsets (via gateway) and wearables (direct to Redis) |
| `examples/python/` | Reading live and history straight from Redis |
| `deploy/` | Docker Compose: Redis + gateway |

## Documents

- [`docs/spec.md`](docs/spec.md) — the specification (draft v0.1)

## Roadmap

1. Specification (this draft) — review with the projects that will use it
2. ✅ Gateway (from the Livestreamed Immersive Player relay) + Redis backend — prototype
3. ✅ JavaScript/WebXR SDK — prototype
4. Archiver (PostgreSQL/TimescaleDB, Parquet)
5. Python and Unity SDKs
6. Evaluation: end-to-end latency, clock error, throughput, loss under disconnection

## License

[Apache-2.0](LICENSE).
