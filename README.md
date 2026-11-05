# XR-telemetry

Interoperable telemetry for extended-reality experiences: head pose and gaze
from headsets, physiological signals from wearables, and events from any
platform, published to one place, read **live** and **afterwards**, on one time
axis.

Developed in the [AMPLIFY](https://github.com/amplify-project) project.

> **Status: early design.** The specification is a draft; there is no code yet.

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

## Documents

- [`docs/spec.md`](docs/spec.md) — the specification (draft v0.1)

## Roadmap

1. Specification (this draft) — review with the projects that will use it
2. Gateway (from the Livestreamed Immersive Player relay) + Redis backend
3. JavaScript/WebXR SDK
4. Archiver (PostgreSQL/TimescaleDB, Parquet)
5. Python and Unity SDKs
6. Evaluation: end-to-end latency, clock error, throughput, loss under disconnection

## License

[Apache-2.0](LICENSE).
