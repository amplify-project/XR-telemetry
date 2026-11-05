# Architecture

How the pieces fit, what happens to one sample from a headset to a CSV, and
why each piece exists. The normative rules are in the [specification](spec.md);
this page explains the reference implementation.

## The pieces

```mermaid
flowchart LR
  subgraph producers[Producers]
    quest["WebXR player<br/>(Quest, PC browser)<br/>JS SDK"]
    other["Unity · Python · Node<br/>any SDK"]
    wear["Wearables<br/>Physiological-Signals"]
  end

  subgraph server[One host · Docker Compose]
    gw["Gateway<br/>WebSocket + HTTP<br/>tokens · sessions · clock"]
    redis[("Redis<br/>Pub/Sub — live<br/>Streams — recent history")]
    ar["Archiver<br/>consumer group"]
    db[("SQLite<br/>every session")]
  end

  subgraph consumers[Consumers]
    ops["Operator console /ui/<br/>xrtel CLI"]
    live["Dashboards · AR views<br/>any WebSocket client"]
    direct["Python · analysis<br/>Physiological-Signals"]
  end

  quest -- "wss /ingest" --> gw
  other -- "wss /ingest" --> gw
  wear -- "PUBLISH + XADD<br/>(no gateway)" --> redis
  gw -- "PUBLISH + XADD" --> redis
  redis -- "PSUBSCRIBE" --> gw
  redis -- "XREADGROUP" --> ar --> db
  gw -- "wss /consume" --> live
  gw -- "/consume · /session · /mark · /sessions" --> ops
  gw -. "/sessions… proxied" .-> ar
  redis -- "SUBSCRIBE · XRANGE" --> direct
```

| Piece | Why it exists |
|---|---|
| **Redis** | The meeting point. Physiological-Signals already publishes there, so anything that follows the key space ([spec §3](spec.md#3-key-space)) interoperates with it without code changes. Pub/Sub fans samples out live; Streams keep the recent past. |
| **Gateway** | Browsers and WebXR cannot open a Redis connection, and Redis must never face the Internet. The gateway is the only public door: it checks tokens, stamps every sample on one reference clock, and exposes the operator API. |
| **Archiver** | Redis streams are capped (~20 000 samples, about 16 minutes of pose at 20 Hz). The archiver is what makes a test durable. |
| **Console and CLI** | So that running a test means typing a name, not touching Redis or WebSockets. |

The gateway and the archiver are the same Docker image started with two
commands (`bin/xrtel-gateway.js`, `bin/xrtel-archiver.js`).

## One sample, end to end

```mermaid
sequenceDiagram
  autonumber
  participant P as Headset (SDK)
  participant G as Gateway
  participant R as Redis
  participant C as Live consumers
  participant A as Archiver
  participant S as SQLite

  P->>G: WebSocket upgrade /ingest?token=…
  alt wrong token
    G-->>P: HTTP 401, no socket
  else
    G-->>P: 101 Switching Protocols
  end
  P->>G: {hello: "quest-07", meta: {kind, frame, participant}}
  G->>R: HSET device:quest-07:meta · participant mapping
  loop every 2 s
    G->>P: {ping: t1}
    P->>G: {pong: t1, c}
    Note over G: clock offset, best of last 16
  end
  P->>G: {s: "pose", b: [{t, w, px…qw, media_t}, …]} every 100 ms
  Note over G: srv_ts = t + offset<br/>session = own, else session:current
  G->>R: PUBLISH + XADD device:quest-07:pose
  G->>R: SET device:quest-07:alive EX 5
  R-->>G: pmessage (PSUBSCRIBE device:*:*)
  G-->>C: {type: "sample", …}
  R-->>A: XREADGROUP xrtel-archiver
  A->>S: INSERT … COMMIT
  A->>R: XACK
```

Consumers are fed **from Redis, not from the producer**. A headset behind the
gateway and a wristband writing to Redis directly therefore reach every
consumer the same way, including those of another gateway on the same Redis.

## Time

Each machine has its own clock, and a headset's wall clock can be seconds off.
Every sample therefore carries up to three times:

```mermaid
flowchart LR
  t["t — producer monotonic ms<br/>intervals within one device"]
  w["timestamp — producer wall clock<br/>compatibility, coarse"]
  srv["srv_ts — gateway clock<br/>comparable across devices<br/>± srv_err_ms"]
  t -- "plus offset<br/>Cristian, min-RTT filter" --> srv
  w -. "fallback when srv_ts is missing<br/>(wearables writing to Redis)" .-> srv
```

```mermaid
sequenceDiagram
  participant G as Gateway clock
  participant P as Producer monotonic clock
  G->>P: ping (sent at t1)
  Note right of P: reads its clock: c
  P->>G: pong c (arrives at t2)
  Note over G: rtt = t2 − t1<br/>offset = t1 + rtt/2 − c<br/>keep the exchange with the smallest rtt<br/>among the last 16 · error ≤ rtt/2
```

A fast exchange had little time to be asymmetric, so the smallest round trip
gives the tightest bound; the sliding window follows oscillator drift. The
end-to-end test checks that a producer whose clock is five hours off lands on
the right `srv_ts`.

### Wearables writing to Redis directly

They carry only `timestamp`, their machine's wall clock, which nobody keeps in
step with the gateway. A PC clock off by seconds is common. Two remedies:

- **The bridge** (`tools/bridge.mjs`, [partners](partners.md#physiological-signals-bridge))
  turns them into gateway producers. Every sample gets `srv_ts`.
- **The clock check**, done by the archiver for every device without `srv_ts`.
  It needs nothing from the producer.

```mermaid
flowchart LR
  d["per device and session:<br/>arrival in Redis − timestamp"] --> q["5th percentile<br/>(fastest deliveries)"]
  q --> neg{"below −50 ms?"}
  neg -- yes --> ahead["clock AHEAD, for certain:<br/>a sample cannot arrive<br/>before it was taken"]
  neg -- no --> small{"below 250 ms?"}
  small -- yes --> ok["in step: the difference is delivery delay"]
  small -- no --> late["clock behind or slow delivery<br/>(indistinguishable)"]
  ahead --> fix["t_ref = timestamp + correction"]
  late --> fix
  ok --> keep["t_ref = timestamp"]
```

Arrival is the Redis server's clock, which is the reference clock when Redis
and the gateway run on one host, as in the reference deployment. The check is
shown per session in the console and in `xrtel show`, with a status of `ok`,
`check` or `off`. The export's `t_ref` column applies the correction, so
samples from different machines interleave correctly.

The correction is good to about the fastest delivery from the device to Redis:
tens of milliseconds on a LAN. That is far below the physiological latencies it
is used to align: EDA responds 1–3 s after a stimulus.

## Sessions

A session is one test. **Whoever runs the test names it, at the moment they
choose**, from the console or with `xrtel start`. Headsets do not need a new
URL per test.

```mermaid
stateDiagram-v2
  direction LR
  [*] --> Idle
  Idle --> Running: start pilot-03
  Running --> Running: mark song-2
  Running --> Idle: stop
  note right of Running
    start: SET session:current, meta.started, event session_start
    mark: event from source operator
    stop: DEL session:current, meta.ended, event session_stop
    start while running: switches to the new name
  end note
```

Which session a sample is filed under:

```mermaid
flowchart TD
  s[sample arrives] --> own{"has its own session?<br/>?session= on the producer URL"}
  own -- yes --> keep[that session]
  own -- no --> cur{"session:current set?"}
  cur -- yes --> run[the running session]
  cur -- no --> none["_none"]
```

A producer that names its own session keeps it, so a fixed installation and an
experiment can share one deployment. The gateway applies the rule for its
producers when it writes. The archiver applies it again when it reads, for
producers that write to Redis directly.

Physiological-Signals' engagement PC has its own session name,
`engagement:session_id`. It is read as a further fallback after
`session:current` and never written ([partners](partners.md#session-names-engagementsession_id)).

Session boundaries and marks are ordinary `event` samples from a source called
`operator`. They are on the same timeline as everything else, live and in the
export.

## Archive

```mermaid
flowchart TD
  scan["SCAN for streams device:{src}:{signal}<br/>every 3 s"] --> new{new stream?}
  new -- yes --> grp["XGROUP CREATE … xrtel-archiver 0<br/>(keeps what Redis still holds)"]
  new -- no --> read
  grp --> read
  start([start / restart]) --> pend["XREADGROUP … 0<br/>pending: delivered, never acked"]
  pend --> read["XREADGROUP … > BLOCK 1 s<br/>up to 1000 entries"]
  read --> sess["session: own · else session:current · else _none"]
  sess --> ts["ts: srv_ts · else timestamp · else stream-id time<br/>(ignored if more than 1 h from arrival)"]
  ts --> ins["INSERT OR IGNORE<br/>unique (device, signal, stream_id)"]
  ins --> commit[COMMIT]
  commit --> ack[XACK]
  ack --> read
```

- **Nothing is lost while it runs.** A sample is acknowledged only after the
  transaction commits. After a crash, the pending entries are read again first,
  and the unique key makes a second insert harmless.
- **It catches up after downtime**, as long as it was down for less than the
  Redis window (~16 minutes at 20 Hz per stream).

```mermaid
erDiagram
  samples {
    text session
    text device
    text signal
    real ts "best capture time, s"
    real srv_ts
    real timestamp
    text stream_id "Redis id, for idempotency"
    text fields "whole sample, JSON"
  }
  session_streams {
    text session PK
    text device PK
    text signal PK
    int n
    real first_ts
    real last_ts
  }
  session_streams ||--o{ samples : "summarises"
```

One long table keeps every signal's own fields without a schema per signal. The
`session_streams` summary makes the session list and rates instant even with
millions of rows.

**Retention.** Every hour the archiver deletes whole sessions older than
`ARCHIVE_RETENTION_DAYS` (30 by default), and, if `ARCHIVE_MAX_MB` is set, the
oldest finished sessions while the file is over it. It then runs `VACUUM`,
because SQLite reuses freed pages but does not return them to the disk. See
[Deployment → Data, disk and backups](deployment.md#data-disk-and-backups).

## Gateway HTTP

```mermaid
flowchart TD
  req[request under BASE_PATH] --> h{path}
  h -- "/health" --> health[JSON status · no auth]
  h -- "/ · /ui/" --> page[operator console · no auth<br/>holds no secret]
  h -- "upgrade /ingest" --> tin{ingest token?}
  h -- "upgrade /consume" --> tout{consume token?}
  h -- "/session · /mark · /sessions…" --> tapi{"consume token?<br/>Bearer header or ?token="}
  tin -- no --> r401[401]
  tout -- no --> r401
  tapi -- no --> r401
  tin -- yes --> prod[producer socket]
  tout -- yes --> cons["consumer socket<br/>snapshot, then samples"]
  tapi -- yes --> which{which}
  which -- "/session, /mark" --> self[handled by the gateway<br/>writes to Redis]
  which -- "/sessions…" --> proxy[proxied to the archiver<br/>token stripped]
  h -- other --> r404["404 that names the base path"]
```

**Two tokens, two trust levels.** The ingest token ships inside the web player,
so anyone can read it. It only lets a client publish, and it deters bots. The
consume token is the real secret: it reads everything, starts and stops
sessions, and downloads data.

## Redis key space

```mermaid
flowchart LR
  subgraph per[per source]
    sig["device:{src}:{signal}<br/>channel + stream"]
    meta["device:{src}:meta<br/>hash"]
    alive["device:{src}:alive<br/>TTL 5 s"]
    part["device:{src}:participant"]
  end
  subgraph global[deployment]
    cur["session:current<br/>(+ engagement:session_id)"]
    smeta["session:{id}:meta<br/>started · ended · note"]
    pmap["participant:{pid}<br/>role → src"]
  end
  part <--> pmap
```

An optional namespace prefix (`amplify:device:…`) isolates several projects on
one Redis.
