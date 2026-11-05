"""Read XR-telemetry straight from Redis — no gateway, no SDK, just redis-py.

    pip install redis
    python consume.py                                  # live: every signal of every device
    python consume.py --signal pose                    # only poses
    python consume.py --signal pose --full             # every field, not a summary
    python consume.py --replay pose --count 5          # history: last samples from the streams

This is all a Python service needs to fuse headset poses with the wearables'
physiology: both follow device:{src}:{signal} (PUBLISH for live, XADD for history).
"""
import argparse
import json

import redis

ap = argparse.ArgumentParser()
ap.add_argument("--redis", default="redis://127.0.0.1:6379")
ap.add_argument("--namespace", default="")
ap.add_argument("--signal", default="*")
ap.add_argument("--replay", metavar="SIGNAL", help="print the last --count samples of each stream of SIGNAL and exit")
ap.add_argument("--count", type=int, default=3)
ap.add_argument("--full", action="store_true", help="print every field of each sample, not a summary")
args = ap.parse_args()

r = redis.Redis.from_url(args.redis, decode_responses=True)
ns = f"{args.namespace}:" if args.namespace else ""

if args.replay:
    for key in sorted(r.scan_iter(f"{ns}device:*:{args.replay}", _type="stream")):
        print(f"── {key}")
        for entry_id, fields in reversed(r.xrevrange(key, count=args.count)):
            # The entry id is the Redis server's clock in ms: a common clock for free (spec §6).
            print(f"  {entry_id}  {fields}")
    raise SystemExit

p = r.pubsub()
p.psubscribe(f"{ns}device:*:{args.signal}")
print(f"listening on {ns}device:*:{args.signal} … Ctrl-C to stop")
for msg in p.listen():
    if msg["type"] != "pmessage":
        continue
    _, src, signal = msg["channel"].rsplit(":", 2)
    data = json.loads(msg["data"])
    if args.full:
        print(f"{src:>16} {signal:<14} {data}")
    elif signal == "pose":
        print(f"{src:>16} pose   p=({data['px']:+.2f},{data['py']:+.2f},{data['pz']:+.2f})  "
              f"q=({data['qx']:+.2f},{data['qy']:+.2f},{data['qz']:+.2f},{data['qw']:+.2f})  "
              f"z={data.get('z', '-')} f={data.get('f', '-')} media_t={data.get('media_t', '-')}  "
              f"srv_ts={data.get('srv_ts')} ±{data.get('srv_err_ms')} ms  {data.get('frame')}  session={data.get('session')}")
    else:
        print(f"{src:>16} {signal:<14} {data}")
