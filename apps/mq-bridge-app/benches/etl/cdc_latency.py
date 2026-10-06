#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["psycopg[binary]>=3.1"]
# ///
"""End-to-end CDC latency: committed in Postgres -> line landed in the route's sink.

Commits single-row transactions at a fixed rate (open loop) and tails the file a
`postgres_cdc -> file` route writes. Both timestamps come from this process's own
monotonic clock, so no clock is compared across the Docker boundary.

Two latencies are reported per event:
  from_ack   sink arrival minus the moment the client saw the COMMIT acknowledged
  from_send  sink arrival minus the moment the INSERT was sent (includes the commit)
"""
import argparse
import re
import statistics
import threading
import time

import psycopg

ID_RE = re.compile(rb'^\{"id":(\d+),')


def tail(path, arrivals, stop):
    with open(path, "rb") as f:
        pending = b""
        while not stop.is_set():
            chunk = f.read(1 << 16)
            if not chunk:
                time.sleep(0.0001)
                continue
            now = time.perf_counter()
            pending += chunk
            *lines, pending = pending.split(b"\n")
            for line in lines:
                m = ID_RE.match(line)
                if m:
                    arrivals[int(m.group(1))] = now


def commit_paced(conn, table, count, rate, payload, sent, acked):
    """Commits `count` single-row transactions at `rate` per second; returns the elapsed time."""
    insert = f"INSERT INTO {table} (payload, ins_ts) VALUES (%s, 0) RETURNING id"
    start = time.perf_counter()
    for i in range(count):
        delay = start + i / rate - time.perf_counter()
        if delay > 0:
            time.sleep(delay)
        t_send = time.perf_counter()
        row_id = conn.execute(insert, (payload,)).fetchone()[0]
        acked[row_id] = time.perf_counter()
        sent[row_id] = t_send
    return time.perf_counter() - start


def quantiles_ms(values):
    values = sorted(values)
    pick = lambda q: values[min(len(values) - 1, int(q * len(values)))] * 1000
    return pick(0.50), pick(0.95), pick(0.99), values[-1] * 1000


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pg-url", required=True)
    ap.add_argument("--table", default="cdc_src")
    ap.add_argument("--sink", required=True, help="file the route writes change events to")
    ap.add_argument("--bytes", type=int, default=256, help="payload size per row")
    ap.add_argument("--count", type=int, default=20000)
    ap.add_argument("--warmup", type=int, default=2000)
    ap.add_argument("--rate", type=float, default=1000, help="commits per second")
    ap.add_argument("--settle", type=float, default=10, help="seconds to wait for stragglers")
    args = ap.parse_args()

    payload = ('{"pad":"' + "x" * args.bytes)[: args.bytes - 2] + '"}'
    arrivals, sent, acked = {}, {}, {}
    stop = threading.Event()
    reader = threading.Thread(target=tail, args=(args.sink, arrivals, stop), daemon=True)
    reader.start()

    with psycopg.connect(args.pg_url, autocommit=True) as conn:
        commit_paced(conn, args.table, args.warmup, args.rate, payload, {}, {})
        elapsed = commit_paced(conn, args.table, args.count, args.rate, payload, sent, acked)

    deadline = time.perf_counter() + args.settle
    while len(arrivals.keys() & sent.keys()) < len(sent) and time.perf_counter() < deadline:
        time.sleep(0.05)
    stop.set()
    reader.join()

    landed = [i for i in sent if i in arrivals]
    if len(landed) != len(sent):
        raise SystemExit(f"only {len(landed)} of {len(sent)} events reached the sink")
    from_ack = quantiles_ms([arrivals[i] - acked[i] for i in landed])
    from_send = quantiles_ms([arrivals[i] - sent[i] for i in landed])
    commit_ms = statistics.median(acked[i] - sent[i] for i in landed) * 1000
    print(
        f"{args.bytes},{len(landed)},{len(landed) / elapsed:.0f},{commit_ms:.3f},"
        + ",".join(f"{v:.3f}" for v in from_ack[:3] + from_send)
    )


if __name__ == "__main__":
    main()
