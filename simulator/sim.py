"""VitalStream device simulator: sends simulated vital-sign readings to api-service over gRPC.

Usage (from the simulator/ folder, after ./generate.sh):
    .venv/bin/python sim.py --create 3               # register 3 new devices via REST, then simulate them
    .venv/bin/python sim.py --devices 4 --duration 10
    .venv/bin/python sim.py --devices 4 --mode unary
    .venv/bin/python sim.py --help

Each device runs in its own thread. Two modes:
  stream (default): the device opens one StreamReadings stream and keeps sending on it, reading acks
                    as they come back. It doesn't wait for one ack before sending the next reading.
  unary:            one SendReading call per reading; each call waits for its answer before the next.
All threads share one gRPC channel: a single HTTP/2 connection that carries every device's calls and
streams at once, so 100 devices don't need 100 connections.

Reliability (turn off with --no-retry / --no-keepalive to see the difference):
  - Every reading gets a reading_id (UUID) when it's measured. Resending it never creates a duplicate:
    the server uses it as the event id, and the processor stores each event id once.
  - Stream mode: readings sent on a stream that breaks before they're acknowledged are resent on the
    next stream; so are readings the server rejected with UNAVAILABLE (e.g. Kafka briefly down).
  - Unary mode: gRPC's built-in retry policy retries UNAVAILABLE with exponential backoff.
  - Keepalive pings detect a connection that silently stopped responding (hung server, network cut)
    instead of waiting on it forever.
"""

import argparse
import itertools
import json
import random
import statistics
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from collections import Counter, deque
from datetime import date, datetime, timezone
from pathlib import Path

import grpc

# The generated code lives in generated/ (created by generate.sh) and imports itself as
# "vitalstream.ingest.v1...", so that folder has to be on the import path.
sys.path.insert(0, str(Path(__file__).parent / "generated"))
from vitalstream.ingest.v1 import ingest_pb2, ingest_pb2_grpc  # noqa: E402

from vitals import METRICS_BY_DEVICE_TYPE, DeviceSignal  # noqa: E402

# gRPC's built-in retries, configured per method as a "service config". Applies to unary calls only:
# a stream can't be replayed automatically once messages have been sent on it (that's handled by hand).
# 5 attempts with waits of about 1s, 2s, 4s, 8s (randomised) covers a server restart of ~10s.
RETRY_SERVICE_CONFIG = json.dumps({
    "methodConfig": [{
        "name": [{"service": "vitalstream.ingest.v1.IngestService", "method": "SendReading"}],
        "retryPolicy": {
            "maxAttempts": 5,
            "initialBackoff": "1s",
            "maxBackoff": "8s",
            "backoffMultiplier": 2,
            "retryableStatusCodes": ["UNAVAILABLE"],
        },
    }]
})


# ---------- REST helpers (device registration lives in the REST API, not in gRPC) ----------

def rest(api: str, method: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(api + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as resp:
        return json.loads(resp.read())


def create_devices(api: str, count: int, rng: random.Random) -> list[dict]:
    """Create one simulated patient and `count` devices of assorted types for them."""
    tag = uuid.uuid4().hex[:8]
    patient = rest(api, "POST", "/api/patients", {
        "mrn": f"SIM-{tag}",
        "firstName": "Sim",
        "lastName": f"Patient-{tag}",
        "dateOfBirth": date(rng.randint(1940, 2005), rng.randint(1, 12), rng.randint(1, 28)).isoformat(),
    })
    types = list(METRICS_BY_DEVICE_TYPE)
    devices = []
    for i in range(count):
        device_type = types[i % len(types)]
        devices.append(rest(api, "POST", "/api/devices", {
            "serialNumber": f"SIM-{tag}-{i}",
            "deviceType": device_type,
            "firmwareVersion": "sim-1.0",
            "patientId": patient["id"],
        }))
    print(f"Created patient {patient['id']} with devices "
          + ", ".join(f"{d['id']} ({d['deviceType']})" for d in devices))
    return devices


# ---------- the simulation ----------

class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.results = Counter()      # final outcome per reading: OK, NOT_FOUND, LOST, ...
        self.latencies_ms = []        # measured -> acknowledged, for accepted readings (includes retries)
        self.stream_breaks = Counter()
        self.resends = 0              # times a reading was sent again

    def add(self, outcome: str, latency_s: float | None = None, count: int = 1):
        with self.lock:
            self.results[outcome] += count
            if latency_s is not None:
                self.latencies_ms.append(latency_s * 1000)

    def add_break(self, code: str):
        with self.lock:
            self.stream_breaks[code] += 1

    def add_resends(self, count: int):
        with self.lock:
            self.resends += count


class Reading:
    """One measurement, with the request that carries it. The request (and so its reading_id) is built
    once, when the reading is taken, and reused unchanged for every resend."""

    def __init__(self, device: dict, metric: str, value: float):
        self.metric = metric
        self.value = value
        self.taken = time.monotonic()
        self.request = ingest_pb2.SendReadingRequest(
            device_id=device["id"],
            metric=f"METRIC_{metric}",
            value=value,
            measured_at=datetime.now(timezone.utc),
            reading_id=str(uuid.uuid4()),
        )


def report(device: dict, reading: Reading, outcome: str, quiet: bool):
    if not quiet:
        print(f"{datetime.now():%H:%M:%S}  device {device['id']:>3}  {reading.metric:<12} "
              f"{reading.value:>7}  {outcome}")


def run_unary(stub, device: dict, interval: float, stop: threading.Event, stats: Stats,
              rng: random.Random, quiet: bool, retry: bool):
    signal = DeviceSignal(device["deviceType"], rng)
    # Start devices at slightly different moments so they don't all fire in lockstep.
    stop.wait(rng.uniform(0, interval))
    while not stop.is_set():
        for metric, value in signal.next():
            reading = Reading(device, metric, value)
            try:
                # timeout = gRPC "deadline" for the whole call, retries included: if there's no answer by
                # then the call fails with DEADLINE_EXCEEDED instead of waiting forever.
                response = stub.SendReading(reading.request, timeout=20 if retry else 5)
                stats.add("OK", time.monotonic() - reading.taken)
                outcome = f"ok  event {response.event_id[:8]}"
            except grpc.RpcError as e:
                # Every gRPC failure carries a status code plus a human-readable message.
                stats.add(e.code().name)
                outcome = f"ERR {e.code().name}: {e.details()}"
            report(device, reading, outcome, quiet)
        stop.wait(interval)


def run_stream(stub, device: dict, interval: float, stop: threading.Event, stats: Stats,
               rng: random.Random, quiet: bool, max_in_flight: int, retry: bool):
    signal = DeviceSignal(device["deviceType"], rng)
    stop.wait(rng.uniform(0, interval))
    sequence = itertools.count(1)
    # Readings whose outcome is unknown or that failed temporarily; sent again before any new reading.
    resend: deque[Reading] = deque()

    while not stop.is_set():
        # Readings sent on this stream but not acknowledged yet: sequence -> Reading.
        pending: dict[int, Reading] = {}
        # At most max_in_flight unacknowledged readings. Without a cap, a fast sender fills every buffer
        # between here and the server, and each reading then waits behind all of them (high latency).
        slots = threading.BoundedSemaphore(max_in_flight)
        # The generator below runs on a gRPC thread and can still be running for a moment after the stream
        # breaks. `closed` + `guard` make sure it can't take a reading off the resend queue after we've
        # collected the unacknowledged ones, which would silently lose that reading.
        closed = threading.Event()
        guard = threading.Lock()

        def outgoing():
            """gRPC pulls from this generator on its own thread and sends each item as it's produced."""
            while not stop.is_set() and not closed.is_set():
                resending = bool(resend)
                queue = resend if resending else deque(Reading(device, m, v) for m, v in signal.next())
                while queue:
                    while not slots.acquire(timeout=0.2):   # wait for an ack to free a slot
                        if stop.is_set() or closed.is_set():
                            return
                    with guard:
                        if closed.is_set():
                            return
                        reading = queue.popleft()
                        n = next(sequence)
                        pending[n] = reading
                    yield ingest_pb2.StreamReadingsRequest(sequence=n, reading=reading.request)
                if not resending:
                    stop.wait(interval)
            # Returning ends our side of the stream; the server then finishes sending acks and closes.

        try:
            # One call, open until we stop: iterating over it yields acks as the server sends them.
            for ack in stub.StreamReadings(outgoing()):
                reading = pending.pop(ack.sequence)
                slots.release()
                if ack.WhichOneof("result") == "event_id":
                    stats.add("OK", time.monotonic() - reading.taken)
                    outcome = f"ok  event {ack.event_id[:8]}"
                elif ack.error.code == "UNAVAILABLE" and retry:
                    resend.append(reading)
                    stats.add_resends(1)
                    outcome = f"will resend ({ack.error.code})"
                else:
                    stats.add(ack.error.code)
                    outcome = f"ERR {ack.error.code}: {ack.error.message}"
                report(device, reading, outcome, quiet)
            closed.set()   # stream ended normally (we stopped)
        except grpc.RpcError as e:
            # The whole stream failed. Readings still in `pending` never got an ack: the server may or may
            # not have stored them. With reading ids, resending is safe either way.
            stats.add_break(e.code().name)
            with guard:
                closed.set()
                unacked = list(pending.values())
                if retry:
                    # Resend everything outstanding, oldest first, so the device's readings stay in order.
                    waiting = sorted(unacked + list(resend), key=lambda r: r.taken)
                    resend.clear()
                    resend.extend(waiting)
            if retry:
                stats.add_resends(len(unacked))
                action = f"{len(unacked)} unacknowledged reading(s) will be resent"
            else:
                stats.add("LOST", count=len(unacked))
                action = f"{len(unacked)} unacknowledged reading(s) lost"
            print(f"{datetime.now():%H:%M:%S}  device {device['id']:>3}  stream broke: {e.code().name} "
                  f"({e.details()}); {action}; reconnecting in 2s")
            stop.wait(2)

    if resend:
        stats.add("NOT_SENT_BEFORE_STOP", count=len(resend))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--grpc", default="localhost:9090", help="api-service gRPC address")
    parser.add_argument("--api", default="http://localhost:8080", help="api-service REST address")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--devices", help="comma-separated ids of existing devices, e.g. 4,6")
    source.add_argument("--create", type=int, metavar="N", help="register N new devices first")
    parser.add_argument("--mode", choices=["stream", "unary"], default="stream", help="how readings are sent")
    parser.add_argument("--max-in-flight", type=int, default=8,
                        help="stream mode: max unacknowledged readings per device (limits queueing latency)")
    parser.add_argument("--interval", type=float, default=1.0,
                        help="seconds between readings per device (0 = as fast as possible)")
    parser.add_argument("--duration", type=float, default=0, help="stop after this many seconds (0 = until Ctrl+C)")
    parser.add_argument("--no-retry", dest="retry", action="store_false", help="don't resend or retry readings")
    parser.add_argument("--no-keepalive", dest="keepalive", action="store_false",
                        help="don't send keepalive pings (a hung connection then goes unnoticed)")
    parser.add_argument("--seed", type=int, help="random seed, for repeatable runs")
    parser.add_argument("--quiet", action="store_true", help="only print the summary")
    args = parser.parse_args()

    rng = random.Random(args.seed)
    try:
        if args.create:
            devices = create_devices(args.api, args.create, rng)
        else:
            devices = [rest(args.api, "GET", f"/api/devices/{int(i)}") for i in args.devices.split(",")]
    except urllib.error.HTTPError as e:  # the API answered, but with an error (e.g. 404 unknown device)
        sys.exit(f"REST API error {e.code} for {e.url}: {e.read().decode()[:200]}")
    except urllib.error.URLError as e:   # no answer at all
        sys.exit(f"Could not reach the REST API at {args.api}: {e.reason}")

    options = []
    if args.keepalive:
        # Ping the server every 5s while calls are open; if a ping isn't answered within 3s, treat the
        # connection as dead and fail its calls with UNAVAILABLE. Without this, a server that hangs (or a
        # network path that silently drops packets) leaves streams waiting forever. The server must allow
        # pings this often (spring.grpc.server.keep-alive.permit-time).
        # In current gRPC Core the wait for a ping's answer is governed by grpc.http2.ping_timeout_ms
        # (default 60s); keepalive_timeout_ms alone didn't fail a frozen connection in testing, so set both.
        options += [
            ("grpc.keepalive_time_ms", 5000),
            ("grpc.keepalive_timeout_ms", 3000),
            ("grpc.http2.ping_timeout_ms", 3000),
        ]
    if args.retry:
        options += [("grpc.enable_retries", 1), ("grpc.service_config", RETRY_SERVICE_CONFIG)]
    else:
        options += [("grpc.enable_retries", 0)]
    channel = grpc.insecure_channel(args.grpc, options=options)  # plaintext, no TLS (fine for local dev)
    stub = ingest_pb2_grpc.IngestServiceStub(channel)

    stop = threading.Event()
    stats = Stats()

    def device_args(d):
        base = (stub, d, args.interval, stop, stats, random.Random(rng.random()), args.quiet)
        return base + ((args.max_in_flight, args.retry) if args.mode == "stream" else (args.retry,))

    run_device = run_stream if args.mode == "stream" else run_unary
    threads = [threading.Thread(target=run_device, daemon=True, args=device_args(d)) for d in devices]
    print(f"Simulating {len(devices)} device(s) -> {args.grpc} in {args.mode} mode, every {args.interval}s "
          f"(retry {'on' if args.retry else 'off'}, keepalive {'on' if args.keepalive else 'off'}). "
          "Ctrl+C to stop.")
    started = time.monotonic()
    for t in threads:
        t.start()
    try:
        while not stop.is_set():
            if args.duration and time.monotonic() - started >= args.duration:
                break
            time.sleep(0.2)
    except KeyboardInterrupt:
        pass
    stop.set()
    for t in threads:
        t.join(timeout=10)
    channel.close()

    elapsed = time.monotonic() - started
    total = sum(stats.results.values())
    print(f"\nTook {total} readings in {elapsed:.1f}s ({total / elapsed:.1f}/s): "
          + ", ".join(f"{k} {v}" for k, v in stats.results.most_common()))
    if stats.resends:
        print(f"Resent {stats.resends} time(s) (same reading_id each time, so no duplicates are stored)")
    if len(stats.latencies_ms) >= 2:
        q = statistics.quantiles(stats.latencies_ms, n=100)
        print(f"Latency until acknowledged: median {q[49]:.1f} ms, p99 {q[98]:.1f} ms")
    if stats.stream_breaks:
        print("Streams broken: " + ", ".join(f"{k} {v}" for k, v in stats.stream_breaks.items()))


if __name__ == "__main__":
    main()
