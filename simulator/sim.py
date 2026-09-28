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
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path

import grpc

# The generated code lives in generated/ (created by generate.sh) and imports itself as
# "vitalstream.ingest.v1...", so that folder has to be on the import path.
sys.path.insert(0, str(Path(__file__).parent / "generated"))
from vitalstream.ingest.v1 import ingest_pb2, ingest_pb2_grpc  # noqa: E402

from vitals import METRICS_BY_DEVICE_TYPE, DeviceSignal  # noqa: E402


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
        self.results = Counter()      # outcome per reading: OK, NOT_FOUND, LOST, ...
        self.latencies_ms = []        # send -> answer time, for accepted readings
        self.stream_breaks = Counter()

    def add(self, outcome: str, latency_s: float | None = None, count: int = 1):
        with self.lock:
            self.results[outcome] += count
            if latency_s is not None:
                self.latencies_ms.append(latency_s * 1000)

    def add_break(self, code: str):
        with self.lock:
            self.stream_breaks[code] += 1


def make_request(device: dict, metric: str, value: float) -> ingest_pb2.SendReadingRequest:
    return ingest_pb2.SendReadingRequest(
        device_id=device["id"],
        metric=f"METRIC_{metric}",
        value=value,
        measured_at=datetime.now(timezone.utc),
    )


def report(device: dict, metric: str, value: float, outcome: str, quiet: bool):
    if not quiet:
        print(f"{datetime.now():%H:%M:%S}  device {device['id']:>3}  {metric:<12} {value:>7}  {outcome}")


def run_unary(stub, device: dict, interval: float, stop: threading.Event, stats: Stats,
              rng: random.Random, quiet: bool):
    signal = DeviceSignal(device["deviceType"], rng)
    # Start devices at slightly different moments so they don't all fire in lockstep.
    stop.wait(rng.uniform(0, interval))
    while not stop.is_set():
        for metric, value in signal.next():
            sent = time.monotonic()
            try:
                # timeout = gRPC "deadline": if no answer within 5s the call fails with DEADLINE_EXCEEDED
                # instead of waiting forever.
                response = stub.SendReading(make_request(device, metric, value), timeout=5)
                stats.add("OK", time.monotonic() - sent)
                outcome = f"ok  event {response.event_id[:8]}"
            except grpc.RpcError as e:
                # Every gRPC failure carries a status code plus a human-readable message.
                stats.add(e.code().name)
                outcome = f"ERR {e.code().name}: {e.details()}"
            report(device, metric, value, outcome, quiet)
        stop.wait(interval)


def run_stream(stub, device: dict, interval: float, stop: threading.Event, stats: Stats,
               rng: random.Random, quiet: bool, max_in_flight: int):
    signal = DeviceSignal(device["deviceType"], rng)
    stop.wait(rng.uniform(0, interval))
    sequence = itertools.count(1)

    while not stop.is_set():
        # Readings sent on this stream but not acknowledged yet: sequence -> (metric, value, send time).
        pending = {}
        # At most max_in_flight unacknowledged readings. Without a cap, a fast sender fills every buffer
        # between here and the server, and each reading then waits behind all of them (high latency).
        slots = threading.BoundedSemaphore(max_in_flight)

        def outgoing():
            """gRPC pulls from this generator on its own thread and sends each item as it's produced."""
            while not stop.is_set():
                for metric, value in signal.next():
                    while not slots.acquire(timeout=0.2):   # wait for an ack to free a slot
                        if stop.is_set():
                            return
                    n = next(sequence)
                    pending[n] = (metric, value, time.monotonic())
                    yield ingest_pb2.StreamReadingsRequest(sequence=n, reading=make_request(device, metric, value))
                stop.wait(interval)
            # Returning ends our side of the stream; the server then finishes sending acks and closes.

        try:
            # One call, open until we stop: iterating over it yields acks as the server sends them.
            for ack in stub.StreamReadings(outgoing()):
                metric, value, sent = pending.pop(ack.sequence)
                slots.release()
                if ack.WhichOneof("result") == "event_id":
                    stats.add("OK", time.monotonic() - sent)
                    outcome = f"ok  event {ack.event_id[:8]}"
                else:
                    stats.add(ack.error.code)
                    outcome = f"ERR {ack.error.code}: {ack.error.message}"
                report(device, metric, value, outcome, quiet)
        except grpc.RpcError as e:
            # The whole stream failed (e.g. server down). Readings still in `pending` never got an ack,
            # so we can't know whether they were stored; count them as lost and open a new stream.
            stats.add_break(e.code().name)
            stats.add("LOST", count=len(pending))
            print(f"{datetime.now():%H:%M:%S}  device {device['id']:>3}  stream broke: {e.code().name}; "
                  f"{len(pending)} unacknowledged reading(s); reconnecting in 2s")
            stop.wait(2)


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

    channel = grpc.insecure_channel(args.grpc)  # "insecure" = plaintext, no TLS (fine for local dev)
    stub = ingest_pb2_grpc.IngestServiceStub(channel)

    stop = threading.Event()
    stats = Stats()
    def device_args(d):
        base = (stub, d, args.interval, stop, stats, random.Random(rng.random()), args.quiet)
        return base + (args.max_in_flight,) if args.mode == "stream" else base

    run_device = run_stream if args.mode == "stream" else run_unary
    threads = [threading.Thread(target=run_device, daemon=True, args=device_args(d)) for d in devices]
    print(f"Simulating {len(devices)} device(s) -> {args.grpc} in {args.mode} mode, "
          f"every {args.interval}s. Ctrl+C to stop.")
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
    print(f"\nSent {total} readings in {elapsed:.1f}s ({total / elapsed:.1f}/s): "
          + ", ".join(f"{k} {v}" for k, v in stats.results.most_common()))
    if len(stats.latencies_ms) >= 2:
        q = statistics.quantiles(stats.latencies_ms, n=100)
        print(f"Latency until acknowledged: median {q[49]:.1f} ms, p99 {q[98]:.1f} ms")
    if stats.stream_breaks:
        print("Streams broken: " + ", ".join(f"{k} {v}" for k, v in stats.stream_breaks.items()))


if __name__ == "__main__":
    main()
