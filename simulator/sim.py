"""VitalStream device simulator: sends simulated vital-sign readings to api-service over gRPC.

Usage (from the simulator/ folder, after ./generate.sh):
    .venv/bin/python sim.py --create 3               # register 3 new devices via REST, then simulate them
    .venv/bin/python sim.py --devices 4 --duration 10
    .venv/bin/python sim.py --help

Each device runs in its own thread and calls the unary SendReading RPC once per interval for each
metric it measures. All threads share one gRPC channel: a single HTTP/2 connection that carries many
concurrent calls, so 100 devices don't need 100 connections.
"""

import argparse
import json
import random
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
        self.results = Counter()

    def add(self, outcome: str):
        with self.lock:
            self.results[outcome] += 1


def run_device(stub, device: dict, interval: float, stop: threading.Event, stats: Stats,
               rng: random.Random, quiet: bool):
    signal = DeviceSignal(device["deviceType"], rng)
    # Start devices at slightly different moments so they don't all fire in lockstep.
    stop.wait(rng.uniform(0, interval))
    while not stop.is_set():
        for metric, value in signal.next():
            request = ingest_pb2.SendReadingRequest(
                device_id=device["id"],
                metric=f"METRIC_{metric}",
                value=value,
                measured_at=datetime.now(timezone.utc),
            )
            try:
                # timeout = gRPC "deadline": if no answer within 5s the call fails with DEADLINE_EXCEEDED
                # instead of waiting forever.
                response = stub.SendReading(request, timeout=5)
                stats.add("OK")
                outcome = f"ok  event {response.event_id[:8]}"
            except grpc.RpcError as e:
                # Every gRPC failure carries a status code plus a human-readable message.
                stats.add(e.code().name)
                outcome = f"ERR {e.code().name}: {e.details()}"
            if not quiet:
                print(f"{datetime.now():%H:%M:%S}  device {device['id']:>3}  {metric:<12} {value:>7}  {outcome}")
        stop.wait(interval)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--grpc", default="localhost:9090", help="api-service gRPC address")
    parser.add_argument("--api", default="http://localhost:8080", help="api-service REST address")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--devices", help="comma-separated ids of existing devices, e.g. 4,6")
    source.add_argument("--create", type=int, metavar="N", help="register N new devices first")
    parser.add_argument("--interval", type=float, default=1.0, help="seconds between readings per device")
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
    threads = [
        threading.Thread(target=run_device, daemon=True,
                         args=(stub, d, args.interval, stop, stats, random.Random(rng.random()), args.quiet))
        for d in devices
    ]
    print(f"Simulating {len(devices)} device(s) -> {args.grpc}, every {args.interval}s. Ctrl+C to stop.")
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


if __name__ == "__main__":
    main()
