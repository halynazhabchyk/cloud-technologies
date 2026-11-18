import argparse
import json
import random
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

import httpx

TABLE = "loadtest"


@dataclass
class Stats:
    latencies_ms: dict[str, list[float]] = field(
        default_factory=lambda: {"read": [], "write": []}
    )
    errors: dict[str, int] = field(default_factory=lambda: {"read": 0, "write": 0})
    lock: threading.Lock = field(default_factory=threading.Lock)

    def record(self, operation: str, latency_ms: float, ok: bool) -> None:
        with self.lock:
            if ok:
                self.latencies_ms[operation].append(latency_ms)
            else:
                self.errors[operation] += 1


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return float("nan")
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round(pct / 100 * len(ordered)) - 1))
    return ordered[index]


def item_url(base_url: str, key_index: int) -> str:
    return f"{base_url}/tables/{TABLE}/items/user-{key_index}?sortKey=profile"


def prepare(base_url: str, keys: int) -> None:
    with httpx.Client(timeout=10) as client:
        response = client.post(
            f"{base_url}/tables",
            json={"tableName": TABLE, "partitionKey": "userId", "sortKey": "kind"},
        )
        if response.status_code not in (201, 409):
            response.raise_for_status()
        for key_index in range(keys):
            client.put(
                item_url(base_url, key_index), json={"value": {"n": 0}}
            ).raise_for_status()


def worker(args: argparse.Namespace, stats: Stats, deadline: float, seed: int) -> None:
    rng = random.Random(seed)
    with httpx.Client(timeout=10) as client:
        while time.perf_counter() < deadline:
            key_index = rng.randrange(args.keys)
            is_read = rng.random() < args.read_ratio
            started = time.perf_counter()
            try:
                if is_read:
                    response = client.get(item_url(args.url, key_index))
                else:
                    response = client.put(
                        item_url(args.url, key_index),
                        json={"value": {"n": rng.randrange(1_000_000)}},
                    )
                ok = response.status_code < 300
            except httpx.HTTPError:
                ok = False
            latency_ms = (time.perf_counter() - started) * 1000
            stats.record("read" if is_read else "write", latency_ms, ok)


def summarize(args: argparse.Namespace, stats: Stats, elapsed: float) -> dict:
    result: dict = {
        "label": args.label,
        "durationSeconds": round(elapsed, 2),
        "concurrency": args.concurrency,
        "readRatio": args.read_ratio,
        "operations": {},
    }
    total = 0
    for operation, latencies in stats.latencies_ms.items():
        total += len(latencies)
        result["operations"][operation] = {
            "count": len(latencies),
            "errors": stats.errors[operation],
            "throughputOpsPerSec": round(len(latencies) / elapsed, 1),
            "meanMs": round(statistics.fmean(latencies), 2) if latencies else None,
            "p50Ms": round(percentile(latencies, 50), 2),
            "p95Ms": round(percentile(latencies, 95), 2),
            "p99Ms": round(percentile(latencies, 99), 2),
        }
    result["totalThroughputOpsPerSec"] = round(total / elapsed, 1)
    return result


def print_result(result: dict) -> None:
    print(f"\n=== {result['label']} ===")
    print(
        f"duration {result['durationSeconds']} s, concurrency "
        f"{result['concurrency']}, read ratio {result['readRatio']}"
    )
    print(f"{'op':<6}{'count':>8}{'err':>6}{'ops/s':>9}{'p50':>9}{'p95':>9}{'p99':>9}")
    for name, op in result["operations"].items():
        print(
            f"{name:<6}{op['count']:>8}{op['errors']:>6}"
            f"{op['throughputOpsPerSec']:>9}{op['p50Ms']:>9}{op['p95Ms']:>9}"
            f"{op['p99Ms']:>9}"
        )
    print(
        f"total throughput: {result['totalThroughputOpsPerSec']} ops/s (ms latencies)"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Closed-loop load test for the key-value store"
    )
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--duration", type=float, default=30)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--read-ratio", type=float, default=0.5)
    parser.add_argument("--keys", type=int, default=1000)
    parser.add_argument("--label", default="run")
    parser.add_argument("--json", help="Append the result as one JSON line here")
    args = parser.parse_args()
    args.url = args.url.rstrip("/")

    prepare(args.url, args.keys)

    stats = Stats()
    started = time.perf_counter()
    deadline = started + args.duration
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        for seed in range(args.concurrency):
            pool.submit(worker, args, stats, deadline, seed)
    elapsed = time.perf_counter() - started

    result = summarize(args, stats, elapsed)
    print_result(result)
    if args.json:
        with open(args.json, "a", encoding="utf-8") as file:
            file.write(json.dumps(result) + "\n")


if __name__ == "__main__":
    main()
