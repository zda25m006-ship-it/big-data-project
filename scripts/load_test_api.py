"""Simple 10x/100x API load test for the final demo."""
from __future__ import annotations

import argparse
import concurrent.futures
import random
import time

import requests


def one_request(url: str) -> tuple[bool, float, str]:
    payload = {
        "PULocationID": random.choice([68, 79, 132, 138, 161, 162, 230, 236, 237]),
        "hour": random.randint(0, 23),
        "day_of_week": random.randint(1, 7),
        "month": random.randint(1, 12),
        "ride_count": random.randint(0, 50),
    }
    start = time.perf_counter()
    try:
        r = requests.post(url, json=payload, timeout=10)
        latency = time.perf_counter() - start
        return r.ok, latency, r.text[:120]
    except Exception as exc:
        latency = time.perf_counter() - start
        return False, latency, str(exc)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:3000/predict")
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--workers", type=int, default=10)
    args = parser.parse_args()

    start = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as ex:
        results = list(ex.map(lambda _: one_request(args.url), range(args.requests)))
    total = time.perf_counter() - start
    ok = sum(1 for success, _, _ in results if success)
    latencies = [latency for _, latency, _ in results]
    print(f"Requests: {args.requests}, Success: {ok}, Failed: {args.requests - ok}")
    print(f"Total time: {total:.2f}s, Throughput: {args.requests / total:.2f} req/s")
    print(f"Avg latency: {sum(latencies) / len(latencies):.3f}s, Max latency: {max(latencies):.3f}s")
    if ok != args.requests:
        print("Sample failure:", next(text for success, _, text in results if not success))


if __name__ == "__main__":
    main()
