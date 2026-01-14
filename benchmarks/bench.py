#!/usr/bin/env python3
"""
S3Proxy Benchmark

Compares direct MinIO access vs S3Proxy (with encryption).
Uses boto3 for S3 operations with async concurrency.

Usage:
    python bench.py                    # Default: small objects, 10 concurrent
    python bench.py --size medium      # 1MB objects
    python bench.py --size large       # 10MB objects
    python bench.py --size xlarge      # 100MB objects
    python bench.py --size huge        # 1GiB objects
    python bench.py --concurrent 50    # 50 concurrent requests
    python bench.py --duration 60      # Run for 60 seconds
    python bench.py --runs 3           # Multiple runs for statistics
"""

import argparse
import asyncio
import os
import time
from dataclasses import dataclass, field
from statistics import mean, stdev

import aioboto3

# Object sizes
SIZES = {
    "tiny": 1024,           # 1 KB
    "small": 64 * 1024,     # 64 KB
    "medium": 1024 * 1024,  # 1 MB
    "large": 10 * 1024 * 1024,  # 10 MB
    "xlarge": 100 * 1024 * 1024,  # 100 MB
    "huge": 1024 * 1024 * 1024,  # 1 GiB
}

# Endpoints
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
PROXY_ENDPOINT = os.environ.get("PROXY_ENDPOINT", "http://localhost:8080")

# Credentials
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "benchmarkadminuser")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "benchmarkadminpassword")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

BUCKET = "bench-test"


@dataclass
class BenchResult:
    """Results from a benchmark run."""
    name: str
    total_requests: int
    duration_sec: float
    put_latencies_ms: list[float]
    get_latencies_ms: list[float]
    errors: int

    @property
    def rps(self) -> float:
        return self.total_requests / self.duration_sec if self.duration_sec > 0 else 0

    @property
    def put_avg_ms(self) -> float:
        return mean(self.put_latencies_ms) if self.put_latencies_ms else 0

    @property
    def get_avg_ms(self) -> float:
        return mean(self.get_latencies_ms) if self.get_latencies_ms else 0

    def percentile(self, latencies: list[float], p: int) -> float:
        if not latencies:
            return 0
        sorted_lat = sorted(latencies)
        idx = int(len(sorted_lat) * p / 100)
        return sorted_lat[min(idx, len(sorted_lat) - 1)]

    @property
    def put_p95_ms(self) -> float:
        return self.percentile(self.put_latencies_ms, 95)

    @property
    def get_p95_ms(self) -> float:
        return self.percentile(self.get_latencies_ms, 95)


async def ensure_bucket(session, endpoint: str):
    """Create bucket if it doesn't exist."""
    async with session.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    ) as s3:
        try:
            await s3.head_bucket(Bucket=BUCKET)
        except Exception:
            try:
                await s3.create_bucket(Bucket=BUCKET)
            except Exception:
                pass  # Bucket might already exist


async def run_benchmark(
    endpoint: str,
    name: str,
    data: bytes,
    duration_sec: int,
    concurrency: int,
) -> BenchResult:
    """Run PUT/GET benchmark against an endpoint."""

    put_latencies: list[float] = []
    get_latencies: list[float] = []
    errors = 0
    counter = 0
    stop_event = asyncio.Event()

    session = aioboto3.Session()
    await ensure_bucket(session, endpoint)

    async def worker(worker_id: int):
        nonlocal counter, errors

        async with session.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION,
        ) as s3:
            iteration = 0
            while not stop_event.is_set():
                key = f"bench-{worker_id}-{iteration}"
                iteration += 1

                # PUT
                try:
                    start = time.perf_counter()
                    await s3.put_object(Bucket=BUCKET, Key=key, Body=data)
                    put_latencies.append((time.perf_counter() - start) * 1000)
                except Exception as e:
                    errors += 1
                    continue

                # GET
                try:
                    start = time.perf_counter()
                    resp = await s3.get_object(Bucket=BUCKET, Key=key)
                    await resp["Body"].read()
                    get_latencies.append((time.perf_counter() - start) * 1000)
                    counter += 1
                except Exception as e:
                    errors += 1

    # Progress reporter
    async def progress_reporter():
        start = time.perf_counter()
        while not stop_event.is_set():
            await asyncio.sleep(5)
            if not stop_event.is_set():
                elapsed = int(time.perf_counter() - start)
                print(f"    [{elapsed}s] {counter:,} requests, {errors} errors", flush=True)

    # Start workers
    start_time = time.perf_counter()
    workers = [asyncio.create_task(worker(i)) for i in range(concurrency)]
    progress_task = asyncio.create_task(progress_reporter())

    # Run for specified duration
    await asyncio.sleep(duration_sec)
    stop_event.set()

    # Wait for workers to finish
    progress_task.cancel()
    await asyncio.gather(*workers, return_exceptions=True)
    total_duration = time.perf_counter() - start_time

    return BenchResult(
        name=name,
        total_requests=counter,
        duration_sec=total_duration,
        put_latencies_ms=put_latencies,
        get_latencies_ms=get_latencies,
        errors=errors,
    )


def print_results(
    baseline_runs: list[BenchResult],
    proxy_runs: list[BenchResult],
    size_name: str,
    size_bytes: int,
):
    """Print comparison table with statistics from multiple runs."""

    def avg(results: list[BenchResult], attr: str) -> float:
        return mean(getattr(r, attr) for r in results) if results else 0

    def std(results: list[BenchResult], attr: str) -> float:
        if len(results) < 2:
            return 0
        return stdev(getattr(r, attr) for r in results)

    def fmt_stat(results: list[BenchResult], attr: str, precision: int = 1) -> str:
        """Format as 'avg ± std' or just 'avg' for single run."""
        a = avg(results, attr)
        s = std(results, attr)
        if s > 0:
            return f"{a:.{precision}f} ± {s:.{precision}f}"
        return f"{a:.{precision}f}"

    print()
    print("=" * 75)
    print(f"  BENCHMARK RESULTS: {size_name} objects ({size_bytes:,} bytes)")
    if len(baseline_runs) > 1:
        print(f"  ({len(baseline_runs)} runs, showing mean ± stddev)")
    print("=" * 75)
    print()
    print(f"{'Metric':<25} {'Baseline (MinIO)':>23} {'S3Proxy':>23}")
    print("-" * 75)

    # Requests
    print(f"{'Requests/sec':<25} {fmt_stat(baseline_runs, 'rps'):>23} {fmt_stat(proxy_runs, 'rps'):>23}")
    print(f"{'Total requests':<25} {sum(r.total_requests for r in baseline_runs):>23,} {sum(r.total_requests for r in proxy_runs):>23,}")
    print(f"{'Errors':<25} {sum(r.errors for r in baseline_runs):>23} {sum(r.errors for r in proxy_runs):>23}")
    print()

    # Latencies
    print(f"{'PUT avg (ms)':<25} {fmt_stat(baseline_runs, 'put_avg_ms', 2):>23} {fmt_stat(proxy_runs, 'put_avg_ms', 2):>23}")
    print(f"{'PUT p95 (ms)':<25} {fmt_stat(baseline_runs, 'put_p95_ms', 2):>23} {fmt_stat(proxy_runs, 'put_p95_ms', 2):>23}")
    print(f"{'GET avg (ms)':<25} {fmt_stat(baseline_runs, 'get_avg_ms', 2):>23} {fmt_stat(proxy_runs, 'get_avg_ms', 2):>23}")
    print(f"{'GET p95 (ms)':<25} {fmt_stat(baseline_runs, 'get_p95_ms', 2):>23} {fmt_stat(proxy_runs, 'get_p95_ms', 2):>23}")
    print()

    # Calculate overhead
    baseline_rps = avg(baseline_runs, 'rps')
    proxy_rps = avg(proxy_runs, 'rps')
    if baseline_rps > 0:
        throughput_overhead = ((baseline_rps - proxy_rps) / baseline_rps) * 100
        print(f"{'Throughput overhead':<25} {throughput_overhead:>23.1f}%")

    baseline_put = avg(baseline_runs, 'put_avg_ms')
    proxy_put = avg(proxy_runs, 'put_avg_ms')
    if baseline_put > 0:
        print(f"{'Added PUT latency':<25} {proxy_put - baseline_put:>22.2f}ms")

    baseline_get = avg(baseline_runs, 'get_avg_ms')
    proxy_get = avg(proxy_runs, 'get_avg_ms')
    if baseline_get > 0:
        print(f"{'Added GET latency':<25} {proxy_get - baseline_get:>22.2f}ms")

    print("=" * 75)
    print()


async def main():
    parser = argparse.ArgumentParser(description="S3Proxy Benchmark")
    parser.add_argument(
        "--size",
        choices=list(SIZES.keys()),
        default="small",
        help="Object size to test (default: small)",
    )
    parser.add_argument(
        "--concurrent",
        type=int,
        default=10,
        help="Number of concurrent requests (default: 10)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=30,
        help="Test duration in seconds (default: 30)",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=1,
        help="Number of runs for statistical significance (default: 1)",
    )
    parser.add_argument(
        "--baseline-only",
        action="store_true",
        help="Only run baseline benchmark",
    )
    parser.add_argument(
        "--proxy-only",
        action="store_true",
        help="Only run proxy benchmark",
    )
    args = parser.parse_args()

    size_bytes = SIZES[args.size]
    test_data = os.urandom(size_bytes)

    print()
    print("S3Proxy Benchmark")
    print("-" * 40)
    print(f"  Object size: {args.size} ({size_bytes:,} bytes)")
    print(f"  Concurrency: {args.concurrent}")
    print(f"  Duration:    {args.duration}s per run")
    print(f"  Runs:        {args.runs}")
    print(f"  MinIO:       {MINIO_ENDPOINT}")
    print(f"  S3Proxy:     {PROXY_ENDPOINT}")
    print()

    baseline_runs: list[BenchResult] = []
    proxy_runs: list[BenchResult] = []

    for run_num in range(1, args.runs + 1):
        if args.runs > 1:
            print(f"--- Run {run_num}/{args.runs} ---")

        if not args.proxy_only:
            print(f"Running baseline benchmark (direct MinIO)...")
            result = await run_benchmark(
                endpoint=MINIO_ENDPOINT,
                name="Baseline (MinIO)",
                data=test_data,
                duration_sec=args.duration,
                concurrency=args.concurrent,
            )
            baseline_runs.append(result)
            print(f"  Completed: {result.total_requests:,} requests, {result.rps:.1f} req/s")

        if not args.baseline_only:
            print(f"Running proxy benchmark (S3Proxy)...")
            result = await run_benchmark(
                endpoint=PROXY_ENDPOINT,
                name="S3Proxy",
                data=test_data,
                duration_sec=args.duration,
                concurrency=args.concurrent,
            )
            proxy_runs.append(result)
            print(f"  Completed: {result.total_requests:,} requests, {result.rps:.1f} req/s")

        # Brief pause between runs
        if run_num < args.runs:
            await asyncio.sleep(1)

    if baseline_runs and proxy_runs:
        print_results(baseline_runs, proxy_runs, args.size, size_bytes)
    elif baseline_runs:
        r = baseline_runs[0]
        print(f"\nBaseline: {r.rps:.1f} req/s, PUT avg: {r.put_avg_ms:.2f}ms")
    elif proxy_runs:
        r = proxy_runs[0]
        print(f"\nProxy: {r.rps:.1f} req/s, PUT avg: {r.put_avg_ms:.2f}ms")


if __name__ == "__main__":
    asyncio.run(main())
