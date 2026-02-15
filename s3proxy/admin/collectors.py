"""Data collectors for admin dashboard."""

from __future__ import annotations

import hashlib
import json
import os
import time
from collections import deque
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING

import structlog

from .. import metrics
from ..state.redis import get_redis, is_using_redis

if TYPE_CHECKING:
    from ..config import Settings
    from ..handlers import S3ProxyHandler

logger = structlog.get_logger(__name__)

ADMIN_KEY_PREFIX = "s3proxy:admin:"
ADMIN_TTL_SECONDS = 30


# ---------------------------------------------------------------------------
# Rate tracker — sliding window over Prometheus counters
# ---------------------------------------------------------------------------


class RateTracker:
    """Tracks counter snapshots over a sliding window to compute per-minute rates."""

    def __init__(self, window_seconds: int = 600):
        self._window = window_seconds
        self._snapshots: deque[tuple[float, dict[str, float]]] = deque()

    def record(self, counters: dict[str, float]) -> None:
        now = time.monotonic()
        self._snapshots.append((now, counters))
        cutoff = now - self._window - 10
        while len(self._snapshots) > 2 and self._snapshots[0][0] < cutoff:
            self._snapshots.popleft()

    def rate_per_minute(self, key: str) -> float:
        if len(self._snapshots) < 2:
            return 0.0
        oldest_ts, oldest_vals = self._snapshots[0]
        newest_ts, newest_vals = self._snapshots[-1]
        elapsed = newest_ts - oldest_ts
        if elapsed < 1:
            return 0.0
        delta = newest_vals.get(key, 0) - oldest_vals.get(key, 0)
        return max(0.0, delta / elapsed * 60)

    def history(self, key: str, max_points: int = 60) -> list[float]:
        """Return per-minute rate history as a list of floats for sparklines."""
        if len(self._snapshots) < 2:
            return []
        rates: list[float] = []
        for i in range(1, len(self._snapshots)):
            prev_ts, prev_vals = self._snapshots[i - 1]
            curr_ts, curr_vals = self._snapshots[i]
            elapsed = curr_ts - prev_ts
            if elapsed < 0.1:
                continue
            delta = curr_vals.get(key, 0) - prev_vals.get(key, 0)
            rates.append(round(max(0.0, delta / elapsed * 60), 1))
        if len(rates) > max_points:
            step = len(rates) / max_points
            rates = [rates[int(i * step)] for i in range(max_points)]
        return rates


_rate_tracker = RateTracker(window_seconds=600)


# ---------------------------------------------------------------------------
# Request log — ring buffer for live feed
# ---------------------------------------------------------------------------


@dataclass(slots=True, frozen=True)
class RequestEntry:
    """Single request log entry for the live feed."""

    timestamp: float
    method: str
    path: str
    operation: str
    status: int
    duration_ms: float
    size: int
    crypto: str


class RequestLog:
    """Fixed-size ring buffer of recent requests for the live feed."""

    ENCRYPT_OPS = frozenset(
        {
            "PutObject",
            "UploadPart",
            "UploadPartCopy",
            "CompleteMultipartUpload",
            "CopyObject",
        }
    )
    DECRYPT_OPS = frozenset({"GetObject"})

    def __init__(self, maxlen: int = 200):
        self._entries: deque[RequestEntry] = deque(maxlen=maxlen)

    def record(
        self,
        method: str,
        path: str,
        operation: str,
        status: int,
        duration: float,
        size: int,
    ) -> None:
        crypto = ""
        if operation in self.ENCRYPT_OPS:
            crypto = "encrypt"
        elif operation in self.DECRYPT_OPS:
            crypto = "decrypt"
        self._entries.append(
            RequestEntry(
                timestamp=time.time(),
                method=method,
                path=path[:120],
                operation=operation,
                status=status,
                duration_ms=round(duration * 1000, 1),
                size=size,
                crypto=crypto,
            )
        )

    def recent(self, limit: int = 50) -> list[dict]:
        """Return most recent entries as dicts, newest first."""
        entries = list(self._entries)
        entries.reverse()
        return [asdict(e) for e in entries[:limit]]


_request_log = RequestLog(maxlen=200)


def record_request(
    method: str,
    path: str,
    operation: str,
    status: int,
    duration: float,
    size: int,
) -> None:
    """Record a completed request to the live feed log."""
    _request_log.record(method, path, operation, status, duration, size)


# ---------------------------------------------------------------------------
# Prometheus helpers
# ---------------------------------------------------------------------------


def _read_gauge(gauge) -> float:
    return gauge._value.get()


def _read_counter(counter) -> float:
    return counter._value.get()


def _read_labeled_counter_sum(counter) -> float:
    total = 0.0
    for sample in counter.collect()[0].samples:
        if sample.name.endswith("_total"):
            total += sample.value
    return total


def _read_labeled_gauge_sum(gauge) -> float:
    total = 0.0
    for sample in gauge.collect()[0].samples:
        total += sample.value
    return total


def _read_errors_by_class() -> tuple[float, float, float]:
    """Read 4xx, 5xx, 503 counts from REQUEST_COUNT labels."""
    errors_4xx = 0.0
    errors_5xx = 0.0
    errors_503 = 0.0
    for sample in metrics.REQUEST_COUNT.collect()[0].samples:
        if not sample.name.endswith("_total"):
            continue
        status = str(sample.labels.get("status", ""))
        if status.startswith("4"):
            errors_4xx += sample.value
        elif status == "503":
            errors_503 += sample.value
            errors_5xx += sample.value
        elif status.startswith("5"):
            errors_5xx += sample.value
    return errors_4xx, errors_5xx, errors_503


# ---------------------------------------------------------------------------
# Collectors
# ---------------------------------------------------------------------------


def collect_pod_identity(settings: Settings, start_time: float) -> dict:
    """Collect pod identity for the header banner."""
    return {
        "pod_name": os.environ.get("HOSTNAME", "unknown"),
        "uptime_seconds": int(time.monotonic() - start_time),
        "storage_backend": "Redis (HA)" if is_using_redis() else "In-memory",
        "kek_fingerprint": hashlib.sha256(settings.kek).hexdigest()[:16],
    }


def collect_health() -> dict:
    """Collect health metrics with error counts."""
    memory_reserved = _read_gauge(metrics.MEMORY_RESERVED_BYTES)
    memory_limit = _read_gauge(metrics.MEMORY_LIMIT_BYTES)
    usage_pct = round(memory_reserved / memory_limit * 100, 1) if memory_limit > 0 else 0
    errors_4xx, errors_5xx, errors_503 = _read_errors_by_class()

    return {
        "memory_reserved_bytes": int(memory_reserved),
        "memory_limit_bytes": int(memory_limit),
        "memory_usage_pct": usage_pct,
        "requests_in_flight": int(_read_labeled_gauge_sum(metrics.REQUESTS_IN_FLIGHT)),
        "errors_4xx": int(errors_4xx),
        "errors_5xx": int(errors_5xx),
        "errors_503": int(errors_503),
    }


def collect_latency() -> dict:
    """Compute approximate p50/p95/p99 from REQUEST_DURATION histogram buckets."""
    buckets: list[tuple[float, float]] = []  # (upper_bound, cumulative_count)
    total_count = 0.0

    for sample in metrics.REQUEST_DURATION.collect()[0].samples:
        if sample.name.endswith("_bucket"):
            le = sample.labels.get("le", "")
            if le == "+Inf":
                total_count = sample.value
            else:
                try:
                    buckets.append((float(le), sample.value))
                except ValueError:
                    continue

    if total_count < 1:
        return {"p50_ms": 0, "p95_ms": 0, "p99_ms": 0, "count": 0}

    buckets.sort(key=lambda b: b[0])

    def _percentile(p: float) -> float:
        threshold = total_count * p
        for upper, count in buckets:
            if count >= threshold:
                return round(upper * 1000, 1)  # seconds → ms
        return round(buckets[-1][0] * 1000, 1) if buckets else 0

    return {
        "p50_ms": _percentile(0.5),
        "p95_ms": _percentile(0.95),
        "p99_ms": _percentile(0.99),
        "count": int(total_count),
    }


def collect_throughput() -> dict:
    """Collect throughput counters and compute per-minute rates."""
    encrypt_ops = 0.0
    decrypt_ops = 0.0
    for sample in metrics.ENCRYPTION_OPERATIONS.collect()[0].samples:
        if sample.name.endswith("_total"):
            if sample.labels.get("operation") == "encrypt":
                encrypt_ops = sample.value
            elif sample.labels.get("operation") == "decrypt":
                decrypt_ops = sample.value

    total_requests = _read_labeled_counter_sum(metrics.REQUEST_COUNT)
    bytes_encrypted = _read_counter(metrics.BYTES_ENCRYPTED)
    bytes_decrypted = _read_counter(metrics.BYTES_DECRYPTED)
    errors_4xx, errors_5xx, errors_503 = _read_errors_by_class()

    counters = {
        "requests": total_requests,
        "encrypt_ops": encrypt_ops,
        "decrypt_ops": decrypt_ops,
        "bytes_encrypted": bytes_encrypted,
        "bytes_decrypted": bytes_decrypted,
        "errors_4xx": errors_4xx,
        "errors_5xx": errors_5xx,
        "errors_503": errors_503,
    }
    _rate_tracker.record(counters)

    return {
        "rates": {
            "requests_per_min": round(_rate_tracker.rate_per_minute("requests"), 1),
            "encrypt_per_min": round(_rate_tracker.rate_per_minute("encrypt_ops"), 1),
            "decrypt_per_min": round(_rate_tracker.rate_per_minute("decrypt_ops"), 1),
            "bytes_encrypted_per_min": int(_rate_tracker.rate_per_minute("bytes_encrypted")),
            "bytes_decrypted_per_min": int(_rate_tracker.rate_per_minute("bytes_decrypted")),
            "errors_4xx_per_min": round(_rate_tracker.rate_per_minute("errors_4xx"), 1),
            "errors_5xx_per_min": round(_rate_tracker.rate_per_minute("errors_5xx"), 1),
            "errors_503_per_min": round(_rate_tracker.rate_per_minute("errors_503"), 1),
        },
        "history": {
            "requests_per_min": _rate_tracker.history("requests"),
            "encrypt_per_min": _rate_tracker.history("encrypt_ops"),
            "decrypt_per_min": _rate_tracker.history("decrypt_ops"),
            "bytes_encrypted_per_min": _rate_tracker.history("bytes_encrypted"),
            "bytes_decrypted_per_min": _rate_tracker.history("bytes_decrypted"),
        },
    }


# ---------------------------------------------------------------------------
# Redis pod metrics publishing (multi-pod view)
# ---------------------------------------------------------------------------


async def publish_pod_metrics(pod_data: dict) -> None:
    """Publish this pod's metrics to Redis so other pods can read them."""
    if not is_using_redis():
        return
    try:
        client = get_redis()
        pod_name = pod_data["pod"]["pod_name"]
        key = f"{ADMIN_KEY_PREFIX}{pod_name}"
        await client.set(key, json.dumps(pod_data).encode(), ex=ADMIN_TTL_SECONDS)
    except Exception:
        logger.debug("Failed to publish pod metrics to Redis", exc_info=True)


async def read_all_pod_metrics() -> list[dict]:
    """Read all pods' metrics from Redis. Returns empty list if not using Redis."""
    if not is_using_redis():
        return []
    try:
        client = get_redis()
        pods = []
        async for key in client.scan_iter(match=f"{ADMIN_KEY_PREFIX}*", count=100):
            data = await client.get(key)
            if data:
                pods.append(json.loads(data))
        pods.sort(key=lambda p: p.get("pod", {}).get("pod_name", ""))
        return pods
    except Exception:
        logger.debug("Failed to read pod metrics from Redis", exc_info=True)
        return []


# ---------------------------------------------------------------------------
# Formatters
# ---------------------------------------------------------------------------


def _format_bytes(n: int) -> str:
    """Format bytes to human-readable string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def _format_uptime(seconds: int) -> str:
    """Format seconds to human-readable uptime string."""
    days, remainder = divmod(seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, _ = divmod(remainder, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    parts.append(f"{minutes}m")
    return " ".join(parts)


# ---------------------------------------------------------------------------
# Aggregate
# ---------------------------------------------------------------------------


async def collect_all(
    settings: Settings,
    handler: S3ProxyHandler,
    start_time: float,
) -> dict:
    """Collect all dashboard data and publish to Redis for multi-pod view."""
    pod = collect_pod_identity(settings, start_time)
    health = collect_health()
    throughput = collect_throughput()
    latency = collect_latency()

    local_data = {
        "pod": pod,
        "health": health,
        "throughput": throughput,
        "latency": latency,
        "formatted": {
            "memory_reserved": _format_bytes(health["memory_reserved_bytes"]),
            "memory_limit": _format_bytes(health["memory_limit_bytes"]),
            "uptime": _format_uptime(pod["uptime_seconds"]),
            "bytes_encrypted_per_min": _format_bytes(
                throughput["rates"]["bytes_encrypted_per_min"]
            ),
            "bytes_decrypted_per_min": _format_bytes(
                throughput["rates"]["bytes_decrypted_per_min"]
            ),
        },
    }

    # Publish this pod's data to Redis (fire-and-forget for other pods to see)
    await publish_pod_metrics(local_data)

    # Read all pods from Redis (includes this pod's just-published data)
    all_pods = await read_all_pod_metrics()

    return {
        **local_data,
        "request_log": _request_log.recent(10),
        "all_pods": all_pods,
    }
