"""Data collectors for the admin dashboard."""

from __future__ import annotations

import hashlib
import os
import time
from collections import defaultdict, deque
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING

from .. import metrics

if TYPE_CHECKING:
    from ..config import Settings


# ---------------------------------------------------------------------------
# Sliding-window rate tracker over Prometheus counters
# ---------------------------------------------------------------------------


class RateTracker:
    """Sample counter values on a schedule, then compute deltas over the window."""

    def __init__(self, window_seconds: int = 3600, max_samples: int = 180):
        self._window = window_seconds
        self._max_samples = max_samples
        self._snapshots: deque[tuple[float, dict[str, float]]] = deque(maxlen=max_samples)

    def record(self, counters: dict[str, float]) -> None:
        now = time.monotonic()
        self._snapshots.append((now, dict(counters)))
        cutoff = now - self._window
        while len(self._snapshots) > 2 and self._snapshots[0][0] < cutoff:
            self._snapshots.popleft()

    def rate_per_second(self, key: str) -> float:
        if len(self._snapshots) < 2:
            return 0.0
        t0, v0 = self._snapshots[0]
        t1, v1 = self._snapshots[-1]
        elapsed = t1 - t0
        if elapsed < 0.5:
            return 0.0
        delta = v1.get(key, 0.0) - v0.get(key, 0.0)
        return max(0.0, delta / elapsed)

    def total(self, key: str) -> float:
        if not self._snapshots:
            return 0.0
        _, v0 = self._snapshots[0]
        _, v1 = self._snapshots[-1]
        return max(0.0, v1.get(key, 0.0) - v0.get(key, 0.0))

    def sparkline(self, key: str, points: int = 30) -> list[float]:
        """Return per-bucket deltas suitable for a sparkline."""
        if len(self._snapshots) < 2:
            return []
        deltas: list[float] = []
        snaps = list(self._snapshots)
        for prev, curr in zip(snaps, snaps[1:], strict=False):
            elapsed = curr[0] - prev[0]
            if elapsed <= 0:
                continue
            deltas.append(max(0.0, curr[1].get(key, 0.0) - prev[1].get(key, 0.0)))
        if len(deltas) > points:
            step = len(deltas) / points
            deltas = [deltas[int(i * step)] for i in range(points)]
        return [round(v, 2) for v in deltas]

    def earliest_value(self, key: str) -> float | None:
        if not self._snapshots:
            return None
        return self._snapshots[0][1].get(key, 0.0)


_rate_tracker = RateTracker()


# ---------------------------------------------------------------------------
# Request log — ring buffer for the activity feed
# ---------------------------------------------------------------------------


@dataclass(slots=True, frozen=True)
class RequestEntry:
    timestamp: float
    method: str
    operation: str
    bucket: str
    key: str
    status: int
    duration_ms: float
    size: int
    client_ip: str


class RequestLog:
    def __init__(self, maxlen: int = 200):
        self._entries: deque[RequestEntry] = deque(maxlen=maxlen)

    def record(self, entry: RequestEntry) -> None:
        self._entries.append(entry)

    def recent(self, limit: int = 10) -> list[dict]:
        entries = list(self._entries)
        entries.reverse()
        return [asdict(e) for e in entries[:limit]]

    def all(self) -> list[RequestEntry]:
        return list(self._entries)


_request_log = RequestLog(maxlen=200)


def record_request(
    method: str,
    path: str,
    operation: str,
    status: int,
    duration: float,
    size: int,
    client_ip: str = "",
) -> None:
    """Append a completed request to the ring buffer."""
    bucket, key = _split_bucket_key(path)
    _request_log.record(
        RequestEntry(
            timestamp=time.time(),
            method=method,
            operation=operation,
            bucket=bucket,
            key=key,
            status=status,
            duration_ms=round(duration * 1000, 1),
            size=size,
            client_ip=client_ip,
        )
    )


def _split_bucket_key(path: str) -> tuple[str, str]:
    stripped = path.lstrip("/")
    if not stripped:
        return "", ""
    if "/" not in stripped:
        return stripped, ""
    bucket, _, key = stripped.partition("/")
    return bucket, key


# ---------------------------------------------------------------------------
# Prometheus helpers
# ---------------------------------------------------------------------------


def _read_counter(counter) -> float:
    return float(counter._value.get())


def _read_labeled_counter_sum(counter) -> float:
    total = 0.0
    for sample in counter.collect()[0].samples:
        if sample.name.endswith("_total"):
            total += sample.value
    return total


def _read_errors_total() -> float:
    errs = 0.0
    for sample in metrics.REQUEST_COUNT.collect()[0].samples:
        if not sample.name.endswith("_total"):
            continue
        status = str(sample.labels.get("status", ""))
        if status.startswith(("4", "5")):
            errs += sample.value
    return errs


# ---------------------------------------------------------------------------
# Formatters
# ---------------------------------------------------------------------------


def _format_bytes(n: float) -> tuple[str, str]:
    """Return (number, unit) pair so the UI can render them distinctly."""
    value = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if abs(value) < 1024 or unit == "PB":
            if unit == "B":
                return f"{int(value)}", unit
            return f"{value:.1f}" if value < 100 else f"{value:.0f}", unit
        value /= 1024
    return f"{value:.0f}", "PB"


def _format_uptime(seconds: float) -> str:
    s = int(seconds)
    days, rem = divmod(s, 86400)
    hours, rem = divmod(rem, 3600)
    minutes = rem // 60
    parts: list[str] = []
    if days:
        parts.append(f"{days}d")
    if hours or days:
        parts.append(f"{hours}h")
    parts.append(f"{minutes}m")
    return " ".join(parts)


def _format_relative(ts: float, now: float | None = None) -> str:
    delta = max(0.0, (now or time.time()) - ts)
    if delta < 60:
        return f"{int(delta)}s ago"
    if delta < 3600:
        return f"{int(delta // 60)}m ago"
    if delta < 86400:
        return f"{int(delta // 3600)}h ago"
    return f"{int(delta // 86400)}d ago"


def _format_size(n: int) -> str:
    if n <= 0:
        return "—"
    num, unit = _format_bytes(n)
    return f"{num} {unit}"


# ---------------------------------------------------------------------------
# Derived aggregations from the request log
# ---------------------------------------------------------------------------


def _derive_buckets(entries: list[RequestEntry]) -> list[dict]:
    by_bucket: dict[str, dict] = defaultdict(
        lambda: {"objects": set(), "bytes": 0, "last_seen": 0.0}
    )
    for e in entries:
        if not e.bucket:
            continue
        info = by_bucket[e.bucket]
        if e.key:
            info["objects"].add(e.key)
        if e.size > 0:
            info["bytes"] += e.size
        if e.timestamp > info["last_seen"]:
            info["last_seen"] = e.timestamp

    out: list[dict] = []
    for name, info in by_bucket.items():
        num, unit = _format_bytes(info["bytes"])
        out.append(
            {
                "name": name,
                "encrypted": True,
                "objects": len(info["objects"]),
                "size": f"{num} {unit}" if info["bytes"] > 0 else "—",
                "last_seen": info["last_seen"],
            }
        )
    out.sort(key=lambda b: b["last_seen"], reverse=True)
    return out


def _derive_keys(settings: Settings) -> list[dict]:
    fp = hashlib.sha256(settings.kek).hexdigest()[:8]
    return [
        {
            "id": f"key-{fp}",
            "type": "Local (KEK)",
            "status": "Active",
            "created": "—",
        }
    ]


# ---------------------------------------------------------------------------
# Aggregate collector
# ---------------------------------------------------------------------------


def collect_all(settings: Settings, start_time: float, version: str = "1.0.0") -> dict:
    """Gather everything the dashboard renders in a single JSON blob."""
    now = time.time()
    uptime_s = max(0.0, time.monotonic() - start_time)

    total_requests = _read_labeled_counter_sum(metrics.REQUEST_COUNT)
    bytes_encrypted = _read_counter(metrics.BYTES_ENCRYPTED)
    bytes_decrypted = _read_counter(metrics.BYTES_DECRYPTED)
    errors_total = _read_errors_total()

    counters = {
        "requests": total_requests,
        "bytes_crypto": bytes_encrypted + bytes_decrypted,
        "errors": errors_total,
    }
    _rate_tracker.record(counters)

    req_rate = _rate_tracker.rate_per_second("requests")
    crypto_rate = _rate_tracker.rate_per_second("bytes_crypto")

    num_enc, unit_enc = _format_bytes(bytes_encrypted)
    num_thr, unit_thr = _format_bytes(crypto_rate)

    entries = _request_log.all()
    buckets = _derive_buckets(entries)
    last_error_ts = next((e.timestamp for e in reversed(entries) if e.status >= 400), None)

    return {
        "header": {
            "title": "S3 Encryption Proxy",
            "status": "Running",
            "uptime": _format_uptime(uptime_s),
            "pod": os.environ.get("HOSTNAME", "local"),
            "version": version,
        },
        "cards": {
            "requests": {
                "label": "Requests",
                "value": f"{int(total_requests):,}",
                "unit": "",
                "spark": _rate_tracker.sparkline("requests"),
            },
            "data_encrypted": {
                "label": "Data Encrypted",
                "value": num_enc,
                "unit": unit_enc,
                "spark": _rate_tracker.sparkline("bytes_crypto"),
            },
            "errors": {
                "label": "Errors",
                "value": f"{int(errors_total):,}",
                "unit": "",
                "spark": _rate_tracker.sparkline("errors"),
            },
            "active_buckets": {
                "label": "Active Buckets",
                "value": str(len(buckets)),
                "unit": "",
                "detail": f"seen in last {len(entries)} reqs",
            },
        },
        "activity": [
            {
                "time": _format_relative(e["timestamp"], now),
                "operation": _operation_display(e["method"], e["operation"]),
                "bucket": e["bucket"] or "—",
                "object": e["key"] or "—",
                "status": "Success" if e["status"] < 400 else "Error",
                "status_code": e["status"],
                "size": _format_size(e["size"]),
                "client_ip": e["client_ip"] or "—",
                "latency": f"{e['duration_ms']:.0f} ms",
            }
            for e in _request_log.recent(10)
        ],
        "buckets": [
            {
                "name": b["name"],
                "encrypted": b["encrypted"],
                "objects": f"{b['objects']:,}",
                "size": b["size"],
            }
            for b in buckets[:8]
        ],
        "keys": _derive_keys(settings),
        "footer": {
            "version": version,
            "req_per_s": f"{req_rate:.0f}",
            "throughput": f"{num_thr} {unit_thr}/s" if crypto_rate > 0 else f"0 {unit_thr}/s",
            "last_error": _format_relative(last_error_ts, now) if last_error_ts else "never",
        },
    }


def _operation_display(method: str, operation: str) -> str:
    """Shorten operation names for the feed (GET, PUT, DELETE, etc.)."""
    return method or operation
