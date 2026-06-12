"""Stats store backends for the dashboard.

Mirrors the Strategy Pattern in ``state/storage.py``: a ``StatsStore`` ABC with
an in-memory implementation (per-pod, the single-instance default) and a
Redis-backed implementation that makes the dashboard numbers cluster-wide.

The Redis path keeps everything under the ``s3proxy:stats:`` prefix with sliding
TTLs and a hard entry cap so it never threatens the multipart-upload state that
shares the same (``maxmemory 200mb, noeviction``) Redis.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING

import orjson
import structlog
from structlog.stdlib import BoundLogger

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from ..config import Settings

logger: BoundLogger = structlog.get_logger(__name__)

STATS_PREFIX = "s3proxy:stats:"

# Latency histogram bucket upper-bounds — mirror metrics.REQUEST_DURATION so the
# percentile walk is identical whether reading Prometheus or Redis.
LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)

# Range -> (window_seconds, bucket_seconds). Bucket sizes chosen so each range
# downsamples to ~60-120 points and stays readable.
RANGE_SPECS: dict[str, tuple[int, int]] = {
    "1h": (3600, 60),
    "3h": (3 * 3600, 120),
    "7h": (7 * 3600, 300),
    "24h": (24 * 3600, 900),
    "7d": (7 * 86400, 7200),
}
DEFAULT_RANGE = "1h"


@dataclass(slots=True, frozen=True)
class RequestSample:
    """A completed request, the unit written to the request log + counters."""

    timestamp: float
    method: str
    operation: str
    bucket: str
    key: str
    status: int
    duration_ms: float
    size: int
    client_ip: str


# A snapshot of this pod's local Prometheus-derived counters, used by the Redis
# store to compute deltas to fold into the shared counters.
@dataclass(slots=True, frozen=True)
class LocalCounters:
    requests: float
    errors: float
    bytes_encrypted: float
    bytes_decrypted: float
    methods: dict[str, float]
    errors_by_class: dict[str, float]
    latency_buckets: dict[str, float]  # le-string -> cumulative count, plus "count"


def _latency_bucket_le(duration_seconds: float) -> str:
    for le in LATENCY_BUCKETS:
        if duration_seconds <= le:
            return str(le)
    return "+Inf"


def error_buckets(status: int | str) -> list[str]:
    """Error-breakdown buckets a status contributes to ('503' implies '5xx')."""
    s = str(status)
    if s == "503":
        return ["503", "5xx"]
    if s.startswith("5"):
        return ["5xx"]
    if s.startswith("4"):
        return ["4xx"]
    return []


def bucket_series(
    points: list[tuple[int, float]],
    window_seconds: int,
    bucket_seconds: int,
    now: float | None = None,
) -> tuple[list[float], list[float]]:
    """Downsample ``(minute_epoch, value)`` pairs into a fixed bucket grid.

    Returns parallel ``(times, values)`` lists with one entry per bucket across
    the window, zero-filling empty buckets so the chart axis stays continuous.
    """
    now = now if now is not None else time.time()
    end = int(now // bucket_seconds) * bucket_seconds
    start = end - window_seconds + bucket_seconds
    n = window_seconds // bucket_seconds
    times = [float(start + i * bucket_seconds) for i in range(n)]
    values = [0.0] * n
    for minute_ts, value in points:
        if minute_ts < start or minute_ts > end:
            continue
        idx = int((minute_ts - start) // bucket_seconds)
        if 0 <= idx < n:
            values[idx] += value
    return times, [round(v, 2) for v in values]


# ---------------------------------------------------------------------------
# In-memory rate tracker + request log (single-instance / per-pod path)
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

    def sparkline_series(self, key: str, points: int = 30) -> tuple[list[float], list[float]]:
        """Return (wall_clock_timestamps, per-bucket deltas) in parallel lists."""
        if len(self._snapshots) < 2:
            return [], []
        snaps = list(self._snapshots)
        mono_now = time.monotonic()
        wall_now = time.time()
        offset = wall_now - mono_now
        pairs: list[tuple[float, float]] = []
        for prev, curr in zip(snaps, snaps[1:], strict=False):
            elapsed = curr[0] - prev[0]
            if elapsed <= 0:
                continue
            pairs.append(
                (curr[0] + offset, max(0.0, curr[1].get(key, 0.0) - prev[1].get(key, 0.0)))
            )
        if len(pairs) > points:
            step = len(pairs) / points
            pairs = [pairs[int(i * step)] for i in range(points)]
        times = [round(p[0], 3) for p in pairs]
        values = [round(p[1], 2) for p in pairs]
        return times, values


class RequestLog:
    def __init__(self, maxlen: int = 10000):
        self._entries: deque[RequestSample] = deque(maxlen=maxlen)

    def record(self, entry: RequestSample) -> None:
        self._entries.append(entry)

    def all(self) -> list[RequestSample]:
        return list(self._entries)


# ---------------------------------------------------------------------------
# Aggregates returned to the collector
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class StatsAggregate:
    """Cluster-wide totals + breakdowns the dashboard cards render."""

    requests: float
    errors: float
    bytes_encrypted: float
    bytes_decrypted: float
    methods: dict[str, float]
    errors_by_class: dict[str, float]
    latency_buckets: dict[str, float]


class StatsStore(ABC):
    """Abstract dashboard stats backend."""

    cluster_wide: bool = False

    async def start(self) -> None:  # noqa: B027 - optional hook; default no-op, RedisStatsStore overrides
        """Start any background machinery (e.g. the Redis flush loop). No-op by default."""

    async def aclose(self) -> None:  # noqa: B027 - optional hook; default no-op, RedisStatsStore overrides
        """Flush pending state and stop background machinery. No-op by default."""

    @abstractmethod
    async def record(self, sample: RequestSample) -> None:
        """Append a completed request to the log and bump counters."""
        ...

    @abstractmethod
    async def sync_local(self, local: LocalCounters) -> None:
        """Fold this pod's local Prometheus deltas into the shared store.

        No-op for the in-memory store (it reads Prometheus directly).
        """
        ...

    @abstractmethod
    async def aggregate(self) -> StatsAggregate | None:
        """Return cluster-wide totals/breakdowns, or None to use the local path."""
        ...

    @abstractmethod
    async def series(self, metric: str, range_key: str) -> tuple[list[float], list[float]]:
        """Return (times, values) for a metric over the requested range."""
        ...

    @abstractmethod
    async def recent(self, limit: int) -> list[dict]:
        """Return the most recent N request entries (newest first) as dicts."""
        ...

    @abstractmethod
    async def buckets(self) -> list[dict]:
        """Return durable per-bucket aggregates for every bucket the proxy served.

        Each dict has ``name``, ``objects`` (distinct keys seen), ``bytes``
        (cumulative), and ``last_seen`` (epoch seconds). Independent of the
        request-log window so quiet buckets are never hidden by chatty ones.
        """
        ...

    @abstractmethod
    async def page(self, offset: int, limit: int, query: str, operation: str, status: str) -> dict:
        """Return a filtered, paginated slice of the request log."""
        ...


class MemoryStatsStore(StatsStore):
    """Per-pod in-memory store — the single-instance default (today's behavior)."""

    cluster_wide = False

    def __init__(self, settings: Settings):
        self._rate = RateTracker()
        self._log = RequestLog(maxlen=max(200, settings.request_log_cap))
        self._buckets: dict[str, dict] = {}

    @property
    def rate(self) -> RateTracker:
        return self._rate

    async def record(self, sample: RequestSample) -> None:
        self._log.record(sample)
        if sample.bucket:
            info = self._buckets.setdefault(
                sample.bucket, {"objects": set(), "bytes": 0.0, "last_seen": 0.0}
            )
            if sample.key:
                info["objects"].add(sample.key)
            if sample.size > 0:
                info["bytes"] += sample.size
            info["last_seen"] = max(info["last_seen"], sample.timestamp)

    async def sync_local(self, local: LocalCounters) -> None:
        # In-memory path reads Prometheus directly in collect_all; just feed the
        # rate tracker so live sparklines work.
        self._rate.record(
            {
                "requests": local.requests,
                "bytes_crypto": local.bytes_encrypted + local.bytes_decrypted,
                "errors": local.errors,
            }
        )

    async def aggregate(self) -> StatsAggregate | None:
        return None  # signal collect_all to use the Prometheus/local path

    async def series(self, metric: str, range_key: str) -> tuple[list[float], list[float]]:
        # Memory mode is single-pod; per-direction byte series aren't tracked
        # separately, so they fall back to the combined crypto sparkline.
        key = {
            "requests": "requests",
            "crypto": "bytes_crypto",
            "errors": "errors",
            "bytes_put": "bytes_crypto",
            "bytes_get": "bytes_crypto",
        }.get(metric, "requests")
        return self._rate.sparkline_series(key)

    async def recent(self, limit: int) -> list[dict]:
        entries = self._log.all()
        entries.reverse()
        return [asdict(e) for e in entries[:limit]]

    async def buckets(self) -> list[dict]:
        return [
            {
                "name": name,
                "objects": len(info["objects"]),
                "bytes": info["bytes"],
                "last_seen": info["last_seen"],
            }
            for name, info in self._buckets.items()
        ]

    async def page(self, offset: int, limit: int, query: str, operation: str, status: str) -> dict:
        entries = self._log.all()
        entries.reverse()
        return _filter_and_paginate(entries, offset, limit, query, operation, status)


class RedisStatsStore(StatsStore):
    """Redis-backed cluster-wide store. Degrades gracefully on Redis errors."""

    cluster_wide = True

    # Recording is decoupled from the request path: record() only enqueues a
    # sample (no Redis I/O), and a background loop drains the queue every
    # _FLUSH_INTERVAL into a single folded pipeline. This keeps a slow Redis off
    # every proxied request's critical path and collapses N per-request
    # round-trips into one per interval. Sub-second flush keeps the 1s SSE
    # dashboard refresh visually lossless.
    _FLUSH_INTERVAL = 0.5
    _MAX_QUEUE = 10_000  # bounded; drop-oldest if Redis stalls so memory can't grow without bound

    def __init__(self, client: Redis, settings: Settings):
        self._client = client
        self._log_cap = settings.request_log_cap
        self._log_ttl = settings.request_log_ttl_seconds
        self._stats_ttl = settings.stats_ttl_seconds
        self._series_ttl = settings.stats_series_ttl_seconds
        self._pod = os.environ.get("HOSTNAME", "local")
        self._queue: deque[RequestSample] = deque()
        self._dropped = 0
        self._flush_task: asyncio.Task[None] | None = None

    def _k(self, name: str) -> str:
        return f"{STATS_PREFIX}{name}"

    async def record(self, sample: RequestSample) -> None:
        # Off the request critical path: enqueue only, never touch Redis here.
        # Bounded with drop-oldest so a stalled Redis can't grow this unbounded.
        if len(self._queue) >= self._MAX_QUEUE:
            self._queue.popleft()
            self._dropped += 1
        self._queue.append(sample)

    async def start(self) -> None:
        if self._flush_task is None:
            self._flush_task = asyncio.create_task(self._flush_loop())

    async def aclose(self) -> None:
        if self._flush_task is not None:
            self._flush_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._flush_task
            self._flush_task = None
        await self.flush()  # drain whatever is still queued before shutdown

    async def _flush_loop(self) -> None:
        while True:
            await asyncio.sleep(self._FLUSH_INTERVAL)
            await self.flush()

    async def flush(self) -> None:
        """Drain the queue into one folded pipeline.

        Best-effort: on a Redis error the batch is dropped (stats never gate or
        break anything), matching the old per-request guard.
        """
        dropped = self._dropped
        self._dropped = 0
        if not self._queue:
            if dropped:
                logger.warning("STATS_SAMPLES_DROPPED", dropped=dropped)
            return
        samples = list(self._queue)
        self._queue.clear()
        try:
            await self._write_batch(samples)
        except Exception as exc:  # never break anything on a stats write
            logger.warning("STATS_FLUSH_FAILED", error=str(exc), samples=len(samples))
        if dropped:
            logger.warning("STATS_SAMPLES_DROPPED", dropped=dropped)

    async def _write_batch(self, samples: list[RequestSample]) -> None:
        # Fold N samples into summed deltas: one round-trip and a handful of
        # commands instead of ~15*N. Counter increments sum, the request log takes
        # every entry in one LPUSH; final Redis state is identical to recording
        # each sample sequentially.
        n = len(samples)
        methods: dict[str, int] = defaultdict(int)
        latency: dict[str, int] = defaultdict(int)
        req_by_min: dict[str, int] = defaultdict(int)
        crypto_by_min: dict[str, float] = defaultdict(float)
        put_by_min: dict[str, float] = defaultdict(float)
        get_by_min: dict[str, float] = defaultdict(float)
        err_by_min: dict[str, int] = defaultdict(int)
        err_class: dict[str, int] = defaultdict(int)
        bytes_enc = 0.0
        bytes_dec = 0.0
        errors = 0
        entries: list[bytes] = []
        bucket_bytes: dict[str, float] = defaultdict(float)
        bucket_seen: dict[str, float] = {}
        bucket_keys: dict[str, set[str]] = defaultdict(set)

        for s in samples:
            entries.append(orjson.dumps(asdict(s)))
            minute = str(int(s.timestamp // 60) * 60)
            methods[s.method or "?"] += 1
            req_by_min[minute] += 1
            latency[_latency_bucket_le(s.duration_ms / 1000.0)] += 1
            if s.size > 0 and s.method in ("PUT", "POST"):
                bytes_enc += s.size
                crypto_by_min[minute] += s.size
                put_by_min[minute] += s.size
            elif s.size > 0 and s.method == "GET":
                bytes_dec += s.size
                crypto_by_min[minute] += s.size
                get_by_min[minute] += s.size
            if s.status >= 400:
                errors += 1
                err_by_min[minute] += 1
                for cls in error_buckets(s.status):
                    err_class[cls] += 1
            if s.bucket:
                bucket_seen[s.bucket] = max(bucket_seen.get(s.bucket, 0.0), s.timestamp)
                if s.size > 0:
                    bucket_bytes[s.bucket] += s.size
                if s.key:
                    bucket_keys[s.bucket].add(s.key)

        c = self._k("counters")
        async with self._client.pipeline(transaction=False) as pipe:
            # Entries are arrival-ordered; LPUSH prepends each in turn, leaving the
            # newest sample at the head — same final order as per-sample LPUSH.
            pipe.lpush(self._k("reqlog"), *entries)
            pipe.ltrim(self._k("reqlog"), 0, self._log_cap - 1)
            pipe.expire(self._k("reqlog"), self._log_ttl)
            pipe.hincrby(c, "requests", n)
            pipe.expire(c, self._stats_ttl)
            for method, cnt in methods.items():
                pipe.hincrby(self._k("methods"), method, cnt)
            pipe.expire(self._k("methods"), self._stats_ttl)
            for minute, cnt in req_by_min.items():
                pipe.hincrby(self._k("ts:requests"), minute, cnt)
            pipe.expire(self._k("ts:requests"), self._series_ttl)
            lat = self._k("latency")
            for le, cnt in latency.items():
                pipe.hincrby(lat, le, cnt)
            pipe.hincrby(lat, "+Inf", n)
            pipe.expire(lat, self._stats_ttl)
            if bytes_enc:
                pipe.hincrbyfloat(c, "bytes_encrypted", bytes_enc)
            if bytes_dec:
                pipe.hincrbyfloat(c, "bytes_decrypted", bytes_dec)
            for minute, v in crypto_by_min.items():
                pipe.hincrbyfloat(self._k("ts:crypto"), minute, v)
            if crypto_by_min:
                pipe.expire(self._k("ts:crypto"), self._series_ttl)
            for minute, v in put_by_min.items():
                pipe.hincrbyfloat(self._k("ts:bytes_put"), minute, v)
            if put_by_min:
                pipe.expire(self._k("ts:bytes_put"), self._series_ttl)
            for minute, v in get_by_min.items():
                pipe.hincrbyfloat(self._k("ts:bytes_get"), minute, v)
            if get_by_min:
                pipe.expire(self._k("ts:bytes_get"), self._series_ttl)
            if errors:
                pipe.hincrby(c, "errors", errors)
                for minute, cnt in err_by_min.items():
                    pipe.hincrby(self._k("ts:errors"), minute, cnt)
                pipe.expire(self._k("ts:errors"), self._series_ttl)
                eb = self._k("errors")
                for cls, cnt in err_class.items():
                    pipe.hincrby(eb, cls, cnt)
                pipe.expire(eb, self._stats_ttl)
            # Durable per-bucket index (every bucket served, not a recent window).
            # last_seen also enumerates buckets; distinct object counts use a
            # bounded HyperLogLog per bucket (~12KB) so it can't threaten the
            # noeviction Redis. Sliding TTL drops buckets idle for the series window.
            if bucket_seen:
                bseen = self._k("bucket:lastseen")
                pipe.hset(bseen, mapping={b: repr(ts) for b, ts in bucket_seen.items()})
                pipe.expire(bseen, self._series_ttl)
                bbytes = self._k("bucket:bytes")
                for b, nb in bucket_bytes.items():
                    pipe.hincrbyfloat(bbytes, b, nb)
                if bucket_bytes:
                    pipe.expire(bbytes, self._series_ttl)
                for b, keys in bucket_keys.items():
                    bk = self._k(f"bucket:obj:{b}")
                    pipe.pfadd(bk, *keys)
                    pipe.expire(bk, self._series_ttl)
            await pipe.execute()

    async def sync_local(self, local: LocalCounters) -> None:
        # No-op: every request is recorded (via record() + the background flush) on
        # every pod into shared cluster-wide counters, so there's no per-pod Prometheus
        # delta to fold in. (Kept on the interface for the in-memory store, which uses
        # it for live sparklines.)
        return

    async def aggregate(self) -> StatsAggregate | None:
        try:
            async with self._client.pipeline(transaction=False) as pipe:
                pipe.hgetall(self._k("counters"))
                pipe.hgetall(self._k("methods"))
                pipe.hgetall(self._k("errors"))
                pipe.hgetall(self._k("latency"))
                counters, methods, errors, latency = await pipe.execute()
            return StatsAggregate(
                requests=_hfloat(counters, b"requests"),
                errors=_hfloat(counters, b"errors"),
                bytes_encrypted=_hfloat(counters, b"bytes_encrypted"),
                bytes_decrypted=_hfloat(counters, b"bytes_decrypted"),
                methods=_decode_float_map(methods),
                errors_by_class=_decode_float_map(errors),
                latency_buckets=_decode_float_map(latency),
            )
        except Exception as exc:
            logger.warning("STATS_AGGREGATE_FAILED", error=str(exc))
            return None

    async def series(self, metric: str, range_key: str) -> tuple[list[float], list[float]]:
        window, bucket = RANGE_SPECS.get(range_key, RANGE_SPECS[DEFAULT_RANGE])
        hkey = self._k(f"ts:{metric}")
        try:
            raw = await self._client.hgetall(hkey)
        except Exception as exc:
            logger.warning("STATS_SERIES_FAILED", error=str(exc))
            return [], []
        points: list[tuple[int, float]] = []
        for k, v in raw.items():
            try:
                points.append((int(k), float(v)))
            except ValueError, TypeError:
                continue
        return bucket_series(points, window, bucket)

    async def recent(self, limit: int) -> list[dict]:
        try:
            raw = await self._client.lrange(self._k("reqlog"), 0, limit - 1)
        except Exception as exc:
            logger.warning("STATS_RECENT_FAILED", error=str(exc))
            return []
        return [_loads_entry(r) for r in raw if r is not None]

    async def buckets(self) -> list[dict]:
        try:
            async with self._client.pipeline(transaction=False) as pipe:
                pipe.hgetall(self._k("bucket:lastseen"))
                pipe.hgetall(self._k("bucket:bytes"))
                seen, byts = await pipe.execute()
        except Exception as exc:
            logger.warning("STATS_BUCKETS_FAILED", error=str(exc))
            return []
        names = [k.decode() if isinstance(k, bytes) else str(k) for k in seen]
        if not names:
            return []
        try:
            async with self._client.pipeline(transaction=False) as pipe:
                for name in names:
                    pipe.pfcount(self._k(f"bucket:obj:{name}"))
                counts = await pipe.execute()
        except Exception as exc:
            logger.warning("STATS_BUCKETS_FAILED", error=str(exc))
            counts = [0] * len(names)
        return [
            {
                "name": name,
                "objects": int(cnt or 0),
                "bytes": _hfloat(byts, name.encode()),
                "last_seen": _hfloat(seen, name.encode()),
            }
            for name, cnt in zip(names, counts, strict=False)
        ]

    async def page(self, offset: int, limit: int, query: str, operation: str, status: str) -> dict:
        try:
            total = await self._client.llen(self._k("reqlog"))
            # Fetch a generous window so post-filtering can still fill a page.
            raw = await self._client.lrange(self._k("reqlog"), 0, max(offset + limit * 5, 1000) - 1)
        except Exception as exc:
            logger.warning("STATS_PAGE_FAILED", error=str(exc))
            return {
                "entries": [],
                "count": 0,
                "offset": offset,
                "limit": limit,
                "total": 0,
                "has_more": False,
                "operations": [],
            }
        samples = [_loads_sample(r) for r in raw if r is not None]
        samples = [s for s in samples if s is not None]
        out = _filter_and_paginate(samples, offset, limit, query, operation, status)
        # Unfiltered: report the full list length (entries may exist beyond the
        # fetched window). Filtered: keep the matched count from the window so
        # pagination over the filtered set is correct.
        if not (query or operation or status):
            out["total"] = int(total)
            out["has_more"] = offset + out["count"] < int(total)
        return out


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _hfloat(h: dict, field: bytes) -> float:
    v = h.get(field)
    if v is None:
        return 0.0
    try:
        return float(v)
    except ValueError, TypeError:
        return 0.0


def _decode_float_map(h: dict) -> dict[str, float]:
    out: dict[str, float] = {}
    for k, v in h.items():
        key = k.decode() if isinstance(k, bytes) else str(k)
        try:
            out[key] = float(v)
        except ValueError, TypeError:
            continue
    return out


def _loads_entry(raw: bytes) -> dict:
    return orjson.loads(raw)


def _loads_sample(raw: bytes) -> RequestSample | None:
    try:
        d = orjson.loads(raw)
        return RequestSample(**d)
    except ValueError, TypeError:
        return None


def _filter_and_paginate(
    samples: list[RequestSample],
    offset: int,
    limit: int,
    query: str,
    operation: str,
    status: str,
) -> dict:
    q = (query or "").strip().lower()
    op_filter = (operation or "").strip().upper()
    status_filter = (status or "").strip().lower()

    matched: list[RequestSample] = []
    for e in samples:
        if op_filter and (e.method or e.operation).upper() != op_filter:
            continue
        if status_filter:
            is_err = e.status >= 400
            if status_filter == "success" and is_err:
                continue
            if status_filter == "error" and not is_err:
                continue
        if q:
            blob = f"{e.bucket} {e.key} {e.client_ip} {e.method} {e.operation} {e.status}".lower()
            if q not in blob:
                continue
        matched.append(e)

    page = matched[offset : offset + limit]
    all_ops = sorted({(e.method or e.operation) for e in samples if e.method or e.operation})
    return {
        "entries": [asdict(e) for e in page],
        "count": len(page),
        "offset": offset,
        "limit": limit,
        "total": len(matched),
        "has_more": offset + len(page) < len(matched),
        "operations": all_ops,
    }


# ---------------------------------------------------------------------------
# Factory + module-global store (used by the synchronous record_request path)
# ---------------------------------------------------------------------------


_store: StatsStore | None = None


def create_stats_store(settings: Settings) -> StatsStore:
    """Build the appropriate stats store based on Redis configuration.

    Call AFTER init_redis(). Mirrors state.redis.create_state_store().
    """
    from ..state.redis import _redis_client, _use_redis

    if _use_redis and _redis_client is not None:
        return RedisStatsStore(_redis_client, settings)
    return MemoryStatsStore(settings)


def set_store(store: StatsStore) -> None:
    global _store
    _store = store


def get_store() -> StatsStore | None:
    return _store
