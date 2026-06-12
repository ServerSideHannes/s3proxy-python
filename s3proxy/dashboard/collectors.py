"""Data collectors for the dashboard."""

from __future__ import annotations

import hashlib
import os
import time
from typing import TYPE_CHECKING

from .. import metrics
from .stats_store import (
    LocalCounters,
    RequestSample,
    StatsStore,
    error_buckets,
    get_store,
)

if TYPE_CHECKING:
    from ..config import Settings


async def record_request(
    method: str,
    path: str,
    operation: str,
    status: int,
    duration: float,
    size: int,
    client_ip: str = "",
) -> None:
    """Append a completed request to the stats store (no-op if unset)."""
    store = get_store()
    if store is None:
        return
    bucket, key = _split_bucket_key(path)
    await store.record(
        RequestSample(
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


def _read_request_breakdowns() -> tuple[dict[str, float], dict[str, float]]:
    """Single pass over REQUEST_COUNT samples: (errors_by_class, methods)."""
    errors = {"4xx": 0.0, "5xx": 0.0, "503": 0.0}
    methods: dict[str, float] = {}
    for sample in metrics.REQUEST_COUNT.collect()[0].samples:
        if not sample.name.endswith("_total"):
            continue
        method = str(sample.labels.get("method", "?"))
        methods[method] = methods.get(method, 0.0) + sample.value
        status = str(sample.labels.get("status", ""))
        for cls in error_buckets(status):
            errors[cls] += sample.value
    return errors, methods


def _read_latency_buckets() -> dict[str, float]:
    """Read the Prometheus histogram as a {le-string: cumulative count} map.

    Includes the synthetic "+Inf" bucket (== total observations). Matches the
    shape the Redis latency hash stores so percentiles can be computed from
    either source.
    """
    out: dict[str, float] = {}
    for sample in metrics.REQUEST_DURATION.collect()[0].samples:
        if not sample.name.endswith("_bucket"):
            continue
        le = sample.labels.get("le", "")
        out[le] = out.get(le, 0.0) + sample.value
    return out


def _latency_percentiles(cumulative: dict[str, float] | None = None) -> dict[str, float]:
    """Approximate p50/p95/p99 by walking the histogram cumulative buckets.

    ``cumulative`` is a {le-string: cumulative count} map. When None, reads the
    per-pod Prometheus histogram.
    """
    if cumulative is None:
        cumulative = _read_latency_buckets()

    buckets: list[tuple[float, float]] = []
    total = float(cumulative.get("+Inf", 0.0))
    for le, count in cumulative.items():
        if le == "+Inf":
            continue
        try:
            buckets.append((float(le), float(count)))
        except ValueError, TypeError:
            continue
    if total < 1 and buckets:
        total = max(c for _, c in buckets)
    if total < 1 or not buckets:
        return {"p50_ms": 0.0, "p95_ms": 0.0, "p99_ms": 0.0, "count": 0}
    buckets.sort(key=lambda b: b[0])

    def _pct(p: float) -> float:
        threshold = total * p
        for upper, count in buckets:
            if count >= threshold:
                return round(upper * 1000, 1)
        return round(buckets[-1][0] * 1000, 1)

    return {
        "p50_ms": _pct(0.5),
        "p95_ms": _pct(0.95),
        "p99_ms": _pct(0.99),
        "count": int(total),
    }


def _local_counters() -> LocalCounters:
    """Snapshot this pod's Prometheus-derived counters for delta-syncing."""
    errors_by_class, methods = _read_request_breakdowns()
    return LocalCounters(
        requests=_read_labeled_counter_sum(metrics.REQUEST_COUNT),
        errors=_read_errors_total(),
        bytes_encrypted=_read_counter(metrics.BYTES_ENCRYPTED),
        bytes_decrypted=_read_counter(metrics.BYTES_DECRYPTED),
        methods=methods,
        errors_by_class=errors_by_class,
        latency_buckets=_read_latency_buckets(),
    )


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


def _format_absolute(ts: float) -> str:
    """Render a Unix epoch as a local 'YYYY-MM-DD HH:MM:SS.mmm' timestamp."""
    ms = int((ts - int(ts)) * 1000)
    return f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts))}.{ms:03d}"


def _format_size(n: int) -> str:
    if n <= 0:
        return "—"
    num, unit = _format_bytes(n)
    return f"{num} {unit}"


# ---------------------------------------------------------------------------
# Derived aggregations from the request log
# ---------------------------------------------------------------------------


def _format_buckets(buckets: list[dict]) -> list[dict]:
    """Add a human-readable size to the store's raw per-bucket aggregates.

    The store owns the durable per-bucket totals (every bucket the proxy has
    served, not just a recent window); this only handles presentation.
    """
    out: list[dict] = []
    for b in buckets:
        nbytes = b.get("bytes", 0)
        num, unit = _format_bytes(nbytes)
        out.append(
            {
                "name": b["name"],
                "objects": int(b.get("objects", 0)),
                "size": f"{num} {unit}" if nbytes > 0 else "\u2014",
                "last_seen": b.get("last_seen", 0.0),
            }
        )
    out.sort(key=lambda b: b["last_seen"], reverse=True)
    return out


def _derive_keys(settings: Settings) -> list[dict]:
    # One KEK per configured AWS login (access key). The kek secret itself is
    # never exposed - only a short fingerprint for identification.
    keys = []
    for entry in settings.credentials:
        fp = hashlib.sha256(entry.kek.encode()).hexdigest()[:8]
        keys.append(
            {
                "id": entry.access_key,
                "type": f"Local (KEK · {fp})",
                "status": "Active",
                "created": "—",
            }
        )
    return keys


# ---------------------------------------------------------------------------
# Aggregate collector
# ---------------------------------------------------------------------------


async def collect_all(
    store: StatsStore,
    settings: Settings,
    start_time: float,
    version: str = "1.0.0",
    range_key: str = "live",
) -> dict:
    """Gather everything the dashboard renders in a single JSON blob.

    Reads cluster-wide aggregates from the Redis store when available, else
    falls back to this pod's local Prometheus counters.
    """
    now = time.time()
    uptime_s = max(0.0, time.monotonic() - start_time)

    # Fold this pod's local Prometheus deltas into the shared store, then read
    # the cluster-wide aggregate. MemoryStatsStore.aggregate() returns None,
    # so single-instance mode keeps reading Prometheus directly below.
    local = _local_counters()
    await store.sync_local(local)
    agg = await store.aggregate()

    if agg is not None:
        total_requests = agg.requests
        bytes_encrypted = agg.bytes_encrypted
        bytes_decrypted = agg.bytes_decrypted
        errors_total = agg.errors
        method_breakdown = agg.methods
        error_breakdown = agg.errors_by_class or {"4xx": 0.0, "5xx": 0.0, "503": 0.0}
        latency = _latency_percentiles(agg.latency_buckets)
    else:
        total_requests = local.requests
        bytes_encrypted = local.bytes_encrypted
        bytes_decrypted = local.bytes_decrypted
        errors_total = local.errors
        method_breakdown = local.methods
        error_breakdown = local.errors_by_class
        latency = _latency_percentiles(local.latency_buckets)

    req_times, req_values = await store.series("requests", range_key)
    crypto_times, crypto_values = await store.series("crypto", range_key)
    err_times, err_values = await store.series("errors", range_key)

    req_rate = (req_values[-1] / 60.0) if req_values else 0.0
    crypto_rate = (crypto_values[-1] / 60.0) if crypto_values else 0.0

    num_enc, unit_enc = _format_bytes(bytes_encrypted)
    num_thr, unit_thr = _format_bytes(crypto_rate)

    activity = await store.recent(10)
    buckets = _format_buckets(await store.buckets())
    last_error_ts = next((e["timestamp"] for e in activity if e["status"] >= 400), None)

    return {
        "header": {
            "title": "S3 Encryption Proxy",
            "status": "Running",
            "uptime": _format_uptime(uptime_s),
            "pod": os.environ.get("HOSTNAME", "local"),
            "version": version,
            "cluster_wide": store.cluster_wide,
            "range": range_key,
        },
        "cards": {
            "requests": {
                "label": "Requests",
                "value": f"{int(total_requests):,}",
                "unit": "",
                "spark": req_values,
                "spark_times": req_times,
                "y_label": "req / bucket",
                "breakdown": [
                    {"label": m, "value": f"{int(v):,}", "weight": float(v)}
                    for m, v in sorted(method_breakdown.items(), key=lambda kv: -kv[1])
                    if v > 0
                ],
            },
            "data_encrypted": {
                "label": "Data Encrypted",
                "value": num_enc,
                "unit": unit_enc,
                "spark": crypto_values,
                "spark_times": crypto_times,
                "y_label": "bytes / bucket",
                "breakdown": [
                    {
                        "label": "Encrypted (PUT)",
                        "value": " ".join(_format_bytes(bytes_encrypted)),
                        "weight": float(bytes_encrypted),
                    },
                    {
                        "label": "Decrypted (GET)",
                        "value": " ".join(_format_bytes(bytes_decrypted)),
                        "weight": float(bytes_decrypted),
                    },
                ],
            },
            "errors": {
                "label": "Errors",
                "value": f"{int(errors_total):,}",
                "unit": "",
                "spark": err_values,
                "spark_times": err_times,
                "y_label": "errors / bucket",
                "breakdown": [
                    {"label": k, "value": f"{int(v):,}", "weight": float(v)}
                    for k, v in error_breakdown.items()
                ],
            },
            "active_buckets": {
                "label": "Active Buckets",
                "value": str(len(buckets)),
                "unit": "",
                "detail": f"seen in last {len(activity)} reqs",
                "breakdown": [
                    {
                        "label": b["name"],
                        "value": f"{b['objects']} obj · {b['size']}",
                        "weight": float(b["objects"]),
                    }
                    for b in buckets[:8]
                ],
            },
        },
        "latency": latency,
        "activity": [
            {
                "time": _format_absolute(e["timestamp"]),
                "time_relative": _format_relative(e["timestamp"], now),
                "timestamp": e["timestamp"],
                "operation": _operation_display(e["method"], e["operation"]),
                "bucket": e["bucket"] or "—",
                "object": e["key"] or "—",
                "status": "Success" if e["status"] < 400 else "Error",
                "status_code": e["status"],
                "size": _format_size(e["size"]),
                "client_ip": e["client_ip"] or "—",
                "latency": f"{e['duration_ms']:.0f} ms",
            }
            for e in activity
        ],
        "buckets": [
            {
                "name": b["name"],
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
            "last_error": _format_absolute(last_error_ts) if last_error_ts else "never",
        },
    }


async def collect_series(store: StatsStore, metric: str, range_key: str) -> dict:
    """Return a single metric's time-series for the chart range selector."""
    times, values = await store.series(metric, range_key)
    return {"metric": metric, "range": range_key, "spark_times": times, "spark": values}


async def collect_throughput(store: StatsStore, range_key: str) -> dict:
    """Return PUT (encrypted) and GET (decrypted) byte series as two lines."""
    put_times, put_values = await store.series("bytes_put", range_key)
    get_times, get_values = await store.series("bytes_get", range_key)
    return {
        "range": range_key,
        "series": [
            {"label": "Encrypted (PUT)", "spark_times": put_times, "spark": put_values},
            {"label": "Decrypted (GET)", "spark_times": get_times, "spark": get_values},
        ],
    }


def _operation_display(method: str, operation: str) -> str:
    """Shorten operation names for the feed (GET, PUT, DELETE, etc.)."""
    return method or operation


# ---------------------------------------------------------------------------
# S3-backed drill-down helpers (bucket list, object head)
# ---------------------------------------------------------------------------


async def list_bucket_objects(
    settings: Settings,
    credentials_store: dict[str, str],
    bucket: str,
    prefix: str = "",
    delimiter: str | None = "/",
    max_keys: int = 1000,
    offset: int = 0,
    page_size: int = 20,
) -> dict:
    """List a "directory" in a bucket using ListObjectsV2 with a delimiter.

    When delimiter is "/" (default), the response is split into sub-prefixes
    (folders) and objects at the current level — the standard S3-console
    file-explorer shape.

    Objects are paginated client-side (offset/page_size). The expensive
    per-object encryption HEAD fan-out runs only on the current page slice, so a
    huge folder stays cheap (≈page_size HEADs, not one per object in the bucket).
    """
    from ..client import S3Client, S3Credentials

    if not credentials_store:
        raise RuntimeError("No S3 credentials available")
    access = next(iter(credentials_store))
    creds = S3Credentials(access, credentials_store[access], settings.region)

    async with S3Client(settings, creds) as client:
        result = await client.list_objects_v2(
            bucket=bucket,
            prefix=prefix or None,
            delimiter=delimiter,
            max_keys=max_keys,
        )

        prefix_len = len(prefix)
        folders: list[dict] = []
        for cp in result.get("CommonPrefixes", []) or []:
            full = cp.get("Prefix", "")
            name = full[prefix_len:].rstrip("/")
            folders.append({"prefix": full, "name": name})

        all_objects: list[dict] = []
        for o in result.get("Contents", []) or []:
            full = o.get("Key", "")
            # Skip the "directory marker" object that some tools create at the prefix itself
            if full == prefix:
                continue
            size = int(o.get("Size", 0))
            lm = o.get("LastModified")
            all_objects.append(
                {
                    "key": full,
                    "name": full[prefix_len:],
                    "size": size,
                    "size_h": _format_size(size),
                    "last_modified": lm.isoformat() if lm else "",
                    "etag": (o.get("ETag") or "").strip('"'),
                }
            )

        total_objects = len(all_objects)
        objects = all_objects[offset : offset + page_size]

        # Per-object encryption status, the same check the object-detail view uses
        # (on-object isec tag, else multipart sidecar). Run only on the current
        # page (≈page_size HEADs). These dashboard HEADs use this S3Client directly
        # (not the proxy), so they don't pollute the dashboard stats.
        await _annotate_encryption(settings, client, bucket, objects)

    return {
        "bucket": bucket,
        "prefix": prefix,
        "delimiter": delimiter or "",
        "folders": folders,
        "objects": objects,
        "offset": offset,
        "page_size": page_size,
        "total_objects": total_objects,
        "has_more": offset + len(objects) < total_objects,
        "count": len(folders) + total_objects,
        "is_truncated": bool(result.get("IsTruncated", False)),
    }


async def _annotate_encryption(settings, client, bucket: str, objects: list[dict]) -> None:
    """Set obj['encrypted'] for each listed object using the GET-path detection."""
    import asyncio

    sem = asyncio.Semaphore(32)

    async def check(obj: dict) -> None:
        async with sem:
            try:
                md = await client.head_object(bucket, obj["key"])
            except Exception:
                obj["encrypted"] = None  # couldn't determine
                return
            user_md = dict(md.get("Metadata") or {})
            has_tag = user_md.get(settings.dektag_name) is not None
            # `or` short-circuits: skip the sidecar lookup when an on-object tag is present.
            obj["encrypted"] = has_tag or await _has_multipart_sidecar(client, bucket, obj["key"])

    if objects:
        await asyncio.gather(*(check(o) for o in objects))


async def list_logs(
    store: StatsStore,
    limit: int = 50,
    offset: int = 0,
    query: str = "",
    operation: str = "",
    status: str = "",
) -> dict:
    """Return a filtered, paginated page of request-log entries for /logs."""
    now = time.time()
    page = await store.page(offset, limit, query, operation, status)
    entries = [
        {
            "time": _format_absolute(e["timestamp"]),
            "time_relative": _format_relative(e["timestamp"], now),
            "timestamp": e["timestamp"],
            "operation": _operation_display(e["method"], e["operation"]),
            "bucket": e["bucket"] or "",
            "object": e["key"] or "",
            "status": "Success" if e["status"] < 400 else "Error",
            "status_code": e["status"],
            "size": _format_size(e["size"]),
            "client_ip": e["client_ip"] or "",
            "latency": f"{e['duration_ms']:.0f} ms",
        }
        for e in page["entries"]
    ]
    return {
        "count": page["count"],
        "offset": page["offset"],
        "limit": page["limit"],
        "total": page["total"],
        "has_more": page["has_more"],
        "entries": entries,
        "operations": page["operations"],
    }


async def head_object_detail(
    settings: Settings,
    credentials_store: dict[str, str],
    bucket: str,
    key: str,
) -> dict:
    """HEAD an object and return user-facing metadata."""
    from ..client import S3Client, S3Credentials

    if not credentials_store:
        raise RuntimeError("No S3 credentials available")
    access = next(iter(credentials_store))
    creds = S3Credentials(access, credentials_store[access], settings.region)

    async with S3Client(settings, creds) as client:
        md = await client.head_object(bucket, key)

        user_metadata = dict(md.get("Metadata") or {})
        # Redact the binary envelope (encrypted DEK) — it's opaque to humans.
        isec = user_metadata.pop(settings.dektag_name, None)
        if isec is not None:
            user_metadata["_encrypted_dek"] = f"<{len(isec)} bytes>"
            enc_via = "metadata"
        elif await _has_multipart_sidecar(client, bucket, key):
            # Multipart objects store the wrapped DEK in a sidecar object, not an
            # on-object tag — the create-time metadata doesn't survive
            # CompleteMultipartUpload. Consult the sidecar before concluding
            # "not encrypted" (mirrors the GET read path).
            enc_via = "sidecar"
        else:
            enc_via = ""

    lm = md.get("LastModified")
    return {
        "bucket": bucket,
        "key": key,
        "content_length": int(md.get("ContentLength", 0)),
        "size_h": _format_size(int(md.get("ContentLength", 0))),
        "content_type": md.get("ContentType", ""),
        "etag": (md.get("ETag") or "").strip('"'),
        "last_modified": lm.isoformat() if lm else "",
        "metadata": user_metadata,
        "encrypted": bool(enc_via),
        "encryption_source": enc_via,
    }


async def _has_multipart_sidecar(client, bucket: str, key: str) -> bool:
    """True if the object has an s3proxy multipart-metadata sidecar (= encrypted).

    Reuses the GET path's unified lookup so detection matches decryption exactly.
    """
    from ..state.metadata import load_multipart_metadata

    try:
        return await load_multipart_metadata(client, bucket, key) is not None
    except Exception:
        return False
