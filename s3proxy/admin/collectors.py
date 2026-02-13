"""Data collectors for admin dashboard."""

from __future__ import annotations

import hashlib
import time
from typing import TYPE_CHECKING

from .. import metrics
from ..state.redis import is_using_redis

if TYPE_CHECKING:
    from ..config import Settings
    from ..handlers import S3ProxyHandler


def collect_key_status(settings: Settings) -> dict:
    """Collect encryption key status. Never exposes raw key material."""
    return {
        "kek_fingerprint": hashlib.sha256(settings.kek).hexdigest()[:16],
        "algorithm": "AES-256-GCM + AES-KWP",
        "dek_tag_name": settings.dektag_name,
    }


async def collect_upload_status(handler: S3ProxyHandler) -> dict:
    """Collect active multipart upload status."""
    uploads = await handler.multipart_manager.list_active_uploads()
    return {
        "active_count": len(uploads),
        "uploads": uploads,
    }


def _read_gauge(gauge) -> float:
    """Read current value from a Prometheus Gauge."""
    return gauge._value.get()


def _read_counter(counter) -> float:
    """Read current value from a Prometheus Counter."""
    return counter._value.get()


def _read_labeled_counter_sum(counter) -> float:
    """Sum all label combinations for a labeled counter."""
    total = 0.0
    for sample in counter.collect()[0].samples:
        if sample.name.endswith("_total"):
            total += sample.value
    return total


def _read_labeled_gauge_sum(gauge) -> float:
    """Sum all label combinations for a labeled gauge."""
    total = 0.0
    for sample in gauge.collect()[0].samples:
        total += sample.value
    return total


def collect_system_health(start_time: float) -> dict:
    """Collect system health metrics."""
    memory_reserved = _read_gauge(metrics.MEMORY_RESERVED_BYTES)
    memory_limit = _read_gauge(metrics.MEMORY_LIMIT_BYTES)
    usage_pct = round(memory_reserved / memory_limit * 100, 1) if memory_limit > 0 else 0

    return {
        "memory_reserved_bytes": int(memory_reserved),
        "memory_limit_bytes": int(memory_limit),
        "memory_usage_pct": usage_pct,
        "requests_in_flight": int(_read_labeled_gauge_sum(metrics.REQUESTS_IN_FLIGHT)),
        "memory_rejections": int(_read_counter(metrics.MEMORY_REJECTIONS)),
        "uptime_seconds": int(time.monotonic() - start_time),
        "storage_backend": ("Redis (HA)" if is_using_redis() else "In-memory"),
    }


def collect_request_stats() -> dict:
    """Collect request statistics."""
    encrypt_ops = 0.0
    decrypt_ops = 0.0
    for sample in metrics.ENCRYPTION_OPERATIONS.collect()[0].samples:
        if sample.name.endswith("_total"):
            if sample.labels.get("operation") == "encrypt":
                encrypt_ops = sample.value
            elif sample.labels.get("operation") == "decrypt":
                decrypt_ops = sample.value

    return {
        "total_requests": int(_read_labeled_counter_sum(metrics.REQUEST_COUNT)),
        "encrypt_ops": int(encrypt_ops),
        "decrypt_ops": int(decrypt_ops),
        "bytes_encrypted": int(_read_counter(metrics.BYTES_ENCRYPTED)),
        "bytes_decrypted": int(_read_counter(metrics.BYTES_DECRYPTED)),
    }


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


async def collect_all(
    settings: Settings,
    handler: S3ProxyHandler,
    start_time: float,
) -> dict:
    """Collect all dashboard data."""
    upload_status = await collect_upload_status(handler)
    health = collect_system_health(start_time)
    stats = collect_request_stats()
    return {
        "key_status": collect_key_status(settings),
        "upload_status": upload_status,
        "system_health": health,
        "request_stats": stats,
        "formatted": {
            "memory_reserved": _format_bytes(health["memory_reserved_bytes"]),
            "memory_limit": _format_bytes(health["memory_limit_bytes"]),
            "uptime": _format_uptime(health["uptime_seconds"]),
            "bytes_encrypted": _format_bytes(stats["bytes_encrypted"]),
            "bytes_decrypted": _format_bytes(stats["bytes_decrypted"]),
        },
    }
