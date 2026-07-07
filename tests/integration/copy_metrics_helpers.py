"""Shared helpers for copy-path governor integration tests (Prometheus scraping)."""

from __future__ import annotations

import contextlib
import threading
import time
import urllib.request
from dataclasses import dataclass, field

from s3proxy import crypto

MB = 1024 * 1024

# Prod pod budget and per-chunk peak for 32MB internal parts.
PROD_GOVERNOR_MB = 192
CHUNK_PEAK = crypto.copy_chunk_peak(crypto.COPY_INTERNAL_PART_SIZE)
# Between internal parts only the POST gate (~64KB) should remain reserved.
BASELINE_CEILING = 2 * MB

# Scylla backup copies ~1.25GB SSTables (40 × 32MB internal parts in s3proxy).
SCYLLA_SSTABLE_MB = 1280
SCYLLA_SSTABLE_SIZE = SCYLLA_SSTABLE_MB * MB


def scrape_reserved_bytes(port: int) -> int:
    with urllib.request.urlopen(f"http://localhost:{port}/metrics", timeout=3) as resp:
        for line in resp.read().decode().splitlines():
            if line.startswith("s3proxy_memory_reserved_bytes "):
                return int(float(line.split()[1]))
    return 0


@dataclass
class MetricsPoller:
    """Poll s3proxy_memory_reserved_bytes in a background thread."""

    port: int
    interval_s: float = 0.02
    samples: list[int] = field(default_factory=list)
    _stop: threading.Event = field(default_factory=threading.Event, repr=False)
    _thread: threading.Thread | None = field(default=None, repr=False)

    def start(self) -> None:
        self._stop.clear()

        def _poll() -> None:
            while not self._stop.is_set():
                with contextlib.suppress(OSError):
                    self.samples.append(scrape_reserved_bytes(self.port))
                time.sleep(self.interval_s)

        self._thread = threading.Thread(target=_poll, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)


def internal_part_count(plaintext_size: int) -> int:
    return -(-plaintext_size // crypto.COPY_INTERNAL_PART_SIZE)


def assert_copy_memory_sawtooth(
    reserved: list[int],
    *,
    plaintext_size: int,
    min_drops: int | None = None,
    min_samples: int = 10,
) -> None:
    """Fail if reserved bytes stayed flat (whole-object hold) during a streaming copy."""
    parts = internal_part_count(plaintext_size)
    if min_drops is None:
        # Need to observe several inter-part gaps; scale with part count.
        min_drops = max(3, parts // 4)

    assert len(reserved) >= min_samples, (
        f"too few metric samples ({len(reserved)}); copy finished before polling?"
    )

    min_reserved = min(reserved)
    max_reserved = max(reserved)
    drops = sum(1 for b in reserved if b <= BASELINE_CEILING)

    assert min_reserved <= BASELINE_CEILING, (
        f"reserved never dropped between internal parts: min={min_reserved / MB:.2f}MB "
        f"(expected <= {BASELINE_CEILING / MB:.2f}MB). "
        f"max={max_reserved / MB:.2f}MB samples={len(reserved)} parts={parts}"
    )
    assert max_reserved <= CHUNK_PEAK * 1.2, (
        f"peak reserved {max_reserved / MB:.2f}MB exceeds one-chunk budget "
        f"{CHUNK_PEAK / MB:.2f}MB — whole-object hold?"
    )
    assert drops >= min_drops, (
        f"sawtooth too shallow: only {drops} samples <= {BASELINE_CEILING / MB:.2f}MB "
        f"(need >= {min_drops} for {parts} internal parts; "
        f"min={min_reserved / MB:.2f}MB max={max_reserved / MB:.2f}MB)"
    )


def assert_concurrent_copies_not_flatlined(
    reserved: list[int],
    *,
    concurrent_copies: int,
    min_samples: int = 20,
) -> None:
    """Two whole-object holds keep min ~concurrent_copies×CHUNK_PEAK with no drops."""
    assert len(reserved) >= min_samples, f"too few samples ({len(reserved)})"
    min_reserved = min(reserved)
    drops = sum(1 for b in reserved if b <= BASELINE_CEILING)
    flat_floor = CHUNK_PEAK * concurrent_copies * 0.85

    assert min_reserved <= BASELINE_CEILING, (
        f"concurrent copies never released between parts: min={min_reserved / MB:.2f}MB "
        f"(old whole-object code stays >= {flat_floor / MB:.2f}MB for {concurrent_copies} copies)"
    )
    assert drops >= 5, (
        f"expected inter-part drops during {concurrent_copies} concurrent copies; "
        f"got {drops} (min={min_reserved / MB:.2f}MB)"
    )
