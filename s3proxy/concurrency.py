"""Memory-based concurrency limiting for S3Proxy."""

from __future__ import annotations

import asyncio
import contextlib
import ctypes
import gc
import os
import sys
from collections.abc import Callable

import structlog

from s3proxy.errors import S3Error
from s3proxy.metrics import MEMORY_LIMIT_BYTES, MEMORY_REJECTIONS, MEMORY_RESERVED_BYTES

logger = structlog.get_logger(__name__)

# Constants
MIN_RESERVATION = 64 * 1024  # 64KB minimum per request
MAX_BUFFER_SIZE = 8 * 1024 * 1024  # 8MB streaming buffer size


def _create_malloc_release() -> Callable[[], int] | None:
    """Create platform-specific function to release memory back to OS.

    Only works on Linux via malloc_trim(0). Returns None on other platforms.
    """
    if sys.platform != "linux":
        return None

    try:
        libc = ctypes.CDLL("libc.so.6")
        libc.malloc_trim.argtypes = [ctypes.c_size_t]
        libc.malloc_trim.restype = ctypes.c_int
        return lambda: libc.malloc_trim(0)
    except OSError, AttributeError:
        return None


_malloc_release = _create_malloc_release()


BACKPRESSURE_TIMEOUT = int(os.environ.get("S3PROXY_BACKPRESSURE_TIMEOUT", "30"))


class ConcurrencyLimiter:
    """Memory-based concurrency limiter with backpressure.

    Tracks reserved memory across concurrent requests. When the limit would be
    exceeded, waits for memory to free up instead of rejecting immediately.
    """

    def __init__(self, limit_mb: int = 128) -> None:
        self._limit_mb = limit_mb
        self._limit_bytes = limit_mb * 1024 * 1024
        self._active_bytes = 0
        self._lock = asyncio.Lock()
        self._condition = asyncio.Condition(self._lock)
        MEMORY_LIMIT_BYTES.set(self._limit_bytes)

    @property
    def limit_bytes(self) -> int:
        return self._limit_bytes

    @property
    def active_bytes(self) -> int:
        return self._active_bytes

    @active_bytes.setter
    def active_bytes(self, value: int) -> None:
        """Set active memory (testing only)."""
        self._active_bytes = value

    def set_memory_limit(self, limit_mb: int) -> None:
        """Update the memory limit."""
        self._limit_mb = limit_mb
        self._limit_bytes = limit_mb * 1024 * 1024
        MEMORY_LIMIT_BYTES.set(self._limit_bytes)

    async def try_acquire(self, bytes_needed: int) -> int:
        """Reserve memory, waiting up to BACKPRESSURE_TIMEOUT if at capacity."""
        if self._limit_bytes <= 0:
            return 0

        to_reserve = max(MIN_RESERVATION, bytes_needed)

        # Single request exceeds entire budget — can never fit, reject immediately
        if to_reserve > self._limit_bytes:
            request_mb = to_reserve / 1024 / 1024
            limit_mb = self._limit_bytes / 1024 / 1024
            logger.warning(
                "MEMORY_TOO_LARGE",
                requested_mb=round(request_mb, 2),
                limit_mb=round(limit_mb, 2),
            )
            MEMORY_REJECTIONS.inc()
            raise S3Error.slow_down(
                f"Request needs {request_mb:.0f}MB but budget is {limit_mb:.0f}MB"
            )

        async with self._condition:
            deadline = asyncio.get_event_loop().time() + BACKPRESSURE_TIMEOUT
            while self._active_bytes + to_reserve > self._limit_bytes:
                remaining = deadline - asyncio.get_event_loop().time()
                if remaining <= 0:
                    active_mb = self._active_bytes / 1024 / 1024
                    request_mb = to_reserve / 1024 / 1024
                    limit_mb = self._limit_bytes / 1024 / 1024
                    logger.warning(
                        "MEMORY_REJECTED",
                        active_mb=round(active_mb, 2),
                        requested_mb=round(request_mb, 2),
                        limit_mb=round(limit_mb, 2),
                        waited_sec=BACKPRESSURE_TIMEOUT,
                    )
                    MEMORY_REJECTIONS.inc()
                    raise S3Error.slow_down(
                        f"Memory limit: {active_mb:.0f}MB + {request_mb:.0f}MB > {limit_mb:.0f}MB"
                    )
                logger.info(
                    "MEMORY_BACKPRESSURE",
                    active_mb=round(self._active_bytes / 1024 / 1024, 2),
                    requested_mb=round(to_reserve / 1024 / 1024, 2),
                    limit_mb=round(self._limit_bytes / 1024 / 1024, 2),
                    remaining_sec=round(remaining, 1),
                )
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._condition.wait(), timeout=remaining)

            self._active_bytes += to_reserve
            MEMORY_RESERVED_BYTES.set(self._active_bytes)
            return to_reserve

    async def release(self, bytes_reserved: int) -> None:
        """Release reserved memory and wake waiting requests."""
        if self._limit_bytes <= 0 or bytes_reserved <= 0:
            return

        async with self._condition:
            self._active_bytes = max(0, self._active_bytes - bytes_reserved)
            MEMORY_RESERVED_BYTES.set(self._active_bytes)
            self._condition.notify_all()

        # Run garbage collection and release memory to OS
        gc.collect(0)
        gc.collect(1)
        gc.collect(2)

        if _malloc_release:
            with contextlib.suppress(OSError):
                _malloc_release()

        # Yield to allow OS memory reclaim
        await asyncio.sleep(0)


# Default instance used by module-level functions
_default = ConcurrencyLimiter(limit_mb=int(os.environ.get("S3PROXY_MEMORY_LIMIT_MB", "64")))


def estimate_memory_footprint(method: str, content_length: int) -> int:
    """Estimate memory needed for a request.

    Streaming PUTs hold an 8MB plaintext buffer + 8MB ciphertext simultaneously,
    so large PUTs need 2x MAX_BUFFER_SIZE. Small PUTs buffer the whole body + ciphertext.
    GETs reserve a baseline here; encrypted GETs acquire additional memory in the handler.
    """
    if method in ("HEAD", "DELETE"):
        return 0
    if method == "GET":
        return MAX_BUFFER_SIZE
    if method == "POST":
        return MIN_RESERVATION
    if content_length <= MAX_BUFFER_SIZE:
        return max(MIN_RESERVATION, content_length * 2)
    return MAX_BUFFER_SIZE * 2


# Module-level convenience functions delegating to the default instance


def get_memory_limit() -> int:
    return _default.limit_bytes


def get_active_memory() -> int:
    return _default.active_bytes


async def try_acquire_memory(bytes_needed: int) -> int:
    return await _default.try_acquire(bytes_needed)


async def release_memory(bytes_reserved: int) -> None:
    await _default.release(bytes_reserved)


def reset_state() -> None:
    """Reset default instance state (testing only)."""
    global _default
    _default = ConcurrencyLimiter(limit_mb=_default._limit_mb)
    # Reset reserved bytes metric to 0 for clean test state
    MEMORY_RESERVED_BYTES.set(0)


def set_memory_limit(limit_mb: int) -> None:
    """Set memory limit on default instance (testing only)."""
    _default.set_memory_limit(limit_mb)


def set_active_memory(bytes_val: int) -> None:
    """Set active memory on default instance (testing only)."""
    _default.active_bytes = bytes_val
