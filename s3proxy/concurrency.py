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

from s3proxy.crypto import (
    copy_governor_clamped_reserve,
    governor_memory_footprint,
    streaming_governor_clamped_reserve,
)
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
        self._pending_exclusive = 0
        self._lock = asyncio.Lock()
        self._condition = asyncio.Condition(self._lock)
        MEMORY_LIMIT_BYTES.set(self._limit_bytes)

    def _can_admit(self, to_reserve: int, exclusive: bool) -> bool:
        """Whether a request may reserve right now.

        A request that needs the whole budget (exclusive -- e.g. a multi-GB copy
        clamped to limit_bytes) is admissible only when nothing else holds
        memory: active_bytes + limit_bytes > limit_bytes for any active_bytes > 0.
        Without fairness the limiter never reaches that state under load -- every
        release wakes all waiters and a small request re-grabs memory before the
        copy can, so active_bytes never drains to 0 and the copy backpressures
        until it 503s. Writer preference fixes it: while an exclusive request is
        waiting, hold back new non-exclusive admissions so active_bytes drains
        and the copy gets its exclusive slot (bounded by in-flight requests).
        """
        if self._active_bytes + to_reserve > self._limit_bytes:
            return False
        return exclusive or self._pending_exclusive == 0

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

    async def try_acquire(self, bytes_needed: int, *, copy: bool = False) -> int:
        """Reserve memory, waiting up to BACKPRESSURE_TIMEOUT if at capacity."""
        if self._limit_bytes <= 0:
            return 0

        to_reserve = max(MIN_RESERVATION, bytes_needed)

        # A single request's honest peak can exceed the governor budget (e.g. a
        # multi-GB PutObject whose internal parts are hundreds of MB). Reserve the
        # routine-workload peak, not the whole budget, so concurrent ~50MB Scylla
        # parts are not starved behind one clamped slot. Rare huge uploads may use
        # more RSS than reserved when run alone; the pod memory limit is the
        # backstop.
        #
        # Server-side copies use copy_governor_clamped_reserve instead: their
        # honest peak already reflects real chunk work and must not be crushed to
        # the routine upload peak (~59MB) or several multi-GB manifest copies run
        # concurrently and OOM the pod.
        if to_reserve > self._limit_bytes:
            honest = to_reserve
            if copy:
                to_reserve = copy_governor_clamped_reserve(honest, self._limit_bytes)
            else:
                to_reserve = streaming_governor_clamped_reserve(honest, self._limit_bytes)
            logger.info(
                "MEMORY_CLAMPED_TO_BUDGET",
                requested_mb=round(honest / 1024 / 1024, 2),
                reserved_mb=round(to_reserve / 1024 / 1024, 2),
                limit_mb=round(self._limit_bytes / 1024 / 1024, 2),
                copy=copy,
            )

        exclusive = to_reserve >= self._limit_bytes
        async with self._condition:
            deadline = asyncio.get_event_loop().time() + BACKPRESSURE_TIMEOUT
            if exclusive:
                self._pending_exclusive += 1
            try:
                while not self._can_admit(to_reserve, exclusive):
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
                            f"Memory limit: {active_mb:.0f}MB + "
                            f"{request_mb:.0f}MB > {limit_mb:.0f}MB"
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
            finally:
                if exclusive:
                    self._pending_exclusive -= 1
                    # Whether we got the slot or gave up, release the non-exclusive
                    # requests we were holding back.
                    self._condition.notify_all()

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

    PUTs stream and encrypt one internal part at a time; reserve the framed
    path's true peak (streaming_upload_peak), which stacks the accumulated
    ciphertext, the encrypt transient, the held frame and the HTTP body copy --
    not just the part size. Reserving the bare part size under-counted ~3x and
    let the limiter admit too many concurrent uploads -> OOM. GETs reserve a
    baseline; encrypted GETs acquire more in the handler.
    """
    if method in ("HEAD", "DELETE"):
        return 0
    if method == "GET":
        return MAX_BUFFER_SIZE
    if method == "POST":
        return MIN_RESERVATION
    return max(MIN_RESERVATION, governor_memory_footprint(content_length))


# Module-level convenience functions delegating to the default instance


def get_memory_limit() -> int:
    return _default.limit_bytes


def get_active_memory() -> int:
    return _default.active_bytes


async def try_acquire_memory(bytes_needed: int) -> int:
    return await _default.try_acquire(bytes_needed)


async def try_acquire_copy_memory(bytes_needed: int) -> int:
    return await _default.try_acquire(bytes_needed, copy=True)


@contextlib.asynccontextmanager
async def reserve_memory(bytes_needed: int):
    """Reserve memory for the duration of a block, releasing on exit.

    For operations whose real peak isn't reflected by the request body size
    (e.g. server-side copies, which decrypt+re-encrypt the source), so they get
    gated by the limiter like uploads instead of running unbounded.
    """
    reserved = await _default.try_acquire(bytes_needed)
    try:
        yield
    finally:
        if reserved > 0:
            await _default.release(reserved)


@contextlib.asynccontextmanager
async def reserve_copy_memory(bytes_needed: int):
    """Reserve memory for a server-side copy (upload-style clamp does not apply)."""
    reserved = await _default.try_acquire(bytes_needed, copy=True)
    try:
        yield
    finally:
        if reserved > 0:
            await _default.release(reserved)


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
