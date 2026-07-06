"""Regression tests for memory-governor peak estimation and clamp starvation.

The production failure: streaming_upload_peak scaled with multi-GB Content-Length
(2*part + 2*frame), and clamp-to-full-budget made one oversized estimate
monopolize the entire governor slot — starving concurrent ~50MB Scylla parts that
only need ~25MB each.
"""

from __future__ import annotations

import asyncio

import pytest

from s3proxy import crypto
from s3proxy.concurrency import (
    estimate_memory_footprint,
    reset_state,
    set_memory_limit,
    try_acquire_memory,
)
from s3proxy.handlers.multipart.upload_part import UploadPartMixin
from s3proxy.state import MultipartUploadState

MB = 1024 * 1024


class _Mgr:
    async def add_part(self, *a, **k):
        return None


class _DiscardingClient:
    async def upload_part(self, bucket, key, upload_id, part_number, body):
        del body
        return {"ETag": '"0"'}


class _Request:
    def __init__(self, total, chunk=64 * 1024):
        self._total = total
        self._chunk = chunk

    async def stream(self):
        for i in range(0, self._total, self._chunk):
            yield b"x" * min(self._chunk, self._total - i)


def _handler():
    h = UploadPartMixin.__new__(UploadPartMixin)
    h.multipart_manager = _Mgr()
    return h


async def _measure_framed_peak(content_length: int) -> int:
    import tracemalloc

    h = _handler()
    state = MultipartUploadState(dek=crypto.generate_dek(), bucket="b", key="k", upload_id="u")
    part_size = crypto.memory_bounded_part_size(content_length)
    tracemalloc.start()
    base = tracemalloc.get_traced_memory()[0]
    await h._stream_and_upload_framed(
        _Request(content_length),
        _DiscardingClient(),
        "b",
        "k",
        "u",
        1,
        state,
        content_length,
        part_size,
        1,
    )
    peak = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()
    return peak - base


@pytest.mark.parametrize(
    "mb",
    [16, 50, 512, 1024, 4096],
)
def test_streaming_upload_peak_formula(mb: int):
    """Peak formula must match the two measured regimes (small 4*part, large 2*part+frame)."""
    cl = mb * MB
    part = crypto.memory_bounded_part_size(cl)
    if part <= crypto.FRAME_PLAINTEXT_SIZE:
        expected = 4 * part
    else:
        expected = 2 * part + crypto.FRAME_PLAINTEXT_SIZE
    assert crypto.streaming_upload_peak(cl) == expected


@pytest.mark.parametrize("mb", [50, 512])
@pytest.mark.asyncio
async def test_reservation_bounds_framed_peak_and_does_not_overcount(mb: int):
    """Reserved memory must cover real peak for routine part sizes."""
    cl = mb * MB
    real_peak = await _measure_framed_peak(cl)
    reserved = estimate_memory_footprint("PUT", cl)

    assert reserved >= real_peak, (
        f"{mb}MB: reserved {reserved / MB:.1f}MB < real {real_peak / MB:.1f}MB"
    )

    routine_cap = crypto.streaming_upload_peak(crypto.STREAMING_GOVERNOR_CLIENT_PART_BYTES)
    assert reserved <= routine_cap * 1.15 + crypto.FRAME_PLAINTEXT_SIZE, (
        f"{mb}MB: reserved {reserved / MB:.1f}MB >> routine cap {routine_cap / MB:.1f}MB"
    )


def test_multi_gb_estimate_capped_at_routine_workload_peak():
    """6.6GB Content-Length must not produce a 500MB+ governor estimate."""
    cl = 6_597_946_546  # observed Scylla SST total_size
    reserved = estimate_memory_footprint("PUT", cl)
    routine = crypto.governor_memory_footprint(cl)
    assert reserved == routine
    assert reserved == crypto.streaming_upload_peak(crypto.STREAMING_GOVERNOR_CLIENT_PART_BYTES)
    assert reserved < 80 * MB


@pytest.mark.asyncio
async def test_clamp_does_not_monopolize_budget():
    """Concurrent ~50MB part must not stall behind one clamped multi-GB estimate."""
    reset_state()
    set_memory_limit(312)
    try:
        scylla = estimate_memory_footprint("PUT", 50 * MB)
        assert scylla < 35 * MB

        r_scylla = await try_acquire_memory(scylla)
        assert r_scylla == scylla

        # Honest peak for multi-GB body exceeds 312MB budget before governor cap.
        huge_honest = crypto.streaming_upload_peak(5 * 1024 * MB)
        assert huge_honest > 312 * MB
        huge_reserved = crypto.governor_memory_footprint(5 * 1024 * MB)

        r_huge = await try_acquire_memory(huge_reserved)
        routine = crypto.streaming_upload_peak(crypto.STREAMING_GOVERNOR_CLIENT_PART_BYTES)
        assert r_huge == routine
        assert r_huge < 312 * MB
        assert scylla + r_huge < 312 * MB
    finally:
        reset_state()


@pytest.mark.asyncio
async def test_many_scylla_sized_parts_fit_deployed_budget():
    """Regression for prod: many concurrent ~50MB parts under a 312MB governor."""
    reset_state()
    set_memory_limit(312)
    try:
        per_part = estimate_memory_footprint("PUT", 50 * MB)
        reservations = []
        while True:
            try:
                reservations.append(await try_acquire_memory(per_part))
            except Exception:
                break
        # At ~25-33MB each, 312MB budget should fit at least 8 concurrent parts.
        assert len(reservations) >= 8, (
            f"only {len(reservations)} x {per_part / MB:.1f}MB parts fit in 312MB"
        )
    finally:
        reset_state()


def test_streaming_governor_clamped_reserve():
    honest = 509 * MB
    budget = 312 * MB
    routine = crypto.streaming_upload_peak(crypto.STREAMING_GOVERNOR_CLIENT_PART_BYTES)
    assert crypto.streaming_governor_clamped_reserve(honest, budget) == routine

    small = 40 * MB
    assert crypto.streaming_governor_clamped_reserve(small, budget) == small
