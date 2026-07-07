"""Regression tests for memory-governor peak estimation and clamp starvation.

The production failure: streaming_upload_peak scaled with multi-GB Content-Length
(2*part + 2*frame), and clamp-to-full-budget made one oversized estimate
monopolize the entire governor slot — starving concurrent ~50MB Scylla parts that
only need ~25MB each.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import Request

from s3proxy import crypto
from s3proxy.concurrency import (
    MIN_RESERVATION,
    estimate_memory_footprint,
    get_active_memory,
    reset_state,
    set_memory_limit,
    try_acquire_memory,
)
from s3proxy.handlers.multipart.upload_part import UploadPartMixin
from s3proxy.state import MultipartUploadState

MB = 1024 * 1024

# Deployed governor budgets we've actually run in prod (MB).
DEPLOYED_BUDGETS_MB = (64, 192, 256, 312)

# Observed prod Content-Length values.
SCYLLA_PART_BYTES = 50 * MB
SCYLLA_OBJECT_BYTES = 6_597_946_546
MANIFEST_BYTES = 0


def _old_streaming_upload_peak(content_length: int) -> int:
    """Pre-fix formula that caused prod starvation (PR #109 regression guard)."""
    part = crypto.memory_bounded_part_size(content_length)
    frame = min(part, crypto.FRAME_PLAINTEXT_SIZE)
    return 2 * part + 2 * frame


def _old_clamped_reserve(honest_peak: int, budget_bytes: int) -> int:
    """Pre-fix clamp: grab the entire budget slot."""
    return budget_bytes if honest_peak > budget_bytes else honest_peak


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


# ---------------------------------------------------------------------------
# Regression guards: document the exact prod failure so we never re-ship it.
# ---------------------------------------------------------------------------


def test_old_formula_overcounted_multi_gb_content_length():
    """Pre-fix estimate for a 6.6GB body was ~508MB; new code caps at ~67MB."""
    old = _old_streaming_upload_peak(SCYLLA_OBJECT_BYTES)
    new = estimate_memory_footprint("PUT", SCYLLA_OBJECT_BYTES)
    assert old > 500 * MB
    assert new < 80 * MB
    assert new < old // 5


def test_old_clamp_monopolized_full_budget():
    """Pre-fix: 509MB honest estimate at 312MB budget reserved all 312MB."""
    honest = _old_streaming_upload_peak(5 * 1024 * MB)
    budget = 312 * MB
    assert honest > budget
    assert _old_clamped_reserve(honest, budget) == budget

    routine = crypto.streaming_upload_peak(crypto.STREAMING_GOVERNOR_CLIENT_PART_BYTES)
    assert crypto.streaming_governor_clamped_reserve(honest, budget) == routine
    assert routine < budget


@pytest.mark.asyncio
async def test_prod_replay_active_33mb_plus_huge_request_fits():
    """Exact prod stall: 33MB Scylla part active + clamped request at 312MB limit.

      Pre-fix: 33 + 312 > 312 -> perpetual MEMORY_BACKPRESSURE.
    Post-fix: 33 + ~67 < 312 -> second acquire succeeds immediately.
    """
    reset_state()
    set_memory_limit(312)
    try:
        scylla_needed = estimate_memory_footprint("PUT", SCYLLA_PART_BYTES)
        scylla_reserved = await try_acquire_memory(scylla_needed)
        assert 24 * MB <= scylla_reserved <= 35 * MB

        huge = estimate_memory_footprint("PUT", SCYLLA_OBJECT_BYTES)
        assert huge < 80 * MB

        huge_reserved = await try_acquire_memory(huge)
        assert scylla_reserved + huge_reserved < 312 * MB
        assert get_active_memory() == scylla_reserved + huge_reserved
    finally:
        reset_state()


@pytest.mark.parametrize("budget_mb", DEPLOYED_BUDGETS_MB)
@pytest.mark.asyncio
async def test_scylla_parts_fit_deployed_budgets(budget_mb: int):
    """At every governor limit we've deployed, multiple ~50MB parts must fit."""
    reset_state()
    set_memory_limit(budget_mb)
    try:
        per_part = estimate_memory_footprint("PUT", SCYLLA_PART_BYTES)
        minimum_parts = 4 if budget_mb >= 192 else 2
        reserved = []
        for _ in range(minimum_parts):
            reserved.append(await try_acquire_memory(per_part))
        assert sum(reserved) <= budget_mb * MB
        assert len(reserved) == minimum_parts
    finally:
        reset_state()


@pytest.mark.parametrize(
    "content_bytes",
    [
        733,  # ES metadata
        16 * MB,
        SCYLLA_PART_BYTES,
        512 * MB,
        SCYLLA_OBJECT_BYTES,
        10 * 1024 * MB,
    ],
)
def test_governor_footprint_never_exceeds_routine_cap(content_bytes: int):
    routine = crypto.streaming_upload_peak(crypto.STREAMING_GOVERNOR_CLIENT_PART_BYTES)
    footprint = estimate_memory_footprint("PUT", content_bytes)
    assert footprint <= routine
    if content_bytes == 0:
        assert footprint == MIN_RESERVATION


@pytest.mark.asyncio
async def test_mixed_scylla_workload_concurrent_acquire():
    """Prod mix: several ~50MB parts + tiny manifest + multi-GB estimate."""
    reset_state()
    set_memory_limit(312)
    try:

        async def acquire(cl: int) -> int:
            return await try_acquire_memory(estimate_memory_footprint("PUT", cl))

        results = await asyncio.gather(
            acquire(SCYLLA_PART_BYTES),
            acquire(SCYLLA_PART_BYTES),
            acquire(SCYLLA_PART_BYTES),
            acquire(MANIFEST_BYTES),
            acquire(SCYLLA_OBJECT_BYTES),
        )
        assert sum(results) < 312 * MB
        assert get_active_memory() == sum(results)
    finally:
        reset_state()


async def _assert_put_reserves_governor_footprint(content_length: int) -> None:
    import s3proxy.concurrency as concurrency_module
    import s3proxy.request_handler as request_handler_module

    request = MagicMock(spec=Request)
    request.method = "PUT"
    request.url = MagicMock()
    request.url.path = "/bucket/scylla-backup/sst/big-Data.db"
    request.url.query = ""
    request.headers = {"content-length": str(content_length)}
    request.scope = {"raw_path": b"/bucket/scylla-backup/sst/big-Data.db"}
    request.client = MagicMock()
    request.client.host = "10.0.0.1"

    expected = estimate_memory_footprint("PUT", content_length)
    acquired: list[int] = []
    original_acquire = concurrency_module.try_acquire_memory

    async def spy_acquire(needed: int) -> int:
        acquired.append(needed)
        return await original_acquire(needed)

    with (
        patch.object(
            request_handler_module, "_handle_proxy_request_impl", new_callable=AsyncMock
        ) as mock_impl,
        patch.object(concurrency_module, "try_acquire_memory", side_effect=spy_acquire),
    ):
        mock_impl.return_value = None
        await request_handler_module.handle_proxy_request(request, MagicMock(), MagicMock())

    assert acquired == [expected]
    assert expected == crypto.governor_memory_footprint(content_length)
    assert concurrency_module.get_active_memory() == 0


@pytest.mark.asyncio
async def test_request_handler_put_reserves_governor_footprint():
    """Request gate must use governor_memory_footprint, not raw streaming peak."""
    reset_state()
    set_memory_limit(312)
    try:
        for content_length in (SCYLLA_PART_BYTES, SCYLLA_OBJECT_BYTES):
            await _assert_put_reserves_governor_footprint(content_length)
    finally:
        reset_state()


@pytest.mark.asyncio
async def test_many_concurrent_scylla_acquires_via_gather():
    """Flood of concurrent ~50MB part admissions under prod budget."""
    reset_state()
    set_memory_limit(312)
    try:
        per_part = estimate_memory_footprint("PUT", SCYLLA_PART_BYTES)

        async def one():
            return await try_acquire_memory(per_part)

        results = await asyncio.gather(*[one() for _ in range(12)])
        assert len(results) == 12
        assert sum(results) <= 312 * MB
        assert all(r == per_part for r in results)
    finally:
        reset_state()


def test_copy_peak_is_bounded_and_independent_of_object_size():
    """Copies frame at a fixed internal part size, so copy_pipeline_peak is O(1)
    in object size -- it no longer tracks streaming_upload_peak (object/20). This
    is what stops a multi-GB copy monopolizing the governor budget/deadlocking.
    """
    huge = 5 * 1024 * MB
    peak = crypto.copy_pipeline_peak(huge)
    assert peak == crypto.copy_pipeline_peak(64 * MB)  # size-independent
    assert peak < 128 * MB
    # far below the old object/20 peak that forced full-budget clamping
    assert peak < crypto.streaming_upload_peak(huge) // 4
