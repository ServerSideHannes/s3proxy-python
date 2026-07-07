"""Server-side copy memory: bounded, concurrent, and no full-budget deadlock.

History of this path:
  * Copies decrypt+re-encrypt in RAM. A dedup flood of unthrottled copies OOMed
    the pod, so copies were gated by the memory limiter (test_copy_memory_governing).
  * The copy pipeline sized its internal parts as object_size/20, so a 4.7GB
    Scylla `.sm_` dedup copy (a single UploadPartCopy, partNumber=1) peaked at
    ~535MB -- larger than the 192MB governor budget. It therefore reserved the
    WHOLE budget to run exclusively, and could only be admitted when active_bytes
    hit exactly 0. Under steady backup traffic that never happens, so every large
    copy backpressured for the full timeout and returned 503 (observed in prod:
    ~50% of requests 503, all MEMORY_CLAMPED_TO_BUDGET copy=true requested_mb=534,
    rejected at active_mb≈0.1).

The fix: copies frame at a FIXED internal part size (crypto.copy_internal_part_size),
so the copy peak is O(1) in object size (~90MB). A copy never exceeds the budget,
never clamps, never runs exclusive -- it is just a normal ~90MB reservation.
Concurrency is then bounded by the budget like any other request, and the OOM
that motivated exclusivity can't happen because each copy is light.
"""

from __future__ import annotations

import asyncio

import pytest

from s3proxy import concurrency, crypto
from s3proxy.concurrency import MIN_RESERVATION
from s3proxy.state import MultipartUploadState
from tests.unit.test_copy_per_part_memory import _TrackingMgr
from tests.unit.test_copy_reservation_vs_real import _Client, _handler, _measure_peak

MB = 1024 * 1024

# Observed prod manifest copy size (single-part UploadPartCopy of a large SSTable).
SCYLLA_MANIFEST_BYTES = 4_767 * MB


def test_copy_peak_is_o1_across_object_sizes():
    """The core guarantee: copy peak does not grow with object size."""
    peaks = {mb: crypto.copy_pipeline_peak(mb * MB) for mb in (33, 64, 512, 4767, 20_000, 100_000)}
    # All well under a 192MB budget, and identical until the 10k-part ceiling
    # forces a larger part for enormous (>288GB) objects.
    for mb, peak in peaks.items():
        assert peak < 128 * MB, f"{mb}MB copy peak {peak / MB:.0f}MB not bounded"
    assert peaks[64] == peaks[4767] == peaks[100_000]  # 100GB < 288GB ceiling


def test_copy_internal_part_size_fixed_until_part_ceiling():
    assert crypto.copy_internal_part_size(4767 * MB) == crypto.COPY_INTERNAL_PART_SIZE
    assert crypto.copy_internal_part_size(100 * 1024 * MB) == crypto.COPY_INTERNAL_PART_SIZE
    # >288GB: part grows to keep internal part count under S3's 10k ceiling.
    huge = 400 * 1024 * MB
    part = crypto.copy_internal_part_size(huge)
    assert part > crypto.COPY_INTERNAL_PART_SIZE
    assert -(-huge // part) <= crypto.COPY_MAX_INTERNAL_PARTS


def test_copy_does_not_monopolize_budget():
    """A 4.7GB copy fits the budget outright -- the governor clamp never engages."""
    honest = crypto.copy_pipeline_peak(SCYLLA_MANIFEST_BYTES)
    budget = 192 * MB
    assert honest < budget
    # try_acquire only calls the clamp when honest > budget; here it must not, so
    # the reservation is the honest peak, not the whole budget.
    assert crypto.copy_governor_clamped_reserve(honest, budget) == honest


@pytest.mark.asyncio
async def test_large_copy_admitted_while_baseline_reservations_held():
    """Exact prod 503 regression.

    Pre-fix: a 4.7GB copy reserved the whole 192MB budget, so with any baseline
    reservation held (other in-flight requests -> active_mb≈0.1) admission needed
    active_bytes == 0 and never happened -> 30s backpressure -> 503. Post-fix the
    copy reserves ~90MB and is admitted immediately alongside the baselines.
    """
    original_timeout = concurrency.BACKPRESSURE_TIMEOUT
    concurrency.BACKPRESSURE_TIMEOUT = 0  # if it can't admit *now*, it rejects
    concurrency.reset_state()
    concurrency.set_memory_limit(192)
    try:
        # Three other in-flight requests hold MIN_RESERVATION each (prod: ~0.18MB).
        for _ in range(3):
            await concurrency.try_acquire_memory(MIN_RESERVATION)
        assert concurrency.get_active_memory() > 0

        reserved = await concurrency.try_acquire_copy_memory(
            crypto.copy_pipeline_peak(SCYLLA_MANIFEST_BYTES)
        )
        assert reserved < 128 * MB
        assert concurrency.get_active_memory() <= 192 * MB
    finally:
        concurrency.BACKPRESSURE_TIMEOUT = original_timeout
        concurrency.reset_state()


@pytest.mark.asyncio
async def test_multiple_copies_run_concurrently_bounded_by_budget():
    """Per-part reservation lets copies interleave: several complete on a 192MB
    budget without holding ~88MB for the whole object duration."""
    original_timeout = concurrency.BACKPRESSURE_TIMEOUT
    concurrency.BACKPRESSURE_TIMEOUT = 30
    concurrency.reset_state()
    concurrency.set_memory_limit(192)
    per_chunk = crypto.copy_chunk_peak(crypto.COPY_INTERNAL_PART_SIZE)
    assert per_chunk < 192 * MB

    completed = 0

    async def one_copy():
        nonlocal completed
        client = _Client(128 * MB)
        handler = _handler()
        handler.multipart_manager = _TrackingMgr()
        await handler._streaming_copy_part_inner(
            client,
            "b",
            "k",
            "u",
            1,
            MultipartUploadState(dek=crypto.generate_dek(), bucket="b", key="k", upload_id="u"),
            "b",
            "src",
            None,
            None,
            None,
            {},
            {},
            128 * MB,
        )
        completed += 1

    try:
        await asyncio.gather(*[one_copy() for _ in range(5)])
    finally:
        concurrency.BACKPRESSURE_TIMEOUT = original_timeout
        concurrency.reset_state()

    assert completed == 5
    assert concurrency.get_active_memory() == 0


@pytest.mark.asyncio
async def test_copy_coexists_with_scylla_upload():
    """A ~25MB Scylla upload and a ~90MB copy fit together (113MB < 192MB); the
    copy no longer blocks the upload (or vice versa)."""
    concurrency.reset_state()
    concurrency.set_memory_limit(192)
    try:
        scylla = await concurrency.try_acquire_memory(crypto.governor_memory_footprint(50 * MB))
        copy = await concurrency.try_acquire_copy_memory(
            crypto.copy_pipeline_peak(SCYLLA_MANIFEST_BYTES)
        )
        assert scylla + copy < 192 * MB
        assert concurrency.get_active_memory() == scylla + copy
    finally:
        concurrency.reset_state()


@pytest.mark.parametrize("mb", [512, 1024, 4767])
@pytest.mark.asyncio
async def test_copy_reservation_bounds_real_peak_large_manifests(mb: int):
    """copy_pipeline_peak must still cover the real tracemalloc peak of the (now
    fixed-part) copy pipeline for prod-sized copies."""
    plaintext_size = mb * MB
    real_peak = await _measure_peak(plaintext_size)
    reserved = crypto.copy_pipeline_peak(plaintext_size)
    assert reserved >= real_peak, (
        f"{mb}MB copy: reserved {reserved / MB:.1f}MB < real {real_peak / MB:.1f}MB"
    )


@pytest.mark.asyncio
async def test_writer_preference_still_protects_a_whole_budget_reservation():
    """Writer preference (kept as a safety net) must still hold back new small
    requests while a genuinely exclusive whole-budget reservation waits, so it
    is not starved. Copies are no longer exclusive, so drive it explicitly with a
    reservation sized to the entire budget.
    """
    original_timeout = concurrency.BACKPRESSURE_TIMEOUT
    concurrency.BACKPRESSURE_TIMEOUT = 5
    concurrency.reset_state()
    concurrency.set_memory_limit(192)
    try:
        blocker = await concurrency.try_acquire_memory(crypto.governor_memory_footprint(50 * MB))

        # An exclusive (whole-budget) waiter.
        exclusive_task = asyncio.create_task(concurrency.try_acquire_memory(192 * MB))
        small_task = asyncio.create_task(concurrency.try_acquire_memory(MIN_RESERVATION))
        await asyncio.sleep(0.05)
        assert not exclusive_task.done()
        assert not small_task.done(), "small request must queue behind the exclusive waiter"

        await concurrency.release_memory(blocker)
        got = await asyncio.wait_for(exclusive_task, timeout=2)
        assert got == 192 * MB
        assert not small_task.done()

        await concurrency.release_memory(got)
        assert await asyncio.wait_for(small_task, timeout=2) == MIN_RESERVATION
    finally:
        concurrency.BACKPRESSURE_TIMEOUT = original_timeout
        concurrency.reset_state()
