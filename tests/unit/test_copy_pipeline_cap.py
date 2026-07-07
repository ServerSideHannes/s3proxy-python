"""Scylla-shaped load tests: pipeline cap + hold-through-upload prevent OOM/503 storms.

These drive the real UploadPartCopy handler path (including the pipeline semaphore)
and assert the governor never admits more than 2 concurrent copy pipelines worth of
memory — the prod failure mode was 15+ copies per pod with untracked RSS.
"""

from __future__ import annotations

import asyncio

import pytest

from s3proxy import concurrency, crypto
from s3proxy.errors import S3Error
from s3proxy.handlers.multipart.copy import reset_copy_pipeline_semaphore
from tests.unit.test_copy_per_part_memory import (
    MB,
    _SlowUploadClient,
    _streaming_copy,
    _TrackingMgr,
)
from tests.unit.test_copy_reservation_vs_real import _Client, _handler

PROD_BUDGET_MB = 192


@pytest.fixture(autouse=True)
def _reset():
    reset_copy_pipeline_semaphore(2)
    yield
    reset_copy_pipeline_semaphore(2)


@pytest.mark.asyncio
async def test_scylla_load_thirteen_copies_all_complete_no_rejections():
    """Simulate 13 Scylla nodes hitting one pod: all copies finish, none 503."""
    original_timeout = concurrency.BACKPRESSURE_TIMEOUT
    concurrency.BACKPRESSURE_TIMEOUT = 60
    concurrency.reset_state()
    concurrency.set_memory_limit(PROD_BUDGET_MB)
    reset_copy_pipeline_semaphore(2)

    # 128MB object: 4 internal parts — enough to exercise pipeline without slow test.
    plaintext_size = 128 * MB
    completed = 0
    rejections = 0

    async def one_copy(idx: int):
        nonlocal completed, rejections
        handler = _handler()
        handler.multipart_manager = _TrackingMgr()
        try:
            await _streaming_copy(handler, _Client(plaintext_size), plaintext_size)
            completed += 1
        except S3Error as e:
            if e.code == "SlowDown":
                rejections += 1
            else:
                raise

    try:
        await asyncio.gather(*[one_copy(i) for i in range(13)])
    finally:
        concurrency.BACKPRESSURE_TIMEOUT = original_timeout
        concurrency.reset_state()

    assert rejections == 0, f"{rejections} copies got 503 SlowDown"
    assert completed == 13


@pytest.mark.asyncio
async def test_governor_never_exceeds_budget_under_scylla_load():
    """Peak reserved bytes must stay within 192MB budget during 13-copy flood."""
    original_timeout = concurrency.BACKPRESSURE_TIMEOUT
    concurrency.BACKPRESSURE_TIMEOUT = 60
    concurrency.reset_state()
    concurrency.set_memory_limit(PROD_BUDGET_MB)
    reset_copy_pipeline_semaphore(2)

    plaintext_size = 64 * MB
    peak_seen = 0
    original_acquire = concurrency._default.try_acquire

    async def tracking_acquire(bytes_needed: int, *, copy: bool = False):
        nonlocal peak_seen
        reserved = await original_acquire(bytes_needed, copy=copy)
        peak_seen = max(peak_seen, concurrency.get_active_memory())
        return reserved

    concurrency._default.try_acquire = tracking_acquire  # type: ignore[method-assign]

    try:
        await asyncio.gather(
            *[
                _streaming_copy(_handler(), _Client(plaintext_size), plaintext_size)
                for _ in range(8)
            ]
        )
    finally:
        concurrency._default.try_acquire = original_acquire  # type: ignore[method-assign]
        concurrency.BACKPRESSURE_TIMEOUT = original_timeout
        concurrency.reset_state()

    budget = PROD_BUDGET_MB * MB
    assert peak_seen <= budget, (
        f"peak governor {peak_seen / MB:.2f}MB exceeded {PROD_BUDGET_MB}MB budget"
    )


@pytest.mark.asyncio
async def test_backpressure_logged_once_per_acquire_wait():
    """MEMORY_BACKPRESSURE must not spam on every condition wakeup."""
    original_timeout = concurrency.BACKPRESSURE_TIMEOUT
    concurrency.BACKPRESSURE_TIMEOUT = 5
    concurrency.reset_state()
    # 96MB budget: one 88MB chunk leaves no room for a second → must wait.
    concurrency.set_memory_limit(96)
    reset_copy_pipeline_semaphore(1)

    held = await concurrency.try_acquire_copy_memory(
        crypto.copy_chunk_peak(crypto.COPY_INTERNAL_PART_SIZE)
    )

    log_events: list[str] = []
    original_info = concurrency.logger.info

    def capture_info(event, **kw):
        log_events.append(event)
        return original_info(event, **kw)

    concurrency.logger.info = capture_info  # type: ignore[method-assign]

    waiter = asyncio.create_task(
        concurrency.try_acquire_copy_memory(crypto.copy_chunk_peak(crypto.COPY_INTERNAL_PART_SIZE))
    )
    await asyncio.sleep(0.2)
    await concurrency.release_memory(held)
    second = await waiter

    concurrency.logger.info = original_info  # type: ignore[method-assign]
    concurrency.BACKPRESSURE_TIMEOUT = original_timeout
    concurrency.reset_state()

    assert second > 0
    backpressure_logs = [e for e in log_events if e == "MEMORY_BACKPRESSURE"]
    assert len(backpressure_logs) == 1, (
        f"expected 1 MEMORY_BACKPRESSURE log per acquire wait, got {len(backpressure_logs)}"
    )


@pytest.mark.asyncio
async def test_hold_through_upload_bounds_two_pipeline_overlap():
    """Two blocked uploads must hold ~2×chunk_peak, not release early."""
    concurrency.reset_state()
    concurrency.set_memory_limit(PROD_BUDGET_MB)
    reset_copy_pipeline_semaphore(2)

    plaintext_size = 64 * MB
    gate1 = asyncio.Event()
    gate2 = asyncio.Event()
    chunk_peak = crypto.copy_chunk_peak(crypto.COPY_INTERNAL_PART_SIZE)

    handler = _handler()
    handler.multipart_manager = _TrackingMgr()

    copy1 = asyncio.create_task(
        _streaming_copy(handler, _SlowUploadClient(plaintext_size, gate1), plaintext_size)
    )
    copy2 = asyncio.create_task(
        _streaming_copy(handler, _SlowUploadClient(plaintext_size, gate2), plaintext_size)
    )

    for _ in range(300):
        active = concurrency.get_active_memory()
        if active >= 2 * chunk_peak * 0.9:
            break
        await asyncio.sleep(0.01)
    else:
        pytest.fail("two pipelines never both held reservation through upload")

    mid_active = concurrency.get_active_memory()
    gate1.set()
    gate2.set()
    await asyncio.gather(copy1, copy2)

    assert mid_active >= 2 * chunk_peak * 0.9, (
        f"expected ~{2 * chunk_peak / MB:.0f}MB held during dual upload wait, "
        f"got {mid_active / MB:.2f}MB (release-before-upload bug)"
    )
    assert concurrency.get_active_memory() == 0
