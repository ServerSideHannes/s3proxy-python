"""Server-side copies must be gated by the memory limiter.

A CopyObject / UploadPartCopy carries no request body, so the request-level
limiter reserved ~nothing for it -- yet each copy decrypts the source and
re-encrypts it in RAM. Under a dedup flood, dozens ran concurrently with nothing
throttling them and the pod was OOMKilled (reproduced locally: a 64-concurrent
copy flood at a 256MiB cap killed the pod, exit 137, 0/64 copies succeeded;
with the reservation it peaks ~195MiB and completes 64/64).

These tests pin the two halves of the fix:
  1. copy_pipeline_peak() reports the real peak so the limiter reserves enough.
  2. reserve_memory() actually bounds concurrent copies to the budget.
"""

import asyncio

import pytest

from s3proxy import concurrency, crypto

MB = 1024 * 1024


def test_copy_pipeline_peak_is_bounded_regardless_of_object_size():
    # Copies frame at a FIXED internal part size, so the peak is O(1) in object
    # size instead of object_size/20. A 4.7GB SSTable dedup copy used to peak at
    # ~535MB -- larger than the whole governor budget -- so it reserved the
    # entire budget and deadlocked against any concurrent request. It now stays
    # ~90MB, identical to a 64MB copy.
    peaks = [crypto.copy_pipeline_peak(s) for s in (64 * MB, 512 * MB, 4767 * MB, 5 * 1024 * MB)]
    assert all(p == peaks[0] for p in peaks), peaks
    part = crypto.COPY_INTERNAL_PART_SIZE
    assert peaks[0] == 2 * part + crypto.FRAME_PLAINTEXT_SIZE + 2 * crypto.MAX_BUFFER_SIZE
    assert peaks[0] < 128 * MB


def test_copy_pipeline_peak_small_is_three_x():
    # Small copies buffer the whole object and re-encrypt it (~3x), floored at
    # one buffer so a tiny copy still reserves something meaningful.
    assert crypto.copy_pipeline_peak(0) == crypto.MAX_BUFFER_SIZE
    assert crypto.copy_pipeline_peak(1 * MB) == crypto.MAX_BUFFER_SIZE  # floor
    assert crypto.copy_pipeline_peak(20 * MB) == 60 * MB  # 3x


@pytest.mark.asyncio
async def test_reserve_memory_bounds_concurrent_copies():
    # With a budget that fits ~2 copy pipelines, a flood of concurrent copies
    # must never let active reservations exceed the budget -- that bound is what
    # stops the OOM. Without the reservation, all of them would run at once.
    # The unit conftest forces immediate rejection (timeout 0); this test needs
    # the wait-then-succeed path, so restore a real timeout for its duration.
    original_timeout = concurrency.BACKPRESSURE_TIMEOUT
    concurrency.BACKPRESSURE_TIMEOUT = 30
    limiter = concurrency._default
    limiter.active_bytes = 0
    per_copy = crypto.copy_pipeline_peak(64 * MB)
    limiter.set_memory_limit((per_copy * 2) // MB)  # budget sized to fit ~2
    budget_fits = limiter.limit_bytes // per_copy

    peak_active = 0
    inside = 0
    max_inside = 0

    async def one_copy():
        nonlocal peak_active, inside, max_inside
        async with concurrency.reserve_copy_memory(per_copy):
            inside += 1
            max_inside = max(max_inside, inside)
            peak_active = max(peak_active, limiter.active_bytes)
            await asyncio.sleep(0.02)  # hold the reservation
            inside -= 1

    try:
        await asyncio.gather(*[one_copy() for _ in range(16)])
    finally:
        concurrency.BACKPRESSURE_TIMEOUT = original_timeout

    assert peak_active <= limiter.limit_bytes  # never overran the budget
    assert max_inside <= budget_fits  # limiter admits only what the budget fits
    assert limiter.active_bytes == 0  # everything released
