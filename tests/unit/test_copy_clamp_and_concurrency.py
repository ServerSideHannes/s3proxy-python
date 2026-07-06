"""Regression tests for copy-path governor clamp and concurrent manifest copies.

Prod OOM: three concurrent 4.7GB Scylla .sm_ manifest UploadPartCopy ops on
one pod each reserved ~59MB (upload-style routine clamp) while holding ~175MB
ciphertext at copy.py:420 — governor active_mb ~178, RSS ~790, OOMKilled.
"""

from __future__ import annotations

import asyncio

import pytest

from s3proxy import concurrency, crypto
from s3proxy.errors import S3Error
from tests.unit.test_copy_reservation_vs_real import _measure_peak

MB = 1024 * 1024

# Observed prod manifest copy size.
SCYLLA_MANIFEST_BYTES = 4_767 * MB


def test_copy_clamp_monopolizes_budget_not_routine_upload_peak():
    """4.7GB copy honest peak ~500MB at 192MB budget must reserve 192, not ~59."""
    honest = crypto.copy_pipeline_peak(SCYLLA_MANIFEST_BYTES)
    budget = 192 * MB
    assert honest > budget
    routine = crypto.streaming_upload_peak(crypto.STREAMING_GOVERNOR_CLIENT_PART_BYTES)
    assert crypto.streaming_governor_clamped_reserve(honest, budget) == routine
    assert crypto.copy_governor_clamped_reserve(honest, budget) == budget


@pytest.mark.asyncio
async def test_three_manifest_copies_do_not_run_concurrently_at_192mb():
    """Prod failure mode: 3× 4.7GB copies must not all hold slots on one pod."""
    original_timeout = concurrency.BACKPRESSURE_TIMEOUT
    concurrency.BACKPRESSURE_TIMEOUT = 0
    concurrency.reset_state()
    concurrency.set_memory_limit(192)
    per_copy = crypto.copy_pipeline_peak(SCYLLA_MANIFEST_BYTES)
    assert per_copy > 192 * MB

    max_inside = 0
    inside = 0
    admitted = 0
    rejected = 0

    async def one_copy():
        nonlocal max_inside, inside, admitted, rejected
        try:
            async with concurrency.reserve_copy_memory(per_copy):
                inside += 1
                max_inside = max(max_inside, inside)
                admitted += 1
                await asyncio.sleep(0.05)
                inside -= 1
        except S3Error:
            rejected += 1

    try:
        await asyncio.gather(*[one_copy() for _ in range(3)])
    finally:
        concurrency.BACKPRESSURE_TIMEOUT = original_timeout
        concurrency.reset_state()

    assert max_inside == 1, f"expected exclusive large copy, saw {max_inside} concurrent"
    assert admitted == 1
    assert rejected == 2


@pytest.mark.asyncio
async def test_manifest_copy_plus_scylla_upload_serialized_at_192mb():
    """Active ~25MB Scylla upload must block a budget-monopolizing manifest copy."""
    original_timeout = concurrency.BACKPRESSURE_TIMEOUT
    concurrency.BACKPRESSURE_TIMEOUT = 0
    concurrency.reset_state()
    concurrency.set_memory_limit(192)
    try:
        scylla = crypto.governor_memory_footprint(50 * MB)
        await concurrency.try_acquire_memory(scylla)

        with pytest.raises(S3Error):
            await concurrency.try_acquire_copy_memory(
                crypto.copy_pipeline_peak(SCYLLA_MANIFEST_BYTES)
            )
    finally:
        concurrency.BACKPRESSURE_TIMEOUT = original_timeout
        concurrency.reset_state()


@pytest.mark.parametrize(
    "mb,slack",
    [
        (512, 1.0),
        (1024, 1.08),  # large internal parts: transport body copy slack
        (4767, 1.08),
    ],
)
@pytest.mark.asyncio
async def test_copy_reservation_bounds_real_peak_large_manifests(mb: int, slack: float):
    """copy_pipeline_peak must cover tracemalloc peak for prod-sized copies."""
    plaintext_size = mb * MB
    real_peak = await _measure_peak(plaintext_size)
    reserved = crypto.copy_pipeline_peak(plaintext_size)
    assert reserved * slack >= real_peak, (
        f"{mb}MB copy: reserved {reserved / MB:.1f}MB * {slack} < real {real_peak / MB:.1f}MB"
    )


@pytest.mark.asyncio
async def test_copy_acquire_uses_copy_clamp_not_upload_clamp():
    concurrency.reset_state()
    concurrency.set_memory_limit(192)
    try:
        honest = crypto.copy_pipeline_peak(SCYLLA_MANIFEST_BYTES)
        reserved = await concurrency.try_acquire_copy_memory(honest)
        assert reserved == 192 * MB
        assert reserved > crypto.streaming_governor_clamped_reserve(honest, 192 * MB)
    finally:
        concurrency.reset_state()
