"""Prove copy-path memory is reserved per internal part, not for the whole object.

These tests drive the real _streaming_copy_part_inner / _pump_copy_chunks path and
observe concurrency.get_active_memory(). They would fail if the copy held one
~88MB reservation for the entire multi-part pipeline (the prod regression where
active_mb stuck at 176.38 for two concurrent 1280MB copies).
"""

from __future__ import annotations

import asyncio

import pytest

from s3proxy import concurrency, crypto
from s3proxy.state import MultipartUploadState
from tests.unit.test_copy_reservation_vs_real import _Client, _handler

MB = 1024 * 1024


class _SlowUploadClient(_Client):
    """Blocks every upload_part until gate is set."""

    def __init__(self, total: int, gate: asyncio.Event):
        super().__init__(total)
        self.gate = gate
        self.upload_calls = 0

    async def upload_part(self, bucket, key, upload_id, part_number, body):
        self.upload_calls += 1
        await self.gate.wait()
        return await super().upload_part(bucket, key, upload_id, part_number, body)


class _TrackingMgr:
    def __init__(self):
        self.allocated = 0

    async def allocate_internal_parts(self, bucket, key, upload_id, count, client_part_number):
        start = self.allocated + 1
        self.allocated += count
        return start

    async def add_part(self, *a, **k):
        return None


def _streaming_copy(handler, client, plaintext_size: int):
    state = MultipartUploadState(dek=crypto.generate_dek(), bucket="b", key="k", upload_id="u")
    return handler._streaming_copy_part_inner(
        client,
        "b",
        "k",
        "u",
        1,
        state,
        "b",
        "src",
        None,
        None,
        None,
        {},
        {},
        plaintext_size,
    )


@pytest.mark.asyncio
async def test_governor_returns_to_baseline_between_internal_parts():
    """Smoking gun: active_bytes must hit 0 between internal parts of one copy."""
    concurrency.reset_state()
    concurrency.set_memory_limit(192)

    # 96MB = 3 internal parts at 32MB each
    plaintext_size = 96 * MB
    gate = asyncio.Event()  # unset: block uploads until test releases
    client = _SlowUploadClient(plaintext_size, gate)
    handler = _handler()
    handler.multipart_manager = _TrackingMgr()

    copy_task = asyncio.create_task(_streaming_copy(handler, client, plaintext_size))

    # Wait until part 1 encrypt finished and upload_part is blocked on gate
    for _ in range(200):
        if client.upload_calls >= 1:
            break
        await asyncio.sleep(0.01)
    assert client.upload_calls >= 1
    mid_upload_active = concurrency.get_active_memory()
    gate.set()
    await copy_task

    assert client.upload_calls == 3
    assert concurrency.get_active_memory() == 0
    assert mid_upload_active == 0, (
        f"reservation still held during S3 upload wait: {mid_upload_active / MB:.2f}MB"
    )


@pytest.mark.asyncio
async def test_second_copy_admitted_while_first_waits_on_s3():
    """Between parts, a second copy must acquire memory while the first uploads."""
    concurrency.reset_state()
    concurrency.set_memory_limit(192)

    plaintext_size = 64 * MB  # 2 internal parts
    gate = asyncio.Event()
    client1 = _SlowUploadClient(plaintext_size, gate)
    handler1 = _handler()
    handler1.multipart_manager = _TrackingMgr()

    acquired_during_upload = asyncio.Event()

    async def second_copy_acquires():
        await asyncio.sleep(0.05)  # let copy1 finish part1 encrypt and block on upload
        assert client1.upload_calls >= 1
        assert concurrency.get_active_memory() == 0
        reserved = await concurrency.try_acquire_copy_memory(crypto.copy_chunk_peak(32 * MB))
        assert reserved > 0
        acquired_during_upload.set()
        await concurrency.release_memory(reserved)

    copy1 = asyncio.create_task(_streaming_copy(handler1, client1, plaintext_size))
    copy2 = asyncio.create_task(second_copy_acquires())

    for _ in range(100):
        if client1.upload_calls >= 1:
            break
        await asyncio.sleep(0.01)
    gate.set()

    await asyncio.wait_for(acquired_during_upload.wait(), timeout=2)
    await asyncio.gather(copy1, copy2)
    assert concurrency.get_active_memory() == 0


@pytest.mark.asyncio
async def test_five_large_copies_all_complete_with_per_part_reservation():
    """Five 128MB copies interleave on a 192MB budget (impossible with whole-object hold)."""
    original_timeout = concurrency.BACKPRESSURE_TIMEOUT
    concurrency.BACKPRESSURE_TIMEOUT = 30
    concurrency.reset_state()
    concurrency.set_memory_limit(192)

    plaintext_size = 128 * MB  # 4 internal parts each
    completed = 0

    async def one_copy(idx: int):
        nonlocal completed
        client = _Client(plaintext_size)
        handler = _handler()
        handler.multipart_manager = _TrackingMgr()
        await _streaming_copy(handler, client, plaintext_size)
        completed += 1

    try:
        await asyncio.gather(*[one_copy(i) for i in range(5)])
    finally:
        concurrency.BACKPRESSURE_TIMEOUT = original_timeout
        concurrency.reset_state()

    assert completed == 5
    peak = max(
        crypto.copy_chunk_peak(crypto.COPY_INTERNAL_PART_SIZE),
        crypto.copy_pipeline_peak(plaintext_size),
    )
    assert peak < 192 * MB


@pytest.mark.asyncio
async def test_per_part_reserve_uses_chunk_peak_not_whole_object():
    """Each internal part reserves copy_chunk_peak(chunk_size), not object size."""
    concurrency.reset_state()
    concurrency.set_memory_limit(192)

    acquired: list[int] = []
    original_acquire = concurrency._default.try_acquire

    async def tracking_acquire(bytes_needed: int, *, copy: bool = False):
        reserved = await original_acquire(bytes_needed, copy=copy)
        if copy:
            acquired.append(reserved)
        return reserved

    concurrency._default.try_acquire = tracking_acquire  # type: ignore[method-assign]

    plaintext_size = 128 * MB
    try:
        handler = _handler()
        handler.multipart_manager = _TrackingMgr()
        await _streaming_copy(handler, _Client(plaintext_size), plaintext_size)
    finally:
        concurrency._default.try_acquire = original_acquire  # type: ignore[method-assign]
        concurrency.reset_state()

    expected = crypto.copy_chunk_peak(crypto.COPY_INTERNAL_PART_SIZE)
    assert len(acquired) == 4, f"expected 4 per-part acquires, got {len(acquired)}"
    assert all(r == expected for r in acquired), (
        f"per-part reserve drifted: {{r / MB for r in acquired}}MB, expected {expected / MB}MB"
    )
    assert expected == crypto.copy_pipeline_peak(plaintext_size)


def test_copy_chunk_peak_32mb_uses_framed_formula_not_small_buffer():
    """32MB internal parts must not hit the small-buffer 3x formula (96MB bug)."""
    peak = crypto.copy_chunk_peak(32 * MB)
    assert peak == 88 * MB, f"32MB chunk peak should be 88MB, got {peak / MB}MB"
    assert peak < 96 * MB


@pytest.mark.asyncio
async def test_whole_copy_duration_active_memory_bounded_to_one_chunk():
    """While one copy runs, governor active must never exceed one chunk peak."""
    concurrency.reset_state()
    concurrency.set_memory_limit(192)

    chunk_peak = crypto.copy_chunk_peak(crypto.COPY_INTERNAL_PART_SIZE)
    peak_seen = 0
    original_acquire = concurrency._default.try_acquire
    original_release = concurrency._default.release

    async def tracking_acquire(bytes_needed: int, *, copy: bool = False):
        nonlocal peak_seen
        reserved = await original_acquire(bytes_needed, copy=copy)
        peak_seen = max(peak_seen, concurrency.get_active_memory())
        return reserved

    async def tracking_release(bytes_reserved: int) -> None:
        await original_release(bytes_reserved)

    concurrency._default.try_acquire = tracking_acquire  # type: ignore[method-assign]
    concurrency._default.release = tracking_release  # type: ignore[method-assign]

    try:
        handler = _handler()
        handler.multipart_manager = _TrackingMgr()
        await _streaming_copy(handler, _Client(256 * MB), 256 * MB)
    finally:
        concurrency._default.try_acquire = original_acquire  # type: ignore[method-assign]
        concurrency._default.release = original_release  # type: ignore[method-assign]
        concurrency.reset_state()

    assert peak_seen <= chunk_peak + 64 * 1024, (
        f"peak active {peak_seen / MB:.2f}MB exceeded one chunk budget {chunk_peak / MB:.2f}MB"
    )
