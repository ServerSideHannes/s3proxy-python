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
from s3proxy.handlers.multipart.copy import reset_copy_pipeline_semaphore
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

    async def take_deferred_copy_tail(self, bucket, key, upload_id):
        return b""

    async def set_deferred_copy_tail(self, bucket, key, upload_id, tail):
        return None


def _streaming_copy_inner(handler, client, plaintext_size: int):
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


def _streaming_copy(handler, client, plaintext_size: int):
    """Full streaming copy path including pipeline semaphore."""
    state = MultipartUploadState(dek=crypto.generate_dek(), bucket="b", key="k", upload_id="u")
    return handler._streaming_copy_part(
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


@pytest.fixture(autouse=True)
def _reset_copy_semaphore():
    reset_copy_pipeline_semaphore(2)
    yield
    reset_copy_pipeline_semaphore(2)


@pytest.mark.asyncio
async def test_reservation_held_during_upload_wait():
    """Governor must track ciphertext through S3 upload, not release before it."""
    concurrency.reset_state()
    concurrency.set_memory_limit(192)

    plaintext_size = 96 * MB
    gate = asyncio.Event()
    client = _SlowUploadClient(plaintext_size, gate)
    handler = _handler()
    handler.multipart_manager = _TrackingMgr()
    chunk_peak = crypto.copy_chunk_peak(crypto.COPY_INTERNAL_PART_SIZE)

    copy_task = asyncio.create_task(_streaming_copy_inner(handler, client, plaintext_size))

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
    assert mid_upload_active == chunk_peak, (
        f"reservation released before upload finished: {mid_upload_active / MB:.2f}MB "
        f"(expected {chunk_peak / MB:.2f}MB)"
    )


@pytest.mark.asyncio
async def test_governor_returns_to_baseline_between_internal_parts():
    """Brief gap between parts after upload completes and next acquire."""
    concurrency.reset_state()
    concurrency.set_memory_limit(192)

    plaintext_size = 96 * MB
    handler = _handler()
    handler.multipart_manager = _TrackingMgr()

    min_active = crypto.copy_chunk_peak(crypto.COPY_INTERNAL_PART_SIZE)
    original_acquire = concurrency._default.try_acquire
    original_release = concurrency._default.release

    async def tracking_acquire(bytes_needed: int, *, copy: bool = False):
        nonlocal min_active
        reserved = await original_acquire(bytes_needed, copy=copy)
        min_active = min(min_active, concurrency.get_active_memory())
        return reserved

    async def tracking_release(bytes_reserved: int) -> None:
        nonlocal min_active
        await original_release(bytes_reserved)
        min_active = min(min_active, concurrency.get_active_memory())

    concurrency._default.try_acquire = tracking_acquire  # type: ignore[method-assign]
    concurrency._default.release = tracking_release  # type: ignore[method-assign]

    try:
        await _streaming_copy_inner(handler, _Client(plaintext_size), plaintext_size)
    finally:
        concurrency._default.try_acquire = original_acquire  # type: ignore[method-assign]
        concurrency._default.release = original_release  # type: ignore[method-assign]
        concurrency.reset_state()

    assert min_active == 0, (
        f"governor never returned to baseline between parts; min_active={min_active / MB:.2f}MB"
    )


@pytest.mark.asyncio
async def test_copy_pipeline_semaphore_limits_concurrent_copies():
    """Third copy must wait while two pipelines hold the semaphore."""
    concurrency.reset_state()
    concurrency.set_memory_limit(192)
    reset_copy_pipeline_semaphore(2)

    plaintext_size = 64 * MB
    gate1 = asyncio.Event()
    gate2 = asyncio.Event()
    client1 = _SlowUploadClient(plaintext_size, gate1)
    client2 = _SlowUploadClient(plaintext_size, gate2)
    handler = _handler()
    handler.multipart_manager = _TrackingMgr()

    third_started = asyncio.Event()

    async def third_copy():
        await _streaming_copy(handler, _Client(plaintext_size), plaintext_size)
        third_started.set()

    copy1 = asyncio.create_task(_streaming_copy(handler, client1, plaintext_size))
    copy2 = asyncio.create_task(_streaming_copy(handler, client2, plaintext_size))

    for _ in range(200):
        if client1.upload_calls >= 1 and client2.upload_calls >= 1:
            break
        await asyncio.sleep(0.01)

    copy3 = asyncio.create_task(third_copy())
    await asyncio.sleep(0.2)
    assert not third_started.is_set(), "third copy started while two pipelines still held semaphore"

    gate1.set()
    gate2.set()
    await asyncio.gather(copy1, copy2, copy3)
    assert third_started.is_set()


@pytest.mark.asyncio
async def test_five_large_copies_all_complete_with_per_part_reservation():
    """Five 128MB copies complete on a 192MB budget (serialized by pipeline cap)."""
    original_timeout = concurrency.BACKPRESSURE_TIMEOUT
    concurrency.BACKPRESSURE_TIMEOUT = 30
    concurrency.reset_state()
    concurrency.set_memory_limit(192)
    reset_copy_pipeline_semaphore(2)

    plaintext_size = 128 * MB
    completed = 0

    async def one_copy(idx: int):
        nonlocal completed
        client = _Client(plaintext_size)
        handler = _handler()
        handler.multipart_manager = _TrackingMgr()
        await handler._streaming_copy_part(
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
            plaintext_size,
        )
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
        await _streaming_copy_inner(handler, _Client(plaintext_size), plaintext_size)
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
        await _streaming_copy_inner(handler, _Client(256 * MB), 256 * MB)
    finally:
        concurrency._default.try_acquire = original_acquire  # type: ignore[method-assign]
        concurrency._default.release = original_release  # type: ignore[method-assign]
        concurrency.reset_state()

    assert peak_seen <= chunk_peak + 64 * 1024, (
        f"peak active {peak_seen / MB:.2f}MB exceeded one chunk budget {chunk_peak / MB:.2f}MB"
    )


@pytest.mark.asyncio
async def test_peak_governor_bounded_with_five_concurrent_copies():
    """Prod regression: 13 Scylla nodes hammer one pod. Semaphore caps active copies."""
    original_timeout = concurrency.BACKPRESSURE_TIMEOUT
    concurrency.BACKPRESSURE_TIMEOUT = 30
    concurrency.reset_state()
    concurrency.set_memory_limit(192)
    reset_copy_pipeline_semaphore(2)

    chunk_peak = crypto.copy_chunk_peak(crypto.COPY_INTERNAL_PART_SIZE)
    peak_seen = 0
    original_acquire = concurrency._default.try_acquire

    async def tracking_acquire(bytes_needed: int, *, copy: bool = False):
        nonlocal peak_seen
        reserved = await original_acquire(bytes_needed, copy=copy)
        peak_seen = max(peak_seen, concurrency.get_active_memory())
        return reserved

    concurrency._default.try_acquire = tracking_acquire  # type: ignore[method-assign]

    completed = 0

    async def one_copy():
        nonlocal completed
        handler = _handler()
        handler.multipart_manager = _TrackingMgr()
        await _streaming_copy(handler, _Client(128 * MB), 128 * MB)
        completed += 1

    try:
        await asyncio.gather(*[one_copy() for _ in range(5)])
    finally:
        concurrency._default.try_acquire = original_acquire  # type: ignore[method-assign]
        concurrency.BACKPRESSURE_TIMEOUT = original_timeout
        concurrency.reset_state()

    assert completed == 5
    # At most 2 pipelines × ~88MB; never 3× (264MB) which caused prod OOM accounting gap.
    assert peak_seen <= 2 * chunk_peak + 1 * MB, (
        f"peak governor {peak_seen / MB:.2f}MB exceeded 2-pipeline cap "
        f"({2 * chunk_peak / MB:.2f}MB)"
    )


@pytest.mark.asyncio
async def test_third_copy_blocked_at_semaphore_before_governor_spikes():
    """Extra copies queue on the semaphore, not as untracked RSS."""
    concurrency.reset_state()
    concurrency.set_memory_limit(192)
    reset_copy_pipeline_semaphore(2)

    plaintext_size = 64 * MB
    gate1 = asyncio.Event()
    gate2 = asyncio.Event()
    client1 = _SlowUploadClient(plaintext_size, gate1)
    client2 = _SlowUploadClient(plaintext_size, gate2)
    handler = _handler()
    handler.multipart_manager = _TrackingMgr()
    chunk_peak = crypto.copy_chunk_peak(crypto.COPY_INTERNAL_PART_SIZE)

    peak_seen = 0
    original_acquire = concurrency._default.try_acquire

    async def tracking_acquire(bytes_needed: int, *, copy: bool = False):
        nonlocal peak_seen
        reserved = await original_acquire(bytes_needed, copy=copy)
        peak_seen = max(peak_seen, concurrency.get_active_memory())
        return reserved

    concurrency._default.try_acquire = tracking_acquire  # type: ignore[method-assign]

    in_pump = 0
    max_in_pump = 0
    original_pump = handler._pump_copy_chunks

    async def counting_pump(*args, **kwargs):
        nonlocal in_pump, max_in_pump
        in_pump += 1
        max_in_pump = max(max_in_pump, in_pump)
        try:
            return await original_pump(*args, **kwargs)
        finally:
            in_pump -= 1

    handler._pump_copy_chunks = counting_pump  # type: ignore[method-assign]

    try:
        copy1 = asyncio.create_task(_streaming_copy(handler, client1, plaintext_size))
        copy2 = asyncio.create_task(_streaming_copy(handler, client2, plaintext_size))

        for _ in range(200):
            if client1.upload_calls >= 1 and client2.upload_calls >= 1:
                break
            await asyncio.sleep(0.01)

        copy3 = asyncio.create_task(
            _streaming_copy(handler, _Client(plaintext_size), plaintext_size)
        )
        await asyncio.sleep(0.2)

        assert max_in_pump <= 2, f"{max_in_pump} copies in pump concurrently (semaphore cap is 2)"
        assert peak_seen <= 2 * chunk_peak + 1 * MB, (
            f"governor spiked to {peak_seen / MB:.2f}MB with >2 pipelines"
        )

        gate1.set()
        gate2.set()
        await asyncio.gather(copy1, copy2, copy3)
        assert max_in_pump <= 2
    finally:
        concurrency._default.try_acquire = original_acquire  # type: ignore[method-assign]
        concurrency.reset_state()
