"""A streaming GET must hold its memory reservation for the whole stream.

A StreamingResponse sends its body AFTER the request handler returns. The
handler used to release the GET's memory reservation in its finally -- i.e.
before a single byte was streamed -- so concurrent streaming GETs ran
ungoverned. Each one holds ~one decrypted frame in the transport send buffer,
so a flood accumulated frames and OOMed the pod while the limiter read ~budget
(reproduced locally: a 90-concurrent multipart GET flood at a 512Mi cap with a
64MB budget OOMKilled the pod, exit 137, 0/180 succeeded; with the reservation
held for the stream lifetime it peaks ~325MiB and completes 180/180).

_release_after_stream wraps the body iterator so the reservation is released
only when the stream is exhausted or the consumer stops early. These tests pin
that timing: the reservation stays held while streaming and is released exactly
once at teardown.
"""

import pytest

from s3proxy import concurrency
from s3proxy.request_handler import _release_after_stream

MB = 1024 * 1024


@pytest.mark.asyncio
async def test_reservation_held_until_stream_exhausted():
    limiter = concurrency._default
    limiter.set_memory_limit(64)
    limiter.active_bytes = 0
    reserved = await limiter.try_acquire(8 * MB)
    assert limiter.active_bytes == reserved > 0

    async def source():
        for i in range(3):
            # Reservation must still be held while the body is being sent.
            assert limiter.active_bytes == reserved
            yield f"chunk{i}".encode()

    chunks = [c async for c in _release_after_stream(source(), reserved)]
    assert chunks == [b"chunk0", b"chunk1", b"chunk2"]
    # Released exactly once after the stream finished.
    assert limiter.active_bytes == 0


@pytest.mark.asyncio
async def test_reservation_released_on_early_consumer_exit():
    limiter = concurrency._default
    limiter.set_memory_limit(64)
    limiter.active_bytes = 0
    reserved = await limiter.try_acquire(8 * MB)

    async def source():
        for i in range(100):
            yield bytes(i)

    wrapped = _release_after_stream(source(), reserved)
    async for _ in wrapped:
        break  # client disconnects after one chunk
    await wrapped.aclose()
    # Reservation released even though the stream was abandoned early.
    assert limiter.active_bytes == 0
