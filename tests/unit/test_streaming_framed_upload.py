"""Framed UploadPart: known-length parts are encrypted frame-by-frame into a
ciphertext buffer and uploaded as bytes. Write memory is O(part ciphertext +
frame), not O(2× part), and the result decrypts back to the original bytes."""

import hashlib
import os
import tracemalloc
import types

import pytest

from s3proxy import crypto
from s3proxy.handlers.multipart.upload_part import UploadPartMixin

UPLOAD_ID = "u" * 40


class _Request:
    def __init__(self, data: bytes, chunk: int = 1 << 20):
        self._data, self._chunk = data, chunk

    async def stream(self):
        for i in range(0, len(self._data), self._chunk):
            yield self._data[i : i + self._chunk]


class _Manager:
    def __init__(self):
        self.part_meta = None

    async def add_part(self, bucket, key, upload_id, part_meta):
        self.part_meta = part_meta


def _handler(manager):
    h = UploadPartMixin.__new__(UploadPartMixin)
    h.multipart_manager = manager
    return h


@pytest.mark.asyncio
async def test_framed_upload_roundtrips_and_sets_metadata():
    dek = crypto.generate_dek()
    optimal = 12 * 1024 * 1024  # > FRAME, so parts span multiple frames
    plaintext = os.urandom(30 * 1024 * 1024)  # -> parts of 12, 12, 6 MB

    captured: dict[int, bytes] = {}

    class _Client:
        async def upload_part(self, bucket, key, upload_id, part_number, body):
            captured[part_number] = body
            return {"ETag": f'"{part_number:032x}"'}

    mgr = _Manager()
    result = await _handler(mgr)._stream_and_upload_framed(
        _Request(plaintext),
        _Client(),
        "b",
        "k",
        UPLOAD_ID,
        1,
        types.SimpleNamespace(dek=dek),
        len(plaintext),
        optimal,
        1,
    )

    # Three internal parts of the planned sizes.
    sizes = [m.plaintext_size for m in mgr.part_meta.internal_parts]
    assert sizes == [12 * 1024 * 1024, 12 * 1024 * 1024, 6 * 1024 * 1024]

    # Each part decrypts via the framed reader; concatenation is the original.
    recovered = b"".join(
        crypto.decrypt_framed(captured[m.internal_part_number], dek, m.plaintext_size)
        for m in sorted(mgr.part_meta.internal_parts, key=lambda m: m.internal_part_number)
    )
    assert recovered == plaintext

    assert result["client_etag"] == hashlib.md5(plaintext, usedforsecurity=False).hexdigest()
    assert result["computed_sha256"] == hashlib.sha256(plaintext).hexdigest()
    assert result["total_plaintext_size"] == len(plaintext)


class _DiscardingClient:
    async def upload_part(self, bucket, key, upload_id, part_number, body):
        del body
        return {"ETag": f'"{part_number:032x}"'}


class _ZeroRequest:
    """Streams `size` bytes from a fixed block so the handler — not the test —
    owns whatever memory is held."""

    def __init__(self, size: int):
        self._size = size

    async def stream(self):
        sent, block = 0, bytes(1 << 20)
        while sent < self._size:
            n = min(1 << 20, self._size - sent)
            sent += n
            yield block[:n]


async def _measure_framed_peak(client_part_size: int) -> int:
    handler = _handler(_Manager())
    tracemalloc.start()
    tracemalloc.reset_peak()
    await handler._stream_and_upload_framed(
        _ZeroRequest(client_part_size),
        _DiscardingClient(),
        "b",
        "k",
        UPLOAD_ID,
        1,
        types.SimpleNamespace(dek=crypto.generate_dek()),
        client_part_size,
        crypto.PART_SIZE,  # same internal part size barman uses for 512MB parts
        1,
    )
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return peak


@pytest.mark.asyncio
async def test_framed_upload_memory_is_independent_of_part_size():
    """A 256MB client part peaks no higher than a 64MB one: ciphertext is built
    one internal part at a time, not scaled by total client part size."""
    small = await _measure_framed_peak(64 * 1024 * 1024)
    large = await _measure_framed_peak(256 * 1024 * 1024)

    # Must stay below the old buffered path (~2× part_size ≈ 257MB per request).
    assert small < 220 * 1024 * 1024, f"small peak {small / 1024 / 1024:.1f}MB"
    assert large < 220 * 1024 * 1024, f"large peak {large / 1024 / 1024:.1f}MB"
    # Larger client part must not scale memory linearly with part count.
    assert large <= small * 1.5 + crypto.FRAME_PLAINTEXT_SIZE
