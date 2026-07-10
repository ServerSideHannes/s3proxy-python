"""FramedStreamBody: sealed frames on the fly, exact Content-Length, no replay."""

import hashlib
import os

import pytest

from s3proxy import crypto
from s3proxy.handlers.multipart.upload_part import _PlaintextReader
from s3proxy.streaming import FramedStreamBody, SourceExhaustedError

UPLOAD_ID = "u" * 40
MB = 1024 * 1024


async def _chunks(data: bytes, chunk: int = 1 << 20):
    for i in range(0, len(data), chunk):
        yield data[i : i + chunk]


def _body(data: bytes, size: int | None = None, **kwargs) -> FramedStreamBody:
    return FramedStreamBody(
        _PlaintextReader(_chunks(data)),
        size if size is not None else len(data),
        b"k" * 32,
        UPLOAD_ID,
        7,
        **kwargs,
    )


async def _collect(body: FramedStreamBody) -> bytes:
    return b"".join([frame async for frame in body])


@pytest.mark.asyncio
async def test_yields_decryptable_frames_with_exact_declared_length():
    dek = crypto.generate_dek()
    plaintext = os.urandom(20 * MB)  # 3 frames: 8, 8, 4
    body = FramedStreamBody(_PlaintextReader(_chunks(plaintext)), len(plaintext), dek, UPLOAD_ID, 7)

    ciphertext = await _collect(body)
    assert len(ciphertext) == len(body) == crypto.framed_ciphertext_size(len(plaintext))
    assert crypto.decrypt_framed(ciphertext, dek, len(plaintext)) == plaintext


@pytest.mark.asyncio
async def test_frame_boundaries_and_nonces_match_buffered_encryption():
    """Streamed frames must be byte-identical to the old buffered encrypt loop."""
    dek = crypto.generate_dek()
    plaintext = os.urandom(10 * MB)
    body = FramedStreamBody(_PlaintextReader(_chunks(plaintext)), len(plaintext), dek, UPLOAD_ID, 7)
    streamed = await _collect(body)

    buffered = bytearray()
    for idx in range(crypto.frame_count(len(plaintext))):
        start = idx * crypto.FRAME_PLAINTEXT_SIZE
        frame_pt = plaintext[start : start + crypto.FRAME_PLAINTEXT_SIZE]
        buffered.extend(crypto.encrypt_frame(frame_pt, dek, UPLOAD_ID, 7, idx))
    assert streamed == bytes(buffered)


@pytest.mark.asyncio
async def test_tracks_plaintext_hashes_and_ciphertext_md5():
    plaintext = os.urandom(9 * MB)
    md5 = hashlib.md5(usedforsecurity=False)
    sha = hashlib.sha256()
    body = _body(plaintext, plaintext_hashes=(md5, sha))

    ciphertext = await _collect(body)
    assert md5.hexdigest() == hashlib.md5(plaintext, usedforsecurity=False).hexdigest()
    assert sha.hexdigest() == hashlib.sha256(plaintext).hexdigest()
    assert body.ciphertext_md5_hexdigest() == hashlib.md5(ciphertext).hexdigest()


@pytest.mark.asyncio
async def test_short_source_raises_and_flags():
    plaintext = os.urandom(3 * MB)
    body = _body(plaintext, size=9 * MB)

    with pytest.raises(SourceExhaustedError):
        await _collect(body)
    assert body.short_read
    assert body.bytes_read == 3 * MB


@pytest.mark.asyncio
async def test_single_shot_cannot_be_replayed():
    body = _body(b"x" * MB)
    await _collect(body)
    with pytest.raises(RuntimeError, match="single-shot"):
        await _collect(body)


def test_botocore_compat_surface():
    """Blob param validation needs `read`; length must be known via len()."""
    body = _body(b"x" * MB)
    assert hasattr(body, "read")
    with pytest.raises(NotImplementedError):
        body.read()
    assert len(body) == crypto.framed_ciphertext_size(MB)
    assert not hasattr(body, "seek")  # botocore must not silently replay it


def test_rejects_empty_plaintext():
    with pytest.raises(ValueError):
        _body(b"", size=0)
