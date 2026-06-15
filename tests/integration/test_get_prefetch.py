"""Tests for single-part lookahead (prefetch) in encrypted multipart GET.

Covers the prefetch orchestration contract directly (ordering, fetch/send
overlap, cancellation+cleanup) and the end-to-end acceptance criteria from the
issue: byte-identical multi-part GETs and a balanced memory ledger after both a
full read and a mid-stream client disconnect.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from s3proxy import concurrency, crypto
from s3proxy.handlers import S3ProxyHandler
from s3proxy.state import (
    InternalPartMetadata,
    MultipartMetadata,
    MultipartStateManager,
    PartMetadata,
    save_multipart_metadata,
)

pytestmark = pytest.mark.asyncio


def _handler(settings):
    return S3ProxyHandler(settings, {}, MultipartStateManager())


# ---------------------------------------------------------------------------
# Prefetch orchestration contract
# ---------------------------------------------------------------------------


async def test_prefetch_yields_in_order(settings):
    h = _handler(settings)

    async def fetch(i):
        await asyncio.sleep(0)
        return f"part-{i}".encode()

    out = [chunk async for _, chunk in h._stream_parts_with_prefetch([1, 2, 3, 4], fetch)]
    assert out == [b"part-1", b"part-2", b"part-3", b"part-4"]


async def test_prefetch_empty_list_is_noop(settings):
    h = _handler(settings)

    async def fetch(i):  # pragma: no cover - must never be called
        raise AssertionError("fetch should not run for an empty list")

    out = [chunk async for _, chunk in h._stream_parts_with_prefetch([], fetch)]
    assert out == []


async def test_next_part_fetched_while_current_consumed(settings):
    """Part N+1's fetch starts while part N is held by the caller (overlap)."""
    h = _handler(settings)
    started: list[int] = []
    gate = asyncio.Event()

    async def fetch(i):
        started.append(i)
        if i == 1:
            await gate.wait()  # keep part 1 in flight while we inspect
        return i

    agen = h._stream_parts_with_prefetch([0, 1], fetch)
    _, chunk = await agen.__anext__()  # part 0 in hand; part 1 prefetch scheduled
    assert chunk == 0
    await asyncio.sleep(0)  # let the lookahead task run
    assert started == [0, 1]  # part 1 fetched before we asked for it
    gate.set()
    rest = [c async for _, c in agen]
    assert rest == [1]


async def test_disconnect_cancels_lookahead_and_runs_cleanup(settings):
    """Closing the stream mid-flight cancels the in-flight prefetch and runs its finally."""
    h = _handler(settings)
    active = 0
    cancelled: list[int] = []
    in_flight = asyncio.Event()

    async def fetch(i):
        nonlocal active
        active += 1
        try:
            if i == 0:
                return b"zero"
            in_flight.set()
            await asyncio.sleep(10)  # still fetching when the client disconnects
            return b"one"
        except asyncio.CancelledError:
            cancelled.append(i)
            raise
        finally:
            active -= 1

    agen = h._stream_parts_with_prefetch([0, 1], fetch)
    _, chunk = await agen.__anext__()
    assert chunk == b"zero"
    await in_flight.wait()
    assert active == 1  # part 1's reservation is held by the lookahead
    await agen.aclose()  # client disconnect
    assert cancelled == [1]
    assert active == 0  # reservation released by fetch's finally


async def test_prefetch_error_propagates_during_iteration(settings):
    h = _handler(settings)

    async def fetch(i):
        if i == 1:
            raise ValueError("boom")
        return i

    agen = h._stream_parts_with_prefetch([0, 1, 2], fetch)
    first = await agen.__anext__()
    assert first[1] == 0
    with pytest.raises(ValueError, match="boom"):
        await agen.__anext__()


# ---------------------------------------------------------------------------
# End-to-end: byte-identity + memory ledger
# ---------------------------------------------------------------------------


async def _make_encrypted_multipart(mock_s3, kek, part_sizes):
    """Upload an encrypted multipart object (one client part, many internal parts)."""
    dek = crypto.generate_dek()
    wrapped = crypto.wrap_key(dek, kek)
    upload_id = "prefetch-upload"
    internal_parts, ct_parts, total_ct = [], [], 0
    plaintext = bytearray()
    for n, sz in enumerate(part_sizes, start=1):
        chunk = bytes([n % 256]) * sz
        plaintext.extend(chunk)
        ct = crypto.encrypt(chunk, dek, crypto.derive_part_nonce(upload_id, n))
        ct_parts.append(ct)
        internal_parts.append(
            InternalPartMetadata(
                internal_part_number=n,
                plaintext_size=sz,
                ciphertext_size=len(ct),
                etag=f"e{n}",
            )
        )
        total_ct += len(ct)
    part_meta = PartMetadata(
        part_number=1,
        plaintext_size=len(plaintext),
        ciphertext_size=total_ct,
        etag="synthetic",
        md5="md5",
        internal_parts=internal_parts,
    )
    meta = MultipartMetadata(
        version=1,
        part_count=1,
        total_plaintext_size=len(plaintext),
        parts=[part_meta],
        wrapped_dek=wrapped,
        kid="AKIAIOSFODNN7EXAMPLE",
    )
    await mock_s3.create_bucket("b")
    await mock_s3.put_object("b", "k", b"".join(ct_parts))
    await save_multipart_metadata(mock_s3, "b", "k", meta)
    return bytes(plaintext)


def _get_request(path="/b/k", range_header=None):
    req = MagicMock()
    req.url.path = path
    req.headers = {"range": range_header} if range_header else {}
    return req


# Internal parts above 4MB force a real memory reservation (ciphertext*2 > the
# 8MB MAX_BUFFER_SIZE), so the ledger assertions actually exercise try_acquire.
PART = 5 * 1024 * 1024


async def test_multipart_get_is_byte_identical_with_prefetch(settings, mock_s3, kek):
    concurrency.set_memory_limit(64)
    concurrency.reset_state()
    handler = _handler(settings)
    handler._client = MagicMock(return_value=mock_s3)
    expected = await _make_encrypted_multipart(mock_s3, kek, [PART, PART, PART])

    resp = await handler.handle_get_object(_get_request(), MagicMock())
    chunks = [c async for c in resp.body_iterator]

    assert b"".join(chunks) == expected
    assert concurrency.get_active_memory() == 0  # no reservation leaked


async def test_ranged_multipart_get_spans_part_boundary(settings, mock_s3, kek):
    concurrency.set_memory_limit(64)
    concurrency.reset_state()
    handler = _handler(settings)
    handler._client = MagicMock(return_value=mock_s3)
    expected = await _make_encrypted_multipart(mock_s3, kek, [PART, PART, PART])

    # Range straddles the 1st/2nd internal-part boundary.
    start, end = PART - 1000, PART + 1000
    resp = await handler.handle_get_object(
        _get_request(range_header=f"bytes={start}-{end}"), MagicMock()
    )
    chunks = [c async for c in resp.body_iterator]

    assert b"".join(chunks) == expected[start : end + 1]
    assert resp.status_code == 206
    assert concurrency.get_active_memory() == 0


async def test_multipart_get_no_leak_on_client_disconnect(settings, mock_s3, kek):
    concurrency.set_memory_limit(64)
    concurrency.reset_state()
    handler = _handler(settings)
    handler._client = MagicMock(return_value=mock_s3)
    await _make_encrypted_multipart(mock_s3, kek, [PART, PART, PART, PART])

    resp = await handler.handle_get_object(_get_request(), MagicMock())
    it = resp.body_iterator
    await it.__anext__()  # read one part; lookahead is in flight
    await it.aclose()  # client goes away mid-stream

    assert concurrency.get_active_memory() == 0  # lookahead reservation released


async def _make_framed_encrypted_multipart(mock_s3, kek, upload_id, part_sizes):
    """Like _make_encrypted_multipart but with multi-frame internal parts (barman path)."""
    dek = crypto.generate_dek()
    wrapped = crypto.wrap_key(dek, kek)
    internal_parts, ct_parts, total_ct = [], [], 0
    plaintext = bytearray()
    for n, sz in enumerate(part_sizes, start=1):
        chunk = bytes([n % 256]) * sz
        plaintext.extend(chunk)
        ct = bytearray()
        for frame_idx in range(0, max(1, sz), crypto.FRAME_PLAINTEXT_SIZE):
            frame_pt = chunk[frame_idx : frame_idx + crypto.FRAME_PLAINTEXT_SIZE]
            ct += crypto.encrypt_frame(frame_pt, dek, upload_id, n, frame_idx // crypto.FRAME_PLAINTEXT_SIZE)
        ct = bytes(ct)
        ct_parts.append(ct)
        internal_parts.append(
            InternalPartMetadata(
                internal_part_number=n,
                plaintext_size=sz,
                ciphertext_size=len(ct),
                etag=f"e{n}",
            )
        )
        total_ct += len(ct)
    part_meta = PartMetadata(
        part_number=1,
        plaintext_size=len(plaintext),
        ciphertext_size=total_ct,
        etag="synthetic",
        md5="md5",
        internal_parts=internal_parts,
    )
    meta = MultipartMetadata(
        version=2,
        part_count=1,
        total_plaintext_size=len(plaintext),
        parts=[part_meta],
        wrapped_dek=wrapped,
        kid="AKIAIOSFODNN7EXAMPLE",
    )
    await mock_s3.create_bucket("b")
    await mock_s3.put_object("b", "k", b"".join(ct_parts))
    await save_multipart_metadata(mock_s3, "b", "k", meta)
    return bytes(plaintext)


FRAMED_PART = crypto.PART_SIZE  # 64MB — would OOM on whole-part decrypt at 64MB budget


async def test_framed_64mb_internal_parts_get_under_memory_limit(settings, mock_s3, kek):
    """Restore path: framed 64MB internal parts must GET at O(frame) memory."""
    concurrency.set_memory_limit(64)
    concurrency.reset_state()
    handler = _handler(settings)
    handler._client = MagicMock(return_value=mock_s3)
    upload_id = "framed-restore-upload"
    expected = await _make_framed_encrypted_multipart(
        mock_s3, kek, upload_id, [FRAMED_PART, FRAMED_PART // 2]
    )

    resp = await handler.handle_get_object(_get_request(), MagicMock())
    chunks = [c async for c in resp.body_iterator]

    assert b"".join(chunks) == expected
    assert concurrency.get_active_memory() == 0
