"""Copy pump retry semantics for streamed internal-part bodies.

Streamed bodies cannot be replayed by botocore, so _pump_copy_chunks retries a
failed internal part itself: it reopens the plaintext source at the part's
offset and re-streams it. These tests inject mid-body backend failures and
assert the copy self-heals with correct data, correct client ETag (the MD5
snapshot must not double-hash retried bytes), and correct reopen offsets.
"""

from __future__ import annotations

import asyncio
import hashlib

import pytest

from s3proxy import concurrency, crypto
from s3proxy.handlers.multipart import MultipartHandlerMixin
from s3proxy.state import MultipartUploadState

MB = 1024 * 1024


class _Body:
    def __init__(self, data: bytes, chunk: int = MB):
        self._data = data
        self._chunk = chunk
        self._pos = 0
        self.content = self

    async def read(self, n: int) -> bytes:
        take = min(n, self._chunk, len(self._data) - self._pos)
        out = self._data[self._pos : self._pos + take]
        self._pos += take
        return out

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class _FlakyClient:
    """Unencrypted source + upload_part that drops the connection mid-body."""

    def __init__(self, data: bytes, fail_parts: dict[int, int]):
        self.data = data
        self.fail = dict(fail_parts)
        self.uploaded: dict[int, bytes] = {}
        self.get_ranges: list[str | None] = []

    async def get_object(self, bucket, key, range_header=None):
        self.get_ranges.append(range_header)
        body = self.data
        if range_header:
            spec = range_header.replace("bytes=", "")
            start_s, end_s = spec.split("-")
            start = int(start_s)
            end = int(end_s) if end_s else len(body) - 1
            body = body[start : end + 1]
        return {"Body": _Body(body)}

    async def upload_part(self, bucket, key, upload_id, part_number, body):
        collected = bytearray()
        if isinstance(body, (bytes, bytearray)):
            collected += body
        else:
            declared = len(body)
            async for chunk in body:
                collected += chunk
                if self.fail.get(part_number, 0) and len(collected) > declared // 2:
                    break
        if self.fail.get(part_number, 0):
            self.fail[part_number] -= 1
            raise ConnectionError("backend reset mid-part")
        self.uploaded[part_number] = bytes(collected)
        return {"ETag": f'"{hashlib.md5(bytes(collected), usedforsecurity=False).hexdigest()}"'}


class _Mgr:
    def __init__(self, deferred_tail: bytes = b""):
        self.deferred_tail = deferred_tail
        self.part_meta = None
        self.allocated = 0

    async def allocate_internal_parts(self, bucket, key, upload_id, count, client_part_number):
        start = self.allocated + 1
        self.allocated += count
        return start

    async def add_part(self, bucket, key, upload_id, part_meta):
        self.part_meta = part_meta

    async def take_deferred_copy_tail(self, bucket, key, upload_id):
        tail, self.deferred_tail = self.deferred_tail, b""
        return tail


def _handler(mgr) -> MultipartHandlerMixin:
    h = MultipartHandlerMixin.__new__(MultipartHandlerMixin)
    h.multipart_manager = mgr
    return h


async def _run_copy(client, mgr, plaintext_size: int) -> tuple[MultipartUploadState, object]:
    state = MultipartUploadState(dek=crypto.generate_dek(), bucket="b", key="k", upload_id="u")
    resp = await _handler(mgr)._streaming_copy_part_inner(
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
    return state, resp


def _recovered_plaintext(client: _FlakyClient, mgr: _Mgr, dek: bytes) -> bytes:
    parts = sorted(mgr.part_meta.internal_parts, key=lambda m: m.internal_part_number)
    return b"".join(
        crypto.decrypt_framed(client.uploaded[m.internal_part_number], dek, m.plaintext_size)
        for m in parts
    )


@pytest.fixture(autouse=True)
def _fresh_governor():
    concurrency.reset_state()
    concurrency.set_memory_limit(192)
    yield
    concurrency.reset_state()


@pytest.mark.asyncio
async def test_mid_part_failure_retries_and_roundtrips():
    data = bytes(80 * MB)  # 32 + 32 + 16 MB internal parts
    client = _FlakyClient(data, fail_parts={2: 1})
    mgr = _Mgr()

    state, _ = await _run_copy(client, mgr, len(data))

    assert _recovered_plaintext(client, mgr, state.dek) == data
    # Client ETag is the plaintext MD5; a double-hashed retry would corrupt it.
    assert mgr.part_meta.etag == hashlib.md5(data, usedforsecurity=False).hexdigest()
    assert mgr.part_meta.plaintext_size == len(data)
    # The retry reopened the source at the failed part's offset (32MB).
    assert f"bytes={32 * MB}-" in client.get_ranges


@pytest.mark.asyncio
async def test_retry_exhaustion_propagates_failure():
    data = bytes(40 * MB)
    client = _FlakyClient(data, fail_parts={1: 10})
    mgr = _Mgr()

    with pytest.raises(ConnectionError):
        await _run_copy(client, mgr, len(data))
    assert mgr.part_meta is None
    assert concurrency.get_active_memory() == 0


@pytest.mark.asyncio
async def test_retry_with_deferred_tail_prefix():
    """A failure in the part containing the deferred tail must rebuild the
    stream as tail-remainder + reopened source."""
    tail = bytes([7]) * (2 * MB)
    data = bytes([9]) * (40 * MB)
    client = _FlakyClient(data, fail_parts={1: 1})
    mgr = _Mgr(deferred_tail=tail)

    state, _ = await _run_copy(client, mgr, len(data))

    assert _recovered_plaintext(client, mgr, state.dek) == tail + data
    expected_md5 = hashlib.md5(tail + data, usedforsecurity=False).hexdigest()
    assert mgr.part_meta.etag == expected_md5
    assert mgr.part_meta.plaintext_size == len(tail) + len(data)


@pytest.mark.asyncio
async def test_short_source_fails_loudly_not_silently_truncated():
    """Source metadata promised more bytes than the stream delivers: the copy
    must error (client retries) instead of storing an object with wrong sizes."""
    data = bytes(20 * MB)
    client = _FlakyClient(data, fail_parts={})
    mgr = _Mgr()

    with pytest.raises(Exception, match="source ended"):
        await _run_copy(client, mgr, 40 * MB)
    assert mgr.part_meta is None


@pytest.mark.asyncio
async def test_no_failure_means_single_pass_no_reopen():
    data = bytes(64 * MB)
    client = _FlakyClient(data, fail_parts={})
    mgr = _Mgr()

    state, _ = await _run_copy(client, mgr, len(data))
    assert _recovered_plaintext(client, mgr, state.dek) == data
    assert client.get_ranges == [None]


@pytest.mark.asyncio
async def test_concurrent_retrying_copies_release_reservations():
    """Governor must return to zero even when copies fail and retry."""
    data = bytes(40 * MB)

    async def one(fail: dict[int, int]):
        client = _FlakyClient(data, fail_parts=fail)
        mgr = _Mgr()
        await _run_copy(client, mgr, len(data))

    await asyncio.gather(one({1: 1}), one({2: 1}), one({}))
    assert concurrency.get_active_memory() == 0
