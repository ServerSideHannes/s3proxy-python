"""Transient source-read failures must be retried, not fail the whole copy.

Prod incident 2026-07-16: the backend (Hetzner) drops long-lived connections
mid-body, so a single frame GET died with ClientPayloadError ("received
8331264 of 8388636 bytes") after botocore's retry window (which only covers
the request up to response headers) had passed. One truncated read failed the
entire UploadPartCopy part, and through rclone's job retries, whole Scylla
backup runs. The same backend also fails UploadPartCopy after streaming a 200
("InternalError: The server did not respond in time" embedded in the body).
"""

import hashlib

import aiohttp
import pytest
from botocore.exceptions import ClientError, ReadTimeoutError

from s3proxy import crypto
from s3proxy.handlers import base as base_handler
from s3proxy.handlers.base import is_retryable_source_error, read_source_bytes
from s3proxy.handlers.multipart import MultipartHandlerMixin
from s3proxy.state import (
    InternalPartMetadata,
    MultipartMetadata,
    PartMetadata,
    save_multipart_metadata,
)
from tests.unit.test_upload_part_copy_passthrough import (
    BUCKET,
    _copy_handler,
    _copy_part_request,
    _patch_client,
    _read,
)


@pytest.fixture(autouse=True)
def _no_backoff(monkeypatch):
    monkeypatch.setattr(base_handler, "SOURCE_READ_BACKOFF_SEC", 0.0)


def _payload_error():
    return aiohttp.ClientPayloadError(
        "Response payload is not completed: <ContentLengthError: 400, "
        "message='Not enough data to satisfy content length header'>"
    )


def _client_error(code, operation="GetObject"):
    return ClientError({"Error": {"Code": code, "Message": code}}, operation)


class _Body:
    def __init__(self, data: bytes):
        self._data = data

    async def read(self):
        return self._data

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return None


class _TruncatingBody:
    """read() dies mid-body, the exact prod failure shape."""

    async def read(self):
        raise _payload_error()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return None


class _FlakyGetClient:
    def __init__(self, data: bytes, body_failures: int = 0, call_exc: Exception | None = None):
        self.data = data
        self.body_failures = body_failures
        self.call_exc = call_exc
        self.calls: list[str | None] = []

    async def get_object(self, bucket, key, range_header=None):
        self.calls.append(range_header)
        if self.call_exc is not None:
            raise self.call_exc
        if len(self.calls) <= self.body_failures:
            return {"Body": _TruncatingBody()}
        return {"Body": _Body(self.data)}


# --- read_source_bytes ------------------------------------------------------


@pytest.mark.asyncio
async def test_read_source_bytes_retries_truncated_body():
    client = _FlakyGetClient(b"frame-bytes", body_failures=2)
    assert await read_source_bytes(client, "b", "k", "bytes=0-10") == b"frame-bytes"
    assert client.calls == ["bytes=0-10"] * 3


@pytest.mark.asyncio
async def test_read_source_bytes_gives_up_after_max_attempts():
    client = _FlakyGetClient(b"", body_failures=10_000)
    with pytest.raises(aiohttp.ClientPayloadError):
        await read_source_bytes(client, "b", "k")
    assert len(client.calls) == base_handler.SOURCE_READ_ATTEMPTS


@pytest.mark.asyncio
async def test_read_source_bytes_does_not_retry_non_retryable():
    client = _FlakyGetClient(b"", call_exc=_client_error("NoSuchKey"))
    with pytest.raises(ClientError):
        await read_source_bytes(client, "b", "k")
    assert len(client.calls) == 1


# --- error classification ---------------------------------------------------


def test_retryable_error_classification():
    assert is_retryable_source_error(_payload_error())
    assert is_retryable_source_error(ReadTimeoutError(endpoint_url="http://s3"))
    assert is_retryable_source_error(TimeoutError())
    assert is_retryable_source_error(_client_error("InternalError"))
    assert is_retryable_source_error(_client_error("SlowDown"))
    assert not is_retryable_source_error(_client_error("NoSuchKey"))
    assert not is_retryable_source_error(_client_error("AccessDenied"))
    assert not is_retryable_source_error(ValueError("boom"))


# --- raw copy-source stream resume ------------------------------------------


class _Stream:
    def __init__(self, data: bytes, die_after: int | None = None):
        self._buf = bytearray(data)
        self._die_after = die_after
        self._served = 0

    @property
    def content(self):
        return self

    async def read(self, n: int = -1):
        if self._die_after is not None and self._served >= self._die_after:
            raise _payload_error()
        limit = len(self._buf) if n == -1 else n
        if self._die_after is not None:
            limit = min(limit, self._die_after - self._served)
        chunk = bytes(self._buf[:limit])
        del self._buf[:limit]
        self._served += len(chunk)
        if not chunk and self._die_after is not None:
            raise _payload_error()
        return chunk

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return None


class _StreamClient:
    """Serves ranged GETs over `data`; the first GET truncates after `die_after` bytes."""

    def __init__(self, data: bytes, die_after: int | None = None):
        self.data = data
        self.die_after = die_after
        self.calls: list[str | None] = []

    async def get_object(self, bucket, key, range_header=None):
        self.calls.append(range_header)
        start, end = 0, len(self.data) - 1
        if range_header:
            spec = range_header.replace("bytes=", "")
            s, e = spec.split("-")
            start = int(s)
            end = int(e) if e else len(self.data) - 1
        body = self.data[start : end + 1]
        die_after = self.die_after if len(self.calls) == 1 else None
        return {"Body": _Stream(body, die_after)}


def _mixin():
    return MultipartHandlerMixin.__new__(MultipartHandlerMixin)


@pytest.mark.asyncio
async def test_stream_resume_whole_object_resumes_at_offset():
    data = bytes(range(256)) * 4
    client = _StreamClient(data, die_after=100)
    chunks = [c async for c in _mixin()._stream_raw_source_with_resume(client, "b", "k", None)]
    assert b"".join(chunks) == data
    assert client.calls == [None, "bytes=100-"]


@pytest.mark.asyncio
async def test_stream_resume_preserves_original_range():
    data = bytes(range(256))
    client = _StreamClient(data, die_after=15)
    chunks = [
        c async for c in _mixin()._stream_raw_source_with_resume(client, "b", "k", "bytes=10-59")
    ]
    assert b"".join(chunks) == data[10:60]
    assert client.calls == ["bytes=10-59", "bytes=25-59"]


@pytest.mark.asyncio
async def test_stream_resume_does_not_mask_non_retryable_errors():
    class _Boom(_StreamClient):
        async def get_object(self, bucket, key, range_header=None):
            self.calls.append(range_header)
            raise _client_error("AccessDenied")

    client = _Boom(b"")
    with pytest.raises(ClientError):
        async for _ in _mixin()._stream_raw_source_with_resume(client, "b", "k", None):
            pass
    assert len(client.calls) == 1


# --- end-to-end: passthrough copy survives transient backend failures --------


class _FlakyS3:
    """Wraps MockS3Client: first upload_part_copy 500s, first ranged GET truncates."""

    def __init__(self, inner, upc_failures=1, get_failures=1):
        self._inner = inner
        self._upc_failures = upc_failures
        self._get_failures = get_failures
        self.upc_attempts = 0

    def __getattr__(self, name):
        return getattr(self._inner, name)

    async def upload_part_copy(self, *args, **kwargs):
        self.upc_attempts += 1
        if self._upc_failures > 0:
            self._upc_failures -= 1
            raise ClientError(
                {
                    "Error": {
                        "Code": "InternalError",
                        "Message": "The server did not respond in time.",
                    }
                },
                "UploadPartCopy",
            )
        return await self._inner.upload_part_copy(*args, **kwargs)

    async def get_object(self, bucket, key, range_header=None):
        if range_header is not None and self._get_failures > 0:
            self._get_failures -= 1
            raise _payload_error()
        return await self._inner.get_object(bucket, key, range_header=range_header)


@pytest.mark.asyncio
async def test_passthrough_copy_survives_transient_backend_failures(
    mock_s3, settings, manager, credentials, monkeypatch
):
    """One 500'd segment copy + one truncated frame GET must not fail the part."""
    monkeypatch.setattr(crypto, "COPY_INTERNAL_PART_SIZE", crypto.MAX_BUFFER_SIZE)
    handler = _copy_handler(settings, manager)
    flaky = _FlakyS3(mock_s3, upc_failures=1, get_failures=1)
    _patch_client(handler, flaky)
    await mock_s3.create_bucket(BUCKET)

    kid, kek = settings.keyring.key_for(credentials.access_key)
    src_dek = crypto.generate_dek()
    src_plaintext = b"S" * (crypto.MAX_BUFFER_SIZE * 4)

    ciphertext_blob = bytearray()
    internal_parts_meta = []
    for i, start in enumerate(range(0, len(src_plaintext), crypto.MAX_BUFFER_SIZE), 1):
        chunk = src_plaintext[start : start + crypto.MAX_BUFFER_SIZE]
        ct = crypto.encrypt_frame(chunk, src_dek, "src-upload", i, 0)
        internal_parts_meta.append(
            InternalPartMetadata(
                internal_part_number=i,
                plaintext_size=len(chunk),
                ciphertext_size=len(ct),
                etag=hashlib.md5(ct, usedforsecurity=False).hexdigest(),
            )
        )
        ciphertext_blob.extend(ct)

    src_meta = MultipartMetadata(
        version=2,
        part_count=1,
        total_plaintext_size=len(src_plaintext),
        parts=[
            PartMetadata(
                part_number=1,
                plaintext_size=len(src_plaintext),
                ciphertext_size=len(ciphertext_blob),
                etag="ignored",
                md5=hashlib.md5(src_plaintext, usedforsecurity=False).hexdigest(),
                internal_parts=internal_parts_meta,
            )
        ],
        wrapped_dek=crypto.wrap_key(src_dek, kek),
        kid=kid,
    )
    await mock_s3.put_object(BUCKET, "sst/big.db", bytes(ciphertext_blob))
    await save_multipart_metadata(mock_s3, BUCKET, "sst/big.db", src_meta)

    resp_create = await mock_s3.create_multipart_upload(BUCKET, "sst/big.db.snap")
    upload_id = resp_create["UploadId"]
    await manager.create_upload(BUCKET, "sst/big.db.snap", upload_id, crypto.generate_dek(), kid)

    resp = await handler.handle_upload_part_copy(
        _copy_part_request(
            f"/{BUCKET}/sst/big.db.snap",
            f"/{BUCKET}/sst/big.db",
            upload_id,
        ),
        credentials,
    )
    body = await _read(resp)
    assert b"<Error>" not in body
    assert flaky.upc_attempts >= 2  # first attempt failed, retry landed

    updated = await manager.get_upload(BUCKET, "sst/big.db.snap", upload_id)
    part = updated.parts[1]
    assert part.plaintext_size == len(src_plaintext)

    recovered = bytearray()
    for ip in sorted(part.internal_parts, key=lambda x: x.internal_part_number):
        ct = mock_s3.multipart_uploads[upload_id]["Parts"][ip.internal_part_number]["Body"]
        recovered.extend(crypto.decrypt_framed(ct, src_dek, ip.plaintext_size))
    assert bytes(recovered) == src_plaintext
