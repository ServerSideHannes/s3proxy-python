"""Server-side passthrough for encrypted COPY (Scylla Manager dedup path).

A plain COPY (MetadataDirective=COPY) of an encrypted object must be a native
server-side CopyObject, not a download + re-encrypt + re-upload. These tests
drive the real handler against the in-memory MockS3Client and assert:

- the source data is never downloaded and the destination is never re-uploaded
  (i.e. no bulk-byte amplification), yet
- the destination round-trips back to the original plaintext, for both single
  and multipart objects (multipart also copies the frame-map sidecar), and
- REPLACE still takes the decrypt/re-encrypt path.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from s3proxy.handlers import S3ProxyHandler
from s3proxy.state import MultipartStateManager
from s3proxy.state.metadata import _internal_meta_key

BUCKET = "backups"


def _handler(settings, mock_s3, credentials):
    handler = S3ProxyHandler(settings, settings.credentials_store, MultipartStateManager())
    mock_s3.credentials = credentials
    handler._client = MagicMock(return_value=mock_s3)
    return handler


def _put_request(path, body):
    req = MagicMock()
    req.url.path = path
    req.headers = {"content-length": str(len(body)), "content-type": "application/octet-stream"}
    req.body = AsyncMock(return_value=body)
    return req


def _stream_put_request(path, body, chunk=64 * 1024):
    """Force the streaming/multipart PUT path via UNSIGNED-PAYLOAD."""
    req = MagicMock()
    req.url.path = path
    req.headers = {
        "content-type": "application/octet-stream",
        "x-amz-content-sha256": "UNSIGNED-PAYLOAD",
    }

    async def _stream():
        for i in range(0, len(body), chunk):
            yield body[i : i + chunk]

    req.stream = _stream
    return req


def _copy_request(dest_path, copy_source, directive="COPY"):
    req = MagicMock()
    req.url.path = dest_path
    req.headers = {"x-amz-copy-source": copy_source, "x-amz-metadata-directive": directive}
    return req


def _get_request(path):
    req = MagicMock()
    req.url.path = path
    req.headers = {}
    return req


async def _read(response) -> bytes:
    if hasattr(response, "body_iterator"):
        return b"".join([c async for c in response.body_iterator])
    return response.body


def _keys_touched(history, op):
    return [c[1].get("key") for c in history if c[0] == op]


@pytest.mark.asyncio
async def test_single_object_copy_is_server_side_passthrough(settings, mock_s3, credentials):
    handler = _handler(settings, mock_s3, credentials)
    await mock_s3.create_bucket(BUCKET)

    body = b"scylla sstable component bytes" * 100
    await handler.handle_put_object(_put_request(f"/{BUCKET}/sst/data.db", body), credentials)

    mark = len(mock_s3.call_history)
    await handler.handle_copy_object(
        _copy_request(f"/{BUCKET}/sst/data.db.snap", f"/{BUCKET}/sst/data.db"), credentials
    )
    during = mock_s3.call_history[mark:]

    # Native copy happened; no download of source data, no re-upload of dest.
    assert "sst/data.db.snap" in _keys_touched(during, "copy_object")
    assert "sst/data.db" not in _keys_touched(during, "get_object")
    assert "sst/data.db.snap" not in _keys_touched(during, "put_object")

    resp = await handler.handle_get_object(_get_request(f"/{BUCKET}/sst/data.db.snap"), credentials)
    assert await _read(resp) == body


@pytest.mark.asyncio
async def test_multipart_object_copy_copies_sidecar_and_roundtrips(settings, mock_s3, credentials):
    handler = _handler(settings, mock_s3, credentials)
    await mock_s3.create_bucket(BUCKET)

    body = b"m" * (256 * 1024)  # streamed as multiple parts -> real sidecar
    await handler.handle_put_object(_stream_put_request(f"/{BUCKET}/sst/big.db", body), credentials)
    # sanity: a multipart sidecar exists for the source
    assert mock_s3._key(BUCKET, _internal_meta_key("sst/big.db")) in mock_s3.objects

    mark = len(mock_s3.call_history)
    await handler.handle_copy_object(
        _copy_request(f"/{BUCKET}/sst/big.db.snap", f"/{BUCKET}/sst/big.db"), credentials
    )
    during = mock_s3.call_history[mark:]

    copied = _keys_touched(during, "copy_object")
    assert "sst/big.db.snap" in copied  # assembled ciphertext, server-side
    assert _internal_meta_key("sst/big.db.snap") in copied  # sidecar, server-side
    assert "sst/big.db" not in _keys_touched(during, "get_object")  # no bulk download
    assert "sst/big.db.snap" not in _keys_touched(during, "put_object")  # no re-upload

    resp = await handler.handle_get_object(_get_request(f"/{BUCKET}/sst/big.db.snap"), credentials)
    assert await _read(resp) == body


@pytest.mark.asyncio
async def test_replace_directive_still_reencrypts(settings, mock_s3, credentials):
    handler = _handler(settings, mock_s3, credentials)
    await mock_s3.create_bucket(BUCKET)

    body = b"needs a real re-encrypt because metadata changes"
    await handler.handle_put_object(_put_request(f"/{BUCKET}/a.db", body), credentials)

    mark = len(mock_s3.call_history)
    await handler.handle_copy_object(
        _copy_request(f"/{BUCKET}/b.db", f"/{BUCKET}/a.db", directive="REPLACE"), credentials
    )
    during = mock_s3.call_history[mark:]

    # REPLACE must NOT use server-side copy; it re-reads and re-writes.
    assert "b.db" not in _keys_touched(during, "copy_object")
    assert "a.db" in _keys_touched(during, "get_object")
    assert "b.db" in _keys_touched(during, "put_object")

    resp = await handler.handle_get_object(_get_request(f"/{BUCKET}/b.db"), credentials)
    assert await _read(resp) == body
