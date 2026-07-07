"""UploadPartCopy server-side passthrough for encrypted sources (Scylla dedup path)."""

import base64
import contextlib
import hashlib
from unittest.mock import AsyncMock, MagicMock

import pytest

from s3proxy import crypto
from s3proxy.handlers import S3ProxyHandler
from s3proxy.handlers.multipart import MultipartHandlerMixin
from s3proxy.state import (
    InternalPartMetadata,
    MultipartMetadata,
    MultipartStateManager,
    PartMetadata,
    load_multipart_metadata,
    save_multipart_metadata,
)

BUCKET = "backups"


def _handler(settings, mock_s3, credentials):
    handler = S3ProxyHandler(settings, settings.credentials_store, MultipartStateManager())
    mock_s3.credentials = credentials
    handler._client = MagicMock(return_value=mock_s3)
    return handler


def _copy_handler(settings, manager):
    return MultipartHandlerMixin(settings, {}, manager)


def _patch_client(handler, mock_s3):
    @contextlib.asynccontextmanager
    async def _fake_client(creds):
        yield mock_s3

    handler._client = _fake_client


def _copy_part_request(dest_path, copy_source, upload_id, part_number=1):
    req = MagicMock()
    req.url.path = dest_path
    req.url.query = f"uploadId={upload_id}&partNumber={part_number}"
    req.headers = {
        "x-amz-copy-source": copy_source,
    }
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
async def test_multipart_encrypted_upload_part_copy_is_server_side_passthrough(
    mock_s3, settings, manager, credentials, monkeypatch
):
    """Large multipart source → native upload_part_copy per internal part, no re-upload."""
    monkeypatch.setattr(crypto, "COPY_INTERNAL_PART_SIZE", crypto.MAX_BUFFER_SIZE)
    handler = _copy_handler(settings, manager)
    _patch_client(handler, mock_s3)
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
    dst_dek = crypto.generate_dek()
    await manager.create_upload(BUCKET, "sst/big.db.snap", upload_id, dst_dek, kid)

    mark = len(mock_s3.call_history)
    await handler.handle_upload_part_copy(
        _copy_part_request(
            f"/{BUCKET}/sst/big.db.snap",
            f"/{BUCKET}/sst/big.db",
            upload_id,
        ),
        credentials,
    )
    during = mock_s3.call_history[mark:]

    assert any(c[0] == "upload_part_copy" for c in during)
    assert "sst/big.db.snap" not in _keys_touched(during, "upload_part")
    assert "sst/big.db.snap" not in _keys_touched(during, "put_object")

    updated = await manager.get_upload(BUCKET, "sst/big.db.snap", upload_id)
    assert updated.dek == src_dek
    part = updated.parts[1]
    assert part.plaintext_size == len(src_plaintext)
    assert len(part.internal_parts) == len(internal_parts_meta)

    recovered = bytearray()
    for ip in sorted(part.internal_parts, key=lambda x: x.internal_part_number):
        ct = mock_s3.multipart_uploads[upload_id]["Parts"][ip.internal_part_number]["Body"]
        recovered.extend(crypto.decrypt_framed(ct, src_dek, ip.plaintext_size))
    assert bytes(recovered) == src_plaintext


@pytest.mark.asyncio
async def test_upload_part_copy_passthrough_roundtrips_via_get(
    mock_s3, settings, credentials, monkeypatch
):
    """End-to-end: passthrough copy → complete → GET returns original plaintext."""
    monkeypatch.setattr(crypto, "COPY_INTERNAL_PART_SIZE", crypto.MAX_BUFFER_SIZE)
    handler = _handler(settings, mock_s3, credentials)
    await mock_s3.create_bucket(BUCKET)

    kid, kek = settings.keyring.key_for(credentials.access_key)
    src_dek = crypto.generate_dek()
    body = b"T" * (crypto.STREAMING_THRESHOLD + crypto.MAX_BUFFER_SIZE)

    ciphertext_blob = bytearray()
    internal_parts_meta = []
    for i, start in enumerate(range(0, len(body), crypto.MAX_BUFFER_SIZE), 1):
        chunk = body[start : start + crypto.MAX_BUFFER_SIZE]
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
        total_plaintext_size=len(body),
        parts=[
            PartMetadata(
                part_number=1,
                plaintext_size=len(body),
                ciphertext_size=len(ciphertext_blob),
                etag="ignored",
                md5=hashlib.md5(body, usedforsecurity=False).hexdigest(),
                internal_parts=internal_parts_meta,
            )
        ],
        wrapped_dek=crypto.wrap_key(src_dek, kek),
        kid=kid,
    )
    await mock_s3.put_object(BUCKET, "sst/source.db", bytes(ciphertext_blob))
    await save_multipart_metadata(mock_s3, BUCKET, "sst/source.db", src_meta)

    create_resp = await handler.handle_create_multipart_upload(
        MagicMock(url=MagicMock(path=f"/{BUCKET}/sst/dest.db"), headers={}),
        credentials,
    )
    upload_id = _extract_upload_id(create_resp.body)

    await handler.handle_upload_part_copy(
        _copy_part_request(f"/{BUCKET}/sst/dest.db", f"/{BUCKET}/sst/source.db", upload_id),
        credentials,
    )

    part_state = await handler.multipart_manager.get_upload(BUCKET, "sst/dest.db", upload_id)
    complete_body = (
        f'<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>'
        f'<ETag>"{part_state.parts[1].etag}"</ETag></Part></CompleteMultipartUpload>'
    ).encode()
    complete_req = MagicMock()
    complete_req.url.path = f"/{BUCKET}/sst/dest.db"
    complete_req.url.query = f"uploadId={upload_id}"
    complete_req.headers = {}
    complete_req.body = AsyncMock(return_value=complete_body)
    await handler.handle_complete_multipart_upload(complete_req, credentials)

    resp = await handler.handle_get_object(_get_request(f"/{BUCKET}/sst/dest.db"), credentials)
    assert await _read(resp) == body


@pytest.mark.asyncio
async def test_range_copy_still_reencrypts(mock_s3, settings, manager, credentials, monkeypatch):
    """CopySourceRange forces the streaming re-encrypt path."""
    monkeypatch.setattr(crypto, "COPY_INTERNAL_PART_SIZE", crypto.MAX_BUFFER_SIZE)
    handler = _copy_handler(settings, manager)
    _patch_client(handler, mock_s3)
    await mock_s3.create_bucket(BUCKET)

    kid, kek = settings.keyring.key_for(credentials.access_key)
    src_dek = crypto.generate_dek()
    src_plaintext = b"U" * (crypto.MAX_BUFFER_SIZE * 3)
    chunk = src_plaintext[: crypto.MAX_BUFFER_SIZE]
    ct = crypto.encrypt_frame(chunk, src_dek, "src", 1, 0)
    src_meta = MultipartMetadata(
        version=2,
        part_count=1,
        total_plaintext_size=len(src_plaintext),
        parts=[
            PartMetadata(
                part_number=1,
                plaintext_size=len(src_plaintext),
                ciphertext_size=len(ct) * 3,
                etag="x",
                internal_parts=[
                    InternalPartMetadata(1, len(chunk), len(ct), "e1"),
                    InternalPartMetadata(2, len(chunk), len(ct), "e2"),
                    InternalPartMetadata(3, len(chunk), len(ct), "e3"),
                ],
            )
        ],
        wrapped_dek=crypto.wrap_key(src_dek, kek),
        kid=kid,
    )
    await mock_s3.put_object(BUCKET, "src", ct * 3)
    await save_multipart_metadata(mock_s3, BUCKET, "src", src_meta)

    resp_create = await mock_s3.create_multipart_upload(BUCKET, "dst")
    upload_id = resp_create["UploadId"]
    await manager.create_upload(BUCKET, "dst", upload_id, crypto.generate_dek(), kid)

    req = _copy_part_request("/backups/dst", "/backups/src", upload_id)
    req.headers["x-amz-copy-source-range"] = f"bytes=0-{crypto.MAX_BUFFER_SIZE - 1}"

    mark = len(mock_s3.call_history)
    await handler.handle_upload_part_copy(req, credentials)
    during = mock_s3.call_history[mark:]

    assert not any(c[0] == "upload_part_copy" for c in during)
    assert any(c[0] == "upload_part" for c in during)


def _extract_upload_id(xml_body: bytes) -> str:
    import xml.etree.ElementTree as ET

    root = ET.fromstring(xml_body.decode())
    for elem in root.iter():
        if elem.tag.endswith("UploadId") and elem.text:
            return elem.text
    raise AssertionError("UploadId not found in create response")
