"""UploadPartCopy server-side passthrough for encrypted sources (Scylla dedup path)."""

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


def _copy_part_request(dest_path, copy_source, upload_id, part_number=1, copy_source_range=None):
    req = MagicMock()
    req.url.path = dest_path
    req.url.query = f"uploadId={upload_id}&partNumber={part_number}"
    req.headers = {
        "x-amz-copy-source": copy_source,
    }
    if copy_source_range is not None:
        req.headers["x-amz-copy-source-range"] = copy_source_range
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
    resp = await handler.handle_upload_part_copy(
        _copy_part_request(
            f"/{BUCKET}/sst/big.db.snap",
            f"/{BUCKET}/sst/big.db",
            upload_id,
        ),
        credentials,
    )
    await _read(resp)
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

    copy_resp = await handler.handle_upload_part_copy(
        _copy_part_request(f"/{BUCKET}/sst/dest.db", f"/{BUCKET}/sst/source.db", upload_id),
        credentials,
    )
    await _read(copy_resp)

    part_state = await handler.multipart_manager.get_upload(BUCKET, "sst/dest.db", upload_id)
    complete_body = (
        f"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>"
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
    await _read(await handler.handle_upload_part_copy(req, credentials))
    during = mock_s3.call_history[mark:]

    assert not any(c[0] == "upload_part_copy" for c in during)
    assert any(c[0] == "upload_part" for c in during)


@pytest.mark.asyncio
async def test_normalize_copy_source_range_treats_full_object_as_whole(
    settings, manager, credentials
):
    handler = _copy_handler(settings, manager)
    total = crypto.STREAMING_THRESHOLD + crypto.MAX_BUFFER_SIZE
    assert handler._normalize_copy_source_range(None, total, {}, None) is None
    assert handler._normalize_copy_source_range(f"bytes=0-{total - 1}", total, {}, None) is None
    assert handler._normalize_copy_source_range(f"bytes=0-{total + 9999}", total, {}, None) is None
    partial = handler._normalize_copy_source_range(
        f"bytes=0-{crypto.MAX_BUFFER_SIZE - 1}", total, {}, None
    )
    assert partial == f"bytes=0-{crypto.MAX_BUFFER_SIZE - 1}"


def test_segments_for_plaintext_range_selects_aligned_prefix(settings, manager):
    from s3proxy.handlers.multipart.copy import _CiphertextSegment

    handler = _copy_handler(settings, manager)
    chunk = crypto.MAX_BUFFER_SIZE
    ct = chunk + 16
    segments = [_CiphertextSegment(chunk, ct, i * ct) for i in range(5)]
    # First two internal parts (64MB).
    selected = handler._segments_for_plaintext_range(segments, 0, chunk * 2 - 1)
    assert selected is not None
    assert len(selected) == 2
    # Partial segment at the end is not passthrough-safe.
    assert handler._segments_for_plaintext_range(segments, 0, chunk) is None


def test_split_plaintext_range_allows_hybrid_tail(settings, manager):
    from s3proxy.handlers.multipart.copy import _CiphertextSegment

    handler = _copy_handler(settings, manager)
    chunk = 1_048_576  # 1MB internal frames (prod uses ~8.33MB; same logic)
    ct = chunk + 64
    segments = [_CiphertextSegment(chunk, ct, i * ct) for i in range(10)]
    # 9.5MB range ends mid 10th segment → 9 passthrough + tail.
    split = handler._split_plaintext_range_on_segments(segments, 0, chunk * 10 - 1 - chunk // 2)
    assert split is not None
    assert len(split.passthrough_segments) == 9
    assert split.streaming_tail == (chunk * 9, chunk * 10 - 1 - chunk // 2)


def _build_scylla_prod_shape_source(
    kid,
    kek,
    *,
    num_internal_parts: int,
    metadata_inflate_ratio: float,
    chunk_size: int | None = None,
    scylla_range_end: int | None = None,
):
    """Prod shape: 8.33MB internal frames, metadata total > Scylla manifest range."""
    if chunk_size is None:
        # 50MB client parts / 6 internal frames (prod INTERNAL_PART_UPLOADED ~8.33MB).
        chunk_size = (50 * 1024 * 1024) // 6
    src_dek = crypto.generate_dek()
    src_plaintext = b"M" * (chunk_size * num_internal_parts)

    ciphertext_blob = bytearray()
    internal_parts_meta = []
    for i, start in enumerate(range(0, len(src_plaintext), chunk_size), 1):
        chunk = src_plaintext[start : start + chunk_size]
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

    inflated_total = int(len(src_plaintext) * metadata_inflate_ratio)
    src_meta = MultipartMetadata(
        version=2,
        part_count=1,
        total_plaintext_size=inflated_total,
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
    return (
        src_dek,
        src_plaintext,
        bytes(ciphertext_blob),
        src_meta,
        inflated_total,
        scylla_range_end if scylla_range_end is not None else len(src_plaintext) - 1,
    )


@pytest.mark.asyncio
async def test_scylla_prod_shape_range_smaller_than_metadata_uses_passthrough(
    mock_s3, settings, manager, credentials, monkeypatch
):
    """Regression: prod range ends mid internal frame → hybrid passthrough + tail."""
    monkeypatch.setattr(crypto, "COPY_INTERNAL_PART_SIZE", crypto.MAX_BUFFER_SIZE)
    handler = _copy_handler(settings, manager)
    _patch_client(handler, mock_s3)
    await mock_s3.create_bucket(BUCKET)

    kid, kek = settings.keyring.key_for(credentials.access_key)
    prod_range_end = 4_999_341_931
    chunk_size = (50 * 1024 * 1024) // 6
    num_parts = (prod_range_end // chunk_size) + 2
    src_dek, _, ciphertext_blob, src_meta, inflated_total, range_end = (
        _build_scylla_prod_shape_source(
            kid,
            kek,
            num_internal_parts=num_parts,
            metadata_inflate_ratio=1.27,
            chunk_size=chunk_size,
            scylla_range_end=prod_range_end,
        )
    )
    assert inflated_total > prod_range_end
    assert prod_range_end + 1 > crypto.STREAMING_THRESHOLD

    await mock_s3.put_object(BUCKET, "sst/big-Data.db", ciphertext_blob)
    await save_multipart_metadata(mock_s3, BUCKET, "sst/big-Data.db", src_meta)

    resp_create = await mock_s3.create_multipart_upload(BUCKET, "sst/big-Data.db.sm_manifest")
    upload_id = resp_create["UploadId"]
    await manager.create_upload(BUCKET, "sst/big-Data.db.sm_manifest", upload_id, src_dek, kid)

    scylla_range = f"bytes=0-{range_end}"
    mark = len(mock_s3.call_history)
    resp = await handler.handle_upload_part_copy(
        _copy_part_request(
            f"/{BUCKET}/sst/big-Data.db.sm_manifest",
            f"/{BUCKET}/sst/big-Data.db",
            upload_id,
            copy_source_range=scylla_range,
        ),
        credentials,
    )
    await _read(resp)
    during = mock_s3.call_history[mark:]

    copy_ops = [c for c in during if c[0] == "upload_part_copy"]
    tail_ops = [c for c in during if c[0] == "upload_part"]
    assert len(copy_ops) >= 500
    assert len(tail_ops) == 1

    updated = await manager.get_upload(BUCKET, "sst/big-Data.db.sm_manifest", upload_id)
    part = updated.parts[1]
    assert part.plaintext_size == prod_range_end + 1
    assert len(part.internal_parts) == len(copy_ops) + len(tail_ops)


def test_prod_shape_passthrough_eligibility_and_route(settings, manager, credentials):
    """Unit check for prod mismatch: metadata total > range, mid-frame tail still eligible."""
    handler = _copy_handler(settings, manager)
    kid, kek = settings.keyring.key_for(credentials.access_key)
    prod_range_end = 4_999_341_931
    chunk_size = (50 * 1024 * 1024) // 6
    num_parts = (prod_range_end // chunk_size) + 2
    _, _, _, src_meta, inflated_total, range_end = _build_scylla_prod_shape_source(
        kid,
        kek,
        num_internal_parts=num_parts,
        metadata_inflate_ratio=1.27,
        chunk_size=chunk_size,
        scylla_range_end=prod_range_end,
    )
    scylla_range = f"bytes=0-{range_end}"
    block = handler._passthrough_block_reason(
        scylla_range,
        scylla_range,
        "wrapped-dek",
        src_meta,
        {},
        credentials,
        prod_range_end + 1,
        {},
    )
    assert block is None
    assert inflated_total > prod_range_end
    assert (
        handler._normalize_copy_source_range(scylla_range, inflated_total, {}, "wrapped-dek")
        == scylla_range
    )


@pytest.mark.asyncio
async def test_small_partial_range_blocked_from_passthrough(settings, manager, credentials):
    handler = _copy_handler(settings, manager)
    kid, kek = settings.keyring.key_for(credentials.access_key)
    chunk = crypto.MAX_BUFFER_SIZE
    src_meta = MultipartMetadata(
        version=2,
        part_count=1,
        total_plaintext_size=chunk * 3,
        parts=[
            PartMetadata(
                part_number=1,
                plaintext_size=chunk * 3,
                ciphertext_size=chunk * 3,
                etag="x",
                internal_parts=[
                    InternalPartMetadata(1, chunk, chunk, "e1"),
                    InternalPartMetadata(2, chunk, chunk, "e2"),
                    InternalPartMetadata(3, chunk, chunk, "e3"),
                ],
            )
        ],
        wrapped_dek=crypto.wrap_key(crypto.generate_dek(), kek),
        kid=kid,
    )
    partial_range = f"bytes=0-{chunk - 1}"
    reason = handler._passthrough_block_reason(
        partial_range,
        partial_range,
        "wrapped",
        src_meta,
        {},
        credentials,
        chunk,
        {},
    )
    assert reason == "small_ranged_copy"


@pytest.mark.asyncio
async def test_normalize_does_not_clear_scylla_shorter_than_metadata_range(settings, manager):
    """Shorter-than-metadata range must stay set for range-aware passthrough."""
    handler = _copy_handler(settings, manager)
    metadata_total = 6_000_000_000
    scylla_end = 4_999_741_439  # ~4767MB, prod-shaped
    raw = f"bytes=0-{scylla_end}"
    assert handler._normalize_copy_source_range(raw, metadata_total, {}, "dek") == raw


def _build_large_multipart_source(mock_s3, kid, kek, *, num_internal_parts: int):
    """Encrypted multipart source above STREAMING_THRESHOLD (Scylla manifest shape)."""
    src_dek = crypto.generate_dek()
    chunk_size = crypto.MAX_BUFFER_SIZE
    src_plaintext = b"M" * (chunk_size * num_internal_parts)

    ciphertext_blob = bytearray()
    internal_parts_meta = []
    for i, start in enumerate(range(0, len(src_plaintext), chunk_size), 1):
        chunk = src_plaintext[start : start + chunk_size]
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
    return src_dek, src_plaintext, bytes(ciphertext_blob), src_meta


@pytest.mark.asyncio
async def test_scylla_manifest_full_range_uses_passthrough_not_streaming(
    mock_s3, settings, manager, credentials, monkeypatch
):
    """Full-object CopySourceRange must not fall into UPLOAD_PART_COPY_STREAMING."""
    monkeypatch.setattr(crypto, "COPY_INTERNAL_PART_SIZE", crypto.MAX_BUFFER_SIZE)
    handler = _copy_handler(settings, manager)
    _patch_client(handler, mock_s3)
    await mock_s3.create_bucket(BUCKET)

    kid, kek = settings.keyring.key_for(credentials.access_key)
    num_parts = (crypto.STREAMING_THRESHOLD // crypto.MAX_BUFFER_SIZE) + 2
    src_dek, src_plaintext, ciphertext_blob, src_meta = _build_large_multipart_source(
        mock_s3, kid, kek, num_internal_parts=num_parts
    )
    assert len(src_plaintext) > crypto.STREAMING_THRESHOLD

    await mock_s3.put_object(BUCKET, "sst/big-Data.db", ciphertext_blob)
    await save_multipart_metadata(mock_s3, BUCKET, "sst/big-Data.db", src_meta)

    resp_create = await mock_s3.create_multipart_upload(BUCKET, "sst/big-Data.db.sm_manifest")
    upload_id = resp_create["UploadId"]
    await manager.create_upload(BUCKET, "sst/big-Data.db.sm_manifest", upload_id, src_dek, kid)

    full_range = f"bytes=0-{len(src_plaintext) - 1}"
    mark = len(mock_s3.call_history)
    resp = await handler.handle_upload_part_copy(
        _copy_part_request(
            f"/{BUCKET}/sst/big-Data.db.sm_manifest",
            f"/{BUCKET}/sst/big-Data.db",
            upload_id,
            copy_source_range=full_range,
        ),
        credentials,
    )
    await _read(resp)
    during = mock_s3.call_history[mark:]

    assert any(c[0] == "upload_part_copy" for c in during)
    assert not any(c[0] == "upload_part" for c in during)

    updated = await manager.get_upload(BUCKET, "sst/big-Data.db.sm_manifest", upload_id)
    assert updated.dek == src_dek
    part = updated.parts[1]
    assert part.plaintext_size == len(src_plaintext)
    assert len(part.internal_parts) == num_parts


@pytest.mark.asyncio
async def test_large_passthrough_not_blocked_by_pipeline_semaphore(
    mock_s3, settings, manager, credentials, monkeypatch
):
    """Passthrough must not queue behind the streaming copy pipeline semaphore."""
    import asyncio

    from s3proxy.handlers.multipart.copy import reset_copy_pipeline_semaphore

    monkeypatch.setattr(crypto, "COPY_INTERNAL_PART_SIZE", crypto.MAX_BUFFER_SIZE)
    reset_copy_pipeline_semaphore(1)

    handler = _copy_handler(settings, manager)
    _patch_client(handler, mock_s3)
    await mock_s3.create_bucket(BUCKET)

    kid, kek = settings.keyring.key_for(credentials.access_key)
    num_parts = (crypto.STREAMING_THRESHOLD // crypto.MAX_BUFFER_SIZE) + 2
    src_dek, src_plaintext, ciphertext_blob, src_meta = _build_large_multipart_source(
        mock_s3, kid, kek, num_internal_parts=num_parts
    )

    await mock_s3.put_object(BUCKET, "sst/big-Data.db", ciphertext_blob)
    await save_multipart_metadata(mock_s3, BUCKET, "sst/big-Data.db", src_meta)

    resp_create = await mock_s3.create_multipart_upload(BUCKET, "sst/big-Data.db.sm_manifest")
    upload_id = resp_create["UploadId"]
    await manager.create_upload(BUCKET, "sst/big-Data.db.sm_manifest", upload_id, src_dek, kid)

    gate = asyncio.Event()

    async def blocked_streaming(*args, **kwargs):
        gate.set()
        await asyncio.Event().wait()

    handler._streaming_copy_part = blocked_streaming  # type: ignore[method-assign]

    async def run_passthrough():
        resp = await handler.handle_upload_part_copy(
            _copy_part_request(
                f"/{BUCKET}/sst/big-Data.db.sm_manifest",
                f"/{BUCKET}/sst/big-Data.db",
                upload_id,
                copy_source_range=f"bytes=0-{len(src_plaintext) - 1}",
            ),
            credentials,
        )
        return await _read(resp)

    passthrough_task = asyncio.create_task(run_passthrough())

    for _ in range(200):
        if passthrough_task.done():
            break
        await asyncio.sleep(0.01)

    assert passthrough_task.done()
    assert not gate.is_set()
    await passthrough_task


def _extract_upload_id(xml_body: bytes) -> str:
    import xml.etree.ElementTree as ET

    root = ET.fromstring(xml_body.decode())
    for elem in root.iter():
        if elem.tag.endswith("UploadId") and elem.text:
            return elem.text
    raise AssertionError("UploadId not found in create response")
