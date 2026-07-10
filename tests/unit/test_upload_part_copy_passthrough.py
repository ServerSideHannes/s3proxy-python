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


@pytest.mark.slow
@pytest.mark.asyncio
async def test_scylla_prod_shape_range_smaller_than_metadata_uses_passthrough(
    mock_s3, settings, manager, credentials, monkeypatch
):
    """Regression: prod range ends mid internal frame → hybrid passthrough + deferred tail.

    Two-part complete (part 2 consumes tail, no EntityTooSmall) is covered by
    test_two_part_hybrid_defer_tail_completes_fast — the full prod-shape two-part
    path OOMs CI runners (~5GB mock source).
    """
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
    assert len(tail_ops) == 0

    updated = await manager.get_upload(BUCKET, "sst/big-Data.db.sm_manifest", upload_id)
    part = updated.parts[1]
    assert part.plaintext_size == prod_range_end + 1
    assert len(part.internal_parts) == len(copy_ops)
    assert len(updated.deferred_copy_tail) > 0
    assert len(updated.deferred_copy_tail) < crypto.MIN_PART_SIZE


def test_should_defer_hybrid_tail_when_more_client_parts_follow(settings, manager):
    from s3proxy.handlers.multipart.copy import _CiphertextSegment

    handler = _copy_handler(settings, manager)
    chunk = (50 * 1024 * 1024) // 6
    ct = chunk + 64
    segments = [_CiphertextSegment(chunk, ct, i * ct) for i in range(600)]
    range_end = 4_999_341_931
    split = handler._split_plaintext_range_on_segments(segments, 0, range_end)
    assert split is not None
    assert split.streaming_tail is not None
    assert handler._should_defer_hybrid_tail(
        split.streaming_tail,
        range_end,
        segments,
        client_part_plaintext_size=range_end + 1,
        part_num=1,
    )
    # Last client part: tail may stay on S3 as the final internal part.
    source_end = handler._source_plaintext_end(segments)
    assert not handler._should_defer_hybrid_tail(
        split.streaming_tail,
        source_end,
        segments,
        client_part_plaintext_size=source_end + 1,
        part_num=1,
    )


def test_should_not_defer_hybrid_tail_for_small_client_part(settings, manager):
    """Single-part partial copies must upload the tail immediately (self-contained)."""
    from s3proxy.handlers.multipart.copy import _CiphertextSegment

    handler = _copy_handler(settings, manager)
    chunk = 1_048_576
    ct = chunk + 64
    segments = [_CiphertextSegment(chunk, ct, i * ct) for i in range(20)]
    range_end = chunk * 10 - 1 - chunk // 2
    split = handler._split_plaintext_range_on_segments(segments, 0, range_end)
    assert split is not None
    assert split.streaming_tail is not None
    tail_bytes = split.streaming_tail[1] - split.streaming_tail[0] + 1
    assert tail_bytes < crypto.MIN_PART_SIZE
    # 10MB client part 1 — below 1 GiB threshold, tail uploads on part 1.
    assert not handler._should_defer_hybrid_tail(
        split.streaming_tail,
        range_end,
        segments,
        client_part_plaintext_size=range_end + 1,
        part_num=1,
    )


@pytest.mark.asyncio
async def test_deferred_copy_tail_state_roundtrip(manager):
    """Deferred tail survives serialize/deserialize between client parts."""
    bucket, key, upload_id = BUCKET, "defer-key", "upload-defer"
    dek = crypto.generate_dek()
    tail = b"T" * (crypto.MIN_PART_SIZE // 2)
    await manager.create_upload(bucket, key, upload_id, dek, kid="kid")
    await manager.set_deferred_copy_tail(bucket, key, upload_id, tail)

    loaded = await manager.get_upload(bucket, key, upload_id)
    assert loaded.deferred_copy_tail == tail

    taken = await manager.take_deferred_copy_tail(bucket, key, upload_id)
    assert taken == tail
    after = await manager.get_upload(bucket, key, upload_id)
    assert not after.deferred_copy_tail


@pytest.mark.asyncio
async def test_two_part_hybrid_defer_tail_completes_fast(
    mock_s3, settings, manager, credentials, monkeypatch
):
    """Fast regression: defer sub-5MB tail on part 1, fold into part 2, complete cleanly."""
    from s3proxy.handlers.multipart import copy as copy_mod

    monkeypatch.setattr(copy_mod, "HYBRID_TAIL_DEFER_MIN_CLIENT_PART", 5 * 1024 * 1024)
    monkeypatch.setattr(crypto, "COPY_INTERNAL_PART_SIZE", crypto.MIN_PART_SIZE)
    handler = _handler(settings, mock_s3, credentials)
    await mock_s3.create_bucket(BUCKET)

    kid, kek = settings.keyring.key_for(credentials.access_key)
    chunk = 8 * 1024 * 1024
    # Part 1 must exceed STREAMING_THRESHOLD (32MB) for hybrid passthrough; part 2
    # must also exceed it so streaming consumes the deferred tail. Internal frames
    # must be >= MIN_PART_SIZE so passthrough segments are valid S3 parts.
    num_segments = 12
    part1_range_end = chunk * 4 + chunk // 2  # 36MB, ~4MB deferred tail
    src_dek, src_plaintext, ciphertext_blob, src_meta, inflated_total, _ = (
        _build_scylla_prod_shape_source(
            kid,
            kek,
            num_internal_parts=num_segments,
            metadata_inflate_ratio=1.2,
            chunk_size=chunk,
            scylla_range_end=part1_range_end,
        )
    )
    assert inflated_total > part1_range_end + 1

    await mock_s3.put_object(BUCKET, "sst/small-Data.db", ciphertext_blob)
    await save_multipart_metadata(mock_s3, BUCKET, "sst/small-Data.db", src_meta)

    resp_create = await mock_s3.create_multipart_upload(BUCKET, "sst/small-Data.db.sm_manifest")
    upload_id = resp_create["UploadId"]
    await handler.multipart_manager.create_upload(
        BUCKET, "sst/small-Data.db.sm_manifest", upload_id, src_dek, kid
    )

    mark = len(mock_s3.call_history)
    resp1 = await handler.handle_upload_part_copy(
        _copy_part_request(
            f"/{BUCKET}/sst/small-Data.db.sm_manifest",
            f"/{BUCKET}/sst/small-Data.db",
            upload_id,
            part_number=1,
            copy_source_range=f"bytes=0-{part1_range_end}",
        ),
        credentials,
    )
    await _read(resp1)
    part1_ops = mock_s3.call_history[mark:]
    assert not [c for c in part1_ops if c[0] == "upload_part"], (
        "deferred tail must not upload on part 1"
    )

    after_part1 = await handler.multipart_manager.get_upload(
        BUCKET, "sst/small-Data.db.sm_manifest", upload_id
    )
    deferred = after_part1.deferred_copy_tail
    assert deferred
    assert len(deferred) < crypto.MIN_PART_SIZE

    part2_start = part1_range_end + 1
    resp2 = await handler.handle_upload_part_copy(
        _copy_part_request(
            f"/{BUCKET}/sst/small-Data.db.sm_manifest",
            f"/{BUCKET}/sst/small-Data.db",
            upload_id,
            part_number=2,
            copy_source_range=f"bytes={part2_start}-{inflated_total - 1}",
        ),
        credentials,
    )
    await _read(resp2)

    after_part2 = await handler.multipart_manager.get_upload(
        BUCKET, "sst/small-Data.db.sm_manifest", upload_id
    )
    assert not after_part2.deferred_copy_tail
    part2_internal = after_part2.parts[2].internal_parts
    assert part2_internal[0].plaintext_size >= crypto.MIN_PART_SIZE

    all_internal = []
    for pn in sorted(after_part2.parts):
        all_internal.extend(after_part2.parts[pn].internal_parts)
    all_internal.sort(key=lambda ip: ip.internal_part_number)
    for ip in all_internal[:-1]:
        assert ip.plaintext_size >= crypto.MIN_PART_SIZE

    complete_body = (
        "<CompleteMultipartUpload>"
        f'<Part><PartNumber>1</PartNumber><ETag>"{after_part2.parts[1].etag}"</ETag></Part>'
        f'<Part><PartNumber>2</PartNumber><ETag>"{after_part2.parts[2].etag}"</ETag></Part>'
        "</CompleteMultipartUpload>"
    ).encode()
    complete_req = MagicMock()
    complete_req.url.path = f"/{BUCKET}/sst/small-Data.db.sm_manifest"
    complete_req.url.query = f"uploadId={upload_id}"
    complete_req.headers = {}
    complete_req.body = AsyncMock(return_value=complete_body)
    await handler.handle_complete_multipart_upload(complete_req, credentials)


async def _complete_upload(handler, dest_path, upload_id, state, credentials):
    parts_xml = "".join(
        f'<Part><PartNumber>{pn}</PartNumber><ETag>"{state.parts[pn].etag}"</ETag></Part>'
        for pn in sorted(state.parts)
    )
    complete_req = MagicMock()
    complete_req.url.path = dest_path
    complete_req.url.query = f"uploadId={upload_id}"
    complete_req.headers = {}
    complete_req.body = AsyncMock(
        return_value=f"<CompleteMultipartUpload>{parts_xml}</CompleteMultipartUpload>".encode()
    )
    await handler.handle_complete_multipart_upload(complete_req, credentials)


@pytest.mark.asyncio
async def test_two_part_defer_tail_total_plaintext_not_double_counted(
    mock_s3, settings, manager, credentials, monkeypatch
):
    """Regression: rclone 2-part copy (>copy-cutoff SSTable) reported size+tail.

    Part 1 defers its sub-5MB tail; part 2's streaming copy folds the tail into
    its own stream and counts it. Part 1's metadata must therefore NOT count the
    tail too, or HEAD Content-Length comes out exactly one tail too large and
    rclone fails the copy with "corrupted on transfer: sizes differ"
    (398 files x +1,129,664 bytes, backup run sm_20260709080013UTC).
    """
    from s3proxy.handlers.multipart import copy as copy_mod
    from s3proxy.state import load_multipart_metadata

    monkeypatch.setattr(copy_mod, "HYBRID_TAIL_DEFER_MIN_CLIENT_PART", 5 * 1024 * 1024)
    monkeypatch.setattr(crypto, "COPY_INTERNAL_PART_SIZE", crypto.MIN_PART_SIZE)
    handler = _handler(settings, mock_s3, credentials)
    await mock_s3.create_bucket(BUCKET)

    kid, kek = settings.keyring.key_for(credentials.access_key)
    chunk = 8 * 1024 * 1024
    src_dek, src_plaintext, ciphertext_blob, src_meta, total, _ = _build_scylla_prod_shape_source(
        kid, kek, num_internal_parts=12, metadata_inflate_ratio=1.0, chunk_size=chunk
    )
    assert total == len(src_plaintext)
    part1_range_end = chunk * 4 + chunk // 2  # ends mid-frame -> ~4MB deferred tail

    await mock_s3.put_object(BUCKET, "sst/big-Data.db", ciphertext_blob)
    await save_multipart_metadata(mock_s3, BUCKET, "sst/big-Data.db", src_meta)

    dest_key = "sst/big-Data.db.sm_20260709080013UTC"
    resp_create = await mock_s3.create_multipart_upload(BUCKET, dest_key)
    upload_id = resp_create["UploadId"]
    await handler.multipart_manager.create_upload(BUCKET, dest_key, upload_id, src_dek, kid)

    for part_number, rng in (
        (1, f"bytes=0-{part1_range_end}"),
        (2, f"bytes={part1_range_end + 1}-{total - 1}"),
    ):
        resp = await handler.handle_upload_part_copy(
            _copy_part_request(
                f"/{BUCKET}/{dest_key}",
                f"/{BUCKET}/sst/big-Data.db",
                upload_id,
                part_number=part_number,
                copy_source_range=rng,
            ),
            credentials,
        )
        await _read(resp)

    state = await handler.multipart_manager.get_upload(BUCKET, dest_key, upload_id)
    for pn, part in state.parts.items():
        assert part.plaintext_size == sum(ip.plaintext_size for ip in part.internal_parts), (
            f"client part {pn} plaintext must match its stored internal parts"
        )
    assert sum(p.plaintext_size for p in state.parts.values()) == total

    await _complete_upload(handler, f"/{BUCKET}/{dest_key}", upload_id, state, credentials)

    dest_meta = await load_multipart_metadata(mock_s3, BUCKET, dest_key)
    assert dest_meta.total_plaintext_size == total, (
        "HEAD Content-Length source: must equal the source size, not size+tail"
    )

    resp = await handler.handle_get_object(_get_request(f"/{BUCKET}/{dest_key}"), credentials)
    assert await _read(resp) == src_plaintext


@pytest.mark.asyncio
async def test_defer_tail_flushed_on_complete_keeps_part_accounting(
    mock_s3, settings, manager, credentials, monkeypatch
):
    """A tail flushed at complete (no part 2 consumed it) lands in part accounting once."""
    from s3proxy.handlers.multipart import copy as copy_mod
    from s3proxy.state import load_multipart_metadata

    monkeypatch.setattr(copy_mod, "HYBRID_TAIL_DEFER_MIN_CLIENT_PART", 5 * 1024 * 1024)
    handler = _handler(settings, mock_s3, credentials)
    await mock_s3.create_bucket(BUCKET)

    kid, kek = settings.keyring.key_for(credentials.access_key)
    chunk = 8 * 1024 * 1024
    src_dek, src_plaintext, ciphertext_blob, src_meta, total, _ = _build_scylla_prod_shape_source(
        kid, kek, num_internal_parts=12, metadata_inflate_ratio=1.0, chunk_size=chunk
    )
    part1_range_end = chunk * 4 + chunk // 2
    part1_span = part1_range_end + 1

    await mock_s3.put_object(BUCKET, "sst/solo-Data.db", ciphertext_blob)
    await save_multipart_metadata(mock_s3, BUCKET, "sst/solo-Data.db", src_meta)

    dest_key = "sst/solo-Data.db.copy"
    resp_create = await mock_s3.create_multipart_upload(BUCKET, dest_key)
    upload_id = resp_create["UploadId"]
    await handler.multipart_manager.create_upload(BUCKET, dest_key, upload_id, src_dek, kid)

    resp = await handler.handle_upload_part_copy(
        _copy_part_request(
            f"/{BUCKET}/{dest_key}",
            f"/{BUCKET}/sst/solo-Data.db",
            upload_id,
            part_number=1,
            copy_source_range=f"bytes=0-{part1_range_end}",
        ),
        credentials,
    )
    await _read(resp)

    state = await handler.multipart_manager.get_upload(BUCKET, dest_key, upload_id)
    assert state.deferred_copy_tail

    await _complete_upload(handler, f"/{BUCKET}/{dest_key}", upload_id, state, credentials)

    dest_meta = await load_multipart_metadata(mock_s3, BUCKET, dest_key)
    assert dest_meta.total_plaintext_size == part1_span
    part = dest_meta.parts[0]
    assert part.plaintext_size == part1_span
    assert part.plaintext_size == sum(ip.plaintext_size for ip in part.internal_parts)

    resp = await handler.handle_get_object(_get_request(f"/{BUCKET}/{dest_key}"), credentials)
    assert await _read(resp) == src_plaintext[:part1_span]


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


@pytest.mark.asyncio
async def test_single_segment_passthrough_complete_presents_backend_etag(
    mock_s3, settings, credentials
):
    """Regression: CompleteMultipartUpload must send the BACKEND part etag to S3.

    The single-segment passthrough copy returns a synthetic plaintext etag to
    the client; forwarding that echoed etag to the backend gets InvalidPart
    from real S3/MinIO (masked in CI for a while because the resulting 500 was
    retried by boto3 and recovered via state reconstruction).
    """
    import base64

    handler = _handler(settings, mock_s3, credentials)
    await mock_s3.create_bucket(BUCKET)

    kid, kek = settings.keyring.key_for(credentials.access_key)
    plaintext = b"E" * (2 * 1024 * 1024)
    enc = crypto.encrypt_object(plaintext, kek)
    await mock_s3.put_object(
        BUCKET,
        "single/src.bin",
        enc.ciphertext,
        metadata={
            settings.dektag_name: base64.b64encode(enc.wrapped_dek).decode(),
            settings.kidtag_name: kid,
            "plaintext-size": str(len(plaintext)),
        },
    )

    create_resp = await handler.handle_create_multipart_upload(
        MagicMock(url=MagicMock(path=f"/{BUCKET}/single/dst.bin"), headers={}),
        credentials,
    )
    upload_id = _extract_upload_id(create_resp.body)

    copy_resp = await handler.handle_upload_part_copy(
        _copy_part_request(f"/{BUCKET}/single/dst.bin", f"/{BUCKET}/single/src.bin", upload_id),
        credentials,
    )
    import xml.etree.ElementTree as ET

    client_etag = ET.fromstring(await _read(copy_resp)).find("{*}ETag").text.strip('"')
    backend_etag = mock_s3.multipart_uploads[upload_id]["Parts"][1]["ETag"]
    assert client_etag == hashlib.md5(plaintext, usedforsecurity=False).hexdigest()
    assert client_etag != backend_etag

    sent_parts = []
    orig_complete = mock_s3.complete_multipart_upload

    async def capturing_complete(bucket, key, upload_id_, parts):
        sent_parts.extend(parts)
        return await orig_complete(bucket, key, upload_id_, parts)

    mock_s3.complete_multipart_upload = capturing_complete

    complete_req = MagicMock()
    complete_req.url.path = f"/{BUCKET}/single/dst.bin"
    complete_req.url.query = f"uploadId={upload_id}"
    complete_req.headers = {}
    complete_req.body = AsyncMock(
        return_value=(
            f"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>"
            f'<ETag>"{client_etag}"</ETag></Part></CompleteMultipartUpload>'
        ).encode()
    )
    await handler.handle_complete_multipart_upload(complete_req, credentials)

    assert sent_parts == [{"PartNumber": 1, "ETag": f'"{backend_etag}"'}]

    resp = await handler.handle_get_object(_get_request(f"/{BUCKET}/single/dst.bin"), credentials)
    assert await _read(resp) == plaintext


def _extract_upload_id(xml_body: bytes) -> str:
    import xml.etree.ElementTree as ET

    root = ET.fromstring(xml_body.decode())
    for elem in root.iter():
        if elem.tag.endswith("UploadId") and elem.text:
            return elem.text
    raise AssertionError("UploadId not found in create response")
