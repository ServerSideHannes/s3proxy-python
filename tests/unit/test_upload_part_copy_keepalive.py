"""UploadPartCopy keepalive streaming, work cancellation, and copy parallelism.

Prod failure mode these guard against: a multi-GB Scylla part copy produced no
response bytes for longer than rclone's 5-minute idle timeout, the client hung
up at exactly 300s, the proxy kept the doomed copy running as a zombie, and
scylla-manager retried forever. The response must start streaming keepalive
whitespace while the copy runs, backend work must be concurrent so the copy
finishes fast, and a client disconnect must cancel the in-flight work.
"""

import asyncio
import contextlib
import hashlib
import time
import xml.etree.ElementTree as ET

import pytest

from s3proxy import crypto
from s3proxy.handlers.multipart import MultipartHandlerMixin
from s3proxy.handlers.multipart import copy as copy_mod
from s3proxy.state import (
    InternalPartMetadata,
    MultipartMetadata,
    PartMetadata,
    save_multipart_metadata,
)

BUCKET = "backups"
SRC_KEY = "sst/source-Data.db"
DST_KEY = "sst/source-Data.db.sm_manifest"


def _copy_handler(settings, manager):
    return MultipartHandlerMixin(settings, {}, manager)


def _patch_client(handler, mock_s3):
    @contextlib.asynccontextmanager
    async def _fake_client(creds):
        yield mock_s3

    handler._client = _fake_client


def _copy_part_request(upload_id, part_number=1, copy_source_range=None):
    from unittest.mock import MagicMock

    req = MagicMock()
    req.url.path = f"/{BUCKET}/{DST_KEY}"
    req.url.query = f"uploadId={upload_id}&partNumber={part_number}"
    req.headers = {"x-amz-copy-source": f"/{BUCKET}/{SRC_KEY}"}
    if copy_source_range is not None:
        req.headers["x-amz-copy-source-range"] = copy_source_range
    return req


async def _build_encrypted_source(mock_s3, settings, credentials, *, frames, frame_size):
    """Encrypted multipart source with one internal part per frame."""
    kid, kek = settings.keyring.key_for(credentials.access_key)
    src_dek = crypto.generate_dek()
    plaintext = bytes(i % 251 for i in range(frames * frame_size))

    ciphertext_blob = bytearray()
    internal_parts_meta = []
    for i, start in enumerate(range(0, len(plaintext), frame_size), 1):
        chunk = plaintext[start : start + frame_size]
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
        total_plaintext_size=len(plaintext),
        parts=[
            PartMetadata(
                part_number=1,
                plaintext_size=len(plaintext),
                ciphertext_size=len(ciphertext_blob),
                etag="ignored",
                md5=hashlib.md5(plaintext, usedforsecurity=False).hexdigest(),
                internal_parts=internal_parts_meta,
            )
        ],
        wrapped_dek=crypto.wrap_key(src_dek, kek),
        kid=kid,
    )
    await mock_s3.create_bucket(BUCKET)
    await mock_s3.put_object(BUCKET, SRC_KEY, bytes(ciphertext_blob))
    await save_multipart_metadata(mock_s3, BUCKET, SRC_KEY, src_meta)
    return src_dek, plaintext, kid


async def _start_upload(mock_s3, manager, dek, kid):
    resp_create = await mock_s3.create_multipart_upload(BUCKET, DST_KEY)
    upload_id = resp_create["UploadId"]
    await manager.create_upload(BUCKET, DST_KEY, upload_id, dek, kid)
    return upload_id


@pytest.mark.asyncio
async def test_keepalive_bytes_stream_while_copy_is_still_running(
    mock_s3, settings, manager, credentials, monkeypatch
):
    """First response bytes must arrive BEFORE the backend copy finishes.

    This is the regression that broke prod: no bytes for the whole copy
    duration, so rclone's idle timeout killed every large part copy.
    """
    monkeypatch.setattr(copy_mod, "COPY_KEEPALIVE_INTERVAL", 0.02)
    handler = _copy_handler(settings, manager)
    _patch_client(handler, mock_s3)
    src_dek, plaintext, kid = await _build_encrypted_source(
        mock_s3, settings, credentials, frames=4, frame_size=256 * 1024
    )
    upload_id = await _start_upload(mock_s3, manager, crypto.generate_dek(), kid)

    work_done = asyncio.Event()
    orig_copy = mock_s3.upload_part_copy

    async def slow_copy(*args, **kwargs):
        await asyncio.sleep(0.1)
        result = await orig_copy(*args, **kwargs)
        work_done.set()
        return result

    mock_s3.upload_part_copy = slow_copy

    resp = await handler.handle_upload_part_copy(_copy_part_request(upload_id), credentials)
    assert resp.status_code == 200

    stream = resp.body_iterator
    first = await anext(stream)
    assert first == b" "
    assert not work_done.is_set(), "keepalive byte must be sent while the copy is in flight"
    body = first + b"".join([c async for c in stream])
    assert work_done.is_set()

    # Whitespace + XML exactly as sent over the wire must parse.
    root = ET.fromstring(body)
    assert root.tag.endswith("CopyPartResult")
    etag = root.find("{*}ETag")
    assert etag.text.strip('"') == hashlib.md5(plaintext, usedforsecurity=False).hexdigest()

    state = await manager.get_upload(BUCKET, DST_KEY, upload_id)
    assert state.parts[1].plaintext_size == len(plaintext)


@pytest.mark.asyncio
async def test_copy_failure_after_200_reports_error_document(
    mock_s3, settings, manager, credentials, monkeypatch
):
    """A copy that fails mid-flight must yield a parseable <Error> body, not hang."""
    monkeypatch.setattr(copy_mod, "COPY_KEEPALIVE_INTERVAL", 0.02)
    handler = _copy_handler(settings, manager)
    _patch_client(handler, mock_s3)
    _, _, kid = await _build_encrypted_source(
        mock_s3, settings, credentials, frames=3, frame_size=256 * 1024
    )
    upload_id = await _start_upload(mock_s3, manager, crypto.generate_dek(), kid)

    async def failing_copy(*args, **kwargs):
        await asyncio.sleep(0.05)
        raise RuntimeError("backend exploded")

    mock_s3.upload_part_copy = failing_copy

    resp = await handler.handle_upload_part_copy(_copy_part_request(upload_id), credentials)
    body = b"".join([c async for c in resp.body_iterator])

    assert resp.status_code == 200
    root = ET.fromstring(body)
    assert root.tag == "Error"
    assert root.find("Code").text == "InternalError"
    assert "backend exploded" in root.find("Message").text


@pytest.mark.asyncio
async def test_client_disconnect_cancels_inflight_copy_work(
    mock_s3, settings, manager, credentials, monkeypatch
):
    """Closing the response stream must cancel backend work (no zombie copies)."""
    monkeypatch.setattr(copy_mod, "COPY_KEEPALIVE_INTERVAL", 0.02)
    handler = _copy_handler(settings, manager)
    _patch_client(handler, mock_s3)
    _, _, kid = await _build_encrypted_source(
        mock_s3, settings, credentials, frames=3, frame_size=256 * 1024
    )
    upload_id = await _start_upload(mock_s3, manager, crypto.generate_dek(), kid)

    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def hanging_copy(*args, **kwargs):
        started.set()
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            cancelled.set()
            raise
        raise AssertionError("copy was not cancelled")

    mock_s3.upload_part_copy = hanging_copy

    resp = await handler.handle_upload_part_copy(_copy_part_request(upload_id), credentials)
    stream = resp.body_iterator
    assert await anext(stream) == b" "
    await asyncio.wait_for(started.wait(), 1)

    await stream.aclose()

    await asyncio.wait_for(cancelled.wait(), 1)
    state = await manager.get_upload(BUCKET, DST_KEY, upload_id)
    assert 1 not in state.parts, "aborted copy must not record a part"


@pytest.mark.asyncio
async def test_passthrough_segments_copy_concurrently(mock_s3, settings, manager, credentials):
    """Segment copies must overlap; sequential backend calls pushed prod copies
    past the client timeout even on the passthrough route."""
    handler = _copy_handler(settings, manager)
    _patch_client(handler, mock_s3)
    _, plaintext, kid = await _build_encrypted_source(
        mock_s3, settings, credentials, frames=12, frame_size=256 * 1024
    )
    upload_id = await _start_upload(mock_s3, manager, crypto.generate_dek(), kid)

    in_flight = 0
    max_in_flight = 0
    orig_copy = mock_s3.upload_part_copy

    async def tracking_copy(*args, **kwargs):
        nonlocal in_flight, max_in_flight
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        try:
            await asyncio.sleep(0.02)
            return await orig_copy(*args, **kwargs)
        finally:
            in_flight -= 1

    mock_s3.upload_part_copy = tracking_copy

    resp = await handler.handle_upload_part_copy(_copy_part_request(upload_id), credentials)
    body = b"".join([c async for c in resp.body_iterator])

    assert ET.fromstring(body).tag.endswith("CopyPartResult")
    assert max_in_flight >= 2, "segment copies ran sequentially"
    assert max_in_flight <= copy_mod.PASSTHROUGH_SEGMENT_CONCURRENCY
    state = await manager.get_upload(BUCKET, DST_KEY, upload_id)
    assert state.parts[1].plaintext_size == len(plaintext)


@pytest.mark.asyncio
async def test_md5_source_pass_overlaps_segment_copies(mock_s3, settings, manager, credentials):
    """The synthetic-ETag read of the source must run alongside the segment
    copies, not serially after them (it re-reads the whole object)."""
    handler = _copy_handler(settings, manager)
    _patch_client(handler, mock_s3)
    _, _, kid = await _build_encrypted_source(
        mock_s3, settings, credentials, frames=8, frame_size=256 * 1024
    )
    upload_id = await _start_upload(mock_s3, manager, crypto.generate_dek(), kid)

    first_md5_read: float | None = None
    last_copy_done: float | None = None
    orig_copy = mock_s3.upload_part_copy
    orig_get = mock_s3.get_object

    async def slow_copy(*args, **kwargs):
        nonlocal last_copy_done
        await asyncio.sleep(0.03)
        result = await orig_copy(*args, **kwargs)
        last_copy_done = time.monotonic()
        return result

    async def tracked_get(*args, **kwargs):
        nonlocal first_md5_read
        if first_md5_read is None:
            first_md5_read = time.monotonic()
        return await orig_get(*args, **kwargs)

    mock_s3.upload_part_copy = slow_copy
    mock_s3.get_object = tracked_get

    resp = await handler.handle_upload_part_copy(_copy_part_request(upload_id), credentials)
    b"".join([c async for c in resp.body_iterator])

    assert first_md5_read is not None and last_copy_done is not None
    assert first_md5_read < last_copy_done, "MD5 pass started only after all copies finished"


@pytest.mark.asyncio
async def test_hybrid_tail_roundtrip_with_parallel_segments(
    mock_s3, settings, manager, credentials
):
    """Mid-frame range: parallel passthrough segments + re-encrypted tail must
    reassemble to exactly the requested plaintext range, in order."""
    handler = _copy_handler(settings, manager)
    _patch_client(handler, mock_s3)
    frame_size = 8 * 1024 * 1024
    src_dek, plaintext, kid = await _build_encrypted_source(
        mock_s3, settings, credentials, frames=6, frame_size=frame_size
    )
    upload_id = await _start_upload(mock_s3, manager, crypto.generate_dek(), kid)

    range_end = 4 * frame_size + frame_size // 2 - 1  # ends mid 5th frame
    assert range_end + 1 > crypto.STREAMING_THRESHOLD

    resp = await handler.handle_upload_part_copy(
        _copy_part_request(upload_id, copy_source_range=f"bytes=0-{range_end}"),
        credentials,
    )
    body = b"".join([c async for c in resp.body_iterator])
    assert ET.fromstring(body).tag.endswith("CopyPartResult")

    state = await manager.get_upload(BUCKET, DST_KEY, upload_id)
    assert state.dek == src_dek  # destination adopted the source DEK
    part = state.parts[1]
    assert part.plaintext_size == range_end + 1
    assert len(part.internal_parts) == 5  # 4 passthrough + 1 tail

    recovered = bytearray()
    for ip in sorted(part.internal_parts, key=lambda x: x.internal_part_number):
        ct = mock_s3.multipart_uploads[upload_id]["Parts"][ip.internal_part_number]["Body"]
        recovered.extend(crypto.decrypt_framed(bytes(ct), src_dek, ip.plaintext_size))
    assert bytes(recovered) == plaintext[: range_end + 1]
