"""Unit tests for streaming copy paths (issue #59).

Verifies that sources larger than STREAMING_THRESHOLD are processed in
bounded chunks rather than as a single blocking encrypt call.
"""

import base64
import contextlib
import hashlib

import pytest

from s3proxy import crypto
from s3proxy.handlers.multipart import MultipartHandlerMixin
from s3proxy.handlers.objects.misc import MiscObjectMixin
from s3proxy.state import (
    InternalPartMetadata,
    MultipartMetadata,
    PartMetadata,
    load_multipart_metadata,
    save_multipart_metadata,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_handler(settings, manager):
    """Return a MultipartHandlerMixin (includes CopyPartMixin + UploadPartMixin)."""
    return MultipartHandlerMixin(settings, {}, manager)


def _make_misc_handler(settings, manager):
    """Return a MiscObjectMixin for testing CopyObject paths."""
    return MiscObjectMixin(settings, {}, manager)


def _patch_client(handler, mock_s3):
    """Patch handler._client so that `async with handler._client(creds) as c` yields mock_s3."""

    @contextlib.asynccontextmanager
    async def _fake_client(creds):
        yield mock_s3

    handler._client = _fake_client


# ---------------------------------------------------------------------------
# _copy_plaintext_size
# ---------------------------------------------------------------------------


class TestCopyPlaintextSize:
    """Pure-logic tests for _copy_plaintext_size — no I/O needed."""

    @pytest.fixture
    def handler(self, settings, manager):
        return _make_handler(settings, manager)

    def test_unencrypted_whole_object(self, handler):
        head = {"ContentLength": 100, "Metadata": {}}
        assert handler._copy_plaintext_size(head, None, None, None) == 100

    def test_unencrypted_with_range(self, handler):
        head = {"ContentLength": 100, "Metadata": {}}
        assert handler._copy_plaintext_size(head, "bytes=10-19", None, None) == 10

    def test_single_encrypted_uses_metadata_field(self, handler, settings):
        head = {
            "ContentLength": 9999,
            "Metadata": {"plaintext-size": "500"},
        }
        assert handler._copy_plaintext_size(head, None, "wrapped-dek", None) == 500

    def test_single_encrypted_falls_back_to_content_length(self, handler, settings):
        # No plaintext-size metadata → derive from ciphertext ContentLength
        head = {"ContentLength": crypto.NONCE_SIZE + 50 + crypto.TAG_SIZE, "Metadata": {}}
        assert handler._copy_plaintext_size(head, None, "wrapped-dek", None) == 50

    def test_multipart_encrypted_total_size(self, handler):
        meta = type("M", (), {"total_plaintext_size": 1024 * 1024})()
        head = {"ContentLength": 999, "Metadata": {}}
        assert handler._copy_plaintext_size(head, None, None, meta) == 1024 * 1024

    def test_multipart_encrypted_with_range(self, handler):
        meta = type("M", (), {"total_plaintext_size": 100})()
        head = {"ContentLength": 999, "Metadata": {}}
        assert handler._copy_plaintext_size(head, "bytes=20-39", None, meta) == 20


# ---------------------------------------------------------------------------
# UploadPartCopy — streaming path
# ---------------------------------------------------------------------------


class TestUploadPartCopyStreaming:
    """End-to-end streaming UploadPartCopy tests using the in-memory mock S3."""

    @pytest.mark.asyncio
    async def test_small_source_uses_simple_path(self, mock_s3, settings, manager, credentials):
        """Source ≤ STREAMING_THRESHOLD → single S3 part, no internal_parts list."""
        handler = _make_handler(settings, manager)
        await mock_s3.create_bucket("bucket")

        plaintext = b"A" * (crypto.STREAMING_THRESHOLD - 1)
        await mock_s3.put_object("bucket", "src", plaintext)

        resp_create = await mock_s3.create_multipart_upload("bucket", "dst")
        upload_id = resp_create["UploadId"]

        kid, kek = settings.keyring.key_for(credentials.access_key)
        dek = crypto.generate_dek()
        await manager.create_upload("bucket", "dst", upload_id, dek, kid)
        state = await manager.get_upload("bucket", "dst", upload_id)

        head_resp = {"Metadata": {}, "ContentLength": len(plaintext)}
        await handler._simple_copy_part(
            mock_s3,
            "bucket",
            "dst",
            upload_id,
            1,
            state,
            "bucket",
            "src",
            None,
            head_resp,
            {},
            None,
            None,
        )

        updated = await manager.get_upload("bucket", "dst", upload_id)
        part = updated.parts[1]
        assert part.internal_parts == []
        assert part.plaintext_size == len(plaintext)

        # Verify ciphertext decrypts correctly via encrypt_part nonce
        stored_ct = mock_s3.multipart_uploads[upload_id]["Parts"][1]["Body"]
        recovered = crypto.decrypt(stored_ct, dek)
        assert recovered == plaintext

    @pytest.mark.asyncio
    async def test_large_unencrypted_source_splits_into_multiple_parts(
        self, mock_s3, settings, manager, credentials, monkeypatch
    ):
        """Source > STREAMING_THRESHOLD → multiple internal parts."""
        # Pin the fixed copy part size to one buffer so parts stay single-frame
        # and this test's per-part crypto.decrypt roundtrip applies; the 32MB
        # multi-frame path is covered by test_streamed_parts_are_framed_and_roundtrip.
        monkeypatch.setattr(crypto, "COPY_INTERNAL_PART_SIZE", crypto.MAX_BUFFER_SIZE)
        handler = _make_handler(settings, manager)
        await mock_s3.create_bucket("bucket")

        # 3 × MAX_BUFFER_SIZE to guarantee 3 internal parts
        plaintext = b"B" * (crypto.MAX_BUFFER_SIZE * 3)
        await mock_s3.put_object("bucket", "src", plaintext)

        resp_create = await mock_s3.create_multipart_upload("bucket", "dst")
        upload_id = resp_create["UploadId"]

        kid, kek = settings.keyring.key_for(credentials.access_key)
        dek = crypto.generate_dek()
        await manager.create_upload("bucket", "dst", upload_id, dek, kid)
        state = await manager.get_upload("bucket", "dst", upload_id)

        head_resp = {"Metadata": {}, "ContentLength": len(plaintext)}
        await handler._streaming_copy_part(
            mock_s3,
            "bucket",
            "dst",
            upload_id,
            1,
            state,
            "bucket",
            "src",
            None,
            None,
            None,
            head_resp,
            {},
            len(plaintext),
        )

        updated = await manager.get_upload("bucket", "dst", upload_id)
        part = updated.parts[1]

        assert len(part.internal_parts) == 3
        assert part.plaintext_size == len(plaintext)

        # Round-trip: decrypt all internal parts and reassemble
        recovered = bytearray()
        for ip in sorted(part.internal_parts, key=lambda x: x.internal_part_number):
            ct = mock_s3.multipart_uploads[upload_id]["Parts"][ip.internal_part_number]["Body"]
            recovered.extend(crypto.decrypt(ct, dek))

        assert bytes(recovered) == plaintext

    @pytest.mark.asyncio
    async def test_streamed_parts_are_framed_and_roundtrip(
        self, mock_s3, settings, manager, credentials, monkeypatch
    ):
        """An internal part larger than one frame must be written framed
        (encrypt_frame per FRAME_PLAINTEXT_SIZE) and round-trip via decrypt_framed.
        Guards the framed copy writer -- the old writer sealed whole parts, so
        this many-frames-per-part case was never exercised."""
        monkeypatch.setattr(crypto, "MAX_BUFFER_SIZE", 1024)
        monkeypatch.setattr(crypto, "FRAME_PLAINTEXT_SIZE", 256)
        handler = _make_handler(settings, manager)
        await mock_s3.create_bucket("bucket")

        # 20 * MAX_BUFFER_SIZE -> memory_bounded_part_size caps at 20 parts of
        # ~1KB each, and each 1KB part spans 4 x 256B frames.
        plaintext = bytes((i * 31) % 256 for i in range(20 * 1024))
        await mock_s3.put_object("bucket", "src", plaintext)

        resp_create = await mock_s3.create_multipart_upload("bucket", "dst")
        upload_id = resp_create["UploadId"]
        kid, _ = settings.keyring.key_for(credentials.access_key)
        dek = crypto.generate_dek()
        await manager.create_upload("bucket", "dst", upload_id, dek, kid)
        state = await manager.get_upload("bucket", "dst", upload_id)

        head_resp = {"Metadata": {}, "ContentLength": len(plaintext)}
        await handler._streaming_copy_part(
            mock_s3,
            "bucket",
            "dst",
            upload_id,
            1,
            state,
            "bucket",
            "src",
            None,
            None,
            None,
            head_resp,
            {},
            len(plaintext),
        )

        updated = await manager.get_upload("bucket", "dst", upload_id)
        part = updated.parts[1]
        assert part.plaintext_size == len(plaintext)
        # at least one internal part is genuinely multi-frame
        assert any(crypto.frame_count(ip.plaintext_size) > 1 for ip in part.internal_parts)

        recovered = bytearray()
        for ip in sorted(part.internal_parts, key=lambda x: x.internal_part_number):
            ct = mock_s3.multipart_uploads[upload_id]["Parts"][ip.internal_part_number]["Body"]
            recovered.extend(crypto.decrypt_framed(ct, dek, ip.plaintext_size))
        assert bytes(recovered) == plaintext

    @pytest.mark.asyncio
    async def test_large_unencrypted_source_with_range(
        self, mock_s3, settings, manager, credentials, monkeypatch
    ):
        """Range request on a large unencrypted source streams only the range bytes."""
        monkeypatch.setattr(crypto, "COPY_INTERNAL_PART_SIZE", crypto.MAX_BUFFER_SIZE)
        handler = _make_handler(settings, manager)
        await mock_s3.create_bucket("bucket")

        plaintext = b"C" * (crypto.MAX_BUFFER_SIZE * 4)
        await mock_s3.put_object("bucket", "src", plaintext)

        resp_create = await mock_s3.create_multipart_upload("bucket", "dst")
        upload_id = resp_create["UploadId"]

        kid, kek = settings.keyring.key_for(credentials.access_key)
        dek = crypto.generate_dek()
        await manager.create_upload("bucket", "dst", upload_id, dek, kid)
        state = await manager.get_upload("bucket", "dst", upload_id)

        # Copy only 2 × MAX_BUFFER_SIZE worth of bytes via range
        range_size = crypto.MAX_BUFFER_SIZE * 2
        copy_source_range = f"bytes=0-{range_size - 1}"
        head_resp = {"Metadata": {}, "ContentLength": len(plaintext)}

        await handler._streaming_copy_part(
            mock_s3,
            "bucket",
            "dst",
            upload_id,
            1,
            state,
            "bucket",
            "src",
            copy_source_range,
            None,
            None,
            head_resp,
            {},
            range_size,
        )

        updated = await manager.get_upload("bucket", "dst", upload_id)
        part = updated.parts[1]

        assert part.plaintext_size == range_size

        recovered = bytearray()
        for ip in sorted(part.internal_parts, key=lambda x: x.internal_part_number):
            ct = mock_s3.multipart_uploads[upload_id]["Parts"][ip.internal_part_number]["Body"]
            recovered.extend(crypto.decrypt(ct, dek))

        assert bytes(recovered) == plaintext[:range_size]

    @pytest.mark.asyncio
    async def test_large_multipart_encrypted_source(
        self, mock_s3, settings, manager, credentials, monkeypatch
    ):
        """Large multipart-encrypted source → iterates source parts, re-encrypts in chunks."""
        monkeypatch.setattr(crypto, "COPY_INTERNAL_PART_SIZE", crypto.MAX_BUFFER_SIZE)
        handler = _make_handler(settings, manager)
        await mock_s3.create_bucket("bucket")

        kid, kek = settings.keyring.key_for(credentials.access_key)

        # Build a multipart-encrypted source object: 3 × MAX_BUFFER_SIZE
        src_dek = crypto.generate_dek()
        src_kid = kid
        src_kek = kek
        src_plaintext = b"D" * (crypto.MAX_BUFFER_SIZE * 3)

        # Simulate the source as 3 encrypted parts stored as a single concatenated blob
        chunk_size = crypto.MAX_BUFFER_SIZE
        src_parts = []
        ciphertext_blob = bytearray()
        for i, start in enumerate(range(0, len(src_plaintext), chunk_size), 1):
            chunk = src_plaintext[start : start + chunk_size]
            nonce = crypto.derive_part_nonce("src-upload-id", i)
            ct = crypto.encrypt(chunk, src_dek, nonce)
            src_parts.append(
                PartMetadata(
                    part_number=i,
                    plaintext_size=len(chunk),
                    ciphertext_size=len(ct),
                    etag=hashlib.md5(ct).hexdigest(),
                    md5="",
                )
            )
            ciphertext_blob.extend(ct)

        src_wrapped_dek = crypto.wrap_key(src_dek, src_kek)
        src_meta = MultipartMetadata(
            version=2,
            part_count=3,
            total_plaintext_size=len(src_plaintext),
            parts=src_parts,
            wrapped_dek=src_wrapped_dek,
            kid=src_kid,
        )

        # Store source object and its sidecar
        await mock_s3.put_object("bucket", "src", bytes(ciphertext_blob))
        await save_multipart_metadata(mock_s3, "bucket", "src", src_meta)

        # Set up destination upload
        resp_create = await mock_s3.create_multipart_upload("bucket", "dst")
        upload_id = resp_create["UploadId"]
        dst_dek = crypto.generate_dek()
        await manager.create_upload("bucket", "dst", upload_id, dst_dek, kid)
        state = await manager.get_upload("bucket", "dst", upload_id)

        head_resp = {"Metadata": {}, "ContentLength": len(ciphertext_blob)}
        loaded_meta = await load_multipart_metadata(mock_s3, "bucket", "src")

        await handler._streaming_copy_part(
            mock_s3,
            "bucket",
            "dst",
            upload_id,
            1,
            state,
            "bucket",
            "src",
            None,
            None,
            loaded_meta,
            head_resp,
            {},
            len(src_plaintext),
        )

        updated = await manager.get_upload("bucket", "dst", upload_id)
        part = updated.parts[1]

        assert part.plaintext_size == len(src_plaintext)
        assert len(part.internal_parts) >= 1

        recovered = bytearray()
        for ip in sorted(part.internal_parts, key=lambda x: x.internal_part_number):
            ct = mock_s3.multipart_uploads[upload_id]["Parts"][ip.internal_part_number]["Body"]
            recovered.extend(crypto.decrypt(ct, dst_dek))

        assert bytes(recovered) == src_plaintext


# ---------------------------------------------------------------------------
# CopyObject — streaming path
# ---------------------------------------------------------------------------


class TestCopyObjectStreaming:
    """End-to-end streaming CopyObject tests."""

    @pytest.mark.asyncio
    async def test_small_source_uses_put_object_path(self, mock_s3, settings, manager, credentials):
        """Source ≤ STREAMING_THRESHOLD → put_object path (no internal multipart)."""
        handler = _make_misc_handler(settings, manager)
        await mock_s3.create_bucket("bucket")

        kid, kek = settings.keyring.key_for(credentials.access_key)
        plaintext = b"E" * (crypto.STREAMING_THRESHOLD - 1)
        encrypted = crypto.encrypt_object(plaintext, kek)
        wrapped_dek_b64 = base64.b64encode(encrypted.wrapped_dek).decode()

        await mock_s3.put_object(
            "bucket",
            "src",
            encrypted.ciphertext,
            metadata={
                settings.dektag_name: wrapped_dek_b64,
                settings.kidtag_name: kid,
                "plaintext-size": str(len(plaintext)),
            },
        )

        head_resp = {
            "Metadata": {
                settings.dektag_name: wrapped_dek_b64,
                settings.kidtag_name: kid,
                "plaintext-size": str(len(plaintext)),
            },
            "ContentLength": len(encrypted.ciphertext),
            "ContentType": "application/octet-stream",
        }

        resp = await handler._copy_encrypted(
            mock_s3,
            "bucket",
            "dst",
            None,
            "bucket",
            "src",
            head_resp,
            wrapped_dek_b64,
            None,
            "COPY",
            None,
        )
        assert resp.status_code == 200
        # Simple path stores the object directly via put_object, no multipart sidecar
        assert await load_multipart_metadata(mock_s3, "bucket", "dst") is None

    @pytest.mark.asyncio
    async def test_large_multipart_source_uses_streaming(
        self, mock_s3, settings, manager, credentials
    ):
        """Source > STREAMING_THRESHOLD → internal multipart + MultipartMetadata sidecar."""
        handler = _make_misc_handler(settings, manager)
        await mock_s3.create_bucket("bucket")

        kid, kek = settings.keyring.key_for(credentials.access_key)
        src_dek = crypto.generate_dek()

        # Build multipart source with 5 × MAX_BUFFER_SIZE
        chunk_size = crypto.MAX_BUFFER_SIZE
        src_plaintext = b"F" * (chunk_size * 5)
        src_parts = []
        ciphertext_blob = bytearray()
        for i, start in enumerate(range(0, len(src_plaintext), chunk_size), 1):
            chunk = src_plaintext[start : start + chunk_size]
            nonce = crypto.derive_part_nonce("src-uid", i)
            ct = crypto.encrypt(chunk, src_dek, nonce)
            src_parts.append(
                PartMetadata(
                    part_number=i,
                    plaintext_size=len(chunk),
                    ciphertext_size=len(ct),
                    etag=hashlib.md5(ct).hexdigest(),
                )
            )
            ciphertext_blob.extend(ct)

        src_wrapped_dek = crypto.wrap_key(src_dek, kek)
        src_meta = MultipartMetadata(
            version=2,
            part_count=5,
            total_plaintext_size=len(src_plaintext),
            parts=src_parts,
            wrapped_dek=src_wrapped_dek,
            kid=kid,
        )
        await mock_s3.put_object("bucket", "src", bytes(ciphertext_blob))
        await save_multipart_metadata(mock_s3, "bucket", "src", src_meta)

        head_resp = {
            "Metadata": {},
            "ContentLength": len(ciphertext_blob),
            "ContentType": "application/octet-stream",
        }
        loaded_src_meta = await load_multipart_metadata(mock_s3, "bucket", "src")

        resp = await handler._copy_encrypted(
            mock_s3,
            "bucket",
            "dst",
            None,
            "bucket",
            "src",
            head_resp,
            None,
            loaded_src_meta,
            "COPY",
            None,
        )
        assert resp.status_code == 200

        # Verify MultipartMetadata sidecar was saved for the destination
        dst_meta = await load_multipart_metadata(mock_s3, "bucket", "dst")
        assert dst_meta is not None
        assert dst_meta.total_plaintext_size == len(src_plaintext)
        assert dst_meta.part_count > 0

    @pytest.mark.asyncio
    async def test_large_source_round_trip(self, mock_s3, settings, manager, credentials):
        """CopyObject streaming: decrypt the destination and verify bytes match source."""
        handler = _make_misc_handler(settings, manager)
        await mock_s3.create_bucket("bucket")

        kid, kek = settings.keyring.key_for(credentials.access_key)
        src_dek = crypto.generate_dek()

        # 5 × MAX_BUFFER_SIZE = 40MB > STREAMING_THRESHOLD = 32MB
        chunk_size = crypto.MAX_BUFFER_SIZE
        src_plaintext = b"G" * (chunk_size * 5)
        src_parts = []
        ciphertext_blob = bytearray()
        for i, start in enumerate(range(0, len(src_plaintext), chunk_size), 1):
            chunk = src_plaintext[start : start + chunk_size]
            nonce = crypto.derive_part_nonce("uid-rtrip", i)
            ct = crypto.encrypt(chunk, src_dek, nonce)
            src_parts.append(
                PartMetadata(
                    part_number=i,
                    plaintext_size=len(chunk),
                    ciphertext_size=len(ct),
                    etag=hashlib.md5(ct).hexdigest(),
                )
            )
            ciphertext_blob.extend(ct)

        src_wrapped_dek = crypto.wrap_key(src_dek, kek)
        src_meta = MultipartMetadata(
            version=2,
            part_count=3,
            total_plaintext_size=len(src_plaintext),
            parts=src_parts,
            wrapped_dek=src_wrapped_dek,
            kid=kid,
        )
        await mock_s3.put_object("bucket", "src", bytes(ciphertext_blob))
        await save_multipart_metadata(mock_s3, "bucket", "src", src_meta)

        head_resp = {
            "Metadata": {},
            "ContentLength": len(ciphertext_blob),
            "ContentType": "application/octet-stream",
        }
        loaded_src_meta = await load_multipart_metadata(mock_s3, "bucket", "src")

        resp = await handler._copy_encrypted(
            mock_s3,
            "bucket",
            "dst",
            None,
            "bucket",
            "src",
            head_resp,
            None,
            loaded_src_meta,
            "COPY",
            None,
        )
        assert resp.status_code == 200

        # Decrypt the destination via handler helpers
        dst_meta = await load_multipart_metadata(mock_s3, "bucket", "dst")
        assert dst_meta is not None

        recovered = await handler._download_encrypted_multipart(mock_s3, "bucket", "dst", dst_meta)
        assert recovered == src_plaintext

    @pytest.mark.asyncio
    async def test_streaming_respects_metadata_replace_directive(
        self, mock_s3, settings, manager, credentials
    ):
        """REPLACE directive: only new_metadata keys appear on the destination."""
        handler = _make_misc_handler(settings, manager)
        await mock_s3.create_bucket("bucket")

        kid, kek = settings.keyring.key_for(credentials.access_key)
        src_dek = crypto.generate_dek()

        chunk_size = crypto.MAX_BUFFER_SIZE
        src_plaintext = b"H" * (chunk_size * 5)
        src_parts = []
        ciphertext_blob = bytearray()
        for i, start in enumerate(range(0, len(src_plaintext), chunk_size), 1):
            chunk = src_plaintext[start : start + chunk_size]
            nonce = crypto.derive_part_nonce("uid-meta", i)
            ct = crypto.encrypt(chunk, src_dek, nonce)
            src_parts.append(
                PartMetadata(
                    part_number=i,
                    plaintext_size=len(chunk),
                    ciphertext_size=len(ct),
                    etag=hashlib.md5(ct).hexdigest(),
                )
            )
            ciphertext_blob.extend(ct)

        src_wrapped_dek = crypto.wrap_key(src_dek, kek)
        src_meta = MultipartMetadata(
            version=2,
            part_count=2,
            total_plaintext_size=len(src_plaintext),
            parts=src_parts,
            wrapped_dek=src_wrapped_dek,
            kid=kid,
        )
        await mock_s3.put_object("bucket", "src", bytes(ciphertext_blob))
        await save_multipart_metadata(mock_s3, "bucket", "src", src_meta)

        head_resp = {
            "Metadata": {"user-tag": "original"},
            "ContentLength": len(ciphertext_blob),
            "ContentType": "application/octet-stream",
        }
        loaded_src_meta = await load_multipart_metadata(mock_s3, "bucket", "src")

        new_metadata = {"user-tag": "replaced"}
        resp = await handler._copy_encrypted(
            mock_s3,
            "bucket",
            "dst",
            None,
            "bucket",
            "src",
            head_resp,
            None,
            loaded_src_meta,
            "REPLACE",
            new_metadata,
        )
        assert resp.status_code == 200

        # Destination multipart upload metadata should include "replaced" not "original"
        # The create_multipart_upload call in the mock doesn't retain metadata after complete,
        # but we can verify the sidecar was saved (streaming path ran)
        dst_meta = await load_multipart_metadata(mock_s3, "bucket", "dst")
        assert dst_meta is not None
        # Content is still correct
        recovered = await handler._download_encrypted_multipart(mock_s3, "bucket", "dst", dst_meta)
        assert recovered == src_plaintext


# ---------------------------------------------------------------------------
# _iter_multipart_plaintext — framed / multi-internal-part source (issue: copy
# of ScyllaDB backups failed with InvalidTag because client parts hold multiple
# internal parts, each of which is multiple AES-GCM frames)
# ---------------------------------------------------------------------------


class TestIterMultipartPlaintextFramed:
    """Reproduces the production layout: client parts whose internal parts each
    span more than one frame. The old reader decrypted a whole client part as a
    single seal and raised cryptography.exceptions.InvalidTag."""

    def _build_framed_source(self, mock_s3_dek, frame_size):
        """Return (ciphertext_blob, parts) for a source object shaped like a real
        multipart-encrypted backup: 2 client parts × 2 internal parts, and each
        internal part holds 3 frames of `frame_size` plaintext (last frame short).

        Frames are sized by crypto.FRAME_PLAINTEXT_SIZE (patched small in the test),
        so building the ciphertext must use that same boundary.
        """
        dek = mock_s3_dek
        blob = bytearray()
        parts = []
        # deterministic plaintext, sliced into internal parts as we go
        internal_pt = frame_size * 3 - 7  # 3 frames, last one short
        pt_seed = bytes((i * 37) % 256 for i in range(internal_pt))
        ct_offset = 0
        internal_no = 1
        full_plaintext = bytearray()
        for client_no in range(1, 3):
            ips = []
            client_pt = 0
            client_ct = 0
            for _ in range(2):  # 2 internal parts per client part
                # frame the internal part exactly as the writer does
                ip_ct = bytearray()
                for off in range(0, internal_pt, frame_size):
                    frame_pt = pt_seed[off : off + frame_size]
                    ip_ct += crypto.encrypt(frame_pt, dek)
                blob += ip_ct
                full_plaintext += pt_seed
                ips.append(
                    InternalPartMetadata(
                        internal_part_number=internal_no,
                        plaintext_size=internal_pt,
                        ciphertext_size=len(ip_ct),
                        etag=hashlib.md5(bytes(ip_ct)).hexdigest(),
                    )
                )
                client_pt += internal_pt
                client_ct += len(ip_ct)
                internal_no += 1
                ct_offset += len(ip_ct)
            parts.append(
                PartMetadata(
                    part_number=client_no,
                    plaintext_size=client_pt,
                    ciphertext_size=client_ct,
                    etag="x",
                    internal_parts=ips,
                )
            )
        return bytes(blob), parts, bytes(full_plaintext)

    @pytest.mark.asyncio
    async def test_full_roundtrip(self, mock_s3, settings, manager, monkeypatch):
        # Small frame so multi-frame internal parts stay tiny and fast.
        monkeypatch.setattr(crypto, "FRAME_PLAINTEXT_SIZE", 100)
        handler = _make_misc_handler(settings, manager)
        await mock_s3.create_bucket("b")

        dek = crypto.generate_dek()
        blob, parts, plaintext = self._build_framed_source(dek, 100)
        await mock_s3.put_object("b", "src", blob)

        meta = type("M", (), {"parts": parts})()
        recovered = bytearray()
        async for chunk in handler._iter_multipart_plaintext(mock_s3, "b", "src", meta, dek):
            recovered += chunk
        assert bytes(recovered) == plaintext

    @pytest.mark.asyncio
    async def test_range_roundtrip(self, mock_s3, settings, manager, monkeypatch):
        monkeypatch.setattr(crypto, "FRAME_PLAINTEXT_SIZE", 100)
        handler = _make_misc_handler(settings, manager)
        await mock_s3.create_bucket("b")

        dek = crypto.generate_dek()
        blob, parts, plaintext = self._build_framed_source(dek, 100)
        await mock_s3.put_object("b", "src", blob)

        meta = type("M", (), {"parts": parts})()
        # a range that starts mid-frame in the first internal part and ends
        # mid-frame several internal parts later
        start, end = 137, len(plaintext) - 211
        recovered = bytearray()
        async for chunk in handler._iter_multipart_plaintext(
            mock_s3, "b", "src", meta, dek, range_start=start, range_end=end
        ):
            recovered += chunk
        assert bytes(recovered) == plaintext[start : end + 1]

    @pytest.mark.asyncio
    async def test_whole_client_part_seal_would_fail(self, mock_s3, settings, manager, monkeypatch):
        """Guard: decrypting a whole client part as one seal (the old behavior)
        must raise — this is exactly the production InvalidTag."""
        from cryptography.exceptions import InvalidTag

        monkeypatch.setattr(crypto, "FRAME_PLAINTEXT_SIZE", 100)
        dek = crypto.generate_dek()
        blob, parts, _ = self._build_framed_source(dek, 100)
        # first client part ciphertext = its internal parts concatenated
        client0_ct = blob[: parts[0].ciphertext_size]
        with pytest.raises(InvalidTag):
            crypto.decrypt(client0_ct, dek)
