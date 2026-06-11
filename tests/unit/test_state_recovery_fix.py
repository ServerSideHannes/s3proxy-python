"""Unit tests for state recovery fix.

These tests verify that the state recovery bug has been fixed:
- State reconstruction from S3 using ListParts API
- Proper restoration of part metadata
- Correct next_internal_part_number tracking
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from s3proxy import crypto
from s3proxy.keyring import KeyRing
from s3proxy.state import (
    MultipartUploadState,
    PartMetadata,
    reconstruct_upload_state_from_s3,
)

# Encryption overhead (nonce + tag)
ENCRYPTION_OVERHEAD = crypto.NONCE_SIZE + crypto.TAG_SIZE  # 12 + 16 = 28


RECOVERY_KID = "AKIA-TEST"


def _single_key_ring(kek: bytes) -> KeyRing:
    """A KeyRing holding `kek` under the kid the recovery state carries."""
    return KeyRing(keys={RECOVERY_KID: kek})


class TestStateReconstruction:
    """Test state reconstruction from S3."""

    @pytest.mark.asyncio
    async def test_reconstruct_state_with_multiple_parts(self):
        """Test reconstructing state from S3 with multiple uploaded parts."""
        # Setup mock S3 client
        mock_s3_client = AsyncMock()
        bucket = "test-bucket"
        key = "test-key"
        upload_id = "test-upload-id"
        kek = crypto.generate_dek()  # Using DEK as KEK for test
        dek = crypto.generate_dek()

        # Mock load_upload_state to return DEK
        wrapped_dek = crypto.wrap_key(dek, kek)
        import base64

        from s3proxy.state import json_dumps

        state_data = json_dumps(
            {"dek": base64.b64encode(wrapped_dek).decode(), "kid": RECOVERY_KID}
        )

        async def mock_get_object(bucket_name, key_name):
            return {"Body": AsyncMock(read=AsyncMock(return_value=state_data))}

        mock_s3_client.get_object = mock_get_object

        # Mock list_all_parts to return 3 internal parts spanning 3 client parts
        # With MAX_INTERNAL_PARTS_PER_CLIENT=20:
        # - Client part 1 -> internal part 1
        # - Client part 2 -> internal part 21
        # - Client part 3 -> internal part 41
        mock_s3_client.list_all_parts = AsyncMock(
            return_value=[
                {
                    "PartNumber": 1,  # Client part 1
                    "Size": 5_242_880 + ENCRYPTION_OVERHEAD,
                    "ETag": '"etag-1"',
                },
                {
                    "PartNumber": 21,  # Client part 2
                    "Size": 5_242_880 + ENCRYPTION_OVERHEAD,
                    "ETag": '"etag-2"',
                },
                {
                    "PartNumber": 41,  # Client part 3
                    "Size": 3_000_000 + ENCRYPTION_OVERHEAD,
                    "ETag": '"etag-3"',
                },
            ]
        )

        # Reconstruct state
        state = await reconstruct_upload_state_from_s3(
            mock_s3_client, bucket, key, upload_id, _single_key_ring(kek)
        )

        # Verify reconstruction
        assert state is not None
        assert state.bucket == bucket
        assert state.key == key
        assert state.upload_id == upload_id
        assert state.dek == dek
        assert len(state.parts) == 3
        assert 1 in state.parts
        assert 2 in state.parts
        assert 3 in state.parts

        # Verify part metadata
        assert state.parts[1].part_number == 1
        assert state.parts[1].etag == "etag-1"
        assert state.parts[2].part_number == 2
        assert state.parts[3].part_number == 3

        # Verify next_internal_part_number is correct (max internal part + 1)
        assert state.next_internal_part_number == 42

        # Verify total size is tracked
        expected_size = (5_242_880 * 2) + 3_000_000
        assert state.total_plaintext_size == expected_size

    @pytest.mark.asyncio
    async def test_reconstruct_state_with_no_parts(self):
        """Test reconstructing state when no parts have been uploaded yet."""
        mock_s3_client = AsyncMock()
        bucket = "test-bucket"
        key = "test-key"
        upload_id = "test-upload-id"
        kek = crypto.generate_dek()
        dek = crypto.generate_dek()

        # Mock load_upload_state to return DEK
        wrapped_dek = crypto.wrap_key(dek, kek)
        import base64

        from s3proxy.state import json_dumps

        state_data = json_dumps(
            {"dek": base64.b64encode(wrapped_dek).decode(), "kid": RECOVERY_KID}
        )

        async def mock_get_object(bucket_name, key_name):
            return {"Body": AsyncMock(read=AsyncMock(return_value=state_data))}

        mock_s3_client.get_object = mock_get_object

        # Mock list_all_parts to return empty list
        mock_s3_client.list_all_parts = AsyncMock(return_value=[])

        # Reconstruct state
        state = await reconstruct_upload_state_from_s3(
            mock_s3_client, bucket, key, upload_id, _single_key_ring(kek)
        )

        # Verify empty state is created
        assert state is not None
        assert len(state.parts) == 0
        assert state.next_internal_part_number == 1
        assert state.total_plaintext_size == 0

    @pytest.mark.asyncio
    async def test_reconstruct_fails_when_dek_missing(self):
        """Test that reconstruction fails gracefully when DEK is not in S3."""
        mock_s3_client = AsyncMock()
        bucket = "test-bucket"
        key = "test-key"
        upload_id = "test-upload-id"
        kek = crypto.generate_dek()

        # Mock load_upload_state to fail (DEK not found)
        async def mock_get_object(bucket_name, key_name):
            raise Exception("Not found")

        mock_s3_client.get_object = mock_get_object

        # Reconstruct state should return None
        state = await reconstruct_upload_state_from_s3(
            mock_s3_client, bucket, key, upload_id, _single_key_ring(kek)
        )

        assert state is None

    @pytest.mark.asyncio
    async def test_reconstruct_fails_when_list_parts_fails(self):
        """Test that reconstruction fails gracefully when ListParts fails."""
        mock_s3_client = AsyncMock()
        bucket = "test-bucket"
        key = "test-key"
        upload_id = "test-upload-id"
        kek = crypto.generate_dek()
        dek = crypto.generate_dek()

        # Mock load_upload_state to return DEK
        wrapped_dek = crypto.wrap_key(dek, kek)
        import base64

        from s3proxy.state import json_dumps

        state_data = json_dumps(
            {"dek": base64.b64encode(wrapped_dek).decode(), "kid": RECOVERY_KID}
        )

        async def mock_get_object(bucket_name, key_name):
            return {"Body": AsyncMock(read=AsyncMock(return_value=state_data))}

        mock_s3_client.get_object = mock_get_object

        # Mock list_all_parts to fail
        mock_s3_client.list_all_parts = AsyncMock(side_effect=Exception("List failed"))

        # Reconstruct state should return None
        state = await reconstruct_upload_state_from_s3(
            mock_s3_client, bucket, key, upload_id, _single_key_ring(kek)
        )

        assert state is None

    @pytest.mark.asyncio
    async def test_reconstruct_with_out_of_order_parts(self):
        """Test reconstructing state when parts were uploaded out of order."""
        mock_s3_client = AsyncMock()
        bucket = "test-bucket"
        key = "test-key"
        upload_id = "test-upload-id"
        kek = crypto.generate_dek()
        dek = crypto.generate_dek()

        # Mock load_upload_state
        wrapped_dek = crypto.wrap_key(dek, kek)
        import base64

        from s3proxy.state import json_dumps

        state_data = json_dumps(
            {"dek": base64.b64encode(wrapped_dek).decode(), "kid": RECOVERY_KID}
        )

        async def mock_get_object(bucket_name, key_name):
            return {"Body": AsyncMock(read=AsyncMock(return_value=state_data))}

        mock_s3_client.get_object = mock_get_object

        # Mock list_all_parts with out-of-order client parts (using internal part numbers)
        # With MAX_INTERNAL_PARTS_PER_CLIENT=20:
        # - Client part 1 -> internal part 1
        # - Client part 3 -> internal part 41
        # - Client part 5 -> internal part 81
        mock_s3_client.list_all_parts = AsyncMock(
            return_value=[
                {
                    "PartNumber": 1,  # Client part 1
                    "Size": 5_242_880 + ENCRYPTION_OVERHEAD,
                    "ETag": '"etag-1"',
                },
                {
                    "PartNumber": 41,  # Client part 3
                    "Size": 5_242_880 + ENCRYPTION_OVERHEAD,
                    "ETag": '"etag-3"',
                },
                {
                    "PartNumber": 81,  # Client part 5
                    "Size": 5_242_880 + ENCRYPTION_OVERHEAD,
                    "ETag": '"etag-5"',
                },
            ]
        )

        # Reconstruct state
        state = await reconstruct_upload_state_from_s3(
            mock_s3_client, bucket, key, upload_id, _single_key_ring(kek)
        )

        # Verify all client parts are present
        assert state is not None
        assert len(state.parts) == 3
        assert 1 in state.parts
        assert 3 in state.parts
        assert 5 in state.parts

        # next_internal_part_number should be max internal + 1
        assert state.next_internal_part_number == 82


class TestStateRecoveryIntegration:
    """Test that state recovery works in the multipart manager."""

    @pytest.mark.asyncio
    async def test_store_and_retrieve_reconstructed_state(self):
        """Test storing and retrieving a reconstructed state."""
        from s3proxy.state import MultipartStateManager

        manager = MultipartStateManager()
        bucket = "test-bucket"
        key = "test-key"
        upload_id = "test-upload"
        dek = crypto.generate_dek()

        # Create a reconstructed state (simulating recovery)
        reconstructed_state = MultipartUploadState(
            bucket=bucket,
            key=key,
            upload_id=upload_id,
            dek=dek,
            parts={
                1: PartMetadata(
                    part_number=1,
                    plaintext_size=5_242_880,
                    ciphertext_size=5_242_880 + ENCRYPTION_OVERHEAD,
                    etag="etag-1",
                    md5="md5-1",
                ),
                2: PartMetadata(
                    part_number=2,
                    plaintext_size=3_000_000,
                    ciphertext_size=3_000_000 + ENCRYPTION_OVERHEAD,
                    etag="etag-2",
                    md5="md5-2",
                ),
            },
            total_plaintext_size=8_242_880,
            next_internal_part_number=3,
            created_at=datetime.now(UTC),
        )

        # Store the reconstructed state
        await manager.store_reconstructed_state(bucket, key, upload_id, reconstructed_state)

        # Retrieve and verify
        retrieved = await manager.get_upload(bucket, key, upload_id)
        assert retrieved is not None
        assert retrieved.bucket == bucket
        assert retrieved.upload_id == upload_id
        assert len(retrieved.parts) == 2
        assert 1 in retrieved.parts
        assert 2 in retrieved.parts
        assert retrieved.next_internal_part_number == 3
        assert retrieved.total_plaintext_size == 8_242_880


class TestListAllPartsMethod:
    """Test S3Client.list_all_parts follows IsTruncated pagination."""

    @pytest.mark.asyncio
    async def test_follows_pagination(self):
        """list_all_parts must issue successive calls until IsTruncated is False."""
        from unittest.mock import patch

        from s3proxy.client.s3 import S3Client

        client = S3Client.__new__(S3Client)

        page1_parts = [
            {"PartNumber": i, "Size": 100, "ETag": f'"etag-{i}"'} for i in range(1, 1001)
        ]
        page2_parts = [{"PartNumber": 1001, "Size": 50, "ETag": '"etag-1001"'}]
        calls: list[int | None] = []

        async def fake_list_parts(bucket, key, upload_id, part_number_marker=None, max_parts=1000):
            calls.append(part_number_marker)
            if part_number_marker is None:
                return {"Parts": page1_parts, "IsTruncated": True, "NextPartNumberMarker": 1000}
            return {"Parts": page2_parts, "IsTruncated": False}

        with patch.object(client, "list_parts", fake_list_parts):
            result = await client.list_all_parts("bucket", "key", "upload-id")

        assert len(result) == 1001
        assert result[-1]["PartNumber"] == 1001
        assert calls == [None, 1000], f"unexpected call sequence: {calls}"

    @pytest.mark.asyncio
    async def test_single_page_no_extra_call(self):
        """list_all_parts stops after the first non-truncated page."""
        from unittest.mock import patch

        from s3proxy.client.s3 import S3Client

        client = S3Client.__new__(S3Client)
        calls: list = []

        async def fake_list_parts(bucket, key, upload_id, part_number_marker=None, max_parts=1000):
            calls.append(part_number_marker)
            return {"Parts": [{"PartNumber": 1}], "IsTruncated": False}

        with patch.object(client, "list_parts", fake_list_parts):
            result = await client.list_all_parts("bucket", "key", "upload-id")

        assert result == [{"PartNumber": 1}]
        assert len(calls) == 1


class TestRecoveryWithPaginatedParts:
    """Recovery must reconstruct state from >1000 internal parts (two ListParts pages)."""

    @pytest.mark.asyncio
    async def test_reconstruct_over_1000_parts(self):
        """reconstruct_upload_state_from_s3 must include parts from all pages."""
        import base64

        from s3proxy.state import json_dumps

        kek = crypto.generate_dek()
        dek = crypto.generate_dek()
        wrapped_dek = crypto.wrap_key(dek, kek)
        state_data = json_dumps(
            {"dek": base64.b64encode(wrapped_dek).decode(), "kid": RECOVERY_KID}
        )

        mock_s3_client = AsyncMock()

        async def mock_get_object(bucket_name, key_name):
            return {"Body": AsyncMock(read=AsyncMock(return_value=state_data))}

        mock_s3_client.get_object = mock_get_object

        # list_all_parts returns 1001 parts across two pages (simulating paginated fetch)
        # Parts 1–1000 → client parts 1–50; part 1001 → client part 51
        all_parts = [
            {"PartNumber": i, "Size": 100 + ENCRYPTION_OVERHEAD, "ETag": f'"etag-{i}"'}
            for i in range(1, 1002)
        ]
        mock_s3_client.list_all_parts = AsyncMock(return_value=all_parts)

        state = await reconstruct_upload_state_from_s3(
            mock_s3_client, "bucket", "key", "upload-id", _single_key_ring(kek)
        )

        assert state is not None
        assert 51 in state.parts, "client part 51 (internal 1001) must be present"
        assert state.next_internal_part_number == 1002
        assert len(state.parts) == 51
