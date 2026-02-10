"""Tests for multipart upload handling."""

import base64
import gzip
import json
from unittest.mock import AsyncMock, patch

import pytest

from s3proxy.state import (
    InternalPartMetadata,
    MultipartMetadata,
    MultipartStateManager,
    MultipartUploadState,
    PartMetadata,
    StateMissingError,
    calculate_part_range,
    decode_multipart_metadata,
    encode_multipart_metadata,
    reconstruct_upload_state_from_s3,
)
from s3proxy.state.serialization import (
    deserialize_upload_state,
    serialize_upload_state,
)


class TestMultipartStateManager:
    """Test multipart state management."""

    @pytest.mark.asyncio
    async def test_create_upload(self):
        """Test creating upload state."""
        manager = MultipartStateManager()
        dek = b"x" * 32

        state = await manager.create_upload("bucket", "key", "upload-123", dek)

        assert state.bucket == "bucket"
        assert state.key == "key"
        assert state.upload_id == "upload-123"
        assert state.dek == dek
        assert len(state.parts) == 0

    @pytest.mark.asyncio
    async def test_get_upload(self):
        """Test retrieving upload state."""
        manager = MultipartStateManager()
        dek = b"x" * 32

        await manager.create_upload("bucket", "key", "upload-123", dek)
        state = await manager.get_upload("bucket", "key", "upload-123")

        assert state is not None
        assert state.upload_id == "upload-123"

    @pytest.mark.asyncio
    async def test_get_nonexistent_upload(self):
        """Test getting non-existent upload returns None."""
        manager = MultipartStateManager()

        state = await manager.get_upload("bucket", "key", "nonexistent")

        assert state is None

    @pytest.mark.asyncio
    async def test_add_part(self):
        """Test adding part to upload."""
        manager = MultipartStateManager()
        dek = b"x" * 32

        await manager.create_upload("bucket", "key", "upload-123", dek)
        part = PartMetadata(
            part_number=1,
            plaintext_size=1000,
            ciphertext_size=1028,
            etag="abc123",
        )
        await manager.add_part("bucket", "key", "upload-123", part)

        state = await manager.get_upload("bucket", "key", "upload-123")
        assert 1 in state.parts
        assert state.parts[1].plaintext_size == 1000
        assert state.total_plaintext_size == 1000

    @pytest.mark.asyncio
    async def test_complete_upload(self):
        """Test completing upload removes state."""
        manager = MultipartStateManager()
        dek = b"x" * 32

        await manager.create_upload("bucket", "key", "upload-123", dek)
        state = await manager.complete_upload("bucket", "key", "upload-123")

        assert state is not None
        assert await manager.get_upload("bucket", "key", "upload-123") is None

    @pytest.mark.asyncio
    async def test_abort_upload(self):
        """Test aborting upload removes state."""
        manager = MultipartStateManager()
        dek = b"x" * 32

        await manager.create_upload("bucket", "key", "upload-123", dek)
        await manager.abort_upload("bucket", "key", "upload-123")

        assert await manager.get_upload("bucket", "key", "upload-123") is None


class TestMetadataEncoding:
    """Test metadata encoding/decoding."""

    def test_encode_decode_roundtrip(self):
        """Test metadata encode/decode roundtrip."""
        meta = MultipartMetadata(
            version=1,
            part_count=3,
            total_plaintext_size=3000,
            wrapped_dek=b"wrapped-key-data",
            parts=[
                PartMetadata(1, 1000, 1028, "etag1", "md5-1"),
                PartMetadata(2, 1000, 1028, "etag2", "md5-2"),
                PartMetadata(3, 1000, 1028, "etag3", "md5-3"),
            ],
        )

        encoded = encode_multipart_metadata(meta)
        decoded = decode_multipart_metadata(encoded)

        assert decoded.version == meta.version
        assert decoded.part_count == meta.part_count
        assert decoded.total_plaintext_size == meta.total_plaintext_size
        assert decoded.wrapped_dek == meta.wrapped_dek
        assert len(decoded.parts) == len(meta.parts)

    def test_encoded_is_compressed(self):
        """Test encoded metadata is base64(gzip(json))."""
        meta = MultipartMetadata(
            version=1,
            part_count=1,
            total_plaintext_size=1000,
            wrapped_dek=b"key",
            parts=[PartMetadata(1, 1000, 1028, "etag", "md5")],
        )

        encoded = encode_multipart_metadata(meta)

        # Should be valid base64
        compressed = base64.b64decode(encoded)

        # Should be valid gzip
        decompressed = gzip.decompress(compressed)

        # Should be valid JSON
        data = json.loads(decompressed)
        assert "v" in data
        assert "parts" in data


class TestCalculatePartRange:
    """Test part range calculation for range requests."""

    @pytest.fixture
    def parts(self):
        """Create sample parts metadata."""
        return [
            PartMetadata(1, 1000, 1028, "etag1"),  # bytes 0-999
            PartMetadata(2, 1000, 1028, "etag2"),  # bytes 1000-1999
            PartMetadata(3, 1000, 1028, "etag3"),  # bytes 2000-2999
        ]

    def test_range_single_part(self, parts):
        """Test range within single part."""
        result = calculate_part_range(parts, 100, 200)

        assert len(result) == 1
        part_num, start, end = result[0]
        assert part_num == 1
        assert start == 100
        assert end == 200

    def test_range_spans_parts(self, parts):
        """Test range spanning multiple parts."""
        result = calculate_part_range(parts, 900, 1100)

        assert len(result) == 2
        # First part: bytes 900-999 of part 1
        assert result[0] == (1, 900, 999)
        # Second part: bytes 0-100 of part 2
        assert result[1] == (2, 0, 100)

    def test_range_full_object(self, parts):
        """Test range for full object."""
        result = calculate_part_range(parts, 0, 2999)

        assert len(result) == 3
        assert result[0] == (1, 0, 999)
        assert result[1] == (2, 0, 999)
        assert result[2] == (3, 0, 999)

    def test_range_open_ended(self, parts):
        """Test open-ended range (bytes 1500-)."""
        result = calculate_part_range(parts, 1500, None)

        assert len(result) == 2
        assert result[0] == (2, 500, 999)  # Part 2: bytes 500-999
        assert result[1] == (3, 0, 999)  # Part 3: full

    def test_range_suffix(self, parts):
        """Test suffix range (last 500 bytes)."""
        # Total size is 3000, so -500 means 2500-2999
        result = calculate_part_range(parts, 2500, 2999)

        assert len(result) == 1
        assert result[0] == (3, 500, 999)

    def test_range_beyond_object(self, parts):
        """Test range starting beyond object."""
        result = calculate_part_range(parts, 5000, 6000)

        assert len(result) == 0


class TestInternalPartMetadata:
    """Test internal part metadata structure."""

    def test_create_internal_part(self):
        """Test creating internal part metadata."""
        ip = InternalPartMetadata(
            internal_part_number=1,
            plaintext_size=16 * 1024 * 1024,  # 16MB
            ciphertext_size=16 * 1024 * 1024 + 28,  # +nonce+tag
            etag="abc123",
        )
        assert ip.internal_part_number == 1
        assert ip.plaintext_size == 16 * 1024 * 1024
        assert ip.etag == "abc123"


class TestPartMetadataWithInternalParts:
    """Test part metadata with internal sub-parts."""

    def test_part_with_single_internal_part(self):
        """Test client part mapped to single internal part."""
        part = PartMetadata(
            part_number=1,
            plaintext_size=5 * 1024 * 1024,  # 5MB
            ciphertext_size=5 * 1024 * 1024 + 28,
            etag="client-md5",
            internal_parts=[
                InternalPartMetadata(
                    internal_part_number=1,
                    plaintext_size=5 * 1024 * 1024,
                    ciphertext_size=5 * 1024 * 1024 + 28,
                    etag="s3-etag-1",
                )
            ],
        )
        assert part.part_number == 1
        assert len(part.internal_parts) == 1
        assert part.internal_parts[0].internal_part_number == 1

    def test_part_with_multiple_internal_parts(self):
        """Test large client part split into multiple internal parts."""
        # 40MB client part split into 3 internal parts (16MB + 16MB + 8MB)
        internal_parts = [
            InternalPartMetadata(1, 16 * 1024 * 1024, 16 * 1024 * 1024 + 28, "etag1"),
            InternalPartMetadata(2, 16 * 1024 * 1024, 16 * 1024 * 1024 + 28, "etag2"),
            InternalPartMetadata(3, 8 * 1024 * 1024, 8 * 1024 * 1024 + 28, "etag3"),
        ]
        total_plaintext = sum(ip.plaintext_size for ip in internal_parts)
        total_ciphertext = sum(ip.ciphertext_size for ip in internal_parts)

        part = PartMetadata(
            part_number=1,
            plaintext_size=total_plaintext,
            ciphertext_size=total_ciphertext,
            etag="client-md5",
            internal_parts=internal_parts,
        )

        assert part.plaintext_size == 40 * 1024 * 1024
        assert len(part.internal_parts) == 3

    def test_part_without_internal_parts_backward_compat(self):
        """Test part metadata without internal parts for backward compatibility."""
        part = PartMetadata(
            part_number=1,
            plaintext_size=1000,
            ciphertext_size=1028,
            etag="etag",
        )
        assert part.internal_parts == []


class TestMultipartStateWithInternalParts:
    """Test multipart upload state with internal part tracking."""

    @pytest.mark.asyncio
    async def test_allocate_internal_parts_basic(self):
        """Test allocating internal part numbers."""
        manager = MultipartStateManager()
        dek = b"x" * 32

        await manager.create_upload("bucket", "key", "upload-123", dek)

        # Allocate 3 parts
        start = await manager.allocate_internal_parts("bucket", "key", "upload-123", 3)
        assert start == 1

        # Next allocation starts at 4
        start2 = await manager.allocate_internal_parts("bucket", "key", "upload-123", 2)
        assert start2 == 4

    @pytest.mark.asyncio
    async def test_allocate_internal_parts_for_nonexistent_upload(self):
        """Test allocating parts for non-existent upload returns default."""
        manager = MultipartStateManager()

        start = await manager.allocate_internal_parts("bucket", "key", "nonexistent", 5)
        assert start == 1

    @pytest.mark.asyncio
    async def test_add_part_with_internal_parts(self):
        """Test adding part with internal sub-parts."""
        manager = MultipartStateManager()
        dek = b"x" * 32

        await manager.create_upload("bucket", "key", "upload-123", dek)

        # Simulate uploading a 40MB client part split into 3 internal parts
        internal_parts = [
            InternalPartMetadata(1, 16 * 1024 * 1024, 16 * 1024 * 1024 + 28, "etag1"),
            InternalPartMetadata(2, 16 * 1024 * 1024, 16 * 1024 * 1024 + 28, "etag2"),
            InternalPartMetadata(3, 8 * 1024 * 1024, 8 * 1024 * 1024 + 28, "etag3"),
        ]
        part = PartMetadata(
            part_number=1,
            plaintext_size=40 * 1024 * 1024,
            ciphertext_size=sum(ip.ciphertext_size for ip in internal_parts),
            etag="client-md5",
            internal_parts=internal_parts,
        )
        await manager.add_part("bucket", "key", "upload-123", part)

        state = await manager.get_upload("bucket", "key", "upload-123")
        assert state is not None
        assert 1 in state.parts
        assert len(state.parts[1].internal_parts) == 3
        assert state.next_internal_part_number == 4

    @pytest.mark.asyncio
    async def test_add_multiple_parts_sequential_internal_numbers(self):
        """Test multiple client parts have sequential internal part numbers."""
        manager = MultipartStateManager()
        dek = b"x" * 32

        await manager.create_upload("bucket", "key", "upload-123", dek)

        # Client part 1: uses internal parts 1-3
        part1 = PartMetadata(
            part_number=1,
            plaintext_size=40 * 1024 * 1024,
            ciphertext_size=40 * 1024 * 1024 + 84,
            etag="md5-1",
            internal_parts=[
                InternalPartMetadata(1, 16 * 1024 * 1024, 16 * 1024 * 1024 + 28, "e1"),
                InternalPartMetadata(2, 16 * 1024 * 1024, 16 * 1024 * 1024 + 28, "e2"),
                InternalPartMetadata(3, 8 * 1024 * 1024, 8 * 1024 * 1024 + 28, "e3"),
            ],
        )
        await manager.add_part("bucket", "key", "upload-123", part1)

        # Client part 2: uses internal parts 4-6
        part2 = PartMetadata(
            part_number=2,
            plaintext_size=40 * 1024 * 1024,
            ciphertext_size=40 * 1024 * 1024 + 84,
            etag="md5-2",
            internal_parts=[
                InternalPartMetadata(4, 16 * 1024 * 1024, 16 * 1024 * 1024 + 28, "e4"),
                InternalPartMetadata(5, 16 * 1024 * 1024, 16 * 1024 * 1024 + 28, "e5"),
                InternalPartMetadata(6, 8 * 1024 * 1024, 8 * 1024 * 1024 + 28, "e6"),
            ],
        )
        await manager.add_part("bucket", "key", "upload-123", part2)

        state = await manager.get_upload("bucket", "key", "upload-123")
        assert state.next_internal_part_number == 7

    @pytest.mark.asyncio
    async def test_initial_next_internal_part_number(self):
        """Test new uploads start with next_internal_part_number=1."""
        manager = MultipartStateManager()
        dek = b"x" * 32

        state = await manager.create_upload("bucket", "key", "upload-123", dek)
        assert state.next_internal_part_number == 1

    @pytest.mark.asyncio
    async def test_allocate_internal_parts_no_collision_with_mixed_sizes(self):
        """Test that allocations don't collide when mixing split and non-split parts.

        This tests the fix for a bug where:
        - Part 1 (large, split) -> internal parts 1-3
        - Part 2 (large, split) -> internal parts 4-6
        - Part 3 (large, split) -> internal parts 7-9
        - Part 7 (small, no split) -> would incorrectly use internal part 7 (collision!)

        After the fix, ALL parts use the allocator, so:
        - Part 1 (large, split) -> internal parts 1-3
        - Part 2 (large, split) -> internal parts 4-6
        - Part 3 (large, split) -> internal parts 7-9
        - Part 7 (small, no split) -> allocates internal part 10 (no collision)
        """
        manager = MultipartStateManager()
        dek = b"x" * 32

        await manager.create_upload("bucket", "key", "upload-123", dek)

        # Simulate 3 large parts that each split into 3 internal parts
        for i in range(3):
            start = await manager.allocate_internal_parts("bucket", "key", "upload-123", 3)
            expected_start = 1 + (i * 3)
            assert start == expected_start, (
                f"Part {i + 1} should start at {expected_start}, got {start}"
            )

        # Now allocate for a small part (1 internal part)
        # This should NOT collide with any previous allocation
        start = await manager.allocate_internal_parts("bucket", "key", "upload-123", 1)
        assert start == 10, f"Small part should get internal part 10, got {start}"

        # All internal part numbers should be unique: 1-9 from large parts, 10 from small part
        state = await manager.get_upload("bucket", "key", "upload-123")
        assert state.next_internal_part_number == 11


class TestUploadStateSerialization:
    """Test serialization of upload state with internal parts."""

    def test_serialize_deserialize_with_internal_parts(self):
        """Test roundtrip of state with internal parts."""
        from datetime import UTC, datetime

        state = MultipartUploadState(
            dek=b"x" * 32,
            bucket="test-bucket",
            key="test-key",
            upload_id="upload-123",
            created_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
            total_plaintext_size=40 * 1024 * 1024,
            next_internal_part_number=4,
        )
        state.parts[1] = PartMetadata(
            part_number=1,
            plaintext_size=40 * 1024 * 1024,
            ciphertext_size=40 * 1024 * 1024 + 84,
            etag="client-md5",
            internal_parts=[
                InternalPartMetadata(1, 16 * 1024 * 1024, 16 * 1024 * 1024 + 28, "e1"),
                InternalPartMetadata(2, 16 * 1024 * 1024, 16 * 1024 * 1024 + 28, "e2"),
                InternalPartMetadata(3, 8 * 1024 * 1024, 8 * 1024 * 1024 + 28, "e3"),
            ],
        )

        serialized = serialize_upload_state(state)
        deserialized = deserialize_upload_state(serialized)

        assert deserialized.next_internal_part_number == 4
        assert 1 in deserialized.parts
        assert len(deserialized.parts[1].internal_parts) == 3
        assert deserialized.parts[1].internal_parts[0].internal_part_number == 1
        assert deserialized.parts[1].internal_parts[2].internal_part_number == 3


class TestMetadataEncodingWithInternalParts:
    """Test metadata encoding/decoding with internal parts."""

    def test_encode_decode_with_internal_parts(self):
        """Test roundtrip with internal parts."""
        meta = MultipartMetadata(
            version=1,
            part_count=2,
            total_plaintext_size=80 * 1024 * 1024,
            wrapped_dek=b"wrapped-key-data",
            parts=[
                PartMetadata(
                    part_number=1,
                    plaintext_size=40 * 1024 * 1024,
                    ciphertext_size=40 * 1024 * 1024 + 84,
                    etag="md5-1",
                    internal_parts=[
                        InternalPartMetadata(1, 16 * 1024 * 1024, 16 * 1024 * 1024 + 28, "e1"),
                        InternalPartMetadata(2, 16 * 1024 * 1024, 16 * 1024 * 1024 + 28, "e2"),
                        InternalPartMetadata(3, 8 * 1024 * 1024, 8 * 1024 * 1024 + 28, "e3"),
                    ],
                ),
                PartMetadata(
                    part_number=2,
                    plaintext_size=40 * 1024 * 1024,
                    ciphertext_size=40 * 1024 * 1024 + 84,
                    etag="md5-2",
                    internal_parts=[
                        InternalPartMetadata(4, 16 * 1024 * 1024, 16 * 1024 * 1024 + 28, "e4"),
                        InternalPartMetadata(5, 16 * 1024 * 1024, 16 * 1024 * 1024 + 28, "e5"),
                        InternalPartMetadata(6, 8 * 1024 * 1024, 8 * 1024 * 1024 + 28, "e6"),
                    ],
                ),
            ],
        )

        encoded = encode_multipart_metadata(meta)
        decoded = decode_multipart_metadata(encoded)

        assert decoded.part_count == 2
        assert len(decoded.parts) == 2
        assert len(decoded.parts[0].internal_parts) == 3
        assert len(decoded.parts[1].internal_parts) == 3
        assert decoded.parts[0].internal_parts[0].internal_part_number == 1
        assert decoded.parts[1].internal_parts[2].internal_part_number == 6

    def test_decode_without_internal_parts_backward_compat(self):
        """Test decoding metadata without internal parts (backward compat)."""
        # Simulate old-format metadata
        data = {
            "v": 1,
            "pc": 1,
            "ts": 1000,
            "dek": base64.b64encode(b"key").decode(),
            "parts": [
                {"pn": 1, "ps": 1000, "cs": 1028, "etag": "etag", "md5": "md5"},
            ],
        }
        json_bytes = json.dumps(data).encode()
        compressed = gzip.compress(json_bytes)
        encoded = base64.b64encode(compressed).decode()

        decoded = decode_multipart_metadata(encoded)

        assert len(decoded.parts) == 1
        assert decoded.parts[0].internal_parts == []

    def test_internal_parts_in_encoded_json(self):
        """Test internal parts are included in encoded format."""
        meta = MultipartMetadata(
            version=1,
            part_count=1,
            total_plaintext_size=16 * 1024 * 1024,
            wrapped_dek=b"key",
            parts=[
                PartMetadata(
                    part_number=1,
                    plaintext_size=16 * 1024 * 1024,
                    ciphertext_size=16 * 1024 * 1024 + 28,
                    etag="client-md5",
                    internal_parts=[
                        InternalPartMetadata(1, 16 * 1024 * 1024, 16 * 1024 * 1024 + 28, "s3-etag"),
                    ],
                ),
            ],
        )

        encoded = encode_multipart_metadata(meta)

        # Decode to check structure
        compressed = base64.b64decode(encoded)
        decompressed = gzip.decompress(compressed)
        data = json.loads(decompressed)

        # Check internal parts are present
        assert "ip" in data["parts"][0]
        assert len(data["parts"][0]["ip"]) == 1
        assert data["parts"][0]["ip"][0]["ipn"] == 1


class TestStateMissingError:
    """Tests for StateMissingError exception during add_part."""

    def test_state_missing_error_is_exception(self):
        """Test StateMissingError is a proper exception."""
        error = StateMissingError("test message")
        assert isinstance(error, Exception)
        assert str(error) == "test message"

    def test_state_missing_error_can_be_raised_and_caught(self):
        """Test StateMissingError can be raised and caught."""
        with pytest.raises(StateMissingError) as exc_info:
            raise StateMissingError("Upload state missing for bucket/key/upload-123")
        assert "bucket/key/upload-123" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_add_part_raises_state_missing_error_when_state_missing(self):
        """Test add_part raises StateMissingError when state is missing."""
        manager = MultipartStateManager()
        part = PartMetadata(
            part_number=1,
            plaintext_size=1000,
            ciphertext_size=1028,
            etag="abc123",
        )

        # No upload created - state is missing
        with pytest.raises(StateMissingError) as exc_info:
            await manager.add_part("bucket", "key", "upload-123", part)

        assert "bucket/key/upload-123" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_add_part_no_error_when_state_exists(self):
        """Test add_part succeeds when state exists in memory."""
        manager = MultipartStateManager()
        dek = b"x" * 32

        # Create upload first
        await manager.create_upload("bucket", "key", "upload-123", dek)

        # Add part should work without raising StateMissingError
        part = PartMetadata(
            part_number=1,
            plaintext_size=1000,
            ciphertext_size=1028,
            etag="abc123",
        )
        # Should not raise
        await manager.add_part("bucket", "key", "upload-123", part)

        # Verify part was added
        state = await manager.get_upload("bucket", "key", "upload-123")
        assert 1 in state.parts

    @pytest.mark.asyncio
    async def test_add_part_raises_state_missing_error_with_custom_store(self):
        """Test add_part raises StateMissingError with any storage backend."""
        from s3proxy.state.storage import MemoryStateStore

        # Create manager with explicit MemoryStateStore
        store = MemoryStateStore()
        manager = MultipartStateManager(store=store)
        part = PartMetadata(
            part_number=1,
            plaintext_size=1000,
            ciphertext_size=1028,
            etag="abc123",
        )

        # State not created - should raise StateMissingError (consistent behavior)
        with pytest.raises(StateMissingError) as exc_info:
            await manager.add_part("bucket", "key", "nonexistent-upload", part)

        assert "bucket/key/nonexistent-upload" in str(exc_info.value)


class TestReconstructUploadStateFromS3:
    """Tests for reconstruct_upload_state_from_s3 function."""

    @pytest.mark.asyncio
    async def test_reconstruct_recovers_state_from_s3(self):
        """Test that reconstruct_upload_state_from_s3 recovers full state from S3."""
        # Setup: create mock S3 client
        mock_client = AsyncMock()

        # The DEK that was wrapped and stored in S3
        original_dek = b"x" * 32
        kek = b"k" * 32

        # Mock the internal upload state object containing wrapped DEK
        # load_upload_state calls get_object on the internal key
        wrapped_dek_data = {"dek": base64.b64encode(b"wrapped-dek-data").decode()}

        async def mock_read():
            return json.dumps(wrapped_dek_data).encode()

        mock_body = AsyncMock()
        mock_body.read = mock_read
        mock_client.get_object = AsyncMock(return_value={"Body": mock_body})

        # Mock list_parts to return some uploaded parts (using internal part numbers)
        # With MAX_INTERNAL_PARTS_PER_CLIENT=20:
        # - Client part 1 -> internal part 1
        # - Client part 2 -> internal part 21
        # - Client part 3 -> internal part 41
        mock_client.list_parts = AsyncMock(
            return_value={
                "Parts": [
                    {"PartNumber": 1, "Size": 1028, "ETag": '"etag1"'},  # Client part 1
                    {"PartNumber": 21, "Size": 2056, "ETag": '"etag2"'},  # Client part 2
                    {"PartNumber": 41, "Size": 1028, "ETag": '"etag3"'},  # Client part 3
                ]
            }
        )

        # Mock the crypto.unwrap_key to return the original DEK
        with patch("s3proxy.crypto.unwrap_key", return_value=original_dek):
            state = await reconstruct_upload_state_from_s3(
                mock_client, "bucket", "key", "upload-123", kek
            )

        # Verify state was reconstructed correctly
        assert state is not None
        assert state.bucket == "bucket"
        assert state.key == "key"
        assert state.upload_id == "upload-123"
        assert state.dek == original_dek

        # Verify all client parts were recovered
        assert len(state.parts) == 3
        assert 1 in state.parts
        assert 2 in state.parts
        assert 3 in state.parts

        # Verify part metadata
        assert state.parts[1].part_number == 1
        assert state.parts[1].ciphertext_size == 1028
        assert state.parts[1].etag == "etag1"

        assert state.parts[2].part_number == 2
        assert state.parts[2].ciphertext_size == 2056
        assert state.parts[2].etag == "etag2"

        # Verify next_internal_part_number is set correctly (max internal + 1)
        assert state.next_internal_part_number == 42

    @pytest.mark.asyncio
    async def test_reconstruct_returns_none_when_dek_not_found(self):
        """Test that reconstruct returns None when DEK is not in S3."""
        mock_client = AsyncMock()
        kek = b"k" * 32

        # Mock get_object to raise an exception (DEK not found)
        mock_client.get_object = AsyncMock(side_effect=Exception("NoSuchKey"))

        state = await reconstruct_upload_state_from_s3(
            mock_client, "bucket", "key", "upload-123", kek
        )

        assert state is None

    @pytest.mark.asyncio
    async def test_reconstruct_returns_none_when_list_parts_fails(self):
        """Test that reconstruct returns None when list_parts fails."""
        mock_client = AsyncMock()
        original_dek = b"x" * 32
        kek = b"k" * 32

        # Mock successful DEK retrieval
        wrapped_dek_data = {"dek": base64.b64encode(b"wrapped-dek-data").decode()}

        async def mock_read():
            return json.dumps(wrapped_dek_data).encode()

        mock_body = AsyncMock()
        mock_body.read = mock_read
        mock_client.get_object = AsyncMock(return_value={"Body": mock_body})

        # Mock list_parts to fail
        mock_client.list_parts = AsyncMock(side_effect=Exception("Upload not found"))

        with patch("s3proxy.crypto.unwrap_key", return_value=original_dek):
            state = await reconstruct_upload_state_from_s3(
                mock_client, "bucket", "key", "upload-123", kek
            )

        assert state is None

    @pytest.mark.asyncio
    async def test_reconstruct_handles_empty_parts_list(self):
        """Test reconstruct with no parts uploaded yet."""
        mock_client = AsyncMock()
        original_dek = b"x" * 32
        kek = b"k" * 32

        # Mock successful DEK retrieval
        wrapped_dek_data = {"dek": base64.b64encode(b"wrapped-dek-data").decode()}

        async def mock_read():
            return json.dumps(wrapped_dek_data).encode()

        mock_body = AsyncMock()
        mock_body.read = mock_read
        mock_client.get_object = AsyncMock(return_value={"Body": mock_body})

        # Mock list_parts to return empty list
        mock_client.list_parts = AsyncMock(return_value={"Parts": []})

        with patch("s3proxy.crypto.unwrap_key", return_value=original_dek):
            state = await reconstruct_upload_state_from_s3(
                mock_client, "bucket", "key", "upload-123", kek
            )

        assert state is not None
        assert len(state.parts) == 0
        assert state.next_internal_part_number == 1
        assert state.total_plaintext_size == 0


class TestParallelInternalPartUploads:
    """Test parallel internal part upload functionality."""

    @pytest.mark.asyncio
    async def test_parallel_uploads_maintain_order(self):
        """Test that parallel uploads maintain correct part order."""
        import asyncio

        # Simulate upload results arriving out of order
        results = []
        order_of_completion = []

        async def mock_upload(part_num: int, delay: float):
            """Simulate upload with variable delay."""
            await asyncio.sleep(delay)
            order_of_completion.append(part_num)
            return InternalPartMetadata(
                internal_part_number=part_num,
                plaintext_size=1000 * part_num,
                ciphertext_size=1000 * part_num + 16,
                etag=f"etag-{part_num}",
            )

        # Create tasks with delays that cause out-of-order completion
        # Part 3 completes first, then 1, then 2
        tasks = {
            1: asyncio.create_task(mock_upload(1, 0.02)),
            2: asyncio.create_task(mock_upload(2, 0.03)),
            3: asyncio.create_task(mock_upload(3, 0.01)),
        }

        # Gather results
        upload_results = await asyncio.gather(*tasks.values())

        # Sort by internal part number (as the real code does)
        part_num_to_result = {r.internal_part_number: r for r in upload_results}
        for pn in sorted(part_num_to_result.keys()):
            results.append(part_num_to_result[pn])

        # Verify completion was out of order
        assert order_of_completion == [3, 1, 2], "Parts should complete out of order"

        # Verify final results are in correct order
        assert [r.internal_part_number for r in results] == [1, 2, 3]
        assert results[0].plaintext_size == 1000
        assert results[1].plaintext_size == 2000
        assert results[2].plaintext_size == 3000

    @pytest.mark.asyncio
    async def test_semaphore_limits_concurrency(self):
        """Test that semaphore limits concurrent uploads."""
        import asyncio

        max_concurrent = 2
        semaphore = asyncio.Semaphore(max_concurrent)
        concurrent_count = 0
        max_observed_concurrent = 0

        async def mock_upload_with_semaphore(part_num: int):
            nonlocal concurrent_count, max_observed_concurrent
            async with semaphore:
                concurrent_count += 1
                max_observed_concurrent = max(max_observed_concurrent, concurrent_count)
                await asyncio.sleep(0.01)  # Simulate upload time
                concurrent_count -= 1
                return InternalPartMetadata(
                    internal_part_number=part_num,
                    plaintext_size=1000,
                    ciphertext_size=1016,
                    etag=f"etag-{part_num}",
                )

        # Start 5 uploads at once
        tasks = [asyncio.create_task(mock_upload_with_semaphore(i)) for i in range(1, 6)]

        await asyncio.gather(*tasks)

        # Verify concurrency was limited
        assert max_observed_concurrent <= max_concurrent
        assert max_observed_concurrent == max_concurrent  # Should hit the limit

    @pytest.mark.asyncio
    async def test_parallel_upload_error_handling(self):
        """Test that errors in parallel uploads are properly propagated."""
        import asyncio

        async def mock_upload(part_num: int):
            if part_num == 2:
                raise Exception("Simulated S3 error")
            return InternalPartMetadata(
                internal_part_number=part_num,
                plaintext_size=1000,
                ciphertext_size=1016,
                etag=f"etag-{part_num}",
            )

        tasks = {
            1: asyncio.create_task(mock_upload(1)),
            2: asyncio.create_task(mock_upload(2)),
            3: asyncio.create_task(mock_upload(3)),
        }

        with pytest.raises(Exception, match="Simulated S3 error"):
            await asyncio.gather(*tasks.values())

    @pytest.mark.asyncio
    async def test_parallel_uploads_aggregate_ciphertext_size(self):
        """Test that ciphertext sizes are correctly aggregated from parallel uploads."""
        import asyncio

        async def mock_upload(part_num: int, ciphertext_size: int):
            return InternalPartMetadata(
                internal_part_number=part_num,
                plaintext_size=ciphertext_size - 16,
                ciphertext_size=ciphertext_size,
                etag=f"etag-{part_num}",
            )

        # Create tasks with known ciphertext sizes
        tasks = {
            1: asyncio.create_task(mock_upload(1, 1000)),
            2: asyncio.create_task(mock_upload(2, 2000)),
            3: asyncio.create_task(mock_upload(3, 3000)),
        }

        results = await asyncio.gather(*tasks.values())

        # Aggregate total ciphertext size (as the real code does)
        total_ciphertext_size = 0
        internal_parts = []
        part_num_to_result = {r.internal_part_number: r for r in results}
        for pn in sorted(part_num_to_result.keys()):
            meta = part_num_to_result[pn]
            internal_parts.append(meta)
            total_ciphertext_size += meta.ciphertext_size

        assert total_ciphertext_size == 6000
        assert len(internal_parts) == 3
