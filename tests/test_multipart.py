"""Tests for multipart upload handling."""

import asyncio
import base64
import gzip
import json

import pytest

from s3proxy.multipart import (
    MultipartMetadata,
    MultipartStateManager,
    PartMetadata,
    calculate_part_range,
    decode_multipart_metadata,
    encode_multipart_metadata,
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

    @pytest.mark.asyncio
    async def test_semaphore_limits_concurrent_uploads(self):
        """Test semaphore limits concurrent uploads."""
        manager = MultipartStateManager(max_concurrent=2)

        # Acquire two slots
        await manager.acquire_slot()
        await manager.acquire_slot()

        # Third should timeout
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(manager.acquire_slot(), timeout=0.01)

        # Release one
        manager.release_slot()

        # Now we can acquire again
        await asyncio.wait_for(manager.acquire_slot(), timeout=0.1)


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
        assert result[1] == (3, 0, 999)    # Part 3: full

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
