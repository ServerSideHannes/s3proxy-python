"""Unit tests for Redis failure scenarios in multipart state management.

These tests verify:
1. Missing state errors are handled gracefully
2. State management works correctly in memory mode
3. Part tracking and completion flows

Note: Complex Redis watch error testing with mocking is difficult due to async context managers.
Full Redis failure scenarios are better tested with integration tests using real Redis.
"""

import pytest

from s3proxy import crypto
from s3proxy.state import MultipartStateManager, PartMetadata, StateMissingError


class TestMemoryModeStateMangement:
    """Test state management in memory mode (no Redis)."""

    @pytest.mark.asyncio
    async def test_create_and_get_upload(self):
        """Test creating and retrieving upload state."""
        manager = MultipartStateManager()
        bucket = "test-bucket"
        key = "test-key"
        upload_id = "test-upload"
        dek = crypto.generate_dek()

        # Create upload
        state = await manager.create_upload(bucket, key, upload_id, dek)
        assert state is not None
        assert state.bucket == bucket
        assert state.key == key
        assert state.upload_id == upload_id
        assert state.dek == dek
        assert len(state.parts) == 0

        # Retrieve upload
        retrieved = await manager.get_upload(bucket, key, upload_id)
        assert retrieved is not None
        assert retrieved.upload_id == upload_id

    @pytest.mark.asyncio
    async def test_get_nonexistent_upload_returns_none(self):
        """Test that getting non-existent upload returns None."""
        manager = MultipartStateManager()
        result = await manager.get_upload("bucket", "key", "nonexistent-id")
        assert result is None

    @pytest.mark.asyncio
    async def test_add_part_to_upload(self):
        """Test adding a part to an upload."""
        manager = MultipartStateManager()
        bucket = "test-bucket"
        key = "test-key"
        upload_id = "test-upload"
        dek = crypto.generate_dek()

        # Create upload
        await manager.create_upload(bucket, key, upload_id, dek)

        # Add part
        part = PartMetadata(
            part_number=1,
            plaintext_size=5_242_880,
            ciphertext_size=5_242_880 + 28,
            etag="test-etag",
            md5="test-md5",
        )
        await manager.add_part(bucket, key, upload_id, part)

        # Verify part was added
        state = await manager.get_upload(bucket, key, upload_id)
        assert state is not None
        assert 1 in state.parts
        assert state.parts[1].etag == "test-etag"

    @pytest.mark.asyncio
    async def test_add_part_to_missing_upload_raises_error(self):
        """Test that adding part to missing upload raises StateMissingError."""
        manager = MultipartStateManager()

        part = PartMetadata(
            part_number=1,
            plaintext_size=5_242_880,
            ciphertext_size=5_242_880 + 28,
            etag="test-etag",
            md5="test-md5",
        )

        # Should raise StateMissingError (consistent behavior after StateStore refactoring)
        with pytest.raises(StateMissingError) as exc_info:
            await manager.add_part("bucket", "key", "missing-id", part)

        assert "bucket/key/missing-id" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_complete_upload_removes_state(self):
        """Test that completing upload removes and returns state."""
        manager = MultipartStateManager()
        bucket = "test-bucket"
        key = "test-key"
        upload_id = "test-upload"
        dek = crypto.generate_dek()

        # Create upload
        await manager.create_upload(bucket, key, upload_id, dek)

        # Complete upload
        state = await manager.complete_upload(bucket, key, upload_id)
        assert state is not None
        assert state.upload_id == upload_id

        # State should be removed
        gone = await manager.get_upload(bucket, key, upload_id)
        assert gone is None

    @pytest.mark.asyncio
    async def test_complete_missing_upload_returns_none(self):
        """Test that completing missing upload returns None."""
        manager = MultipartStateManager()
        result = await manager.complete_upload("bucket", "key", "missing-id")
        assert result is None

    @pytest.mark.asyncio
    async def test_abort_upload_removes_state(self):
        """Test that aborting upload removes state."""
        manager = MultipartStateManager()
        bucket = "test-bucket"
        key = "test-key"
        upload_id = "test-upload"
        dek = crypto.generate_dek()

        # Create upload
        await manager.create_upload(bucket, key, upload_id, dek)

        # Abort upload
        await manager.abort_upload(bucket, key, upload_id)

        # State should be removed
        state = await manager.get_upload(bucket, key, upload_id)
        assert state is None


class TestPartTracking:
    """Test part number tracking and management."""

    @pytest.mark.asyncio
    async def test_multiple_parts_tracking(self):
        """Test tracking multiple parts."""
        manager = MultipartStateManager()
        bucket = "test-bucket"
        key = "test-key"
        upload_id = "test-upload"
        dek = crypto.generate_dek()

        await manager.create_upload(bucket, key, upload_id, dek)

        # Add multiple parts
        for i in range(1, 4):
            part = PartMetadata(
                part_number=i,
                plaintext_size=5_242_880,
                ciphertext_size=5_242_880 + 28,
                etag=f"etag-{i}",
                md5=f"md5-{i}",
            )
            await manager.add_part(bucket, key, upload_id, part)

        # Verify all parts tracked
        state = await manager.get_upload(bucket, key, upload_id)
        assert state is not None
        assert len(state.parts) == 3
        assert 1 in state.parts
        assert 2 in state.parts
        assert 3 in state.parts

    @pytest.mark.asyncio
    async def test_out_of_order_parts(self):
        """Test adding parts out of order."""
        manager = MultipartStateManager()
        bucket = "test-bucket"
        key = "test-key"
        upload_id = "test-upload"
        dek = crypto.generate_dek()

        await manager.create_upload(bucket, key, upload_id, dek)

        # Add parts out of order: 3, 1, 2
        for part_num in [3, 1, 2]:
            part = PartMetadata(
                part_number=part_num,
                plaintext_size=5_242_880,
                ciphertext_size=5_242_880 + 28,
                etag=f"etag-{part_num}",
                md5=f"md5-{part_num}",
            )
            await manager.add_part(bucket, key, upload_id, part)

        # Verify all parts tracked correctly
        state = await manager.get_upload(bucket, key, upload_id)
        assert state is not None
        assert len(state.parts) == 3
        assert sorted(state.parts.keys()) == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_total_plaintext_size_tracking(self):
        """Test that total plaintext size is tracked correctly."""
        manager = MultipartStateManager()
        bucket = "test-bucket"
        key = "test-key"
        upload_id = "test-upload"
        dek = crypto.generate_dek()

        await manager.create_upload(bucket, key, upload_id, dek)

        # Add parts with different sizes
        sizes = [5_242_880, 3_000_000, 1_000_000]
        for i, size in enumerate(sizes, 1):
            part = PartMetadata(
                part_number=i,
                plaintext_size=size,
                ciphertext_size=size + 28,
                etag=f"etag-{i}",
                md5=f"md5-{i}",
            )
            await manager.add_part(bucket, key, upload_id, part)

        # Verify total size
        state = await manager.get_upload(bucket, key, upload_id)
        assert state is not None
        assert state.total_plaintext_size == sum(sizes)
