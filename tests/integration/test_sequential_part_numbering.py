"""Tests for sequential internal part numbering fix (EntityTooSmall)."""

import pytest

from s3proxy import crypto
from s3proxy.state import InternalPartMetadata, PartMetadata


class TestSequentialPartNumbering:
    """Tests verifying sequential internal part numbering fix for EntityTooSmall."""

    @pytest.mark.asyncio
    async def test_two_part_upload_sequential_numbering(self, manager, settings):
        """
        Test that a 2-part upload gets sequential internal part numbers [1, 2].

        Before fix (with +5 buffer):
        - Client Part 2 uploaded first → allocates [1-6], uses Part 1 (4.24MB)
        - Client Part 1 uploaded second → allocates [7-12], uses Part 7 (5.00MB)
        - MinIO sees [Part 1: 4.24MB, Part 7: 5.00MB]
        - MinIO thinks Part 1 is NOT the last part → EntityTooSmall ❌

        After fix (exact allocation):
        - Client Part 2 uploaded first → allocates [1], uses Part 1 (4.24MB)
        - Client Part 1 uploaded second → allocates [2], uses Part 2 (5.00MB)
        - MinIO sees [Part 1: 4.24MB, Part 2: 5.00MB]  (after sorting: [Part 1, Part 2])
        - MinIO correctly identifies Part 2 as last → Success ✅
        """
        bucket = "clickhouse-backups"
        key = "backup.tar"
        upload_id = "test-two-part-upload"

        # Create upload
        dek = crypto.generate_dek()
        await manager.create_upload(bucket, key, upload_id, dek)

        # Simulate Client Part 2 uploaded first (smaller, 4.24MB)
        # With exact allocation (no +5 buffer), should get internal part 1
        start1 = await manager.allocate_internal_parts(bucket, key, upload_id, 1)
        assert start1 == 1, f"First allocation should start at 1, got {start1}"

        part2 = PartMetadata(
            part_number=2,
            plaintext_size=4441600,  # 4.24MB
            ciphertext_size=4441628,
            etag="etag-2",
            md5="md5-2",
            internal_parts=[
                InternalPartMetadata(
                    internal_part_number=start1,  # Should be 1
                    plaintext_size=4441600,
                    ciphertext_size=4441628,
                    etag="internal-etag-1",
                ),
            ],
        )
        await manager.add_part(bucket, key, upload_id, part2)

        # Simulate Client Part 1 uploaded second (5.00MB)
        # With exact allocation, should get internal part 2 (not 7!)
        start2 = await manager.allocate_internal_parts(bucket, key, upload_id, 1)
        assert start2 == 2, f"Second allocation should start at 2, got {start2}"

        part1 = PartMetadata(
            part_number=1,
            plaintext_size=5242880,  # 5.00MB
            ciphertext_size=5242908,
            etag="etag-1",
            md5="md5-1",
            internal_parts=[
                InternalPartMetadata(
                    internal_part_number=start2,  # Should be 2
                    plaintext_size=5242880,
                    ciphertext_size=5242908,
                    etag="internal-etag-2",
                ),
            ],
        )
        await manager.add_part(bucket, key, upload_id, part1)

        # Verify: Internal parts are sequential [1, 2]
        state = await manager.get_upload(bucket, key, upload_id)
        assert state is not None
        assert len(state.parts) == 2

        # Get all internal part numbers
        internal_numbers = []
        for client_part in state.parts.values():
            for internal_part in client_part.internal_parts:
                internal_numbers.append(internal_part.internal_part_number)

        # Sort to match what CompleteMultipartUpload will send
        internal_numbers.sort()

        # CRITICAL: Must be [1, 2] not [1, 7]
        assert internal_numbers == [1, 2], (
            f"Internal part numbers must be sequential [1, 2] "
            f"for MinIO to identify last part correctly. "
            f"Got {internal_numbers} which would cause EntityTooSmall!"
        )

    @pytest.mark.asyncio
    async def test_concurrent_uploads_independent_numbering(self, manager, settings):
        """
        Test that two concurrent uploads have independent sequential numbering.

        Upload A: parts [1, 2]
        Upload B: parts [1, 2]  (not [3, 4]!)
        """
        dek = crypto.generate_dek()

        # Create two independent uploads
        await manager.create_upload("bucket", "file-a.tar", "upload-a", dek)
        await manager.create_upload("bucket", "file-b.tar", "upload-b", dek)

        # Upload A allocates parts
        start_a1 = await manager.allocate_internal_parts("bucket", "file-a.tar", "upload-a", 1)
        start_a2 = await manager.allocate_internal_parts("bucket", "file-a.tar", "upload-a", 1)

        # Upload B allocates parts independently
        start_b1 = await manager.allocate_internal_parts("bucket", "file-b.tar", "upload-b", 1)
        start_b2 = await manager.allocate_internal_parts("bucket", "file-b.tar", "upload-b", 1)

        # Both uploads should have sequential [1, 2]
        assert start_a1 == 1 and start_a2 == 2, (
            f"Upload A should be [1, 2], got [{start_a1}, {start_a2}]"
        )
        assert start_b1 == 1 and start_b2 == 2, (
            f"Upload B should be [1, 2], got [{start_b1}, {start_b2}]"
        )

    @pytest.mark.asyncio
    async def test_eight_part_upload_all_sequential(self, manager, settings):
        """
        Test that an 8-part upload (like ClickHouse 35MB files) gets sequential [1-8].

        ClickHouse splits 35MB files into:
        - Parts 1-7: 5.00MB each
        - Part 8: 0.34MB (last part, valid)

        With exact allocation, internal parts should be [1, 2, 3, 4, 5, 6, 7, 8].
        """
        bucket = "clickhouse-backups"
        key = "large-backup.tar"
        upload_id = "test-eight-part"

        dek = crypto.generate_dek()
        await manager.create_upload(bucket, key, upload_id, dek)

        internal_numbers = []
        for part_num in range(1, 9):
            # Allocate 1 internal part (exact, no buffer)
            start = await manager.allocate_internal_parts(bucket, key, upload_id, 1)
            internal_numbers.append(start)

            # Add the part
            part_size = 5242880 if part_num < 8 else 356864  # Last part is 0.34MB
            part = PartMetadata(
                part_number=part_num,
                plaintext_size=part_size,
                ciphertext_size=part_size + 28,
                etag=f"etag-{part_num}",
                md5=f"md5-{part_num}",
                internal_parts=[
                    InternalPartMetadata(
                        internal_part_number=start,
                        plaintext_size=part_size,
                        ciphertext_size=part_size + 28,
                        etag=f"internal-etag-{start}",
                    ),
                ],
            )
            await manager.add_part(bucket, key, upload_id, part)

        # Verify: Sequential [1, 2, 3, 4, 5, 6, 7, 8]
        assert internal_numbers == list(range(1, 9)), (
            f"8-part upload must have sequential internal parts [1-8], got {internal_numbers}"
        )

        # Verify state
        state = await manager.get_upload(bucket, key, upload_id)
        assert len(state.parts) == 8

        # Verify Part 8 is small but valid (last part)
        assert state.parts[8].plaintext_size == 356864  # 0.34MB, last part OK

    @pytest.mark.asyncio
    async def test_old_behavior_with_buffer_would_fail(self, manager, settings):
        """
        Demonstrate that the OLD behavior (with +5 buffer) would create gaps.

        This test shows WHY we removed the +5 buffer.
        """
        bucket = "test"
        key = "test.tar"
        upload_id = "test-buffer-demo"

        dek = crypto.generate_dek()
        await manager.create_upload(bucket, key, upload_id, dek)

        # Simulate OLD behavior: allocate with +5 buffer
        # Client Part 1 (estimated 1 part) → allocates 1+5=6 parts [1-6]
        start1 = await manager.allocate_internal_parts(bucket, key, upload_id, 6)  # OLD: 1+5

        # Client Part 2 (estimated 1 part) → allocates 1+5=6 parts [7-12]
        start2 = await manager.allocate_internal_parts(bucket, key, upload_id, 6)  # OLD: 1+5

        # With OLD behavior: [1, 7] → Gap!
        assert start1 == 1
        assert start2 == 7  # Gap: 2-6 unused!

        # This would cause EntityTooSmall:
        # MinIO sees [Part 1: 4.24MB, Part 7: 5.00MB]
        # MinIO thinks Part 1 is NOT the last → Rejects!

        # NEW behavior (tested above): allocate exactly [1], [2] → Sequential ✅
