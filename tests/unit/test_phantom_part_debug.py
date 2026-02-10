"""Test to reproduce and debug the phantom part 4 issue."""

import pytest

from s3proxy import crypto
from s3proxy.state import InternalPartMetadata, PartMetadata


class TestPhantomPartDebug:
    """Test to reproduce the phantom part 4 scenario."""

    @pytest.mark.asyncio
    async def test_sequential_parts_with_skip(self, manager, settings):
        """Test uploading parts 1, 2, 3, 5 (skipping 4) to reproduce phantom part."""
        bucket = "test-bucket"
        key = "test-key"
        upload_id = "test-upload-123"

        # Create upload
        dek = crypto.generate_dek()
        state = await manager.create_upload(bucket, key, upload_id, dek)

        # Simulate uploading part 1 (with internal parts 1, 2)
        part1 = PartMetadata(
            part_number=1,
            plaintext_size=20 * 1024 * 1024,  # 20MB
            ciphertext_size=20 * 1024 * 1024 + 56,  # 2 internal parts
            etag="etag-1",
            md5="md5-1",
            internal_parts=[
                InternalPartMetadata(
                    internal_part_number=1,
                    plaintext_size=16 * 1024 * 1024,
                    ciphertext_size=16 * 1024 * 1024 + 28,
                    etag="internal-etag-1",
                ),
                InternalPartMetadata(
                    internal_part_number=2,
                    plaintext_size=4 * 1024 * 1024,
                    ciphertext_size=4 * 1024 * 1024 + 28,
                    etag="internal-etag-2",
                ),
            ],
        )
        await manager.add_part(bucket, key, upload_id, part1)

        # Verify state has only part 1
        state = await manager.get_upload(bucket, key, upload_id)
        assert state is not None
        assert sorted(state.parts.keys()) == [1]

        # Simulate uploading part 2 (with internal parts 3, 4)
        part2 = PartMetadata(
            part_number=2,
            plaintext_size=20 * 1024 * 1024,
            ciphertext_size=20 * 1024 * 1024 + 56,
            etag="etag-2",
            md5="md5-2",
            internal_parts=[
                InternalPartMetadata(
                    internal_part_number=3,
                    plaintext_size=16 * 1024 * 1024,
                    ciphertext_size=16 * 1024 * 1024 + 28,
                    etag="internal-etag-3",
                ),
                InternalPartMetadata(
                    internal_part_number=4,
                    plaintext_size=4 * 1024 * 1024,
                    ciphertext_size=4 * 1024 * 1024 + 28,
                    etag="internal-etag-4",
                ),
            ],
        )
        await manager.add_part(bucket, key, upload_id, part2)

        # Verify state has parts 1, 2
        state = await manager.get_upload(bucket, key, upload_id)
        assert state is not None
        assert sorted(state.parts.keys()) == [1, 2]

        # Simulate uploading part 3 (with internal parts 5, 6)
        part3 = PartMetadata(
            part_number=3,
            plaintext_size=20 * 1024 * 1024,
            ciphertext_size=20 * 1024 * 1024 + 56,
            etag="etag-3",
            md5="md5-3",
            internal_parts=[
                InternalPartMetadata(
                    internal_part_number=5,
                    plaintext_size=16 * 1024 * 1024,
                    ciphertext_size=16 * 1024 * 1024 + 28,
                    etag="internal-etag-5",
                ),
                InternalPartMetadata(
                    internal_part_number=6,
                    plaintext_size=4 * 1024 * 1024,
                    ciphertext_size=4 * 1024 * 1024 + 28,
                    etag="internal-etag-6",
                ),
            ],
        )
        await manager.add_part(bucket, key, upload_id, part3)

        # Verify state has parts 1, 2, 3 (NOT 4!)
        state = await manager.get_upload(bucket, key, upload_id)
        assert state is not None
        assert sorted(state.parts.keys()) == [1, 2, 3], (
            f"Expected [1, 2, 3] but got {sorted(state.parts.keys())}"
        )

        # Now simulate uploading part 5 (NOT 4!) with internal parts 7, 8
        # This is where the phantom part 4 appeared in production
        part5 = PartMetadata(
            part_number=5,
            plaintext_size=20 * 1024 * 1024,
            ciphertext_size=20 * 1024 * 1024 + 56,
            etag="etag-5",
            md5="md5-5",
            internal_parts=[
                InternalPartMetadata(
                    internal_part_number=7,
                    plaintext_size=16 * 1024 * 1024,
                    ciphertext_size=16 * 1024 * 1024 + 28,
                    etag="internal-etag-7",
                ),
                InternalPartMetadata(
                    internal_part_number=8,
                    plaintext_size=4 * 1024 * 1024,
                    ciphertext_size=4 * 1024 * 1024 + 28,
                    etag="internal-etag-8",
                ),
            ],
        )
        await manager.add_part(bucket, key, upload_id, part5)

        # CRITICAL CHECK: State should have parts 1, 2, 3, 5 (NOT 4!)
        state = await manager.get_upload(bucket, key, upload_id)
        assert state is not None

        actual_parts = sorted(state.parts.keys())
        print(f"\nActual parts after adding part 5: {actual_parts}")

        # This is the bug check - part 4 should NOT appear
        assert 4 not in state.parts, (
            f"PHANTOM PART 4 BUG: Part 4 appeared without being uploaded! "
            f"Parts in state: {actual_parts}"
        )

        # Verify we have exactly the parts we uploaded
        assert actual_parts == [1, 2, 3, 5], (
            f"Expected [1, 2, 3, 5] but got {actual_parts}. Phantom parts detected!"
        )
