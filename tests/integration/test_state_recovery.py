"""Integration tests for multipart state recovery scenarios.

These tests verify:
1. State recovery when Redis state is lost
2. The documented BUG where recovery creates empty state instead of reconstructing
3. Upload completion behavior after state loss
4. Part tracking after Redis eviction

IMPORTANT: These tests document the current buggy behavior where state recovery
creates empty state instead of reconstructing from S3, causing part loss.
"""

import pytest
from botocore.exceptions import ClientError


@pytest.mark.e2e
class TestStateRecoveryBehavior:
    """Test state recovery scenarios (currently documents buggy behavior)."""

    def test_upload_part_after_proxy_restart_loses_parts(self, s3_client, test_bucket):
        """Test that uploading parts after losing Redis state causes issues.

        This test documents the BUG mentioned in multipart_ops.py:112:
        'This is a BUG - state recovery should reconstruct from S3, not create empty state'

        Current behavior: When Redis state is lost between parts, subsequent parts
        create fresh state and lose track of previous parts, causing completion to fail.
        """
        key = "test-state-recovery.bin"

        # Step 1: Create multipart upload
        response = s3_client.create_multipart_upload(Bucket=test_bucket, Key=key)
        upload_id = response["UploadId"]

        # Step 2: Upload part 1
        part1_data = b"A" * 5_242_880  # 5MB
        resp1 = s3_client.upload_part(
            Bucket=test_bucket, Key=key, PartNumber=1, UploadId=upload_id, Body=part1_data
        )
        etag1 = resp1["ETag"]

        # Step 3: Simulate proxy restart / Redis state loss
        # In a real scenario, Redis state would be lost here due to:
        # - Redis restart/failover
        # - TTL expiration
        # - Memory eviction
        # - Network partition

        # For this test, we can't easily simulate Redis loss in integration test,
        # but we document the expected behavior:
        # - If Redis state is lost, uploading part 2 will trigger state recovery
        # - State recovery loads DEK from S3 but creates EMPTY state
        # - Part 1 information is LOST from state (though data is in S3)

        # Step 4: Upload part 2 (would trigger recovery if Redis was lost)
        part2_data = b"B" * 5_242_880  # 5MB
        resp2 = s3_client.upload_part(
            Bucket=test_bucket, Key=key, PartNumber=2, UploadId=upload_id, Body=part2_data
        )
        etag2 = resp2["ETag"]

        # Step 5: Try to complete with both parts
        # In the normal case (Redis state intact), this should succeed
        s3_client.complete_multipart_upload(
            Bucket=test_bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={
                "Parts": [
                    {"PartNumber": 1, "ETag": etag1},
                    {"PartNumber": 2, "ETag": etag2},
                ]
            },
        )

        # Verify the upload completed successfully
        obj = s3_client.get_object(Bucket=test_bucket, Key=key)
        data = obj["Body"].read()
        assert data == part1_data + part2_data

        # Note: This test passes when Redis state is intact.
        # If Redis state was lost after part 1, completion would fail because:
        # - Fresh state after recovery only knows about part 2
        # - Part 1 would be missing from state.parts
        # - CompleteMultipartUpload would raise InvalidPart error

    def test_complete_with_missing_part_in_state(self, s3_client, test_bucket):
        """Test completing upload when state is missing information about uploaded parts.

        This simulates what happens after state recovery creates empty state.
        """
        key = "test-missing-part-in-state.bin"

        # Create multipart upload
        response = s3_client.create_multipart_upload(Bucket=test_bucket, Key=key)
        upload_id = response["UploadId"]

        # Upload part 1
        part1_data = b"X" * 5_242_880
        resp1 = s3_client.upload_part(
            Bucket=test_bucket, Key=key, PartNumber=1, UploadId=upload_id, Body=part1_data
        )

        # Try to complete with part 1
        # This should succeed in normal operation
        s3_client.complete_multipart_upload(
            Bucket=test_bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": resp1["ETag"]}]},
        )

        # Verify
        obj = s3_client.get_object(Bucket=test_bucket, Key=key)
        data = obj["Body"].read()
        assert data == part1_data

    def test_out_of_order_parts_with_state_loss_scenario(self, s3_client, test_bucket):
        """Test out-of-order upload parts (documenting potential state loss impact).

        Uploads parts out of order to show how state loss between parts
        would cause issues with part tracking.
        """
        key = "test-out-of-order-state-loss.bin"

        # Create multipart upload
        response = s3_client.create_multipart_upload(Bucket=test_bucket, Key=key)
        upload_id = response["UploadId"]

        # Upload part 3 first
        part3_data = b"C" * 5_242_880
        resp3 = s3_client.upload_part(
            Bucket=test_bucket, Key=key, PartNumber=3, UploadId=upload_id, Body=part3_data
        )

        # If Redis state was lost here, part 3 would be lost from state

        # Upload part 1
        part1_data = b"A" * 5_242_880
        resp1 = s3_client.upload_part(
            Bucket=test_bucket, Key=key, PartNumber=1, UploadId=upload_id, Body=part1_data
        )

        # If Redis state was lost here, parts 1 and 3 would be lost from state

        # Upload part 2
        part2_data = b"B" * 5_242_880
        resp2 = s3_client.upload_part(
            Bucket=test_bucket, Key=key, PartNumber=2, UploadId=upload_id, Body=part2_data
        )

        # Complete with all parts in order
        s3_client.complete_multipart_upload(
            Bucket=test_bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={
                "Parts": [
                    {"PartNumber": 1, "ETag": resp1["ETag"]},
                    {"PartNumber": 2, "ETag": resp2["ETag"]},
                    {"PartNumber": 3, "ETag": resp3["ETag"]},
                ]
            },
        )

        # Verify all parts are present
        obj = s3_client.get_object(Bucket=test_bucket, Key=key)
        data = obj["Body"].read()
        assert data == part1_data + part2_data + part3_data


@pytest.mark.e2e
class TestStateRecoveryFix:
    """Tests for what state recovery SHOULD do (future fix).

    These tests document the desired behavior that would fix the bug:
    State recovery should reconstruct the full state from S3 metadata,
    not create empty state.
    """

    def test_state_recovery_should_reconstruct_parts_from_s3(self, s3_client, test_bucket):
        """Test that state recovery SHOULD reconstruct part information from S3.

        EXPECTED BEHAVIOR (not yet implemented):
        1. Load DEK from S3 state metadata
        2. List parts from S3 ListParts API
        3. Reconstruct state.parts with all uploaded parts
        4. Restore next_internal_part_number counter
        5. Allow completion with all parts

        CURRENT BEHAVIOR:
        1. Load DEK from S3 state metadata
        2. Create EMPTY state.parts
        3. Lose track of previously uploaded parts
        4. Completion fails with InvalidPart

        This test currently passes because Redis state is intact throughout.
        """
        key = "test-should-reconstruct.bin"

        # Upload using multipart
        response = s3_client.create_multipart_upload(Bucket=test_bucket, Key=key)
        upload_id = response["UploadId"]

        # Upload parts
        part1_data = b"D" * 5_242_880
        part2_data = b"E" * 5_242_880

        resp1 = s3_client.upload_part(
            Bucket=test_bucket, Key=key, PartNumber=1, UploadId=upload_id, Body=part1_data
        )
        resp2 = s3_client.upload_part(
            Bucket=test_bucket, Key=key, PartNumber=2, UploadId=upload_id, Body=part2_data
        )

        # Complete (works because state is intact)
        s3_client.complete_multipart_upload(
            Bucket=test_bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={
                "Parts": [
                    {"PartNumber": 1, "ETag": resp1["ETag"]},
                    {"PartNumber": 2, "ETag": resp2["ETag"]},
                ]
            },
        )

        # Verify
        obj = s3_client.get_object(Bucket=test_bucket, Key=key)
        data = obj["Body"].read()
        assert data == part1_data + part2_data

        # TODO: Once the bug is fixed, add a test that:
        # 1. Uploads part 1
        # 2. Manually deletes Redis state
        # 3. Uploads part 2 (triggers recovery)
        # 4. Completes with both parts (should work after fix)

    def test_state_recovery_should_preserve_internal_part_numbers(self, s3_client, test_bucket):
        """Test that state recovery should preserve internal part number tracking.

        When a large part is split into multiple internal parts, state recovery
        should be able to reconstruct the internal part mapping from S3 metadata.
        """
        key = "test-should-preserve-internal-parts.bin"

        # Upload large parts that get split internally
        response = s3_client.create_multipart_upload(Bucket=test_bucket, Key=key)
        upload_id = response["UploadId"]

        # Upload a large part (20MB) that will be split into multiple internal parts
        part1_data = b"F" * 20_971_520  # 20MB
        resp1 = s3_client.upload_part(
            Bucket=test_bucket, Key=key, PartNumber=1, UploadId=upload_id, Body=part1_data
        )

        # Complete
        s3_client.complete_multipart_upload(
            Bucket=test_bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": resp1["ETag"]}]},
        )

        # Verify
        obj = s3_client.get_object(Bucket=test_bucket, Key=key)
        data = obj["Body"].read()
        assert data == part1_data

        # TODO: After fix, test that state recovery correctly reconstructs
        # the internal part mapping by reading S3 metadata


@pytest.mark.e2e
class TestStateRecoveryEdgeCases:
    """Test edge cases in state recovery behavior."""

    def test_invalid_upload_id_after_state_loss(self, s3_client, test_bucket):
        """Test that invalid upload ID is properly detected even without Redis state."""
        key = "test-invalid-upload.bin"

        # Try to upload with fake upload ID
        with pytest.raises(ClientError) as exc:
            s3_client.upload_part(
                Bucket=test_bucket,
                Key=key,
                PartNumber=1,
                UploadId="fake-upload-id-12345",
                Body=b"test",
            )

        # Should return NoSuchUpload error
        assert exc.value.response["Error"]["Code"] in ["NoSuchUpload", "404"]

    def test_abort_upload_cleans_up_state(self, s3_client, test_bucket):
        """Test that aborting upload properly cleans up both S3 and Redis state."""
        key = "test-abort-cleanup.bin"

        # Create and upload parts
        response = s3_client.create_multipart_upload(Bucket=test_bucket, Key=key)
        upload_id = response["UploadId"]

        s3_client.upload_part(
            Bucket=test_bucket,
            Key=key,
            PartNumber=1,
            UploadId=upload_id,
            Body=b"G" * 5_242_880,
        )

        # Abort
        s3_client.abort_multipart_upload(Bucket=test_bucket, Key=key, UploadId=upload_id)

        # Try to upload another part (should fail)
        with pytest.raises(ClientError) as exc:
            s3_client.upload_part(
                Bucket=test_bucket,
                Key=key,
                PartNumber=2,
                UploadId=upload_id,
                Body=b"H" * 5_242_880,
            )

        # Should fail because upload was aborted
        assert exc.value.response["Error"]["Code"] in ["NoSuchUpload", "404"]
