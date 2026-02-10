"""Integration tests for UploadPartCopy operations.

These tests verify:
1. Copying encrypted objects
2. Copying ranges from encrypted objects
3. Copying between buckets
4. Metadata preservation during copy
"""

import pytest


@pytest.mark.e2e
class TestUploadPartCopy:
    """Test UploadPartCopy with encrypted objects."""

    def test_copy_full_encrypted_object(self, s3_client, test_bucket):
        """Test copying a full encrypted object."""
        source_key = "source-object.bin"
        dest_key = "dest-object.bin"

        # Upload source object
        source_data = b"S" * 5_242_880  # 5MB
        s3_client.put_object(Bucket=test_bucket, Key=source_key, Body=source_data)

        # Create multipart upload for destination
        response = s3_client.create_multipart_upload(Bucket=test_bucket, Key=dest_key)
        upload_id = response["UploadId"]

        # Copy source as part 1
        copy_resp = s3_client.upload_part_copy(
            Bucket=test_bucket,
            Key=dest_key,
            PartNumber=1,
            UploadId=upload_id,
            CopySource={"Bucket": test_bucket, "Key": source_key},
        )

        # Complete multipart
        s3_client.complete_multipart_upload(
            Bucket=test_bucket,
            Key=dest_key,
            UploadId=upload_id,
            MultipartUpload={
                "Parts": [{"PartNumber": 1, "ETag": copy_resp["CopyPartResult"]["ETag"]}]
            },
        )

        # Verify copied object
        obj = s3_client.get_object(Bucket=test_bucket, Key=dest_key)
        copied_data = obj["Body"].read()

        assert copied_data == source_data
        assert len(copied_data) == len(source_data)

    def test_copy_range_from_encrypted_object(self, s3_client, test_bucket):
        """Test copying a specific range from an encrypted object."""
        source_key = "source-range.bin"
        dest_key = "dest-range.bin"

        # Upload source object (10MB)
        source_data = b"R" * 10_485_760
        s3_client.put_object(Bucket=test_bucket, Key=source_key, Body=source_data)

        # Create multipart upload
        response = s3_client.create_multipart_upload(Bucket=test_bucket, Key=dest_key)
        upload_id = response["UploadId"]

        # Copy range bytes=1000000-5999999 (5MB range)
        copy_resp = s3_client.upload_part_copy(
            Bucket=test_bucket,
            Key=dest_key,
            PartNumber=1,
            UploadId=upload_id,
            CopySource={"Bucket": test_bucket, "Key": source_key},
            CopySourceRange="bytes=1000000-5999999",
        )

        # Complete
        s3_client.complete_multipart_upload(
            Bucket=test_bucket,
            Key=dest_key,
            UploadId=upload_id,
            MultipartUpload={
                "Parts": [{"PartNumber": 1, "ETag": copy_resp["CopyPartResult"]["ETag"]}]
            },
        )

        # Verify
        obj = s3_client.get_object(Bucket=test_bucket, Key=dest_key)
        copied_data = obj["Body"].read()

        expected = source_data[1000000:6000000]
        assert copied_data == expected
        assert len(copied_data) == 5_000_000

    def test_copy_multiple_ranges_as_parts(self, s3_client, test_bucket):
        """Test copying multiple ranges from source as separate parts."""
        source_key = "source-multi-range.bin"
        dest_key = "dest-multi-range.bin"

        # Upload source (20MB)
        source_data = b"T" * 20_971_520
        s3_client.put_object(Bucket=test_bucket, Key=source_key, Body=source_data)

        # Create multipart upload
        response = s3_client.create_multipart_upload(Bucket=test_bucket, Key=dest_key)
        upload_id = response["UploadId"]

        # Copy 3 different ranges as 3 parts
        ranges = [
            (1, "bytes=0-5242879"),  # First 5MB
            (2, "bytes=10485760-15728639"),  # Middle 5MB
            (3, "bytes=15728640-20971519"),  # Last ~5MB
        ]

        parts = []
        for part_num, byte_range in ranges:
            copy_resp = s3_client.upload_part_copy(
                Bucket=test_bucket,
                Key=dest_key,
                PartNumber=part_num,
                UploadId=upload_id,
                CopySource={"Bucket": test_bucket, "Key": source_key},
                CopySourceRange=byte_range,
            )
            parts.append({"PartNumber": part_num, "ETag": copy_resp["CopyPartResult"]["ETag"]})

        # Complete
        s3_client.complete_multipart_upload(
            Bucket=test_bucket,
            Key=dest_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

        # Verify
        obj = s3_client.get_object(Bucket=test_bucket, Key=dest_key)
        copied_data = obj["Body"].read()

        # Expected: first 5MB + middle 5MB + last 5MB
        expected = (
            source_data[0:5242880] + source_data[10485760:15728640] + source_data[15728640:20971520]
        )
        assert copied_data == expected

    def test_copy_from_multipart_source(self, s3_client, test_bucket):
        """Test copying from a multipart encrypted source."""
        source_key = "source-multipart.bin"
        dest_key = "dest-from-multipart.bin"

        # Upload source as multipart (3 parts)
        response = s3_client.create_multipart_upload(Bucket=test_bucket, Key=source_key)
        source_upload_id = response["UploadId"]

        part1 = b"P" * 5_242_880
        part2 = b"Q" * 5_242_880
        part3 = b"R" * 5_242_880

        parts = []
        for i, data in enumerate([part1, part2, part3], 1):
            resp = s3_client.upload_part(
                Bucket=test_bucket,
                Key=source_key,
                PartNumber=i,
                UploadId=source_upload_id,
                Body=data,
            )
            parts.append({"PartNumber": i, "ETag": resp["ETag"]})

        s3_client.complete_multipart_upload(
            Bucket=test_bucket,
            Key=source_key,
            UploadId=source_upload_id,
            MultipartUpload={"Parts": parts},
        )

        # Now copy entire source to destination
        response = s3_client.create_multipart_upload(Bucket=test_bucket, Key=dest_key)
        dest_upload_id = response["UploadId"]

        copy_resp = s3_client.upload_part_copy(
            Bucket=test_bucket,
            Key=dest_key,
            PartNumber=1,
            UploadId=dest_upload_id,
            CopySource={"Bucket": test_bucket, "Key": source_key},
        )

        s3_client.complete_multipart_upload(
            Bucket=test_bucket,
            Key=dest_key,
            UploadId=dest_upload_id,
            MultipartUpload={
                "Parts": [{"PartNumber": 1, "ETag": copy_resp["CopyPartResult"]["ETag"]}]
            },
        )

        # Verify
        obj = s3_client.get_object(Bucket=test_bucket, Key=dest_key)
        copied_data = obj["Body"].read()

        expected = part1 + part2 + part3
        assert copied_data == expected
        assert len(copied_data) == 15_728_640
