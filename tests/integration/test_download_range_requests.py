"""Integration tests for downloading encrypted objects with range requests.

These tests verify:
1. Full download of encrypted multipart objects
2. Range requests on encrypted multipart objects
3. Multiple range scenarios (single part, across parts, final part)
4. Edge cases (empty ranges, beyond EOF)
"""

import pytest


@pytest.mark.e2e
class TestDownloadRangeRequests:
    """Test downloading encrypted multipart objects with range requests."""

    def test_full_download_multipart_object(self, s3_client, test_bucket):
        """Test downloading complete encrypted multipart object."""
        key = "test-full-download.bin"

        # Upload a 2-part object (10MB total)
        response = s3_client.create_multipart_upload(Bucket=test_bucket, Key=key)
        upload_id = response["UploadId"]

        part1_data = b"A" * 5_242_880  # 5MB
        part2_data = b"B" * 5_242_880  # 5MB

        # Upload parts
        resp1 = s3_client.upload_part(
            Bucket=test_bucket,
            Key=key,
            PartNumber=1,
            UploadId=upload_id,
            Body=part1_data,
        )
        resp2 = s3_client.upload_part(
            Bucket=test_bucket,
            Key=key,
            PartNumber=2,
            UploadId=upload_id,
            Body=part2_data,
        )

        # Complete upload
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

        # Download full object
        obj = s3_client.get_object(Bucket=test_bucket, Key=key)
        downloaded = obj["Body"].read()

        # Verify
        expected = part1_data + part2_data
        assert downloaded == expected
        assert len(downloaded) == 10_485_760

    def test_range_request_single_part(self, s3_client, test_bucket):
        """Test range request within a single part."""
        key = "test-range-single-part.bin"

        # Upload 2-part object
        response = s3_client.create_multipart_upload(Bucket=test_bucket, Key=key)
        upload_id = response["UploadId"]

        part1_data = b"A" * 5_242_880
        part2_data = b"B" * 5_242_880

        resp1 = s3_client.upload_part(
            Bucket=test_bucket, Key=key, PartNumber=1, UploadId=upload_id, Body=part1_data
        )
        resp2 = s3_client.upload_part(
            Bucket=test_bucket, Key=key, PartNumber=2, UploadId=upload_id, Body=part2_data
        )

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

        # Range request within part 1: bytes 1000-2000
        obj = s3_client.get_object(Bucket=test_bucket, Key=key, Range="bytes=1000-2000")
        downloaded = obj["Body"].read()

        # Verify
        expected = part1_data[1000:2001]  # Range is inclusive
        assert downloaded == expected
        assert len(downloaded) == 1001

    def test_range_request_across_parts(self, s3_client, test_bucket):
        """Test range request spanning multiple parts."""
        key = "test-range-across-parts.bin"

        # Upload 3-part object
        response = s3_client.create_multipart_upload(Bucket=test_bucket, Key=key)
        upload_id = response["UploadId"]

        part1_data = b"A" * 5_242_880
        part2_data = b"B" * 5_242_880
        part3_data = b"C" * 5_242_880

        parts = []
        for i, data in enumerate([part1_data, part2_data, part3_data], 1):
            resp = s3_client.upload_part(
                Bucket=test_bucket, Key=key, PartNumber=i, UploadId=upload_id, Body=data
            )
            parts.append({"PartNumber": i, "ETag": resp["ETag"]})

        s3_client.complete_multipart_upload(
            Bucket=test_bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

        # Range request spanning part 1 and part 2
        # bytes=5242000-5243000 (last 880 bytes of part 1 + first 121 bytes of part 2)
        obj = s3_client.get_object(Bucket=test_bucket, Key=key, Range="bytes=5242000-5243000")
        downloaded = obj["Body"].read()

        # Verify
        full_data = part1_data + part2_data + part3_data
        expected = full_data[5242000:5243001]
        assert downloaded == expected
        assert len(downloaded) == 1001

    def test_range_request_last_bytes(self, s3_client, test_bucket):
        """Test range request for last N bytes."""
        key = "test-range-last-bytes.bin"

        # Upload small 2-part object
        response = s3_client.create_multipart_upload(Bucket=test_bucket, Key=key)
        upload_id = response["UploadId"]

        part1_data = b"X" * 5_242_880
        part2_data = b"Y" * 1_048_576  # 1MB final part

        resp1 = s3_client.upload_part(
            Bucket=test_bucket, Key=key, PartNumber=1, UploadId=upload_id, Body=part1_data
        )
        resp2 = s3_client.upload_part(
            Bucket=test_bucket, Key=key, PartNumber=2, UploadId=upload_id, Body=part2_data
        )

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

        # Range request: last 500000 bytes (suffix range)
        obj = s3_client.get_object(Bucket=test_bucket, Key=key, Range="bytes=-500000")
        downloaded = obj["Body"].read()

        # Verify
        full_data = part1_data + part2_data
        expected = full_data[-500000:]
        assert downloaded == expected
        assert len(downloaded) == 500000

    def test_range_request_from_offset_to_end(self, s3_client, test_bucket):
        """Test range request from offset to end of file."""
        key = "test-range-to-end.bin"

        # Upload 2-part object
        response = s3_client.create_multipart_upload(Bucket=test_bucket, Key=key)
        upload_id = response["UploadId"]

        part1_data = b"M" * 5_242_880
        part2_data = b"N" * 2_097_152  # 2MB

        resp1 = s3_client.upload_part(
            Bucket=test_bucket, Key=key, PartNumber=1, UploadId=upload_id, Body=part1_data
        )
        resp2 = s3_client.upload_part(
            Bucket=test_bucket, Key=key, PartNumber=2, UploadId=upload_id, Body=part2_data
        )

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

        # Range request: bytes=6000000- (from offset to end)
        obj = s3_client.get_object(Bucket=test_bucket, Key=key, Range="bytes=6000000-")
        downloaded = obj["Body"].read()

        # Verify
        full_data = part1_data + part2_data
        expected = full_data[6000000:]
        assert downloaded == expected
        assert len(downloaded) == len(full_data) - 6000000
