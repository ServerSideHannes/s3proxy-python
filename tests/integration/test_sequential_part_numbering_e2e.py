"""End-to-end integration tests for sequential part numbering fix (EntityTooSmall).

These tests verify:
1. Full HTTP request flow with AWS SigV4 signing (via boto3)
2. Real s3proxy + MinIO interaction (not mocked)
3. CompleteMultipartUpload with sequential part numbers
4. Out-of-order upload handling (Part 2 before Part 1)
5. Actual verification that MinIO accepts the uploads

Inspired by production logs showing:
- Client Part 2 uploaded first → Internal Part 1
- Client Part 1 uploaded second → Internal Part 2
- MinIO receives sequential [1, 2] and accepts the upload ✅
"""

import contextlib

import boto3
import pytest

from .conftest import run_s3proxy

# Run sequential part numbering tests in isolation to avoid port conflicts
pytestmark = pytest.mark.xdist_group("sequential")


@pytest.fixture(scope="module")
def s3proxy_server():
    """Start s3proxy server for e2e tests."""
    # Port 4470 avoids conflicts with integration (4433+), HA (4450-4451), and memory (4460) tests
    with run_s3proxy(4470, log_output=False) as (endpoint, _):
        yield endpoint


@pytest.fixture
def s3_client(s3proxy_server):
    """Create boto3 S3 client pointing to s3proxy."""
    client = boto3.client(
        "s3",
        endpoint_url=s3proxy_server,
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )
    return client


@pytest.fixture
def test_bucket(s3_client):
    """Create and cleanup test bucket."""
    bucket = "test-sequential-parts"

    # Create bucket
    with contextlib.suppress(s3_client.exceptions.BucketAlreadyOwnedByYou):
        s3_client.create_bucket(Bucket=bucket)

    yield bucket

    # Cleanup: delete all objects and bucket
    try:
        # List and delete all objects
        response = s3_client.list_objects_v2(Bucket=bucket)
        if "Contents" in response:
            objects = [{"Key": obj["Key"]} for obj in response["Contents"]]
            s3_client.delete_objects(Bucket=bucket, Delete={"Objects": objects})

        # Delete bucket
        s3_client.delete_bucket(Bucket=bucket)
    except Exception:
        pass


class TestSequentialPartNumberingE2E:
    """End-to-end tests for sequential part numbering with real S3."""

    @pytest.mark.e2e
    def test_out_of_order_upload_two_parts_real_s3(self, s3_client, test_bucket):
        """
        Test uploading parts out of order with real s3proxy + MinIO.

        Scenario (from production logs):
        - Upload Part 2 first (4.24MB) → Gets internal part 1
        - Upload Part 1 second (5.00MB) → Gets internal part 2
        - CompleteMultipartUpload → MinIO sees [Part 1: 5.00MB, Part 2: 4.24MB]
        - MinIO accepts because part 2 is the last part ✅

        This is the EXACT scenario that was failing before the fix!
        """
        key = "test-out-of-order-2parts.bin"

        # Step 1: Initiate multipart upload
        response = s3_client.create_multipart_upload(Bucket=test_bucket, Key=key)
        upload_id = response["UploadId"]

        # Step 2: Upload Part 2 FIRST (4.24MB - smaller)
        part2_data = b"B" * 4_441_600  # 4.24MB
        response2 = s3_client.upload_part(
            Bucket=test_bucket,
            Key=key,
            PartNumber=2,
            UploadId=upload_id,
            Body=part2_data,
        )
        etag2 = response2["ETag"]

        # Step 3: Upload Part 1 SECOND (5.00MB - larger)
        part1_data = b"A" * 5_242_880  # 5.00MB
        response1 = s3_client.upload_part(
            Bucket=test_bucket,
            Key=key,
            PartNumber=1,
            UploadId=upload_id,
            Body=part1_data,
        )
        etag1 = response1["ETag"]

        # Step 4: Complete multipart upload
        response = s3_client.complete_multipart_upload(
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

        # BEFORE FIX: Would get 400 EntityTooSmall
        # AFTER FIX: Should succeed
        assert "ETag" in response, (
            "CompleteMultipartUpload failed. This indicates MinIO rejected the upload - "
            "likely due to non-sequential internal part numbers [1, 7] instead of [1, 2]."
        )

        # Step 5: Verify object exists and has correct size
        head_response = s3_client.head_object(Bucket=test_bucket, Key=key)
        expected_size = len(part1_data) + len(part2_data)  # 9,684,480 bytes
        assert head_response["ContentLength"] == expected_size

    @pytest.mark.e2e
    def test_clickhouse_eight_part_upload_real_s3(self, s3_client, test_bucket):
        """
        Test 8-part upload (ClickHouse 35MB files) with real s3proxy + MinIO.

        ClickHouse splits 35MB files into:
        - Parts 1-7: 5.00MB each
        - Part 8: 0.34MB (last part, valid)

        Upload order: 8, 7, 6, 5, 4, 3, 2, 1 (reverse order)
        Internal parts should still be sequential: [1, 2, 3, 4, 5, 6, 7, 8]
        """
        key = "test-clickhouse-8parts.bin"

        # Initiate multipart upload
        response = s3_client.create_multipart_upload(Bucket=test_bucket, Key=key)
        upload_id = response["UploadId"]

        # Upload parts in REVERSE order (8 → 1)
        parts = []
        for part_num in range(8, 0, -1):
            # Parts 1-7: 5MB, Part 8: 0.34MB
            part_size = 5_242_880 if part_num < 8 else 356_864
            part_data = bytes([part_num] * part_size)

            response = s3_client.upload_part(
                Bucket=test_bucket,
                Key=key,
                PartNumber=part_num,
                UploadId=upload_id,
                Body=part_data,
            )
            parts.append({"PartNumber": part_num, "ETag": response["ETag"]})

        # Complete multipart (sort parts by PartNumber)
        parts.sort(key=lambda p: p["PartNumber"])
        response = s3_client.complete_multipart_upload(
            Bucket=test_bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

        assert "ETag" in response, (
            "8-part upload should succeed with sequential internal parts [1-8]"
        )

        # Verify object size
        head_response = s3_client.head_object(Bucket=test_bucket, Key=key)
        expected_size = 7 * 5_242_880 + 356_864  # 37,056,224 bytes
        assert head_response["ContentLength"] == expected_size

    @pytest.mark.e2e
    def test_concurrent_uploads_independent_numbering_real_s3(self, s3_client, test_bucket):
        """
        Test two concurrent uploads have independent sequential numbering.

        Upload A: parts [1, 2]
        Upload B: parts [1, 2]  (not [3, 4]!)
        """
        key_a = "concurrent-upload-a.bin"
        key_b = "concurrent-upload-b.bin"

        # Initiate both uploads
        resp_a = s3_client.create_multipart_upload(Bucket=test_bucket, Key=key_a)
        resp_b = s3_client.create_multipart_upload(Bucket=test_bucket, Key=key_b)
        upload_id_a = resp_a["UploadId"]
        upload_id_b = resp_b["UploadId"]

        # Upload parts interleaved: A1, B1, A2, B2
        part_data_1 = b"X" * 5_242_880  # 5MB
        part_data_2 = b"Y" * 4_500_000  # 4.29MB

        # Upload A Part 1
        resp = s3_client.upload_part(
            Bucket=test_bucket,
            Key=key_a,
            PartNumber=1,
            UploadId=upload_id_a,
            Body=part_data_1,
        )
        etag_a1 = resp["ETag"]

        # Upload B Part 1
        resp = s3_client.upload_part(
            Bucket=test_bucket,
            Key=key_b,
            PartNumber=1,
            UploadId=upload_id_b,
            Body=part_data_1,
        )
        etag_b1 = resp["ETag"]

        # Upload A Part 2
        resp = s3_client.upload_part(
            Bucket=test_bucket,
            Key=key_a,
            PartNumber=2,
            UploadId=upload_id_a,
            Body=part_data_2,
        )
        etag_a2 = resp["ETag"]

        # Upload B Part 2
        resp = s3_client.upload_part(
            Bucket=test_bucket,
            Key=key_b,
            PartNumber=2,
            UploadId=upload_id_b,
            Body=part_data_2,
        )
        etag_b2 = resp["ETag"]

        # Complete both uploads
        for key, upload_id, etag1, etag2 in [
            (key_a, upload_id_a, etag_a1, etag_a2),
            (key_b, upload_id_b, etag_b1, etag_b2),
        ]:
            response = s3_client.complete_multipart_upload(
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
            assert "ETag" in response, (
                f"Concurrent upload {key} failed - "
                f"each upload should have independent sequential numbering"
            )

        # Verify both objects exist
        head_a = s3_client.head_object(Bucket=test_bucket, Key=key_a)
        head_b = s3_client.head_object(Bucket=test_bucket, Key=key_b)
        assert head_a["ContentLength"] == len(part_data_1) + len(part_data_2)
        assert head_b["ContentLength"] == len(part_data_1) + len(part_data_2)

    @pytest.mark.e2e
    def test_elasticsearch_scenario_real_s3(self, s3_client, test_bucket):
        """
        Test Elasticsearch typical scenario: 5 parts of ~61MB each (305MB total).

        From production logs (shard 3):
        - Before fix: 5 client parts → 23 internal parts → EntityTooSmall
        - After fix: 5 client parts → 5 internal parts → Success

        Upload order: 3, 1, 5, 2, 4 (random, as Elasticsearch does)
        """
        key = "elasticsearch-shard3.bin"

        # Initiate upload
        response = s3_client.create_multipart_upload(Bucket=test_bucket, Key=key)
        upload_id = response["UploadId"]

        # Upload 5 parts in random order: 3, 1, 5, 2, 4
        total_size = 305_654_456
        avg_part_size = total_size // 5  # ~61MB per part
        upload_order = [3, 1, 5, 2, 4]

        parts = []
        for part_num in upload_order:
            part_data = bytes([part_num] * avg_part_size)
            response = s3_client.upload_part(
                Bucket=test_bucket,
                Key=key,
                PartNumber=part_num,
                UploadId=upload_id,
                Body=part_data,
            )
            parts.append({"PartNumber": part_num, "ETag": response["ETag"]})

        # Complete multipart (sort by PartNumber)
        parts.sort(key=lambda p: p["PartNumber"])
        response = s3_client.complete_multipart_upload(
            Bucket=test_bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

        assert "ETag" in response, (
            "Elasticsearch scenario failed - expected 5 sequential internal parts"
        )

        # Verify object size
        head_response = s3_client.head_object(Bucket=test_bucket, Key=key)
        expected_size = 5 * avg_part_size
        assert head_response["ContentLength"] == expected_size

    @pytest.mark.e2e
    def test_single_small_part_succeeds(self, s3_client, test_bucket):
        """
        Test that a single small part (< 5MB) works correctly.

        S3 spec: Last part can be any size, so single-part upload of 1MB should work.
        """
        key = "small-single-part.bin"

        # Initiate upload
        response = s3_client.create_multipart_upload(Bucket=test_bucket, Key=key)
        upload_id = response["UploadId"]

        # Upload single small part (1MB)
        part_data = b"S" * 1_048_576
        response = s3_client.upload_part(
            Bucket=test_bucket,
            Key=key,
            PartNumber=1,
            UploadId=upload_id,
            Body=part_data,
        )
        etag = response["ETag"]

        # Complete multipart
        response = s3_client.complete_multipart_upload(
            Bucket=test_bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": etag}]},
        )

        # Should succeed: single part is always "last"
        assert "ETag" in response, "Single small part should succeed (last part can be any size)"

        # Verify object
        head_response = s3_client.head_object(Bucket=test_bucket, Key=key)
        assert head_response["ContentLength"] == len(part_data)
