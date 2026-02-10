"""Integration tests for concurrent operations and throttling."""

import concurrent.futures
import sys

import pytest
from botocore.exceptions import ClientError


def log(msg):
    print(f"[DEBUG] {msg}", file=sys.stderr, flush=True)


@pytest.mark.e2e
class TestConcurrentUploads:
    """Test concurrent upload scenarios."""

    def test_concurrent_multipart_uploads(self, s3_client, test_bucket):
        """Test multiple concurrent multipart uploads complete successfully."""
        log("test_concurrent_multipart_uploads START")
        num_uploads = 3  # reduced
        part_size = 5_242_880  # 5MB - S3 requires non-final parts >= 5MB

        def upload_file(file_num):
            log(f"  upload_file({file_num}) START")
            key = f"concurrent-{file_num}.bin"

            log(f"  upload_file({file_num}) create_multipart_upload")
            response = s3_client.create_multipart_upload(Bucket=test_bucket, Key=key)
            upload_id = response["UploadId"]
            log(f"  upload_file({file_num}) got upload_id")

            parts = []
            for i in range(1, 3):  # 2 parts only
                log(f"  upload_file({file_num}) uploading part {i}")
                part_data = bytes([file_num + i]) * part_size
                resp = s3_client.upload_part(
                    Bucket=test_bucket,
                    Key=key,
                    PartNumber=i,
                    UploadId=upload_id,
                    Body=part_data,
                )
                parts.append({"PartNumber": i, "ETag": resp["ETag"]})
                log(f"  upload_file({file_num}) part {i} done")

            log(f"  upload_file({file_num}) complete_multipart_upload")
            s3_client.complete_multipart_upload(
                Bucket=test_bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
            log(f"  upload_file({file_num}) DONE")
            return key

        log("Starting concurrent uploads...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_uploads) as executor:
            futures = [executor.submit(upload_file, i) for i in range(num_uploads)]
            keys = [f.result() for f in concurrent.futures.as_completed(futures)]

        log(f"Got {len(keys)} results")
        assert len(keys) == num_uploads

        for key in keys:
            log(f"Verifying {key}")
            obj = s3_client.head_object(Bucket=test_bucket, Key=key)
            assert obj["ContentLength"] == part_size * 2
        log("test_concurrent_multipart_uploads DONE")

    def test_concurrent_simple_uploads(self, s3_client, test_bucket):
        """Test concurrent simple uploads."""
        log("test_concurrent_simple_uploads START")
        num_uploads = 5

        def upload_file(file_num):
            log(f"  simple upload {file_num}")
            key = f"simple-concurrent-{file_num}.txt"
            data = f"File {file_num}".encode() * 100
            s3_client.put_object(Bucket=test_bucket, Key=key, Body=data)
            log(f"  simple upload {file_num} done")
            return key

        log("Starting concurrent simple uploads...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_uploads) as executor:
            futures = [executor.submit(upload_file, i) for i in range(num_uploads)]
            keys = [f.result() for f in concurrent.futures.as_completed(futures)]

        log(f"Got {len(keys)} results")
        assert len(keys) == num_uploads
        log("test_concurrent_simple_uploads DONE")

    def test_concurrent_mixed_operations(self, s3_client, test_bucket):
        """Test concurrent mixed operations."""
        log("test_concurrent_mixed_operations START")

        log("Creating test objects...")
        for i in range(3):
            log(f"  put_object mixed-op-{i}.txt")
            s3_client.put_object(
                Bucket=test_bucket,
                Key=f"mixed-op-{i}.txt",
                Body=b"test data" * 10,
            )

        def perform_operations():
            log("  perform_operations START")
            results = []
            try:
                s3_client.get_object(Bucket=test_bucket, Key="mixed-op-0.txt")
                results.append(("GET", "success"))
            except ClientError:
                results.append(("GET", "fail"))
            try:
                s3_client.head_object(Bucket=test_bucket, Key="mixed-op-1.txt")
                results.append(("HEAD", "success"))
            except ClientError:
                results.append(("HEAD", "fail"))
            log("  perform_operations DONE")
            return results

        log("Running mixed ops concurrently...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(perform_operations) for _ in range(5)]
            all_results = [f.result() for f in concurrent.futures.as_completed(futures)]

        log(f"Got {len(all_results)} results")
        assert len(all_results) == 5
        log("test_concurrent_mixed_operations DONE")

    def test_concurrent_downloads(self, s3_client, test_bucket):
        """Test concurrent downloads."""
        log("test_concurrent_downloads START")

        key = "concurrent-download-test.bin"
        test_data = b"X" * 1_000_000  # 1MB
        log(f"Uploading {len(test_data)} bytes")
        s3_client.put_object(Bucket=test_bucket, Key=key, Body=test_data)

        def download_and_verify():
            log("  downloading...")
            obj = s3_client.get_object(Bucket=test_bucket, Key=key)
            downloaded = obj["Body"].read()
            log(f"  downloaded {len(downloaded)} bytes")
            assert downloaded == test_data
            return len(downloaded)

        log("Starting concurrent downloads...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(download_and_verify) for _ in range(5)]
            sizes = [f.result() for f in concurrent.futures.as_completed(futures)]

        assert all(size == len(test_data) for size in sizes)
        log("test_concurrent_downloads DONE")


@pytest.mark.e2e
class TestThrottling:
    """Test throttling behavior."""

    def test_many_concurrent_requests(self, s3_client, test_bucket):
        """Test many concurrent requests."""
        log("test_many_concurrent_requests START")
        num_requests = 20  # reduced

        def upload_small_file(num):
            log(f"  small file {num}")
            key = f"throttle-test-{num}.txt"
            s3_client.put_object(Bucket=test_bucket, Key=key, Body=b"test" * 10)
            log(f"  small file {num} done")
            return key

        log(f"Sending {num_requests} concurrent requests...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_requests) as executor:
            futures = [executor.submit(upload_small_file, i) for i in range(num_requests)]
            keys = [f.result() for f in concurrent.futures.as_completed(futures)]

        log(f"Got {len(keys)} results")
        assert len(keys) == num_requests
        log("test_many_concurrent_requests DONE")

    def test_throttle_with_large_files(self, s3_client, test_bucket):
        """Test throttling with large file uploads."""
        log("test_throttle_with_large_files START")
        num_uploads = 2  # reduced

        def upload_large_file(num):
            log(f"  large file {num} START")
            key = f"large-throttle-{num}.bin"

            log(f"  large file {num} create_multipart_upload")
            response = s3_client.create_multipart_upload(Bucket=test_bucket, Key=key)
            upload_id = response["UploadId"]

            parts = []
            for i in range(1, 3):  # 2 parts
                log(f"  large file {num} uploading part {i}")
                part_data = bytes([num + i]) * 5_242_880  # 5MB - S3 requires non-final parts >= 5MB
                resp = s3_client.upload_part(
                    Bucket=test_bucket,
                    Key=key,
                    PartNumber=i,
                    UploadId=upload_id,
                    Body=part_data,
                )
                parts.append({"PartNumber": i, "ETag": resp["ETag"]})
                log(f"  large file {num} part {i} done")

            log(f"  large file {num} complete_multipart_upload")
            s3_client.complete_multipart_upload(
                Bucket=test_bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
            log(f"  large file {num} DONE")
            return key

        log("Starting large file uploads...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_uploads) as executor:
            futures = [executor.submit(upload_large_file, i) for i in range(num_uploads)]
            keys = [f.result() for f in concurrent.futures.as_completed(futures)]

        log(f"Got {len(keys)} results")
        assert len(keys) == num_uploads

        for key in keys:
            log(f"Verifying {key}")
            obj = s3_client.head_object(Bucket=test_bucket, Key=key)
            assert obj["ContentLength"] == 5_242_880 * 2
        log("test_throttle_with_large_files DONE")
