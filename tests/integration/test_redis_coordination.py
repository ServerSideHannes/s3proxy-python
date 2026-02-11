"""Integration tests for Redis coordination across concurrent operations.

These tests verify:
1. Redis WATCH mechanism handles concurrent state updates
2. Multiple concurrent part uploads to the same multipart upload work correctly
3. State is consistent despite race conditions
4. Retry logic handles optimistic locking conflicts
"""

import concurrent.futures
import time

import pytest
from botocore.exceptions import ClientError


@pytest.mark.e2e
class TestRedisCoordination:
    """Test Redis coordination for multipart uploads."""

    def test_concurrent_parts_same_upload(self, s3_client, test_bucket):
        """Test uploading multiple parts concurrently to the same multipart upload.

        This simulates what happens when different s3proxy pods handle different
        parts of the same upload. Redis WATCH should coordinate the state updates.
        """
        key = "redis-coordination-test.bin"

        # Create multipart upload
        response = s3_client.create_multipart_upload(Bucket=test_bucket, Key=key)
        upload_id = response["UploadId"]

        # Upload 20 parts concurrently
        num_parts = 20
        part_size = 5_242_880  # 5MB

        def upload_part(part_num):
            """Upload a single part."""
            part_data = bytes([part_num]) * part_size
            resp = s3_client.upload_part(
                Bucket=test_bucket,
                Key=key,
                PartNumber=part_num,
                UploadId=upload_id,
                Body=part_data,
            )
            return {"PartNumber": part_num, "ETag": resp["ETag"]}

        # Upload all parts concurrently (simulates different pods)
        start_time = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(upload_part, i) for i in range(1, num_parts + 1)]
            parts = [f.result() for f in concurrent.futures.as_completed(futures)]
        elapsed = time.time() - start_time

        # All parts should succeed despite concurrent Redis updates
        assert len(parts) == num_parts
        print(f"Uploaded {num_parts} parts concurrently in {elapsed:.2f}s")

        # Sort parts by part number for completion
        parts.sort(key=lambda p: p["PartNumber"])

        # Complete the upload
        s3_client.complete_multipart_upload(
            Bucket=test_bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

        # Verify object was created successfully
        obj = s3_client.head_object(Bucket=test_bucket, Key=key)
        assert obj["ContentLength"] == part_size * num_parts

        # Verify data integrity by downloading
        download_obj = s3_client.get_object(Bucket=test_bucket, Key=key)
        downloaded = download_obj["Body"].read()
        assert len(downloaded) == part_size * num_parts

        # Verify each part's data is correct
        for part_num in range(1, num_parts + 1):
            start = (part_num - 1) * part_size
            end = start + part_size
            part_data = downloaded[start:end]
            # All bytes in this part should be the part number
            assert all(b == part_num for b in part_data[:100])  # Check first 100 bytes

    def test_multiple_uploads_concurrent_parts(self, s3_client, test_bucket):
        """Test multiple multipart uploads with concurrent parts each.

        This creates high Redis contention to verify the coordination works.
        """
        num_uploads = 5
        parts_per_upload = 10

        def upload_file_with_concurrent_parts(file_num):
            """Upload a complete file using concurrent parts."""
            key = f"redis-multi-{file_num}.bin"
            part_size = 5_242_880

            # Create multipart upload
            response = s3_client.create_multipart_upload(Bucket=test_bucket, Key=key)
            upload_id = response["UploadId"]

            def upload_part(part_num):
                part_data = bytes([file_num + part_num]) * part_size
                resp = s3_client.upload_part(
                    Bucket=test_bucket,
                    Key=key,
                    PartNumber=part_num,
                    UploadId=upload_id,
                    Body=part_data,
                )
                return {"PartNumber": part_num, "ETag": resp["ETag"]}

            # Upload all parts for this file concurrently
            with concurrent.futures.ThreadPoolExecutor(max_workers=parts_per_upload) as executor:
                futures = [executor.submit(upload_part, i) for i in range(1, parts_per_upload + 1)]
                parts = [f.result() for f in concurrent.futures.as_completed(futures)]

            # Sort and complete
            parts.sort(key=lambda p: p["PartNumber"])
            s3_client.complete_multipart_upload(
                Bucket=test_bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )

            return key

        # Upload multiple files, each with concurrent parts
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_uploads) as executor:
            futures = [
                executor.submit(upload_file_with_concurrent_parts, i) for i in range(num_uploads)
            ]
            keys = [f.result() for f in concurrent.futures.as_completed(futures)]

        # Verify all uploads succeeded
        assert len(keys) == num_uploads

        # Verify all objects exist with correct size
        for key in keys:
            obj = s3_client.head_object(Bucket=test_bucket, Key=key)
            assert obj["ContentLength"] == 5_242_880 * parts_per_upload

    def test_rapid_part_uploads_same_upload(self, s3_client, test_bucket):
        """Test rapid successive part uploads to stress Redis WATCH retries."""
        key = "redis-rapid-test.bin"

        # Create multipart upload
        response = s3_client.create_multipart_upload(Bucket=test_bucket, Key=key)
        upload_id = response["UploadId"]

        # Upload 20 parts rapidly (5MB minimum required by S3)
        num_parts = 20
        part_size = 5_242_880  # 5MB (minimum valid part size)

        def upload_part(part_num):
            part_data = bytes([part_num % 256]) * part_size
            resp = s3_client.upload_part(
                Bucket=test_bucket,
                Key=key,
                PartNumber=part_num,
                UploadId=upload_id,
                Body=part_data,
            )
            return {"PartNumber": part_num, "ETag": resp["ETag"]}

        # Upload all parts with high concurrency
        start_time = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            futures = [executor.submit(upload_part, i) for i in range(1, num_parts + 1)]
            parts = [f.result() for f in concurrent.futures.as_completed(futures)]
        elapsed = time.time() - start_time

        rate = num_parts / elapsed
        print(f"Uploaded {num_parts} parts rapidly in {elapsed:.2f}s ({rate:.1f} parts/sec)")

        # All parts should succeed
        assert len(parts) == num_parts

        # Complete
        parts.sort(key=lambda p: p["PartNumber"])
        s3_client.complete_multipart_upload(
            Bucket=test_bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

        # Verify
        obj = s3_client.head_object(Bucket=test_bucket, Key=key)
        assert obj["ContentLength"] == part_size * num_parts

    def test_out_of_order_concurrent_parts(self, s3_client, test_bucket):
        """Test uploading parts out of order concurrently.

        This verifies Redis state tracks parts correctly regardless of upload order.
        """
        key = "redis-out-of-order.bin"

        # Create multipart upload
        response = s3_client.create_multipart_upload(Bucket=test_bucket, Key=key)
        upload_id = response["UploadId"]

        # Upload parts in reverse order, concurrently
        num_parts = 15
        part_size = 5_242_880

        def upload_part(part_num):
            part_data = bytes([part_num]) * part_size
            resp = s3_client.upload_part(
                Bucket=test_bucket,
                Key=key,
                PartNumber=part_num,
                UploadId=upload_id,
                Body=part_data,
            )
            return {"PartNumber": part_num, "ETag": resp["ETag"]}

        # Upload in reverse order: 15, 14, 13, ..., 1
        reverse_order = list(range(num_parts, 0, -1))
        with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
            futures = [executor.submit(upload_part, i) for i in reverse_order]
            parts = [f.result() for f in concurrent.futures.as_completed(futures)]

        # Complete with correct order
        parts.sort(key=lambda p: p["PartNumber"])
        s3_client.complete_multipart_upload(
            Bucket=test_bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

        # Verify correct assembly
        obj = s3_client.get_object(Bucket=test_bucket, Key=key)
        downloaded = obj["Body"].read()

        # Verify parts are in correct order
        for part_num in range(1, num_parts + 1):
            start = (part_num - 1) * part_size
            # Check that this section contains the expected byte value
            assert downloaded[start] == part_num

    def test_concurrent_complete_attempts(self, s3_client, test_bucket):
        """Test that concurrent completion attempts don't cause issues.

        This tests what happens if multiple requests try to complete the same upload.
        """
        key = "redis-concurrent-complete.bin"

        # Create and upload parts
        response = s3_client.create_multipart_upload(Bucket=test_bucket, Key=key)
        upload_id = response["UploadId"]

        parts = []
        for i in range(1, 6):
            part_data = bytes([i]) * 5_242_880
            resp = s3_client.upload_part(
                Bucket=test_bucket,
                Key=key,
                PartNumber=i,
                UploadId=upload_id,
                Body=part_data,
            )
            parts.append({"PartNumber": i, "ETag": resp["ETag"]})

        # Try to complete from multiple threads simultaneously
        def complete_upload():
            try:
                s3_client.complete_multipart_upload(
                    Bucket=test_bucket,
                    Key=key,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": parts},
                )
                return "success"
            except ClientError as e:
                # Second attempt may fail with NoSuchUpload (already completed)
                return f"error: {e.response['Error']['Code']}"

        # Attempt concurrent completions
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(complete_upload) for _ in range(5)]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]

        # At least one should succeed
        assert "success" in results

        # Verify object exists
        obj = s3_client.head_object(Bucket=test_bucket, Key=key)
        assert obj["ContentLength"] == 5_242_880 * 5


@pytest.mark.e2e
class TestRedisWatchRetries:
    """Test that Redis WATCH retries work correctly under contention."""

    def test_high_contention_scenario(self, s3_client, test_bucket):
        """Test extreme contention scenario with many concurrent parts.

        This should trigger multiple Redis WATCH retries.
        """
        key = "redis-high-contention.bin"

        # Create multipart upload
        response = s3_client.create_multipart_upload(Bucket=test_bucket, Key=key)
        upload_id = response["UploadId"]

        # Upload 30 parts with very high concurrency
        num_parts = 30

        def upload_part(part_num):
            part_data = bytes([part_num]) * 5_242_880
            try:
                resp = s3_client.upload_part(
                    Bucket=test_bucket,
                    Key=key,
                    PartNumber=part_num,
                    UploadId=upload_id,
                    Body=part_data,
                )
                return {"PartNumber": part_num, "ETag": resp["ETag"], "status": "success"}
            except Exception as e:
                return {"PartNumber": part_num, "status": "failed", "error": str(e)}

        # Maximum concurrency to maximize Redis contention
        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
            futures = [executor.submit(upload_part, i) for i in range(1, num_parts + 1)]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]

        # All should succeed (retries should handle conflicts)
        successful = [r for r in results if r["status"] == "success"]
        failed = [r for r in results if r["status"] == "failed"]

        assert len(successful) == num_parts, f"Failed parts: {failed}"

        # Complete
        parts = [{"PartNumber": r["PartNumber"], "ETag": r["ETag"]} for r in successful]
        parts.sort(key=lambda p: p["PartNumber"])

        s3_client.complete_multipart_upload(
            Bucket=test_bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

        # Verify
        obj = s3_client.head_object(Bucket=test_bucket, Key=key)
        assert obj["ContentLength"] == 5_242_880 * num_parts

    def test_interleaved_uploads_different_files(self, s3_client, test_bucket):
        """Test interleaved part uploads to different files.

        This ensures Redis state isolation between different uploads.
        """
        num_files = 3
        parts_per_file = 10

        upload_info = {}
        for i in range(num_files):
            key = f"redis-interleaved-{i}.bin"
            response = s3_client.create_multipart_upload(Bucket=test_bucket, Key=key)
            upload_info[i] = {
                "key": key,
                "upload_id": response["UploadId"],
                "parts": [],
            }

        # Upload parts to all files in interleaved fashion
        def upload_part(file_num, part_num):
            info = upload_info[file_num]
            part_data = bytes([file_num * 10 + part_num]) * 5_242_880
            resp = s3_client.upload_part(
                Bucket=test_bucket,
                Key=info["key"],
                PartNumber=part_num,
                UploadId=info["upload_id"],
                Body=part_data,
            )
            return file_num, {"PartNumber": part_num, "ETag": resp["ETag"]}

        # Create interleaved upload tasks
        tasks = []
        for part_num in range(1, parts_per_file + 1):
            for file_num in range(num_files):
                tasks.append((file_num, part_num))

        # Execute all uploads concurrently (interleaved across files)
        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
            futures = [executor.submit(upload_part, f, p) for f, p in tasks]
            for future in concurrent.futures.as_completed(futures):
                file_num, part = future.result()
                upload_info[file_num]["parts"].append(part)

        # Complete all uploads
        for info in upload_info.values():
            info["parts"].sort(key=lambda p: p["PartNumber"])
            s3_client.complete_multipart_upload(
                Bucket=test_bucket,
                Key=info["key"],
                UploadId=info["upload_id"],
                MultipartUpload={"Parts": info["parts"]},
            )

        # Verify all files
        for i in range(num_files):
            key = f"redis-interleaved-{i}.bin"
            obj = s3_client.head_object(Bucket=test_bucket, Key=key)
            assert obj["ContentLength"] == 5_242_880 * parts_per_file
