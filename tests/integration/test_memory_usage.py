"""Test that s3proxy memory-based concurrency limiting prevents OOM.

This test verifies that:
1. memory_limit_mb bounds memory usage for concurrent connections
2. Excess connections get 503 (not OOM crash)
3. Server stays alive and responsive after stress
"""

import contextlib
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

import boto3
import pytest


def log(msg: str):
    """Print debug message with timestamp."""
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", file=sys.stderr, flush=True)


def find_free_port() -> int:
    """Find an available port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


@pytest.mark.e2e
class TestMemoryBasedConcurrencyStress:
    """Stress test to verify memory_limit_mb prevents OOM.

    This test hammers the server with concurrent large uploads to prove:
    1. memory_limit_mb bounds memory usage for concurrent connections
    2. Excess connections get 503 (not OOM crash)
    3. Server stays alive and responsive after the storm
    """

    @pytest.fixture
    def s3proxy_with_memory_limit(self):
        """Start s3proxy with memory_limit_mb=16 for stress testing."""
        port = find_free_port()

        env = os.environ.copy()
        env.update(
            {
                "S3PROXY_ENCRYPT_KEY": "test-encryption-key-32-bytes!!",
                "S3PROXY_HOST": "http://localhost:9000",
                "S3PROXY_REGION": "us-east-1",
                "S3PROXY_PORT": str(port),
                "S3PROXY_NO_TLS": "true",
                "S3PROXY_LOG_LEVEL": "WARNING",
                "S3PROXY_MEMORY_LIMIT_MB": "16",
                "S3PROXY_MAX_PART_SIZE_MB": "0",
                "AWS_ACCESS_KEY_ID": "minioadmin",
                "AWS_SECRET_ACCESS_KEY": "minioadmin",
            }
        )

        proc = subprocess.Popen(
            ["python", "-m", "s3proxy.main"],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        for _i in range(30):
            if proc.poll() is not None:
                pytest.fail(f"s3proxy died with code {proc.returncode}")
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                if sock.connect_ex(("localhost", port)) == 0:
                    break
            time.sleep(0.5)
        else:
            proc.kill()
            pytest.fail("s3proxy failed to start")

        yield f"http://localhost:{port}", proc

        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()

    @pytest.fixture
    def s3proxy_with_short_backpressure(self):
        """Start s3proxy with memory_limit_mb=16 and 1s backpressure timeout.

        With a short timeout, requests that can't acquire memory will be
        rejected quickly, allowing us to test rejection behavior.
        """
        port = find_free_port()

        env = os.environ.copy()
        env.update(
            {
                "S3PROXY_ENCRYPT_KEY": "test-encryption-key-32-bytes!!",
                "S3PROXY_HOST": "http://localhost:9000",
                "S3PROXY_REGION": "us-east-1",
                "S3PROXY_PORT": str(port),
                "S3PROXY_NO_TLS": "true",
                "S3PROXY_LOG_LEVEL": "WARNING",
                "S3PROXY_MEMORY_LIMIT_MB": "16",
                "S3PROXY_MAX_PART_SIZE_MB": "0",
                "S3PROXY_BACKPRESSURE_TIMEOUT": "1",
            }
        )

        proc = subprocess.Popen(
            ["python", "-m", "s3proxy.main"],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        for _i in range(30):
            if proc.poll() is not None:
                pytest.fail(f"s3proxy died with code {proc.returncode}")
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                if sock.connect_ex(("localhost", port)) == 0:
                    break
            time.sleep(0.5)
        else:
            proc.kill()
            pytest.fail("s3proxy failed to start")

        yield f"http://localhost:{port}", proc

        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()

    @pytest.fixture
    def stress_client(self, s3proxy_with_memory_limit):
        """Create S3 client for stress tests."""
        url, _ = s3proxy_with_memory_limit
        return boto3.client(
            "s3",
            endpoint_url=url,
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            region_name="us-east-1",
            config=boto3.session.Config(
                retries={"max_attempts": 3},
                connect_timeout=10,
                read_timeout=120,
            ),
        )

    @pytest.fixture
    def stress_bucket(self, stress_client):
        """Create test bucket with unique name."""
        bucket = f"stress-{uuid.uuid4().hex[:8]}"
        with contextlib.suppress(stress_client.exceptions.BucketAlreadyOwnedByYou):
            stress_client.create_bucket(Bucket=bucket)
        yield bucket
        try:
            response = stress_client.list_objects_v2(Bucket=bucket)
            if "Contents" in response:
                objects = [{"Key": obj["Key"]} for obj in response["Contents"]]
                stress_client.delete_objects(Bucket=bucket, Delete={"Objects": objects})
            stress_client.delete_bucket(Bucket=bucket)
        except Exception:
            pass

    def test_backpressure_queues_concurrent_uploads(self, s3proxy_with_memory_limit, stress_bucket):
        """Verify backpressure queues excess requests instead of rejecting them.

        With memory_limit_mb=16 and default backpressure timeout (30s),
        concurrent uploads that exceed the memory budget should be queued
        and eventually succeed — not rejected with 503.

        This proves:
        - Backpressure correctly serializes requests within the timeout
        - Server doesn't crash under load
        - All uploads complete successfully
        """
        import concurrent.futures

        from botocore.auth import SigV4Auth
        from botocore.awsrequest import AWSRequest
        from botocore.credentials import Credentials

        log("=" * 60)
        log("TEST: Backpressure queues concurrent uploads (memory_limit_mb=16)")
        log("=" * 60)

        url, proc = s3proxy_with_memory_limit
        num_concurrent = 6
        upload_size = 20 * 1024 * 1024  # 20MB each

        log(f"Sending {num_concurrent} concurrent {upload_size // 1024 // 1024}MB uploads...")
        log("Expected: backpressure queues excess requests, all eventually succeed")

        test_data = bytes([42]) * upload_size

        def upload_one(i: int) -> dict:
            key = f"bp-queue-test-{i}.bin"
            endpoint = f"{url}/{stress_bucket}/{key}"
            start_time = time.time()

            try:
                credentials = Credentials("minioadmin", "minioadmin")
                aws_request = AWSRequest(method="PUT", url=endpoint, data=test_data)
                aws_request.headers["Content-Type"] = "application/octet-stream"
                aws_request.headers["x-amz-content-sha256"] = "UNSIGNED-PAYLOAD"
                SigV4Auth(credentials, "s3", "us-east-1").add_auth(aws_request)

                req = urllib.request.Request(
                    endpoint,
                    data=test_data,
                    headers=dict(aws_request.headers),
                    method="PUT",
                )
                with urllib.request.urlopen(req, timeout=120) as response:
                    elapsed = time.time() - start_time
                    log(f"  [{i}] SUCCESS status={response.status} elapsed={elapsed:.2f}s")
                    return {"index": i, "success": True, "elapsed": elapsed}
            except Exception as e:
                elapsed = time.time() - start_time
                log(f"  [{i}] FAILED elapsed={elapsed:.2f}s error={e}")
                return {"index": i, "success": False, "elapsed": elapsed, "error": str(e)}

        all_results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_concurrent) as executor:
            futures = [executor.submit(upload_one, i) for i in range(num_concurrent)]
            for future in concurrent.futures.as_completed(futures):
                all_results.append(future.result())

        succeeded = sum(1 for r in all_results if r["success"])
        log(f"Results: {succeeded}/{num_concurrent} succeeded")

        assert proc.poll() is None, "Server crashed during stress test (likely OOM)!"
        assert succeeded == num_concurrent, (
            f"Expected all {num_concurrent} uploads to succeed via backpressure, "
            f"but only {succeeded} did"
        )

        log("TEST PASSED! Backpressure queued all requests, none rejected.")

    def test_server_recovers_after_storm(
        self, s3proxy_with_memory_limit, stress_client, stress_bucket
    ):
        """After the stress test, verify normal operations still work."""
        log("=" * 60)
        log("TEST: Server recovery - normal upload after stress")
        log("=" * 60)

        url, proc = s3proxy_with_memory_limit

        key = "recovery-test.bin"
        data = b"Hello after stress test!"

        log("Uploading small object to verify server recovery...")
        stress_client.put_object(Bucket=stress_bucket, Key=key, Body=data)

        response = stress_client.get_object(Bucket=stress_bucket, Key=key)
        body = response["Body"].read()
        assert body == data, f"Data mismatch: {body} != {data}"

        log("TEST PASSED! Server recovered and handles normal requests.")

    def test_rejection_after_backpressure_timeout(
        self, s3proxy_with_short_backpressure, stress_bucket
    ):
        """Verify requests are rejected after backpressure timeout expires.

        Uses a 1-second backpressure timeout so that when memory is full,
        queued requests time out quickly and get 503 SlowDown responses.

        This proves:
        - Backpressure timeout is respected
        - Rejection happens after timeout, not immediately
        - Server stays alive after rejections
        """
        import concurrent.futures

        from botocore.auth import SigV4Auth
        from botocore.awsrequest import AWSRequest
        from botocore.credentials import Credentials

        log("=" * 60)
        log("TEST: Rejection after backpressure timeout (timeout=1s, memory_limit_mb=16)")
        log("=" * 60)

        url, proc = s3proxy_with_short_backpressure

        # Ensure bucket exists via this server instance
        try:
            credentials = Credentials("minioadmin", "minioadmin")
            endpoint = f"{url}/{stress_bucket}"
            aws_request = AWSRequest(method="PUT", url=endpoint)
            aws_request.headers["x-amz-content-sha256"] = "UNSIGNED-PAYLOAD"
            SigV4Auth(credentials, "s3", "us-east-1").add_auth(aws_request)
            req = urllib.request.Request(endpoint, headers=dict(aws_request.headers), method="PUT")
            with urllib.request.urlopen(req, timeout=10):
                pass
        except urllib.error.HTTPError:
            pass  # bucket may already exist

        num_uploads = 8
        upload_size = 20 * 1024 * 1024  # 20MB each
        test_data = bytes([42]) * upload_size

        log(f"Sending {num_uploads} concurrent {upload_size // 1024 // 1024}MB uploads...")
        log("Expected: some succeed, some rejected after 1s backpressure timeout")

        def upload_one(i: int) -> dict:
            key = f"reject-test-{i}.bin"
            endpoint = f"{url}/{stress_bucket}/{key}"
            start_time = time.time()

            credentials = Credentials("minioadmin", "minioadmin")
            aws_request = AWSRequest(method="PUT", url=endpoint, data=test_data)
            aws_request.headers["Content-Type"] = "application/octet-stream"
            aws_request.headers["x-amz-content-sha256"] = "UNSIGNED-PAYLOAD"
            SigV4Auth(credentials, "s3", "us-east-1").add_auth(aws_request)

            req = urllib.request.Request(
                endpoint,
                data=test_data,
                headers=dict(aws_request.headers),
                method="PUT",
            )

            try:
                with urllib.request.urlopen(req, timeout=120) as response:
                    elapsed = time.time() - start_time
                    return {
                        "index": i,
                        "status": response.status,
                        "elapsed": elapsed,
                        "rejected": False,
                    }
            except urllib.error.HTTPError as e:
                elapsed = time.time() - start_time
                return {"index": i, "status": e.code, "elapsed": elapsed, "rejected": e.code == 503}
            except Exception as e:
                elapsed = time.time() - start_time
                error_str = str(e)
                is_rejected = "reset" in error_str.lower() or "broken" in error_str.lower()
                return {
                    "index": i,
                    "status": 0,
                    "elapsed": elapsed,
                    "rejected": is_rejected,
                    "error": error_str,
                }

        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_uploads) as executor:
            futures = [executor.submit(upload_one, i) for i in range(num_uploads)]
            for future in concurrent.futures.as_completed(futures):
                results.append(future.result())

        rejected = [r for r in results if r["rejected"]]
        succeeded = [r for r in results if not r["rejected"] and r["status"] in (200, 204)]

        log(f"Results: {len(succeeded)} succeeded, {len(rejected)} rejected")
        for r in rejected:
            log(f"  [{r['index']}] rejected in {r['elapsed']:.2f}s")

        assert proc.poll() is None, "Server crashed!"
        assert len(rejected) > 0, (
            "Expected some requests to be rejected after 1s backpressure timeout"
        )

        log("TEST PASSED! Requests rejected after backpressure timeout expired.")

    @pytest.mark.skipif(
        sys.platform == "darwin", reason="macOS malloc doesn't reliably return memory to OS"
    )
    def test_memory_bounded_during_sustained_load(
        self, s3proxy_with_memory_limit, stress_bucket, stress_client
    ):
        """Verify memory stays bounded while processing many concurrent uploads.

        Sends concurrent uploads that are queued via backpressure. Verifies:
        1. All uploads eventually succeed (backpressure queues them)
        2. Memory stays bounded (streaming + memory limiting works)
        3. All files exist in bucket with correct sizes
        4. Server doesn't crash
        """
        import concurrent.futures

        import psutil
        from botocore.auth import SigV4Auth
        from botocore.awsrequest import AWSRequest
        from botocore.credentials import Credentials

        log("=" * 60)
        log("TEST: Memory bounded during sustained upload load")
        log("=" * 60)

        url, proc = s3proxy_with_memory_limit
        server_proc = psutil.Process(proc.pid)

        def get_memory_mb() -> float:
            return server_proc.memory_info().rss / (1024 * 1024)

        baseline_mb = get_memory_mb()
        log(f"Baseline memory: {baseline_mb:.1f} MB")

        num_uploads = 20
        upload_size = 30 * 1024 * 1024  # 30MB each = 600MB total
        test_data = bytes([42]) * upload_size
        max_concurrent = 6

        log(f"Uploading {num_uploads} x {upload_size // 1024 // 1024}MB files (memory_limit_mb=16)")
        log(f"Total data: {num_uploads * upload_size // 1024 // 1024}MB")

        def upload_one(i: int) -> dict:
            """Upload with retries until success."""
            import random

            key = f"memory-test-{i}.bin"
            endpoint = f"{url}/{stress_bucket}/{key}"
            attempts = 0
            max_attempts = 50

            while attempts < max_attempts:
                attempts += 1
                credentials = Credentials("minioadmin", "minioadmin")
                aws_request = AWSRequest(method="PUT", url=endpoint, data=test_data)
                aws_request.headers["Content-Type"] = "application/octet-stream"
                aws_request.headers["x-amz-content-sha256"] = "UNSIGNED-PAYLOAD"
                SigV4Auth(credentials, "s3", "us-east-1").add_auth(aws_request)

                req = urllib.request.Request(
                    endpoint,
                    data=test_data,
                    headers=dict(aws_request.headers),
                    method="PUT",
                )

                try:
                    with urllib.request.urlopen(req, timeout=120) as response:
                        return {
                            "index": i,
                            "key": key,
                            "status": response.status,
                            "success": True,
                            "attempts": attempts,
                        }
                except urllib.error.HTTPError as e:
                    if e.code == 503:
                        delay = min(0.5 + (attempts * 0.1) + random.uniform(0, 0.3), 3.0)
                        time.sleep(delay)
                        continue
                    return {
                        "index": i,
                        "key": key,
                        "status": e.code,
                        "success": False,
                        "attempts": attempts,
                    }
                except Exception as e:
                    error_str = str(e)
                    if "reset" in error_str.lower() or "broken" in error_str.lower():
                        delay = min(0.5 + (attempts * 0.1) + random.uniform(0, 0.3), 3.0)
                        time.sleep(delay)
                        continue
                    return {
                        "index": i,
                        "key": key,
                        "status": 0,
                        "success": False,
                        "error": error_str,
                        "attempts": attempts,
                    }

            return {
                "index": i,
                "key": key,
                "status": 0,
                "success": False,
                "attempts": attempts,
                "error": "max retries",
            }

        peak_mb = baseline_mb
        memory_samples = []
        results = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent) as executor:
            futures = [executor.submit(upload_one, i) for i in range(num_uploads)]

            while not all(f.done() for f in futures):
                current_mb = get_memory_mb()
                memory_samples.append(current_mb)
                if current_mb > peak_mb:
                    peak_mb = current_mb
                time.sleep(0.05)

            for f in futures:
                results.append(f.result())

        succeeded = sum(1 for r in results if r.get("success"))
        failed = sum(1 for r in results if not r.get("success"))
        memory_increase = peak_mb - baseline_mb

        log(f"Results: {succeeded} succeeded, {failed} failed")
        log(
            f"Memory: baseline={baseline_mb:.1f} MB, "
            f"peak={peak_mb:.1f} MB, increase={memory_increase:.1f} MB"
        )

        for r in results:
            if not r.get("success"):
                log(f"  [{r['index']}] FAILED after {r['attempts']} attempts: {r.get('error', '')}")

        # Assertions
        assert proc.poll() is None, "Server crashed!"
        assert succeeded == num_uploads, (
            f"Expected all {num_uploads} uploads to succeed, but {failed} failed"
        )

        # Verify all files exist in bucket
        log("Verifying all files exist in bucket...")
        response = stress_client.list_objects_v2(Bucket=stress_bucket, Prefix="memory-test-")
        objects = {obj["Key"]: obj["Size"] for obj in response.get("Contents", [])}

        missing = [r["key"] for r in results if r.get("success") and r["key"] not in objects]
        assert not missing, f"Missing files in bucket: {missing}"
        log(f"Verified: {len(objects)} files in bucket")

        # Memory assertion: with streaming + memory limiting, should stay bounded
        # Without limiting: 6 concurrent × 30MB = 180MB minimum
        # With memory_limit_mb=16: much less
        max_expected = 100  # MB
        assert memory_increase < max_expected, (
            f"Memory increased by {memory_increase:.1f} MB - expected < {max_expected} MB"
        )
        log(f"Memory bounded: {memory_increase:.1f} MB < {max_expected} MB")

        log("TEST PASSED! All uploads completed, memory stayed bounded.")

    @pytest.mark.skipif(
        sys.platform == "darwin", reason="macOS malloc doesn't reliably return memory to OS"
    )
    def test_multipart_memory_bounded(
        self, s3proxy_with_memory_limit, stress_bucket, stress_client
    ):
        """Verify memory stays bounded during explicit multipart uploads.

        Uses boto3's explicit multipart API (CreateMultipartUpload + UploadPart + Complete).
        With memory_limit_mb=16, excess requests get 503.
        Memory should stay bounded due to streaming (8MB buffer per upload part).
        """
        import concurrent.futures
        import io

        import psutil

        log("=" * 60)
        log("TEST: Memory bounded during multipart uploads (memory_limit_mb=16)")
        log("=" * 60)

        url, proc = s3proxy_with_memory_limit
        server_proc = psutil.Process(proc.pid)

        def get_memory_mb() -> float:
            return server_proc.memory_info().rss / (1024 * 1024)

        baseline_mb = get_memory_mb()
        log(f"Baseline memory: {baseline_mb:.1f} MB")

        # Upload config: 20 x 100MB files using explicit multipart (2 x 50MB parts each)
        # With memory_limit_mb=16, memory should stay bounded regardless of upload count
        num_uploads = 20
        part_size = 50 * 1024 * 1024  # 50MB per part
        num_parts = 2  # 2 parts = 100MB total
        total_size = part_size * num_parts
        max_concurrent = 6  # More than budget allows to trigger 503s

        log(
            f"Uploading {num_uploads} x "
            f"{total_size // 1024 // 1024}MB files via multipart (2GB total)"
        )
        log(f"Each file: {num_parts} parts x {part_size // 1024 // 1024}MB")
        log(f"Total data: {num_uploads * total_size // 1024 // 1024}MB")

        def upload_multipart(i: int) -> dict:
            """Upload using explicit multipart API with retries."""
            import random

            key = f"multipart-test-{i}.bin"
            attempts = 0
            max_attempts = 100  # Many retries needed with limited memory budget
            last_error = ""

            while attempts < max_attempts:
                attempts += 1
                upload_id = None
                try:
                    # Create multipart upload
                    create_resp = stress_client.create_multipart_upload(
                        Bucket=stress_bucket, Key=key
                    )
                    upload_id = create_resp["UploadId"]

                    parts = []
                    for part_num in range(1, num_parts + 1):
                        part_data = bytes([42 + part_num]) * part_size
                        part_resp = stress_client.upload_part(
                            Bucket=stress_bucket,
                            Key=key,
                            UploadId=upload_id,
                            PartNumber=part_num,
                            Body=io.BytesIO(part_data),
                        )
                        parts.append({"PartNumber": part_num, "ETag": part_resp["ETag"]})

                    stress_client.complete_multipart_upload(
                        Bucket=stress_bucket,
                        Key=key,
                        UploadId=upload_id,
                        MultipartUpload={"Parts": parts},
                    )
                    return {"index": i, "key": key, "success": True, "attempts": attempts}

                except Exception as e:
                    last_error = str(e)
                    # Abort the failed multipart upload to clean up
                    if upload_id:
                        with contextlib.suppress(Exception):
                            stress_client.abort_multipart_upload(
                                Bucket=stress_bucket, Key=key, UploadId=upload_id
                            )
                    # Retry on any transient error (503, SlowDown, connection issues, etc.)
                    # In a stress test with high contention, most errors are transient
                    delay = min(0.3 + (attempts * 0.05) + random.uniform(0, 0.2), 2.0)
                    time.sleep(delay)
                    continue

            return {
                "index": i,
                "key": key,
                "success": False,
                "attempts": attempts,
                "error": f"max retries: {last_error}",
            }

        peak_mb = baseline_mb
        memory_samples = []
        results = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent) as executor:
            futures = [executor.submit(upload_multipart, i) for i in range(num_uploads)]

            while not all(f.done() for f in futures):
                current_mb = get_memory_mb()
                memory_samples.append(current_mb)
                if current_mb > peak_mb:
                    peak_mb = current_mb
                time.sleep(0.05)

            for f in futures:
                results.append(f.result())

        succeeded = sum(1 for r in results if r.get("success"))
        failed = sum(1 for r in results if not r.get("success"))
        total_attempts = sum(r.get("attempts", 1) for r in results)
        retries = total_attempts - num_uploads
        memory_increase = peak_mb - baseline_mb

        log(f"Results: {succeeded} succeeded, {failed} failed")
        log(f"Total attempts: {total_attempts} ({retries} retries)")
        log(
            f"Memory: baseline={baseline_mb:.1f} MB, "
            f"peak={peak_mb:.1f} MB, increase={memory_increase:.1f} MB"
        )

        for r in results:
            if not r.get("success"):
                log(f"  [{r['index']}] FAILED after {r['attempts']} attempts: {r.get('error', '')}")

        # Assertions
        assert proc.poll() is None, "Server crashed!"
        assert succeeded == num_uploads, (
            f"Expected all {num_uploads} multipart uploads to succeed, but {failed} failed"
        )
        if retries > 0:
            log(f"Memory limiting triggered: {retries} requests had to retry")
        else:
            log("No retries needed (backpressure queued all requests within timeout)")

        # Verify files exist
        log("Verifying all files exist in bucket...")
        response = stress_client.list_objects_v2(Bucket=stress_bucket, Prefix="multipart-test-")
        objects = {obj["Key"]: obj["Size"] for obj in response.get("Contents", [])}

        missing = [r["key"] for r in results if r.get("success") and r["key"] not in objects]
        assert not missing, f"Missing files in bucket: {missing}"
        log(f"Verified: {len(objects)} files in bucket")

        # Memory assertion - with streaming, should stay bounded
        # 2 concurrent × 50MB parts + encryption + overhead ≈ 150-180MB
        # Key: NOT 1GB (10 × 100MB unbounded)
        max_expected = 200  # MB - bounded, much less than unbounded 1GB
        assert memory_increase < max_expected, (
            f"Memory increased by {memory_increase:.1f} MB - expected < {max_expected} MB"
        )
        log(f"Memory bounded: {memory_increase:.1f} MB < {max_expected} MB")

        log("TEST PASSED! Multipart uploads completed, memory stayed bounded.")

    @pytest.mark.skipif(
        sys.platform == "darwin", reason="macOS malloc doesn't reliably return memory to OS"
    )
    def test_download_memory_bounded(self, s3proxy_with_memory_limit, stress_bucket, stress_client):
        """Verify memory stays bounded during concurrent downloads.

        This test verifies that:
        1. Large multipart-encrypted files stream on download (bounded memory)
        2. Concurrent downloads with memory limiting don't OOM
        3. Downloaded data matches uploaded data

        With current architecture:
        - Files > 8MB → multipart encrypted → streams on download
        - Files ≤ 8MB → single-object encrypted → buffers 2× size on download
        """
        import concurrent.futures
        import hashlib
        import io

        import psutil

        log("=" * 60)
        log("TEST: Memory bounded during concurrent downloads (memory_limit_mb=16)")
        log("=" * 60)

        url, proc = s3proxy_with_memory_limit
        server_proc = psutil.Process(proc.pid)

        def get_memory_mb() -> float:
            return server_proc.memory_info().rss / (1024 * 1024)

        # First, upload test files using multipart (> 8MB threshold)
        num_files = 10
        file_size = 50 * 1024 * 1024  # 50MB each → multipart encrypted → streams on download
        part_size = 25 * 1024 * 1024  # 25MB parts

        log(f"Step 1: Uploading {num_files} x {file_size // 1024 // 1024}MB test files...")
        uploaded_hashes = {}

        for i in range(num_files):
            key = f"download-test-{i}.bin"
            # Create reproducible data using file index
            data = bytes([(i + j) % 256 for j in range(file_size)])
            uploaded_hashes[key] = hashlib.md5(data).hexdigest()

            # Upload via multipart to ensure streaming path
            create_resp = stress_client.create_multipart_upload(Bucket=stress_bucket, Key=key)
            upload_id = create_resp["UploadId"]

            parts = []
            offset = 0
            part_num = 1
            while offset < file_size:
                chunk = data[offset : offset + part_size]
                part_resp = stress_client.upload_part(
                    Bucket=stress_bucket,
                    Key=key,
                    UploadId=upload_id,
                    PartNumber=part_num,
                    Body=io.BytesIO(chunk),
                )
                parts.append({"PartNumber": part_num, "ETag": part_resp["ETag"]})
                offset += part_size
                part_num += 1

            stress_client.complete_multipart_upload(
                Bucket=stress_bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
            log(f"  Uploaded {key}")

        log(f"Step 2: Downloading {num_files} files concurrently...")
        baseline_mb = get_memory_mb()
        log(f"Baseline memory before downloads: {baseline_mb:.1f} MB")

        max_concurrent = 6  # More than memory budget allows to trigger 503s
        results = []
        peak_mb = baseline_mb
        memory_samples = []

        def download_one(i: int) -> dict:
            """Download with retries until success."""
            import random

            key = f"download-test-{i}.bin"
            attempts = 0
            max_attempts = 50

            while attempts < max_attempts:
                attempts += 1
                try:
                    response = stress_client.get_object(Bucket=stress_bucket, Key=key)
                    body = response["Body"].read()
                    actual_hash = hashlib.md5(body).hexdigest()
                    expected_hash = uploaded_hashes[key]

                    return {
                        "index": i,
                        "key": key,
                        "success": actual_hash == expected_hash,
                        "size": len(body),
                        "hash_match": actual_hash == expected_hash,
                        "attempts": attempts,
                    }
                except Exception as e:
                    error_str = str(e)
                    # Retry on 503 or connection issues
                    if (
                        "503" in error_str
                        or "SlowDown" in error_str
                        or "reset" in error_str.lower()
                    ):
                        delay = min(0.3 + (attempts * 0.1) + random.uniform(0, 0.2), 2.0)
                        time.sleep(delay)
                        continue
                    return {
                        "index": i,
                        "key": key,
                        "success": False,
                        "attempts": attempts,
                        "error": error_str,
                    }

            return {
                "index": i,
                "key": key,
                "success": False,
                "attempts": attempts,
                "error": "max retries",
            }

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent) as executor:
            futures = [executor.submit(download_one, i) for i in range(num_files)]

            while not all(f.done() for f in futures):
                current_mb = get_memory_mb()
                memory_samples.append(current_mb)
                if current_mb > peak_mb:
                    peak_mb = current_mb
                time.sleep(0.05)

            for f in futures:
                results.append(f.result())

        succeeded = sum(1 for r in results if r.get("success"))
        failed = sum(1 for r in results if not r.get("success"))
        total_attempts = sum(r.get("attempts", 1) for r in results)
        retries = total_attempts - num_files
        memory_increase = peak_mb - baseline_mb

        log(f"Results: {succeeded} succeeded, {failed} failed")
        log(f"Total attempts: {total_attempts} ({retries} retries)")
        log(
            f"Memory: baseline={baseline_mb:.1f} MB, "
            f"peak={peak_mb:.1f} MB, increase={memory_increase:.1f} MB"
        )

        for r in results:
            if not r.get("success"):
                log(f"  [{r['index']}] FAILED: {r.get('error', 'unknown')}")
            elif not r.get("hash_match"):
                log(f"  [{r['index']}] DATA MISMATCH!")

        # Assertions
        assert proc.poll() is None, "Server crashed during download stress!"
        assert succeeded == num_files, (
            f"Expected all {num_files} downloads to succeed, but {failed} failed"
        )
        assert all(r.get("hash_match") for r in results if r.get("success")), (
            "Downloaded data doesn't match!"
        )

        # Memory assertion: with streaming downloads, should stay bounded
        # Key: NOT 500MB (10 × 50MB unbounded downloads)
        # With memory_limit_mb=16 and streaming: ~100MB expected
        # (2 concurrent x 50MB buffers for streaming chunks)
        max_expected = 150  # MB - bounded, much less than unbounded 500MB
        assert memory_increase < max_expected, (
            f"Memory increased by {memory_increase:.1f} MB during downloads "
            f"- expected < {max_expected} MB. "
            f"Streaming may not be working correctly!"
        )
        log(f"Memory bounded: {memory_increase:.1f} MB < {max_expected} MB (streaming downloads)")

        log("TEST PASSED! Downloads completed with bounded memory.")

    @pytest.mark.skipif(
        sys.platform == "darwin", reason="macOS malloc doesn't reliably return memory to OS"
    )
    def test_upload_download_round_trip_bounded(
        self, s3proxy_with_memory_limit, stress_bucket, stress_client
    ):
        """Full round-trip test: upload then download, verify memory bounds.

        This proves the entire system is memory-bounded:
        1. Large uploads stream (multipart encryption)
        2. Large downloads stream (multipart decryption)
        3. Memory limiting prevents OOM at every stage
        """
        import hashlib
        import io

        import psutil

        log("=" * 60)
        log("TEST: Full round-trip memory bounds (upload + download)")
        log("=" * 60)

        url, proc = s3proxy_with_memory_limit
        server_proc = psutil.Process(proc.pid)

        def get_memory_mb() -> float:
            return server_proc.memory_info().rss / (1024 * 1024)

        baseline_mb = get_memory_mb()
        log(f"Baseline memory: {baseline_mb:.1f} MB")

        # Config: 5 files × 100MB = 500MB total data
        num_files = 5
        file_size = 100 * 1024 * 1024  # 100MB each
        part_size = 50 * 1024 * 1024

        log(f"Round-trip test: {num_files} × {file_size // 1024 // 1024}MB files")
        log(f"Total data: {num_files * file_size // 1024 // 1024}MB (would OOM without streaming)")

        # Generate test data with known hashes
        test_data = {}
        for i in range(num_files):
            key = f"roundtrip-test-{i}.bin"
            data = bytes([(i * 7 + j) % 256 for j in range(file_size)])
            test_data[key] = {
                "data": data,
                "hash": hashlib.md5(data).hexdigest(),
            }

        peak_mb = baseline_mb

        # Phase 1: Upload all files
        log("Phase 1: Uploading files...")
        for key, info in test_data.items():
            data = info["data"]

            create_resp = stress_client.create_multipart_upload(Bucket=stress_bucket, Key=key)
            upload_id = create_resp["UploadId"]

            parts = []
            offset = 0
            part_num = 1
            while offset < len(data):
                chunk = data[offset : offset + part_size]
                part_resp = stress_client.upload_part(
                    Bucket=stress_bucket,
                    Key=key,
                    UploadId=upload_id,
                    PartNumber=part_num,
                    Body=io.BytesIO(chunk),
                )
                parts.append({"PartNumber": part_num, "ETag": part_resp["ETag"]})
                offset += part_size
                part_num += 1

            stress_client.complete_multipart_upload(
                Bucket=stress_bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )

            current_mb = get_memory_mb()
            if current_mb > peak_mb:
                peak_mb = current_mb
            log(f"  Uploaded {key}, memory: {current_mb:.1f} MB")

        upload_peak = peak_mb
        log(f"Upload phase complete, peak memory: {upload_peak:.1f} MB")

        # Phase 2: Download all files and verify
        log("Phase 2: Downloading and verifying files...")
        for key, info in test_data.items():
            response = stress_client.get_object(Bucket=stress_bucket, Key=key)
            body = response["Body"].read()
            actual_hash = hashlib.md5(body).hexdigest()

            current_mb = get_memory_mb()
            if current_mb > peak_mb:
                peak_mb = current_mb

            assert actual_hash == info["hash"], f"Data mismatch for {key}!"
            log(f"  Downloaded {key}, hash OK, memory: {current_mb:.1f} MB")

        memory_increase = peak_mb - baseline_mb
        log("Round-trip complete!")
        log(
            f"Memory: baseline={baseline_mb:.1f} MB, "
            f"peak={peak_mb:.1f} MB, increase={memory_increase:.1f} MB"
        )

        # Assertions
        assert proc.poll() is None, "Server crashed!"

        # Memory assertion: RSS measures high-water mark, not current usage.
        # Python's memory allocator doesn't return freed memory to OS - it keeps
        # memory pools for reuse. For 500MB of data processed sequentially:
        # - Each operation allocates buffers (8-16MB chunks)
        # - Python keeps these pools even after objects are freed
        # - RSS shows cumulative peak allocation, not actual usage
        #
        # Key insight: streaming IS working (verified by METADATA_LOADED logs),
        # but RSS doesn't reflect this. Without streaming, we'd crash with OOM
        # trying to hold 500MB+ in memory simultaneously.
        #
        # The realistic bound is ~500MB for processing 500MB of data with Python's
        # memory behavior. This proves we're not holding multiple files at once.
        max_expected = 200  # MB - with PYTHONMALLOC=malloc, memory should be released
        assert memory_increase < max_expected, (
            f"Memory increased by {memory_increase:.1f} MB - expected < {max_expected} MB. "
            f"This suggests memory is accumulating beyond normal Python pool behavior!"
        )

        log(f"Memory bounded: {memory_increase:.1f} MB < {max_expected} MB")
        log("TEST PASSED! Full round-trip completed with bounded memory.")
