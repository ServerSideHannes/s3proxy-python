"""HA integration tests with multiple s3proxy pods and real Redis.

These tests verify:
1. Multiple s3proxy pods share state via real Redis
2. Part number allocation is atomic across pods
3. Sequential numbering works when uploads hit different pods
4. Concurrent uploads to different pods maintain consistency

Production scenario:
- 2+ s3proxy pods behind load balancer
- Shared Redis for distributed state
- Uploads can hit any pod for any part
"""

import os
import shutil
import subprocess
import time

import boto3
import httpx
import pytest


def is_docker_available():
    """Check if Docker is available and running."""
    if not shutil.which("docker"):
        return False
    try:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            timeout=5,
        )
        return result.returncode == 0
    except Exception:
        return False


# Skip all HA tests if Docker isn't available
# Use xdist_group to run all HA tests in the same worker (isolated from integration tests)
pytestmark = [
    pytest.mark.skipif(
        not is_docker_available(),
        reason="Docker not available - required for HA tests with Redis",
    ),
    pytest.mark.xdist_group("ha"),
]


def _is_redis_running():
    """Check if Redis is already running on port 6379."""
    import socket

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        result = sock.connect_ex(("localhost", 6379))
        sock.close()
        return result == 0
    except Exception:
        return False


@pytest.fixture(scope="module")
def redis_server():
    """Use existing Redis (from make test-all) or skip if not available."""
    # Expect Redis to already be running (started by Makefile)
    if not _is_redis_running():
        pytest.skip("Redis not running - run 'make test-all' or 'make test-ha' to start services")

    return "redis://localhost:6379"


@pytest.fixture(scope="module")
def s3proxy_pods(redis_server):
    """Start 2 s3proxy pods sharing Redis using subprocess."""
    pods = []
    # Use ports 4450-4451 to avoid conflicts with integration tests (which use 4433+worker_num)
    ports = [4450, 4451]

    # Start 2 s3proxy pods as subprocesses
    for port in ports:
        env = os.environ.copy()
        env.update(
            {
                "S3PROXY_ENCRYPT_KEY": "test-encryption-key-32-bytes!!",
                "S3PROXY_HOST": "http://localhost:9000",
                "S3PROXY_REGION": "us-east-1",
                "S3PROXY_PORT": str(port),
                "S3PROXY_NO_TLS": "true",
                "S3PROXY_REDIS_URL": redis_server,
                "S3PROXY_LOG_LEVEL": "WARNING",
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
        pods.append({"port": port, "process": proc})

    # Wait for pods to be ready with health check
    for pod in pods:
        port = pod["port"]
        ready = False
        for _ in range(30):  # 15 seconds timeout
            try:
                with httpx.Client() as client:
                    resp = client.get(f"http://localhost:{port}/healthz", timeout=1.0)
                    if resp.status_code == 200:
                        ready = True
                        break
            except Exception:
                pass
            time.sleep(0.5)

        if not ready:
            # Cleanup all pods
            for p in pods:
                p["process"].terminate()
                p["process"].wait(timeout=2)
            pytest.skip(f"s3proxy pod on port {port} failed to start")

    yield pods

    # Cleanup
    for pod in pods:
        pod["process"].terminate()
        try:
            pod["process"].wait(timeout=5)
        except subprocess.TimeoutExpired:
            pod["process"].kill()


@pytest.fixture
def s3_clients(s3proxy_pods):
    """Create boto3 clients for each s3proxy pod."""
    clients = []
    for pod in s3proxy_pods:
        client = boto3.client(
            "s3",
            endpoint_url=f"http://localhost:{pod['port']}",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            region_name="us-east-1",
        )
        clients.append(client)
    return clients


@pytest.fixture
def test_bucket(s3_clients):
    """Create and cleanup test bucket."""
    bucket = "test-ha-sequential"

    # Create bucket via first pod
    try:
        s3_clients[0].create_bucket(Bucket=bucket)
    except s3_clients[0].exceptions.BucketAlreadyOwnedByYou:
        pass

    yield bucket

    # Cleanup via first pod
    try:
        response = s3_clients[0].list_objects_v2(Bucket=bucket)
        if "Contents" in response:
            objects = [{"Key": obj["Key"]} for obj in response["Contents"]]
            s3_clients[0].delete_objects(Bucket=bucket, Delete={"Objects": objects})
        s3_clients[0].delete_bucket(Bucket=bucket)
    except Exception:
        pass


class TestHASequentialPartNumbering:
    """HA tests with multiple s3proxy pods and real Redis."""

    @pytest.mark.e2e
    @pytest.mark.ha
    def test_upload_parts_to_different_pods(self, s3_clients, test_bucket):
        """
        Test uploading parts to different pods maintains sequential numbering.

        Scenario:
        - Part 1 → Pod A (port 4450)
        - Part 2 → Pod B (port 4451)
        - Both pods share Redis state
        - Internal parts should be [1, 2] (sequential)
        """
        key = "cross-pod-upload.bin"

        # Initiate upload via Pod A
        response = s3_clients[0].create_multipart_upload(Bucket=test_bucket, Key=key)
        upload_id = response["UploadId"]

        # Upload Part 1 via Pod A
        part1_data = b"A" * 5_242_880  # 5MB
        response1 = s3_clients[0].upload_part(
            Bucket=test_bucket,
            Key=key,
            PartNumber=1,
            UploadId=upload_id,
            Body=part1_data,
        )
        etag1 = response1["ETag"]

        # Upload Part 2 via Pod B (different pod!)
        part2_data = b"B" * 4_500_000  # 4.29MB
        response2 = s3_clients[1].upload_part(
            Bucket=test_bucket,
            Key=key,
            PartNumber=2,
            UploadId=upload_id,
            Body=part2_data,
        )
        etag2 = response2["ETag"]

        # Complete via Pod A
        response = s3_clients[0].complete_multipart_upload(
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

        assert "ETag" in response, "Cross-pod upload failed - Redis state sharing issue"

        # Verify object
        head = s3_clients[0].head_object(Bucket=test_bucket, Key=key)
        assert head["ContentLength"] == len(part1_data) + len(part2_data)

    @pytest.mark.e2e
    @pytest.mark.ha
    def test_concurrent_uploads_different_pods(self, s3_clients, test_bucket):
        """
        Test concurrent uploads hitting different pods.

        Upload A: Parts via Pod A
        Upload B: Parts via Pod B
        Both should get independent sequential [1, 2]
        """
        key_a = "pod-a-upload.bin"
        key_b = "pod-b-upload.bin"

        # Initiate both uploads (different pods)
        resp_a = s3_clients[0].create_multipart_upload(Bucket=test_bucket, Key=key_a)
        resp_b = s3_clients[1].create_multipart_upload(Bucket=test_bucket, Key=key_b)
        upload_id_a = resp_a["UploadId"]
        upload_id_b = resp_b["UploadId"]

        part_data = b"X" * 5_242_880  # 5MB

        # Upload A parts via Pod A
        resp = s3_clients[0].upload_part(
            Bucket=test_bucket,
            Key=key_a,
            PartNumber=1,
            UploadId=upload_id_a,
            Body=part_data,
        )
        etag_a1 = resp["ETag"]

        resp = s3_clients[0].upload_part(
            Bucket=test_bucket,
            Key=key_a,
            PartNumber=2,
            UploadId=upload_id_a,
            Body=part_data,
        )
        etag_a2 = resp["ETag"]

        # Upload B parts via Pod B
        resp = s3_clients[1].upload_part(
            Bucket=test_bucket,
            Key=key_b,
            PartNumber=1,
            UploadId=upload_id_b,
            Body=part_data,
        )
        etag_b1 = resp["ETag"]

        resp = s3_clients[1].upload_part(
            Bucket=test_bucket,
            Key=key_b,
            PartNumber=2,
            UploadId=upload_id_b,
            Body=part_data,
        )
        etag_b2 = resp["ETag"]

        # Complete both
        resp_a = s3_clients[0].complete_multipart_upload(
            Bucket=test_bucket,
            Key=key_a,
            UploadId=upload_id_a,
            MultipartUpload={
                "Parts": [
                    {"PartNumber": 1, "ETag": etag_a1},
                    {"PartNumber": 2, "ETag": etag_a2},
                ]
            },
        )

        resp_b = s3_clients[1].complete_multipart_upload(
            Bucket=test_bucket,
            Key=key_b,
            UploadId=upload_id_b,
            MultipartUpload={
                "Parts": [
                    {"PartNumber": 1, "ETag": etag_b1},
                    {"PartNumber": 2, "ETag": etag_b2},
                ]
            },
        )

        assert "ETag" in resp_a and "ETag" in resp_b, "Concurrent uploads to different pods failed"

    @pytest.mark.e2e
    @pytest.mark.ha
    def test_out_of_order_cross_pod_upload(self, s3_clients, test_bucket):
        """
        Test out-of-order upload across pods (production scenario).

        - Part 2 → Pod B first
        - Part 1 → Pod A second
        - Should get sequential [1, 2] via Redis coordination
        """
        key = "out-of-order-cross-pod.bin"

        # Initiate via Pod A
        response = s3_clients[0].create_multipart_upload(Bucket=test_bucket, Key=key)
        upload_id = response["UploadId"]

        # Upload Part 2 FIRST via Pod B
        part2_data = b"B" * 4_441_600  # 4.24MB
        response2 = s3_clients[1].upload_part(
            Bucket=test_bucket,
            Key=key,
            PartNumber=2,
            UploadId=upload_id,
            Body=part2_data,
        )
        etag2 = response2["ETag"]

        # Upload Part 1 SECOND via Pod A
        part1_data = b"A" * 5_242_880  # 5.00MB
        response1 = s3_clients[0].upload_part(
            Bucket=test_bucket,
            Key=key,
            PartNumber=1,
            UploadId=upload_id,
            Body=part1_data,
        )
        etag1 = response1["ETag"]

        # Complete via Pod A
        response = s3_clients[0].complete_multipart_upload(
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
            "Out-of-order cross-pod upload failed - Redis atomic allocation not working correctly"
        )

        # Verify object
        head = s3_clients[0].head_object(Bucket=test_bucket, Key=key)
        assert head["ContentLength"] == len(part1_data) + len(part2_data)

    @pytest.mark.e2e
    @pytest.mark.ha
    def test_interleaved_parts_across_pods(self, s3_clients, test_bucket):
        """
        Test highly interleaved upload pattern across pods.

        Upload pattern (simulating load balancer):
        - Part 3 → Pod A
        - Part 1 → Pod B
        - Part 5 → Pod A
        - Part 2 → Pod B
        - Part 4 → Pod A

        Internal parts should be [1, 2, 3, 4, 5] (sequential)
        """
        key = "interleaved-cross-pod.bin"

        # Initiate via Pod A
        response = s3_clients[0].create_multipart_upload(Bucket=test_bucket, Key=key)
        upload_id = response["UploadId"]

        part_size = 5_242_880  # 5MB
        parts = []

        # Interleaved upload pattern
        upload_pattern = [
            (3, 0),  # Part 3 → Pod A
            (1, 1),  # Part 1 → Pod B
            (5, 0),  # Part 5 → Pod A
            (2, 1),  # Part 2 → Pod B
            (4, 0),  # Part 4 → Pod A
        ]

        for part_num, pod_idx in upload_pattern:
            part_data = bytes([part_num] * part_size)
            response = s3_clients[pod_idx].upload_part(
                Bucket=test_bucket,
                Key=key,
                PartNumber=part_num,
                UploadId=upload_id,
                Body=part_data,
            )
            parts.append({"PartNumber": part_num, "ETag": response["ETag"]})

        # Complete via Pod B (different from initiate)
        parts.sort(key=lambda p: p["PartNumber"])
        response = s3_clients[1].complete_multipart_upload(
            Bucket=test_bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

        assert "ETag" in response, "Interleaved cross-pod upload failed - Redis coordination issue"

        # Verify object
        head = s3_clients[0].head_object(Bucket=test_bucket, Key=key)
        assert head["ContentLength"] == 5 * part_size
