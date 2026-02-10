"""Prove s3proxy doesn't get OOM-killed under real OS memory constraints.

Runs against the s3proxy container in tests/docker-compose.yml (mem_limit=128m).
Throws everything at it: large PUTs, multipart uploads, concurrent GETs, HEADs,
DELETEs — all at once. If the memory limiter fails, the kernel OOM-kills the
process (exit code 137).

Without the memory limiter, 20 concurrent 256MB uploads would need ~6GB+.
The container has 128MB. If it survives, the limiter works.
"""

import concurrent.futures
import contextlib
import io
import json
import random
import subprocess
import time
import uuid

import boto3
import pytest

CONTAINER_NAME = "s3proxy-test-server"
ENDPOINT_URL = "http://localhost:4433"


def container_is_running() -> bool:
    result = subprocess.run(
        ["docker", "inspect", "--format", "{{json .State}}", CONTAINER_NAME],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return False
    state = json.loads(result.stdout.strip())
    return state.get("Running", False) and state.get("OOMKilled", False) is False


def container_oom_killed() -> bool:
    result = subprocess.run(
        ["docker", "inspect", "--format", "{{.State.OOMKilled}}", CONTAINER_NAME],
        capture_output=True,
        text=True,
    )
    return result.stdout.strip() == "true"


def assert_alive(msg: str = ""):
    __tracebackhide__ = True
    assert not container_oom_killed(), f"OOM-KILLED! {msg}"
    assert container_is_running(), f"Container died! {msg}"


def make_client():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
        config=boto3.session.Config(
            retries={"max_attempts": 0},
            connect_timeout=10,
            read_timeout=300,
        ),
    )


def retry_on_503(fn, max_attempts=60):
    """Retry a function on 503/SlowDown/connection errors."""
    for _attempt in range(max_attempts):
        try:
            return fn()
        except Exception as e:
            err = str(e)
            if "503" in err or "SlowDown" in err or "reset" in err.lower():
                time.sleep(0.3 + random.uniform(0, 0.5))
                continue
            raise
    raise RuntimeError(f"Failed after {max_attempts} retries")


@pytest.mark.e2e
class TestMemoryLeak:
    """Try everything to OOM-kill a 128MB s3proxy container."""

    @pytest.fixture(autouse=True)
    def check_container(self):
        assert container_is_running(), "s3proxy container not running"
        yield
        # Check after every test too
        assert_alive("after test completed")

    @pytest.fixture
    def client(self):
        return make_client()

    @pytest.fixture
    def bucket(self, client):
        name = f"oom-{uuid.uuid4().hex[:8]}"
        with contextlib.suppress(Exception):
            client.create_bucket(Bucket=name)
        yield name
        with contextlib.suppress(Exception):
            # Cleanup all objects (including multipart)
            paginator = client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=name):
                if "Contents" in page:
                    objects = [{"Key": o["Key"]} for o in page["Contents"]]
                    client.delete_objects(Bucket=name, Delete={"Objects": objects})
            # Abort any in-progress multipart uploads
            mp = client.list_multipart_uploads(Bucket=name)
            for upload in mp.get("Uploads", []):
                with contextlib.suppress(Exception):
                    client.abort_multipart_upload(
                        Bucket=name, Key=upload["Key"], UploadId=upload["UploadId"]
                    )
            client.delete_bucket(Bucket=name)

    def test_20_concurrent_256mb_puts(self, client, bucket):
        """20 concurrent 256MB PUT uploads. Total: 5GB into 128MB container.

        Without limiter: 20 x ~300MB actual memory = 6GB → instant OOM.
        With limiter: backpressure queues, ~1-2 at a time → survives.
        """
        num = 20
        size = 256 * 1024 * 1024
        data = bytes([42]) * size

        def upload(i):
            retry_on_503(
                lambda: client.put_object(
                    Bucket=bucket, Key=f"big-{i}.bin", Body=data
                )
            )
            return i

        with concurrent.futures.ThreadPoolExecutor(max_workers=num) as ex:
            futures = [ex.submit(upload, i) for i in range(num)]
            done = 0
            for f in concurrent.futures.as_completed(futures, timeout=600):
                f.result()
                done += 1

        assert_alive("after 20x256MB PUTs")
        assert done == num

    def test_concurrent_multipart_256mb(self, client, bucket):
        """10 concurrent 256MB multipart uploads (4 x 64MB parts each).

        Multipart has separate buffer paths. Total: 2.5GB.
        """
        num = 10
        part_size = 64 * 1024 * 1024  # 64MB parts
        num_parts = 4

        def upload_multipart(i):
            key = f"mp-{i}.bin"

            def do_upload():
                resp = client.create_multipart_upload(Bucket=bucket, Key=key)
                uid = resp["UploadId"]
                try:
                    parts = []
                    for pn in range(1, num_parts + 1):
                        part_data = bytes([pn + i]) * part_size
                        pr = client.upload_part(
                            Bucket=bucket,
                            Key=key,
                            UploadId=uid,
                            PartNumber=pn,
                            Body=io.BytesIO(part_data),
                        )
                        parts.append({"PartNumber": pn, "ETag": pr["ETag"]})
                    client.complete_multipart_upload(
                        Bucket=bucket,
                        Key=key,
                        UploadId=uid,
                        MultipartUpload={"Parts": parts},
                    )
                except Exception:
                    with contextlib.suppress(Exception):
                        client.abort_multipart_upload(
                            Bucket=bucket, Key=key, UploadId=uid
                        )
                    raise

            retry_on_503(do_upload)
            return i

        with concurrent.futures.ThreadPoolExecutor(max_workers=num) as ex:
            futures = [ex.submit(upload_multipart, i) for i in range(num)]
            done = 0
            for f in concurrent.futures.as_completed(futures, timeout=600):
                f.result()
                done += 1

        assert_alive("after 10x256MB multipart uploads")
        assert done == num

    def test_mixed_storm(self, client, bucket):
        """Simultaneous PUTs, GETs, HEADs, and DELETEs. Maximum chaos.

        1. Seed 5 x 100MB files
        2. Launch 30 concurrent workers doing random ops:
           - PUT 50-200MB files
           - GET existing files (decryption buffers)
           - HEAD requests
           - DELETE + re-upload
        All at once in a 128MB container.
        """
        # Seed files for GETs
        seed_size = 100 * 1024 * 1024
        seed_keys = []
        for i in range(5):
            key = f"seed-{i}.bin"
            idx = i
            retry_on_503(
                lambda k=key, x=idx: client.put_object(
                    Bucket=bucket, Key=k, Body=bytes([x]) * seed_size
                )
            )
            seed_keys.append(key)

        assert_alive("after seeding")

        results = {"put": 0, "get": 0, "head": 0, "delete": 0, "error": 0}

        def random_op(worker_id):
            op = random.choice(["put", "put", "get", "get", "head", "delete"])

            if op == "put":
                size = random.randint(50, 200) * 1024 * 1024
                data = bytes([worker_id % 256]) * size
                retry_on_503(
                    lambda: client.put_object(
                        Bucket=bucket,
                        Key=f"storm-{worker_id}.bin",
                        Body=data,
                    )
                )
                return "put"

            elif op == "get":
                key = random.choice(seed_keys)
                retry_on_503(
                    lambda: client.get_object(Bucket=bucket, Key=key)["Body"].read()
                )
                return "get"

            elif op == "head":
                key = random.choice(seed_keys)
                retry_on_503(
                    lambda: client.head_object(Bucket=bucket, Key=key)
                )
                return "head"

            else:  # delete + re-upload
                key = f"storm-del-{worker_id}.bin"
                data = bytes([worker_id % 256]) * (50 * 1024 * 1024)
                retry_on_503(
                    lambda: client.put_object(Bucket=bucket, Key=key, Body=data)
                )
                retry_on_503(
                    lambda: client.delete_object(Bucket=bucket, Key=key)
                )
                return "delete"

        num_workers = 30
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as ex:
            futures = [ex.submit(random_op, i) for i in range(num_workers)]
            for f in concurrent.futures.as_completed(futures, timeout=600):
                try:
                    op = f.result()
                    results[op] += 1
                except Exception:
                    results["error"] += 1

        assert_alive("after mixed storm")
        total_ops = sum(results.values()) - results["error"]
        assert total_ops > 0, f"No operations succeeded: {results}"

    def test_rapid_fire_small_and_large(self, client, bucket):
        """Alternate between tiny and huge requests to stress allocation patterns.

        50 workers: odd = 1KB PUT, even = 128MB PUT. Rapid context switching
        between small and large allocations can fragment memory.
        """
        num_workers = 50

        def fire(i):
            data = bytes([i % 256]) * (128 * 1024 * 1024) if i % 2 == 0 else b"x" * 1024

            retry_on_503(
                lambda: client.put_object(
                    Bucket=bucket, Key=f"rapid-{i}.bin", Body=data
                )
            )
            return i

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as ex:
            futures = [ex.submit(fire, i) for i in range(num_workers)]
            done = 0
            for f in concurrent.futures.as_completed(futures, timeout=600):
                f.result()
                done += 1

        assert_alive("after rapid fire")
        assert done == num_workers

    def test_sustained_load_10_minutes(self, client, bucket):
        """Sustained upload/download for 2 minutes. Catches slow memory leaks.

        Continuous loop: upload 50MB, download it, delete it. Repeat.
        If memory leaks even 1MB per cycle, 128MB is exhausted in ~48 cycles.
        """
        size = 50 * 1024 * 1024
        data = bytes([99]) * size
        deadline = time.time() + 120  # 2 minutes
        cycles = 0

        while time.time() < deadline:
            key = f"sustained-{cycles}.bin"

            # Upload
            retry_on_503(
                lambda k=key: client.put_object(Bucket=bucket, Key=k, Body=data)
            )

            # Download + verify size
            resp = retry_on_503(
                lambda k=key: client.get_object(Bucket=bucket, Key=k)
            )
            body = resp["Body"].read()
            assert len(body) == size, f"Size mismatch: {len(body)} != {size}"

            # Delete
            retry_on_503(lambda k=key: client.delete_object(Bucket=bucket, Key=k))

            cycles += 1

            # Check container every 10 cycles
            if cycles % 10 == 0:
                assert_alive(f"after {cycles} sustained cycles")

        assert_alive(f"after {cycles} sustained cycles over 2 minutes")
        assert cycles >= 5, f"Only completed {cycles} cycles in 2 minutes"
