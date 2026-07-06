"""Integration: copy-path memory governor under a real s3proxy subprocess.

These tests need MinIO on localhost:9000 (same as other e2e tests).
"""

from __future__ import annotations

import concurrent.futures
import socket
import uuid

import boto3
import pytest
from botocore.exceptions import ClientError

from tests.integration.conftest import run_s3proxy

MB = 1024 * 1024


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def _client(endpoint: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
        config=boto3.session.Config(
            retries={"max_attempts": 0},
            connect_timeout=10,
            read_timeout=300,
        ),
    )


def _put_large_source(client, bucket: str, key: str, size: int, chunk: int = 8 * MB) -> None:
    """Stream a large plaintext object through the proxy (encrypted on store)."""
    body = _RepeatingBody(size, chunk)
    client.put_object(Bucket=bucket, Key=key, Body=body)


class _RepeatingBody:
    """File-like stream without holding *size* bytes (boto3 checksum needs tell/seek)."""

    def __init__(self, size: int, chunk: int):
        self._size = size
        self._chunk = chunk
        self._pos = 0

    def tell(self) -> int:
        return self._pos

    def seek(self, offset: int, whence: int = 0) -> int:
        if whence == 0:
            self._pos = offset
        elif whence == 1:
            self._pos += offset
        elif whence == 2:
            self._pos = self._size + offset
        else:
            raise ValueError(f"invalid whence: {whence}")
        self._pos = max(0, min(self._pos, self._size))
        return self._pos

    def read(self, amt=-1):
        if self._pos >= self._size:
            return b""
        if amt is None or amt < 0:
            amt = self._chunk
        take = min(amt, self._chunk, self._size - self._pos)
        self._pos += take
        return b"x" * take


def _upload_part_copy(client, bucket: str, dest_key: str, source_key: str) -> dict:
    resp = client.create_multipart_upload(Bucket=bucket, Key=dest_key)
    upload_id = resp["UploadId"]
    try:
        copy_resp = client.upload_part_copy(
            Bucket=bucket,
            Key=dest_key,
            PartNumber=1,
            UploadId=upload_id,
            CopySource={"Bucket": bucket, "Key": source_key},
        )
        client.complete_multipart_upload(
            Bucket=bucket,
            Key=dest_key,
            UploadId=upload_id,
            MultipartUpload={
                "Parts": [{"PartNumber": 1, "ETag": copy_resp["CopyPartResult"]["ETag"]}]
            },
        )
        return {"success": True}
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        return {"success": False, "code": code, "error": str(e)}
    except Exception as e:
        return {"success": False, "code": "error", "error": str(e)}


def _upload_part(client, bucket: str, key: str, part_number: int, upload_id: str, size: int):
    body = _RepeatingBody(size, 8 * MB)
    try:
        resp = client.upload_part(
            Bucket=bucket,
            Key=key,
            PartNumber=part_number,
            UploadId=upload_id,
            Body=body,
        )
        return {"success": True, "etag": resp["ETag"]}
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        return {"success": False, "code": code, "error": str(e)}


# 64MB source: copy_pipeline_peak ~48MB. Governor must exceed the clamped copy
# slot plus the POST gate's MIN_RESERVATION (64KB); at 32MB the clamp monopolizes
# the budget and even a single copy fails (64KB+32MB > 32MB while the gate is held).
# 96MB fits one ~48MB copy plus concurrent ~20MB upload reservations, and still
# rejects a second concurrent copy (48+48+gate > 96).
SOURCE_SIZE = 64 * MB
GOVERNOR_MB = "96"
SOURCE_KEY = "shared-large-source.bin"


@pytest.fixture(scope="module")
def copy_stress_server():
    port = _find_free_port()
    with run_s3proxy(
        port,
        log_output=False,
        S3PROXY_MEMORY_LIMIT_MB=GOVERNOR_MB,
        # 0 = reject immediately (SlowDown), don't queue — otherwise all 3 copies
        # succeed serially within the 2s wait and the concurrency assertion fails.
        S3PROXY_BACKPRESSURE_TIMEOUT="0",
        S3PROXY_MAX_PART_SIZE_MB="0",
    ) as (endpoint, proc):
        yield endpoint, proc


@pytest.fixture(scope="module")
def copy_bucket(copy_stress_server):
    endpoint, _ = copy_stress_server
    client = _client(endpoint)
    bucket = f"copy-gov-{uuid.uuid4().hex[:8]}"
    try:
        client.create_bucket(Bucket=bucket)
    except ClientError as exc:
        pytest.skip(f"MinIO/S3 backend not available for e2e: {exc}")
    yield client, bucket
    try:
        resp = client.list_objects_v2(Bucket=bucket)
        if "Contents" in resp:
            client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": o["Key"]} for o in resp["Contents"]]},
            )
        client.delete_bucket(Bucket=bucket)
    except Exception:
        pass


@pytest.fixture(scope="module")
def large_source(copy_bucket):
    """Upload once for all copy-governor tests (avoids 3× redundant 256MB puts)."""
    client, bucket = copy_bucket
    _put_large_source(client, bucket, SOURCE_KEY, SOURCE_SIZE)
    return SOURCE_KEY


@pytest.mark.e2e
class TestCopyMemoryGovernorSubprocess:
    """Real subprocess + boto3: copies must not OOM the server."""

    def test_three_concurrent_large_copies_do_not_oom_server(
        self, copy_stress_server, copy_bucket, large_source
    ):
        """Prod regression: 3 concurrent large copies must not all run + OOM."""
        endpoint, proc = copy_stress_server
        client, bucket = copy_bucket
        source_key = large_source

        def one_copy(i: int) -> dict:
            return _upload_part_copy(client, bucket, f"dest-{i}.bin", source_key)

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            results = list(pool.map(one_copy, range(3)))

        assert proc.poll() is None, "s3proxy OOMKilled or crashed (exit 137?)"
        succeeded = sum(1 for r in results if r["success"])
        slowed = sum(1 for r in results if r.get("code") == "SlowDown")
        assert succeeded >= 1, f"expected at least one copy to succeed: {results}"
        # 96MB budget fits two ~48MB copy slots; the third must be rejected, not queued.
        assert slowed >= 1, f"expected backpressure on concurrent copies: {results}"
        assert succeeded <= 2, f"more than two copies ran concurrently: {results}"
        assert succeeded + slowed == len(results), f"unexpected errors: {results}"

    def test_mixed_scylla_uploads_and_large_copy_server_survives(
        self, copy_stress_server, copy_bucket, large_source
    ):
        """Prod mix: concurrent small parts + large manifest-style copy."""
        endpoint, proc = copy_stress_server
        client, bucket = copy_bucket
        source_key = large_source

        def scylla_part(i: int) -> dict:
            key = f"sst-part-{i}.bin"
            resp = client.create_multipart_upload(Bucket=bucket, Key=key)
            upload_id = resp["UploadId"]
            # 5MB parts reserve ~20MB (not 32MB like 16MB parts) so two can run
            # alongside one ~48MB copy within the 96MB mixed-workload governor.
            r = _upload_part(client, bucket, key, 1, upload_id, 5 * MB)
            if r["success"]:
                client.complete_multipart_upload(
                    Bucket=bucket,
                    Key=key,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": r["etag"]}]},
                )
            return r

        def manifest_copy() -> dict:
            return _upload_part_copy(client, bucket, "manifest-copy.bin", source_key)

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            upload_futs = [pool.submit(scylla_part, i) for i in range(3)]
            copy_fut = pool.submit(manifest_copy)
            copy_result = copy_fut.result()
            upload_results = [f.result() for f in upload_futs]

        assert proc.poll() is None, "server crashed under mixed workload"
        assert sum(1 for r in upload_results if r["success"]) >= 2
        assert copy_result["success"] or copy_result.get("code") == "SlowDown"
