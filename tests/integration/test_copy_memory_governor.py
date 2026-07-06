"""Integration: copy-path memory governor under a real s3proxy subprocess.

These tests need MinIO on localhost:9000 (same as other e2e tests).
"""

from __future__ import annotations

import concurrent.futures
import socket
import sys
import time
import uuid

import boto3
import psutil
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


@pytest.mark.e2e
class TestCopyMemoryGovernorSubprocess:
    """Real subprocess + boto3: copies must not OOM the server."""

    # 512MB source: copy_pipeline_peak ~75MB > 48MB governor → exclusive slot.
    SOURCE_SIZE = 512 * MB
    GOVERNOR_MB = "48"

    @pytest.fixture
    def copy_stress_server(self):
        port = _find_free_port()
        with run_s3proxy(
            port,
            log_output=False,
            S3PROXY_MEMORY_LIMIT_MB=self.GOVERNOR_MB,
            S3PROXY_BACKPRESSURE_TIMEOUT="2",
            S3PROXY_MAX_PART_SIZE_MB="0",
        ) as (endpoint, proc):
            yield endpoint, proc

    @pytest.fixture
    def copy_bucket(self, copy_stress_server):
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

    def test_three_concurrent_large_copies_do_not_oom_server(self, copy_stress_server, copy_bucket):
        """Prod regression: 3 concurrent large copies must not all run + OOM."""
        endpoint, proc = copy_stress_server
        client, bucket = copy_bucket
        source_key = "large-source.bin"
        _put_large_source(client, bucket, source_key, self.SOURCE_SIZE)

        def one_copy(i: int) -> dict:
            return _upload_part_copy(client, bucket, f"dest-{i}.bin", source_key)

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            results = list(pool.map(one_copy, range(3)))

        assert proc.poll() is None, "s3proxy OOMKilled or crashed (exit 137?)"
        succeeded = sum(1 for r in results if r["success"])
        slowed = sum(1 for r in results if r.get("code") == "SlowDown")
        assert succeeded >= 1, f"expected at least one copy to succeed: {results}"
        assert slowed >= 1, f"expected backpressure on concurrent copies: {results}"
        assert succeeded + slowed == len(results), f"unexpected errors: {results}"

    def test_mixed_scylla_uploads_and_large_copy_server_survives(
        self, copy_stress_server, copy_bucket
    ):
        """Prod mix: concurrent ~50MB parts + large manifest-style copy."""
        endpoint, proc = copy_stress_server
        client, bucket = copy_bucket
        source_key = "manifest-source.bin"
        _put_large_source(client, bucket, source_key, self.SOURCE_SIZE)

        def scylla_part(i: int) -> dict:
            key = f"sst-part-{i}.bin"
            resp = client.create_multipart_upload(Bucket=bucket, Key=key)
            upload_id = resp["UploadId"]
            r = _upload_part(client, bucket, key, 1, upload_id, 50 * MB)
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

    def test_single_large_copy_rss_stays_bounded(self, copy_stress_server, copy_bucket):
        """One large copy must not push RSS toward pod limit (1Gi in prod)."""
        endpoint, proc = copy_stress_server
        client, bucket = copy_bucket
        source_key = "rss-source.bin"
        _put_large_source(client, bucket, source_key, self.SOURCE_SIZE)

        ps_proc = psutil.Process(proc.pid)
        peak_rss = ps_proc.memory_info().rss

        def poll_rss():
            nonlocal peak_rss
            while proc.poll() is None:
                peak_rss = max(peak_rss, ps_proc.memory_info().rss)
                time.sleep(0.2)

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            monitor = pool.submit(poll_rss)
            result = _upload_part_copy(client, bucket, "rss-dest.bin", source_key)
            monitor.cancel()

        assert proc.poll() is None
        assert result["success"], result
        peak_mb = peak_rss / MB
        print(f"[copy-rss] peak={peak_mb:.1f}MB", file=sys.stderr)
        # Local dev has no 1Gi cap; prod manifest copy peaked ~535MB tracemalloc heap.
        assert peak_mb < 900, (
            f"RSS {peak_mb:.0f}MB too high for single {self.SOURCE_SIZE // MB}MB copy"
        )
