"""E2E: presigned PUT through real s3proxy + MinIO.

Covers both presigned URL styles the proxy recognizes:
- SigV4 (X-Amz-Signature): Scylla Manager / boto3 with signature_version=s3v4
- Legacy V2 (Signature=): simple query-only presigns matching our verifier

PR #116 wrongly routed large presigned PUTs into deferred header-auth → 403.
"""

from __future__ import annotations

import base64
import contextlib
import hashlib
import hmac
import http.client
import socket
import uuid
from datetime import UTC, datetime
from urllib.parse import quote, urlparse

import boto3
import pytest
from botocore.config import Config

from tests.integration.conftest import minio_backend, run_s3proxy

MB = 1024 * 1024
SCYLLA_SST_MB = 179
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"

pytestmark = pytest.mark.xdist_group("presigned_put")


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


class _ZeroBody:
    """Stream size bytes of zeros without allocating the full buffer."""

    def __init__(self, size: int, chunk: int = MB):
        self._size = size
        self._chunk = chunk
        self._pos = 0

    def read(self, amt: int = -1) -> bytes:
        if self._pos >= self._size:
            return b""
        if amt is None or amt < 0:
            amt = self._chunk
        take = min(amt, self._chunk, self._size - self._pos)
        self._pos += take
        return b"\x00" * take

    def __len__(self) -> int:
        return self._size


def _s3_client_v4(endpoint: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )


def _md5_zeros(size: int, chunk: int = MB) -> str:
    h = hashlib.md5()
    block = b"\x00" * chunk
    full, rem = divmod(size, chunk)
    for _ in range(full):
        h.update(block)
    if rem:
        h.update(block[:rem])
    return base64.b64encode(h.digest()).decode()


def _presigned_url_v4(
    endpoint: str,
    *,
    bucket: str,
    key: str,
    content_md5: str,
    mtime: str = "1700000000",
) -> str:
    """boto3 SigV4 presign — matches Scylla Manager (X-Amz-Signature in query)."""
    client = _s3_client_v4(endpoint)
    return client.generate_presigned_url(
        "put_object",
        Params={
            "Bucket": bucket,
            "Key": key,
            "ContentType": "application/octet-stream",
            "ContentMD5": content_md5,
            "ACL": "private",
            "Metadata": {"mtime": mtime},
        },
        ExpiresIn=900,
        HttpMethod="PUT",
    )


def _presigned_url_v2(endpoint: str, *, bucket: str, key: str) -> str:
    """Legacy V2 presign (AWSAccessKeyId + Expires + Signature only).

    boto3's default V2 presign adds content-type/content-md5 to the query string,
    which our simplified V2 verifier does not cover. Hand-craft the URL the
    verifier expects: PUT\\n\\n\\n{expires}\\n{path}
    """
    expires = str(int(datetime.now(UTC).timestamp()) + 900)
    path = f"/{bucket}/{key}"
    string_to_sign = f"PUT\n\n\n{expires}\n{path}"
    signature = base64.b64encode(
        hmac.new(SECRET_KEY.encode(), string_to_sign.encode(), hashlib.sha1).digest()
    ).decode()
    return (
        f"{endpoint}{path}"
        f"?AWSAccessKeyId={quote(ACCESS_KEY)}"
        f"&Expires={expires}"
        f"&Signature={quote(signature)}"
    )


def _http_put_presigned(
    url: str,
    *,
    size: int,
    content_md5: str | None = None,
    mtime: str | None = None,
) -> None:
    parsed = urlparse(url)
    path_query = parsed.path
    if parsed.query:
        path_query = f"{path_query}?{parsed.query}"
    headers: dict[str, str] = {
        "Content-Type": "application/octet-stream",
        "Content-Length": str(size),
    }
    if content_md5 is not None:
        headers["Content-MD5"] = content_md5
    if mtime is not None:
        headers["x-amz-acl"] = "private"
        headers["x-amz-meta-mtime"] = mtime
    conn = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=600)
    conn.request("PUT", path_query, body=_ZeroBody(size), headers=headers)
    resp = conn.getresponse()
    if resp.status not in (200, 201):
        body = resp.read()
        raise AssertionError(f"presigned PUT failed: {resp.status} {body!r}")
    resp.read()


@pytest.fixture(scope="module")
def presigned_server():
    port = _find_free_port()
    with (
        minio_backend(isolated=True) as minio_host,
        run_s3proxy(port, log_output=False, S3PROXY_HOST=minio_host) as (endpoint, proc),
    ):
        yield endpoint, proc


@pytest.fixture
def presigned_bucket(presigned_server):
    endpoint, _ = presigned_server
    client = _s3_client_v4(endpoint)
    bucket = f"presigned-e2e-{uuid.uuid4().hex[:8]}"
    with contextlib.suppress(client.exceptions.BucketAlreadyOwnedByYou):
        client.create_bucket(Bucket=bucket)
    yield endpoint, bucket, client
    try:
        resp = client.list_objects_v2(Bucket=bucket)
        if contents := resp.get("Contents"):
            client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": o["Key"]} for o in contents]},
            )
        client.delete_bucket(Bucket=bucket)
    except Exception:
        pass


@pytest.mark.e2e
class TestPresignedPutV4E2E:
    """SigV4 presigned PUT (Scylla Manager shape)."""

    def test_small_roundtrip(self, presigned_bucket, presigned_server):
        endpoint, bucket, client = presigned_bucket
        _, proc = presigned_server
        key = "backup/sst/small-Data.db"
        size = 1 * MB
        content_md5 = _md5_zeros(size)
        url = _presigned_url_v4(endpoint, bucket=bucket, key=key, content_md5=content_md5)
        _http_put_presigned(url, size=size, content_md5=content_md5, mtime="1700000000")
        assert proc.poll() is None
        assert client.head_object(Bucket=bucket, Key=key)["ContentLength"] == size

    def test_large_scylla_sst_shape(self, presigned_bucket, presigned_server):
        endpoint, bucket, client = presigned_bucket
        _, proc = presigned_server
        key = (
            "backup/sst/cluster/test/dc/eu-north-1/node/test/"
            "keyspace/ai_center/table/job_titles/uuid/me-big-Data.db"
        )
        size = SCYLLA_SST_MB * MB
        content_md5 = _md5_zeros(size)
        url = _presigned_url_v4(endpoint, bucket=bucket, key=key, content_md5=content_md5)
        _http_put_presigned(url, size=size, content_md5=content_md5, mtime="1700000000")
        assert proc.poll() is None
        assert client.head_object(Bucket=bucket, Key=key)["ContentLength"] == size

    def test_bad_signature_rejected(self, presigned_bucket):
        endpoint, bucket, client = presigned_bucket
        key = "backup/sst/bad-sig-v4-Data.db"
        size = 10 * MB
        content_md5 = _md5_zeros(size)
        url = _presigned_url_v4(endpoint, bucket=bucket, key=key, content_md5=content_md5)
        url = url.replace("X-Amz-Signature=", "X-Amz-Signature=deadbeef", 1)
        parsed = urlparse(url)
        path_query = parsed.path
        if parsed.query:
            path_query = f"{path_query}?{parsed.query}"
        conn = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=120)
        conn.request(
            "PUT",
            path_query,
            body=_ZeroBody(size),
            headers={
                "Content-Type": "application/octet-stream",
                "Content-MD5": content_md5,
                "Content-Length": str(size),
            },
        )
        resp = conn.getresponse()
        assert resp.status == 403, resp.read()
        with pytest.raises(client.exceptions.ClientError):
            client.head_object(Bucket=bucket, Key=key)


@pytest.mark.e2e
class TestPresignedPutV2E2E:
    """Legacy V2 presigned PUT (Signature= in query)."""

    def test_small_roundtrip(self, presigned_bucket, presigned_server):
        endpoint, bucket, client = presigned_bucket
        _, proc = presigned_server
        key = "legacy/small.bin"
        size = 1 * MB
        url = _presigned_url_v2(endpoint, bucket=bucket, key=key)
        _http_put_presigned(url, size=size)
        assert proc.poll() is None
        assert client.head_object(Bucket=bucket, Key=key)["ContentLength"] == size

    def test_large_put(self, presigned_bucket, presigned_server):
        endpoint, bucket, client = presigned_bucket
        _, proc = presigned_server
        key = "legacy/large.bin"
        size = SCYLLA_SST_MB * MB
        url = _presigned_url_v2(endpoint, bucket=bucket, key=key)
        _http_put_presigned(url, size=size)
        assert proc.poll() is None
        assert client.head_object(Bucket=bucket, Key=key)["ContentLength"] == size

    def test_bad_signature_rejected(self, presigned_bucket):
        endpoint, bucket, client = presigned_bucket
        key = "legacy/bad-sig.bin"
        size = 10 * MB
        url = _presigned_url_v2(endpoint, bucket=bucket, key=key)
        url = url.replace("Signature=", "Signature=deadbeef", 1)
        parsed = urlparse(url)
        path_query = parsed.path
        if parsed.query:
            path_query = f"{path_query}?{parsed.query}"
        conn = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=120)
        conn.request(
            "PUT",
            path_query,
            body=_ZeroBody(size),
            headers={
                "Content-Type": "application/octet-stream",
                "Content-Length": str(size),
            },
        )
        resp = conn.getresponse()
        assert resp.status == 403, resp.read()
        with pytest.raises(client.exceptions.ClientError):
            client.head_object(Bucket=bucket, Key=key)
