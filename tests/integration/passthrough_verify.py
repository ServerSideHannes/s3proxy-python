"""Shared UploadPartCopy passthrough verification (real s3proxy subprocess + MinIO).

Used by tests/integration/test_upload_part_copy_passthrough_e2e.py and
scripts/verify_upload_part_copy_passthrough.py.
"""

from __future__ import annotations

import concurrent.futures
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
import uuid
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

import boto3
from botocore.config import Config

from s3proxy import crypto
from s3proxy.state.metadata import decode_multipart_metadata
from tests.integration.conftest import _wait_for_port, minio_backend
from tests.integration.copy_metrics_helpers import SCYLLA_SSTABLE_SIZE

MB = 1024 * 1024
CHUNK_PEAK = crypto.copy_chunk_peak(crypto.COPY_INTERNAL_PART_SIZE)
PROD_BUDGET_MB = 192
QUICK_SIZE = 96 * MB
PART_SIZE = 48 * MB


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str = ""


@dataclass
class RunContext:
    port: int
    endpoint: str
    minio_host: str
    bucket: str
    log_path: Path
    proxy: boto3.client
    raw: boto3.client
    failures: list[CheckResult] = field(default_factory=list)

    def ok(self, name: str, cond: bool, detail: str = "") -> None:
        self.failures.append(CheckResult(name, cond, detail))


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def s3_client(endpoint: str, *, read_timeout: int = 600):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
        config=Config(
            retries={"max_attempts": 3, "mode": "standard"},
            read_timeout=read_timeout,
            connect_timeout=10,
        ),
    )


def scrape_metric(port: int, name: str) -> float:
    with urllib.request.urlopen(f"http://localhost:{port}/metrics", timeout=5) as resp:
        for line in resp.read().decode().splitlines():
            if line.startswith(f"{name} ") or line.startswith(f"{name}_total "):
                return float(line.split()[1])
    return 0.0


class RepeatingBody:
    def __init__(self, size: int, chunk: int = 8 * MB):
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
        return self._pos

    def read(self, n: int = -1) -> bytes:
        if self._pos >= self._size:
            return b""
        if n < 0:
            n = self._size - self._pos
        n = min(n, self._size - self._pos)
        self._pos += n
        return b"X" * n


def upload_multipart(ctx: RunContext, key: str, size: int) -> None:
    resp = ctx.proxy.create_multipart_upload(Bucket=ctx.bucket, Key=key)
    upload_id = resp["UploadId"]
    parts = []
    pn = 1
    for off in range(0, size, PART_SIZE):
        chunk = min(PART_SIZE, size - off)
        up = ctx.proxy.upload_part(
            Bucket=ctx.bucket,
            Key=key,
            PartNumber=pn,
            UploadId=upload_id,
            Body=RepeatingBody(chunk),
        )
        parts.append({"PartNumber": pn, "ETag": up["ETag"]})
        pn += 1
    ctx.proxy.complete_multipart_upload(
        Bucket=ctx.bucket,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )
    meta = f".s3proxy-internal/{key}.meta"
    assert ctx.raw.head_object(Bucket=ctx.bucket, Key=meta)["ContentLength"] > 0


def upload_part_copy(
    ctx: RunContext,
    dest: str,
    source: str,
    *,
    byte_range: str | None = None,
) -> None:
    resp = ctx.proxy.create_multipart_upload(Bucket=ctx.bucket, Key=dest)
    uid = resp["UploadId"]
    kw: dict = {
        "Bucket": ctx.bucket,
        "Key": dest,
        "PartNumber": 1,
        "UploadId": uid,
        "CopySource": {"Bucket": ctx.bucket, "Key": source},
    }
    if byte_range:
        kw["CopySourceRange"] = byte_range
    cp = ctx.proxy.upload_part_copy(**kw)
    ctx.proxy.complete_multipart_upload(
        Bucket=ctx.bucket,
        Key=dest,
        UploadId=uid,
        MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": cp["CopyPartResult"]["ETag"]}]},
    )


def poll_during(ctx: RunContext, fn) -> tuple[int, int]:
    enc0 = scrape_metric(ctx.port, "s3proxy_bytes_encrypted_total")
    peak = 0
    stop = threading.Event()

    def _poll() -> None:
        nonlocal peak
        while not stop.is_set():
            peak = max(peak, int(scrape_metric(ctx.port, "s3proxy_memory_reserved_bytes")))
            time.sleep(0.02)

    t = threading.Thread(target=_poll, daemon=True)
    t.start()
    try:
        fn()
    finally:
        stop.set()
        t.join(timeout=3)
    enc_delta = int(scrape_metric(ctx.port, "s3proxy_bytes_encrypted_total") - enc0)
    return peak, enc_delta


def load_sidecar(ctx: RunContext, key: str):
    meta_key = f".s3proxy-internal/{key}.meta"
    raw = ctx.raw.get_object(Bucket=ctx.bucket, Key=meta_key)["Body"].read()
    return decode_multipart_metadata(raw.decode())


def raw_ciphertext(ctx: RunContext, key: str) -> bytes:
    return ctx.raw.get_object(Bucket=ctx.bucket, Key=key)["Body"].read()


def plaintext_via_proxy(ctx: RunContext, key: str, size: int) -> bytes:
    body = ctx.proxy.get_object(Bucket=ctx.bucket, Key=key)["Body"]
    out = bytearray()
    while True:
        chunk = body.read(8 * MB)
        if not chunk:
            break
        out.extend(chunk)
    assert len(out) == size
    return bytes(out)


def log_has_passthrough(log_path: Path) -> bool:
    return "UPLOAD_PART_COPY_PASSTHROUGH" in log_path.read_text(errors="replace")


@contextmanager
def verify_session(
    port: int | None = None,
    log_path: Path | None = None,
) -> Generator[tuple[RunContext, subprocess.Popen[bytes], Path]]:
    port = port or free_port()
    log_path = log_path or Path(tempfile.gettempdir()) / f"s3proxy-passthrough-verify-{port}.log"
    bucket = f"verify-{uuid.uuid4().hex[:8]}"

    with minio_backend(isolated=True) as minio_host, open(log_path, "w") as log_file:
        env = os.environ.copy()
        env.update(
            {
                "S3PROXY_CREDENTIALS": (
                    '[{"access_key":"minioadmin","secret_key":"minioadmin",'
                    '"kek":"test-encryption-key-32-bytes!!"}]'
                ),
                "S3PROXY_HOST": minio_host,
                "S3PROXY_REGION": "us-east-1",
                "S3PROXY_PORT": str(port),
                "S3PROXY_NO_TLS": "true",
                "S3PROXY_LOG_LEVEL": "INFO",
                "S3PROXY_MEMORY_LIMIT_MB": str(PROD_BUDGET_MB),
                "S3PROXY_MAX_PARALLEL_COPIES": "2",
                "AWS_ACCESS_KEY_ID": "minioadmin",
                "AWS_SECRET_ACCESS_KEY": "minioadmin",
            }
        )
        proc = subprocess.Popen(
            [sys.executable, "-m", "s3proxy.main"],
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
        endpoint = f"http://localhost:{port}"
        try:
            _wait_for_port(port, proc, timeout=30)
            proxy = s3_client(endpoint)
            raw = s3_client(minio_host)
            proxy.create_bucket(Bucket=bucket)
            ctx = RunContext(
                port=port,
                endpoint=endpoint,
                minio_host=minio_host,
                bucket=bucket,
                log_path=log_path,
                proxy=proxy,
                raw=raw,
            )
            yield ctx, proc, log_path
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()


def check_basic_passthrough(ctx: RunContext, source: str, dest: str, size: int) -> None:
    peak, enc = poll_during(ctx, lambda: upload_part_copy(ctx, dest, source))
    ctx.ok("zero bytes_encrypted", enc <= 1 * MB, f"{enc / MB:.2f}MB")
    ctx.ok(
        "low peak memory",
        peak <= CHUNK_PEAK * 0.5,
        f"{peak / MB:.2f}MB vs {CHUNK_PEAK / MB:.0f}MB chunk",
    )
    pt = plaintext_via_proxy(ctx, dest, size)
    ctx.ok("GET round-trip", pt == b"X" * size, f"len={len(pt)}")


def check_ciphertext_identity(ctx: RunContext, source: str, dest: str) -> None:
    src_ct = raw_ciphertext(ctx, source)
    dst_ct = raw_ciphertext(ctx, dest)
    ctx.ok("ciphertext bytes identical", src_ct == dst_ct, f"{len(src_ct)} bytes")


def check_dek_adoption(ctx: RunContext, source: str, dest: str) -> None:
    src_meta = load_sidecar(ctx, source)
    dst_meta = load_sidecar(ctx, dest)
    ctx.ok("wrapped_dek matches source", src_meta.wrapped_dek == dst_meta.wrapped_dek)
    ctx.ok("kid matches source", src_meta.kid == dst_meta.kid)
    if src_meta.parts:
        src_n = sum(len(p.internal_parts) for p in src_meta.parts)
        dst_n = sum(len(p.internal_parts) for p in dst_meta.parts)
        ctx.ok("internal part count preserved", src_n == dst_n, f"{dst_n} vs {src_n}")


def check_reencrypt_control(ctx: RunContext, source: str, dest: str, size: int) -> None:
    peak, enc = poll_during(
        ctx,
        lambda: upload_part_copy(ctx, dest, source, byte_range=f"bytes=0-{size - 1}"),
    )
    ctx.ok("encrypts full object", enc >= size * 0.5, f"{enc / MB:.0f}MB")
    ctx.ok("high peak memory", peak >= CHUNK_PEAK * 0.5, f"{peak / MB:.2f}MB")


def check_scylla_scale(ctx: RunContext, source: str, dest: str) -> None:
    t0 = time.monotonic()
    peak, enc = poll_during(ctx, lambda: upload_part_copy(ctx, dest, source))
    elapsed = time.monotonic() - t0
    ctx.ok("zero bytes_encrypted at 1280MB", enc <= 2 * MB, f"{enc / MB:.2f}MB")
    ctx.ok("peak memory bounded", peak <= CHUNK_PEAK * 0.5, f"{peak / MB:.2f}MB")
    head = ctx.proxy.get_object(Bucket=ctx.bucket, Key=dest, Range="bytes=0-1048575")["Body"].read()
    tail_off = SCYLLA_SSTABLE_SIZE - MB
    tail = ctx.proxy.get_object(
        Bucket=ctx.bucket, Key=dest, Range=f"bytes={tail_off}-{SCYLLA_SSTABLE_SIZE - 1}"
    )["Body"].read()
    ctx.ok("GET head 1MB", head == b"X" * MB)
    ctx.ok("GET tail 1MB", tail == b"X" * MB)
    ctx.ok("completed in reasonable time", elapsed < 600, f"{elapsed:.1f}s")
    src_ct = raw_ciphertext(ctx, source)
    dst_ct = raw_ciphertext(ctx, dest)
    ctx.ok("1280MB ciphertext identical", src_ct == dst_ct, f"{len(src_ct)} bytes")


def check_concurrent_flood(ctx: RunContext, source: str, n: int = 10) -> None:
    enc0 = scrape_metric(ctx.port, "s3proxy_bytes_encrypted_total")
    peak = 0
    stop = threading.Event()

    def _poll() -> None:
        nonlocal peak
        while not stop.is_set():
            peak = max(peak, int(scrape_metric(ctx.port, "s3proxy_memory_reserved_bytes")))
            time.sleep(0.02)

    poller = threading.Thread(target=_poll, daemon=True)
    poller.start()

    def one(i: int) -> bool:
        try:
            upload_part_copy(ctx, f"sst/concurrent-{i}.bin", source)
            return True
        except Exception:
            return False

    t0 = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=n) as pool:
        results = list(pool.map(one, range(n)))
    elapsed = time.monotonic() - t0
    stop.set()
    poller.join(timeout=3)
    enc_delta = int(scrape_metric(ctx.port, "s3proxy_bytes_encrypted_total") - enc0)

    ctx.ok("all copies succeeded", all(results), f"{sum(results)}/{n}")
    ctx.ok(
        "peak memory under prod budget",
        peak <= PROD_BUDGET_MB * MB,
        f"{peak / MB:.1f}MB / {PROD_BUDGET_MB}MB",
    )
    ctx.ok("zero encrypt during flood", enc_delta <= 2 * MB, f"{enc_delta / MB:.2f}MB")
    ctx.ok("flood completed", elapsed < 900, f"{elapsed:.1f}s")
