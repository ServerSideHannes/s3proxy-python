"""Fixtures for integration tests that use real s3proxy + MinIO."""

import contextlib
import os
import socket
import subprocess
import sys
import time
from collections.abc import Generator
from contextlib import contextmanager

import boto3
import pytest

# === SHARED S3PROXY HELPER ===


@contextmanager
def run_s3proxy(
    port: int,
    *,
    log_output: bool = True,
    **env_overrides: str,
) -> Generator[tuple[str, subprocess.Popen]]:
    """Start s3proxy server and yield (endpoint_url, process).

    Args:
        port: Port to run s3proxy on
        log_output: Whether to show server logs (default True)
        **env_overrides: Override default environment variables

    Yields:
        Tuple of (endpoint_url, process) for the running server

    Example:
        with run_s3proxy(4433) as (endpoint, proc):
            client = boto3.client("s3", endpoint_url=endpoint, ...)
            # use client...

        # With custom settings:
        with run_s3proxy(4460, S3PROXY_MEMORY_LIMIT_MB="16") as (endpoint, proc):
            ...
    """
    env = os.environ.copy()
    env.update(
        {
            "S3PROXY_ENCRYPT_KEY": "test-encryption-key-32-bytes!!",
            "S3PROXY_HOST": "http://localhost:9000",
            "S3PROXY_REGION": "us-east-1",
            "S3PROXY_PORT": str(port),
            "S3PROXY_NO_TLS": "true",
            "S3PROXY_LOG_LEVEL": "WARNING",
            "AWS_ACCESS_KEY_ID": "minioadmin",
            "AWS_SECRET_ACCESS_KEY": "minioadmin",
        }
    )
    env.update(env_overrides)

    output = sys.stderr if log_output else subprocess.DEVNULL
    proc = subprocess.Popen(
        ["python", "-m", "s3proxy.main"],
        env=env,
        stdout=output,
        stderr=output,
    )

    # Wait for server to be ready
    try:
        _wait_for_port(port, proc, timeout=15)
        yield f"http://localhost:{port}", proc
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


def _wait_for_port(port: int, proc: subprocess.Popen, timeout: float = 15) -> None:
    """Wait for a port to become available."""
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        if proc.poll() is not None:
            raise RuntimeError(f"s3proxy died with code {proc.returncode}")
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex(("localhost", port))
            sock.close()
            if result == 0:
                return
        except Exception:
            pass
        time.sleep(0.5)
    proc.kill()
    raise RuntimeError(f"s3proxy failed to start on port {port} after {timeout}s")


@pytest.fixture(scope="session")
def s3proxy_server():
    """Start s3proxy server for e2e tests.

    Session-scoped to share one server across all integration tests.
    Each pytest-xdist worker gets its own port to avoid conflicts.
    """
    # Get xdist worker ID (gw0, gw1, etc.) or "master" if not running in parallel
    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "master")
    if worker_id == "master":
        port = 4433
    else:
        # Extract worker number from "gw0", "gw1", etc.
        worker_num = int(worker_id.replace("gw", ""))
        port = 4433 + worker_num

    print(f"\n[FIXTURE] Starting s3proxy on port {port} (worker={worker_id})...")

    with run_s3proxy(port) as (endpoint, proc):
        print(f"[FIXTURE] s3proxy ready (pid={proc.pid})")
        yield endpoint
        print(f"[FIXTURE] Stopping s3proxy (pid={proc.pid})...")


@pytest.fixture
def s3_client(s3proxy_server):
    """Create boto3 S3 client pointing to s3proxy."""
    return boto3.client(
        "s3",
        endpoint_url=s3proxy_server,
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )


@pytest.fixture
def test_bucket(s3_client, request):
    """Create and cleanup test bucket with unique name per test."""
    import hashlib

    # Create unique bucket name based on test node id
    test_id = hashlib.md5(request.node.nodeid.encode()).hexdigest()[:8]
    bucket = f"test-bucket-{test_id}"

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
