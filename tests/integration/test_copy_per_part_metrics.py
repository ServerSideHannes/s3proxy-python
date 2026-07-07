"""Subprocess + Prometheus proof: copy governor releases memory per internal part.

Starts a real s3proxy subprocess, drives real boto3 UploadPartCopy against MinIO,
and polls ``s3proxy_memory_reserved_bytes`` — the same signal Grafana charts in prod.

Includes a Scylla-backup workload simulation: 1280MB SSTable copies (40 internal
parts), 192MB governor, two concurrent copies (the prod regression scenario).
"""

from __future__ import annotations

import concurrent.futures
import socket
import uuid

import pytest
from botocore.exceptions import ClientError

from tests.integration.conftest import minio_backend, run_s3proxy
from tests.integration.copy_metrics_helpers import (
    BASELINE_CEILING,
    CHUNK_PEAK,
    MB,
    PROD_GOVERNOR_MB,
    SCYLLA_SSTABLE_SIZE,
    MetricsPoller,
    assert_concurrent_copies_not_flatlined,
    assert_copy_memory_sawtooth,
)
from tests.integration.test_copy_memory_governor import (
    _client,
    _put_large_source,
    _upload_part_copy,
)

# 256MB → 8 internal parts; fast sawtooth check.
FAST_SOURCE_SIZE = 256 * MB


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def metrics_server():
    port = _find_free_port()
    with (
        minio_backend() as minio_host,
        run_s3proxy(
            port,
            log_output=False,
            S3PROXY_HOST=minio_host,
            S3PROXY_MEMORY_LIMIT_MB=str(PROD_GOVERNOR_MB),
            S3PROXY_BACKPRESSURE_TIMEOUT="30",
            S3PROXY_MAX_PART_SIZE_MB="0",
        ) as (endpoint, proc),
    ):
        yield port, endpoint, proc


@pytest.fixture(scope="module")
def metrics_bucket(metrics_server):
    _, endpoint, _ = metrics_server
    client = _client(endpoint)
    bucket = f"copy-metrics-{uuid.uuid4().hex[:8]}"
    try:
        client.create_bucket(Bucket=bucket)
    except ClientError as exc:
        pytest.skip(f"MinIO/S3 backend not available: {exc}")
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
def fast_source(metrics_bucket):
    client, bucket = metrics_bucket
    key = "fast-source.bin"
    _put_large_source(client, bucket, key, FAST_SOURCE_SIZE)
    return key


@pytest.fixture(scope="module")
def scylla_sstable_source(metrics_bucket):
    """1280MB encrypted object — same size as prod Scylla backup SSTable copies."""
    client, bucket = metrics_bucket
    key = "scylla-sstable-1280mb.bin"
    _put_large_source(client, bucket, key, SCYLLA_SSTABLE_SIZE)
    return key


@pytest.mark.e2e
class TestCopyPerPartMetricsSubprocess:
    """Real subprocess + Prometheus: reserved bytes must sawtooth during copy."""

    def test_memory_reserved_drops_between_internal_parts(
        self, metrics_server, metrics_bucket, fast_source
    ):
        port, _, proc = metrics_server
        client, bucket = metrics_bucket

        poller = MetricsPoller(port)
        poller.start()

        result = _upload_part_copy(client, bucket, "metrics-dest.bin", fast_source)
        poller.stop()

        assert proc.poll() is None, "s3proxy crashed during copy"
        assert result["success"], f"copy failed: {result}"
        assert_copy_memory_sawtooth(poller.samples, plaintext_size=FAST_SOURCE_SIZE)

    def test_three_concurrent_copies_all_succeed_on_192mb(
        self, metrics_server, metrics_bucket, fast_source
    ):
        """Per-part interleaving: 3 large copies fit where whole-object hold allowed 2."""
        _, _, proc = metrics_server
        client, bucket = metrics_bucket

        def one_copy(i: int) -> dict:
            return _upload_part_copy(client, bucket, f"concurrent-{i}.bin", fast_source)

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            results = list(pool.map(one_copy, range(3)))

        assert proc.poll() is None
        succeeded = sum(1 for r in results if r["success"])
        slowed = sum(1 for r in results if r.get("code") == "SlowDown")
        assert succeeded == 3, (
            f"expected all 3 copies to succeed with per-part reservation; "
            f"succeeded={succeeded} slowed={slowed} results={results}"
        )


@pytest.mark.e2e
class TestScyllaBackupCopyLoad:
    """Prod-shaped load: 1280MB SSTables, 192MB governor, concurrent UploadPartCopy."""

    def test_scylla_1280mb_copy_sawtooths_across_40_internal_parts(
        self, metrics_server, metrics_bucket, scylla_sstable_source
    ):
        port, _, proc = metrics_server
        client, bucket = metrics_bucket

        poller = MetricsPoller(port, interval_s=0.05)
        poller.start()

        result = _upload_part_copy(client, bucket, "scylla-backup-dest.bin", scylla_sstable_source)
        poller.stop()

        assert proc.poll() is None, "s3proxy crashed during 1280MB copy"
        assert result["success"], f"1280MB copy failed: {result}"
        # 40 internal parts — expect many inter-part drops, not a flat ~88MB line.
        assert_copy_memory_sawtooth(
            poller.samples,
            plaintext_size=SCYLLA_SSTABLE_SIZE,
            min_drops=10,
            min_samples=15,
        )

    def test_two_concurrent_scylla_1280mb_copies_succeed_without_flat_reservation(
        self, metrics_server, metrics_bucket, scylla_sstable_source
    ):
        """Prod regression: 2×1280MB copies held ~176MB flat; per-part must interleave."""
        port, _, proc = metrics_server
        client, bucket = metrics_bucket

        poller = MetricsPoller(port, interval_s=0.05)
        poller.start()

        def one_copy(i: int) -> dict:
            return _upload_part_copy(
                client, bucket, f"scylla-concurrent-{i}.bin", scylla_sstable_source
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(one_copy, range(2)))

        poller.stop()

        assert proc.poll() is None, "s3proxy crashed under Scylla backup load"
        assert all(r["success"] for r in results), (
            f"expected both 1280MB copies to succeed on {PROD_GOVERNOR_MB}MB budget: {results}"
        )
        assert_concurrent_copies_not_flatlined(poller.samples, concurrent_copies=2)
        # Old code: min stays ~176MB (2 × 88MB whole-object holds). Per-part frees budget.
        assert min(poller.samples) <= BASELINE_CEILING, (
            f"reservation never dropped during concurrent Scylla copies "
            f"(min={min(poller.samples) / MB:.2f}MB, expected <= {BASELINE_CEILING / MB:.2f}MB)"
        )
        assert max(poller.samples) <= CHUNK_PEAK * 2.5, (
            f"peak reserved {max(poller.samples) / MB:.2f}MB — more than brief 2-chunk overlap?"
        )
