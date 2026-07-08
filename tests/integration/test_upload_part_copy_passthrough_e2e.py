"""CI e2e: UploadPartCopy passthrough on real s3proxy subprocess + MinIO.

Mirrors scripts/verify_upload_part_copy_passthrough.py (checks A–F).
Runs in the memory_copy integration shard (-n0).
"""

from __future__ import annotations

import pytest

from tests.integration.copy_metrics_helpers import SCYLLA_SSTABLE_SIZE
from tests.integration.passthrough_verify import (
    CHUNK_PEAK,
    MB,
    QUICK_SIZE,
    check_basic_passthrough,
    check_concurrent_flood,
    check_dek_adoption,
    check_reencrypt_control,
    check_scylla_manifest_full_range_passthrough,
    check_scylla_scale,
    log_has_passthrough,
    poll_during,
    raw_ciphertext,
    upload_multipart,
    upload_part_copy,
    verify_session,
)


@pytest.fixture(scope="module")
def passthrough_env():
    """One s3proxy subprocess + MinIO; upload 96MB and 1280MB sidecar sources."""
    with verify_session() as (ctx, proc, log_path):
        upload_multipart(ctx, "sst/source-96mb.bin", QUICK_SIZE)
        upload_multipart(ctx, "sst/source-1280mb.bin", SCYLLA_SSTABLE_SIZE)
        yield ctx, proc, log_path


def _assert_check(ctx, name: str) -> None:
    for c in ctx.failures:
        if c.name == name:
            assert c.passed, f"{name}: {c.detail}"
            return
    raise AssertionError(f"check {name!r} was not recorded")


@pytest.mark.e2e
class TestUploadPartCopyPassthroughQuick:
    """Checks A–D (96MB) — fast regression gate."""

    def test_zero_encrypt_and_low_memory(self, passthrough_env):
        ctx, proc, _ = passthrough_env
        ctx.failures.clear()
        check_basic_passthrough(ctx, "sst/source-96mb.bin", "sst/dest-a.bin", QUICK_SIZE)
        assert proc.poll() is None
        _assert_check(ctx, "zero bytes_encrypted")
        _assert_check(ctx, "low peak memory")
        _assert_check(ctx, "GET round-trip")

    def test_ciphertext_byte_identical_on_minio(self, passthrough_env):
        ctx, proc, _ = passthrough_env
        upload_part_copy(ctx, "sst/dest-b.bin", "sst/source-96mb.bin")
        assert proc.poll() is None
        assert raw_ciphertext(ctx, "sst/source-96mb.bin") == raw_ciphertext(ctx, "sst/dest-b.bin")

    def test_dek_adoption_in_sidecar(self, passthrough_env):
        ctx, proc, _ = passthrough_env
        ctx.failures.clear()
        upload_part_copy(ctx, "sst/dest-c.bin", "sst/source-96mb.bin")
        check_dek_adoption(ctx, "sst/source-96mb.bin", "sst/dest-c.bin")
        assert proc.poll() is None
        _assert_check(ctx, "wrapped_dek matches source")
        _assert_check(ctx, "kid matches source")
        _assert_check(ctx, "internal part count preserved")

    def test_range_copy_reencrypts_with_high_memory(self, passthrough_env):
        ctx, proc, _ = passthrough_env
        ctx.failures.clear()
        check_reencrypt_control(ctx, "sst/source-96mb.bin", "sst/dest-d.bin", QUICK_SIZE)
        assert proc.poll() is None
        _assert_check(ctx, "encrypts partial range")
        _assert_check(ctx, "high peak memory")

    def test_scylla_manifest_full_range_passthrough(self, passthrough_env):
        ctx, proc, _ = passthrough_env
        ctx.failures.clear()
        check_scylla_manifest_full_range_passthrough(
            ctx, "sst/source-1280mb.bin", "sst/dest-1280mb-fullrange.bin", SCYLLA_SSTABLE_SIZE
        )
        assert proc.poll() is None
        _assert_check(ctx, "full-range zero bytes_encrypted")
        _assert_check(ctx, "full-range low peak memory")
        _assert_check(ctx, "full-range ciphertext identical")
        _assert_check(ctx, "UPLOAD_PART_COPY_PASSTHROUGH logged")

    def test_passthrough_log_line(self, passthrough_env):
        _, proc, log_path = passthrough_env
        assert proc.poll() is None
        assert log_has_passthrough(log_path)


@pytest.mark.e2e
class TestUploadPartCopyPassthroughScylla:
    """Checks E–F (1280MB + concurrent flood) — prod-shaped load."""

    def test_scylla_scale_passthrough(self, passthrough_env):
        ctx, proc, _ = passthrough_env
        ctx.failures.clear()
        check_scylla_scale(ctx, "sst/source-1280mb.bin", "sst/dest-1280mb.bin")
        assert proc.poll() is None
        _assert_check(ctx, "zero bytes_encrypted at 1280MB")
        _assert_check(ctx, "peak memory bounded")
        _assert_check(ctx, "1280MB ciphertext identical")

    def test_ten_concurrent_passthrough_copies(self, passthrough_env):
        ctx, proc, _ = passthrough_env
        ctx.failures.clear()
        # 96MB source: flood tests pipeline/memory, not 1280MB scale (saves ~12GB on CI).
        check_concurrent_flood(ctx, "sst/source-96mb.bin", n=10)
        assert proc.poll() is None
        _assert_check(ctx, "all copies succeeded")
        _assert_check(ctx, "peak memory under prod budget")
        _assert_check(ctx, "zero encrypt during flood")

    def test_passthrough_encrypt_delta_vs_reencrypt(self, passthrough_env):
        """Passthrough must not touch encrypt counter; range copy must."""
        ctx, proc, _ = passthrough_env
        peak_pt, enc_pt = poll_during(
            ctx, lambda: upload_part_copy(ctx, "sst/dest-compare-pt.bin", "sst/source-96mb.bin")
        )
        partial_end = min(QUICK_SIZE - 1, 32 * MB - 1)
        peak_re, enc_re = poll_during(
            ctx,
            lambda: upload_part_copy(
                ctx,
                "sst/dest-compare-re.bin",
                "sst/source-96mb.bin",
                byte_range=f"bytes=0-{partial_end}",
            ),
        )
        assert proc.poll() is None
        assert enc_pt <= 1 * MB
        assert enc_re >= (partial_end + 1) * 0.5
        assert peak_pt <= CHUNK_PEAK * 0.5
        assert peak_re >= CHUNK_PEAK * 0.5
