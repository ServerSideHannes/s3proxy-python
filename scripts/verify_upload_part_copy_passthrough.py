#!/usr/bin/env python3
"""CLI wrapper for UploadPartCopy passthrough verification.

CI runs the pytest suite instead:
  uv run pytest tests/integration/test_upload_part_copy_passthrough_e2e.py -m e2e -n0 -v

Manual / pre-merge:
  uv run python scripts/verify_upload_part_copy_passthrough.py
  uv run python scripts/verify_upload_part_copy_passthrough.py --quick
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.integration.copy_metrics_helpers import SCYLLA_SSTABLE_SIZE  # noqa: E402
from tests.integration.passthrough_verify import (  # noqa: E402
    QUICK_SIZE,
    check_basic_passthrough,
    check_ciphertext_identity,
    check_concurrent_flood,
    check_dek_adoption,
    check_reencrypt_control,
    check_scylla_scale,
    log_has_passthrough,
    upload_multipart,
    verify_session,
)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--quick", action="store_true", help="Skip 1280MB + concurrent flood")
    args = p.parse_args()

    print("=== UploadPartCopy passthrough verification ===")
    with verify_session() as (ctx, _proc, log_path):
        print(f"log={log_path}")
        upload_multipart(ctx, "sst/source-96mb.bin", QUICK_SIZE)
        check_basic_passthrough(ctx, "sst/source-96mb.bin", "sst/dest-96mb.bin", QUICK_SIZE)
        check_ciphertext_identity(ctx, "sst/source-96mb.bin", "sst/dest-96mb.bin")
        check_dek_adoption(ctx, "sst/source-96mb.bin", "sst/dest-96mb.bin")
        check_reencrypt_control(ctx, "sst/source-96mb.bin", "sst/dest-reencrypt.bin", QUICK_SIZE)

        if not args.quick:
            upload_multipart(ctx, "sst/source-1280mb.bin", SCYLLA_SSTABLE_SIZE)
            check_scylla_scale(ctx, "sst/source-1280mb.bin", "sst/dest-1280mb.bin")
            check_concurrent_flood(ctx, "sst/source-1280mb.bin", n=10)

    failed = [c for c in ctx.failures if not c.passed]
    if not log_has_passthrough(log_path):
        print("FAIL: missing UPLOAD_PART_COPY_PASSTHROUGH log line")
        failed.append(None)

    for c in ctx.failures:
        mark = "PASS" if c.passed else "FAIL"
        detail = f": {c.detail}" if c.detail else ""
        print(f"  [{mark}] {c.name}{detail}")

    if failed:
        print(f"\n{len(failed)} check(s) failed")
        return 1
    print("\nALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
