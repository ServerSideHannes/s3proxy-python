"""The reservation must bound the framed UploadPart path's REAL peak memory.

This is the test that would have caught the Elasticsearch OOM: the memory
governor reserved only the internal-part size (8MB for a 16MB part) while the
framed path's real peak is ~3x that (accumulated ciphertext + AES-GCM encrypt
transient + held plaintext frame + the transport's copy of the body). The
governor therefore admitted ~3x too many concurrent uploads and the pod was
OOMKilled.

It drives the ACTUAL handler method (not a re-implementation) under tracemalloc,
so it fails if either the reservation drifts below reality OR the upload path
starts allocating more. The mock S3 client copies the body the way aiobotocore
does for the signed HTTP request, which is part of the real peak.
"""

import hashlib
import tracemalloc

import pytest

from s3proxy import crypto
from s3proxy.concurrency import estimate_memory_footprint, set_memory_limit
from s3proxy.handlers.multipart.upload_part import UploadPartMixin
from s3proxy.state import MultipartUploadState

MB = 1024 * 1024


class _Mgr:
    async def add_part(self, *a, **k):
        return None


class _Client:
    async def upload_part(self, bucket, key, upload_id, part_number, body):
        # aiobotocore copies the body to sign and send it; mirror that so the
        # measured peak reflects what the real transport holds.
        sent = bytes(body)
        return {"ETag": hashlib.md5(sent).hexdigest()}


class _Request:
    def __init__(self, total, chunk=64 * 1024):
        self._total = total
        self._chunk = chunk

    async def stream(self):
        for i in range(0, self._total, self._chunk):
            yield b"x" * min(self._chunk, self._total - i)


def _handler():
    h = UploadPartMixin.__new__(UploadPartMixin)  # bypass BaseHandler.__init__
    h.multipart_manager = _Mgr()
    return h


async def _measure_peak(content_length):
    h = _handler()
    state = MultipartUploadState(dek=crypto.generate_dek(), bucket="b", key="k", upload_id="u")
    part_size = crypto.memory_bounded_part_size(content_length)
    tracemalloc.start()
    base = tracemalloc.get_traced_memory()[0]
    await h._stream_and_upload_framed(
        _Request(content_length),
        _Client(),
        "b",
        "k",
        "u",
        1,
        state,
        content_length,
        part_size,
        1,
    )
    peak = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()
    return peak - base


@pytest.mark.asyncio
@pytest.mark.parametrize("mb", [16, 64, 512])
async def test_reservation_bounds_real_framed_peak(mb):
    content_length = mb * MB
    real_peak = await _measure_peak(content_length)
    reserved = estimate_memory_footprint("PUT", content_length)

    # The whole point: the reservation must cover the REAL peak, not the bare
    # part size. A bare-part-size reservation (the old behaviour) fails here.
    assert reserved >= real_peak, (
        f"{mb}MB part: reserved {reserved / MB:.1f}MB < real peak "
        f"{real_peak / MB:.1f}MB -- governor under-counts, will OOM"
    )
    # And the old under-count must be demonstrably insufficient.
    assert crypto.memory_bounded_part_size(content_length) < real_peak


# Production governor budget (chart/values.yaml performance.memoryLimitMb).
PROD_LIMIT_MB = 256


@pytest.mark.parametrize("mb", [16, 512])
def test_workload_parts_fit_prod_budget(mb):
    """At the deployed governor budget, the real workloads (16MB ES snapshot
    parts, 512MB barman parts) must fit a single reservation -- otherwise the
    limiter rejects them outright (S3 SlowDown) and the upload can never run."""
    set_memory_limit(PROD_LIMIT_MB)
    try:
        reserved = estimate_memory_footprint("PUT", mb * MB)
        assert reserved <= PROD_LIMIT_MB * MB, (
            f"{mb}MB part reserves {reserved / MB:.1f}MB > {PROD_LIMIT_MB}MB budget"
        )
    finally:
        set_memory_limit(64)
