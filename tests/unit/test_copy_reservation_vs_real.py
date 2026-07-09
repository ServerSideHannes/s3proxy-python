"""The copy reservation must bound the streaming UploadPartCopy path's REAL peak.

Analogue of test_upload_reservation_vs_real.py, for the copy path. This is the
test that would have caught the ScyllaDB backup OOM: copy_pipeline_peak()
reserved a size-independent 32MB (4*MAX_BUFFER_SIZE) on the false premise that
the copy "streams in MAX_BUFFER_SIZE chunks", while _pump_copy_chunks actually
buffers a full internal part and _upload_internal_part_with_semaphore holds
plaintext + ciphertext of that part at once, times MAX_PARALLEL_INTERNAL_UPLOADS.
The governor therefore admitted ~6x too many concurrent copies -> OOMKilled.

It drives the ACTUAL handler method (not a re-implementation) under tracemalloc,
so it fails if the reservation drifts below reality OR the copy path starts
allocating more. The mock client copies the uploaded body the way aiobotocore
does for the signed HTTP request, which is part of the real peak.
"""

import hashlib
import tracemalloc

import pytest

from s3proxy import crypto
from s3proxy.handlers.multipart import MultipartHandlerMixin
from s3proxy.state import MultipartUploadState

MB = 1024 * 1024


class _Mgr:
    async def allocate_internal_parts(self, bucket, key, upload_id, count, client_part_number):
        return 1

    async def add_part(self, *a, **k):
        return None

    async def take_deferred_copy_tail(self, bucket, key, upload_id):
        return b""

    async def set_deferred_copy_tail(self, bucket, key, upload_id, tail):
        return None


class _Body:
    """aiohttp-like streaming body: `async with body`, `body.content.read(n)`."""

    def __init__(self, total, chunk):
        self._total = total
        self._chunk = chunk
        self._sent = 0
        self.content = self

    async def read(self, n):
        if self._sent >= self._total:
            return b""
        take = min(n, self._total - self._sent)
        self._sent += take
        return b"x" * take

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class _Client:
    def __init__(self, total):
        self._total = total

    async def get_object(self, bucket, key, range_header=None):
        return {"Body": _Body(self._total, crypto.MAX_BUFFER_SIZE)}

    async def upload_part(self, bucket, key, upload_id, part_number, body):
        # aiobotocore copies the body to sign and send it; mirror that so the
        # measured peak reflects what the real transport holds.
        sent = bytes(body)
        return {"ETag": hashlib.md5(sent).hexdigest()}


def _handler():
    h = MultipartHandlerMixin.__new__(MultipartHandlerMixin)  # bypass BaseHandler.__init__
    h.multipart_manager = _Mgr()
    return h


async def _measure_peak(plaintext_size):
    h = _handler()
    state = MultipartUploadState(dek=crypto.generate_dek(), bucket="b", key="k", upload_id="u")
    tracemalloc.start()
    base = tracemalloc.get_traced_memory()[0]
    # Plaintext (unencrypted) source: src_wrapped_dek=None, src_multipart_meta=None.
    await h._streaming_copy_part_inner(
        _Client(plaintext_size),
        "b",
        "k",
        "u",
        1,
        state,
        "b",
        "src",
        None,
        None,
        None,
        {},
        {},
        plaintext_size,
    )
    peak = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()
    return peak - base


@pytest.mark.asyncio
@pytest.mark.parametrize("mb", [64, 128, 512])
async def test_reservation_bounds_real_copy_peak(mb):
    plaintext_size = mb * MB
    real_peak = await _measure_peak(plaintext_size)
    reserved = crypto.copy_pipeline_peak(plaintext_size)

    # The whole point: the reservation must cover the REAL peak. The old
    # size-independent 4*MAX_BUFFER_SIZE reservation fails here.
    assert reserved >= real_peak, (
        f"{mb}MB copy: reserved {reserved / MB:.1f}MB < real peak "
        f"{real_peak / MB:.1f}MB -- governor under-counts, will OOM"
    )
