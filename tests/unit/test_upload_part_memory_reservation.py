"""UploadPart must reserve each internal part's real memory from the shared
limiter (like the download path), so concurrent multipart uploads are throttled
instead of silently exceeding the memory budget and OOM-killing the pod."""

import asyncio
import types

import pytest

from s3proxy import concurrency, crypto
from s3proxy.handlers.multipart.upload_part import UploadPartMixin


@pytest.mark.asyncio
async def test_internal_part_upload_reserves_and_releases_memory():
    concurrency.set_memory_limit(256)  # large enough to admit one internal part
    concurrency.set_active_memory(0)

    data_size = 16 * 1024 * 1024
    expected_reservation = data_size * 2 - crypto.MAX_BUFFER_SIZE  # plaintext + ciphertext
    active_during_upload = 0

    class _Client:
        async def upload_part(self, bucket, key, upload_id, part_number, ciphertext):
            nonlocal active_during_upload
            active_during_upload = concurrency.get_active_memory()
            return {"ETag": '"abc"'}

    handler = UploadPartMixin.__new__(UploadPartMixin)
    state = types.SimpleNamespace(dek=bytes(32))

    await handler._upload_internal_part_with_semaphore(
        _Client(), "b", "k", "u" * 40, 1, state, bytes(data_size), 1, asyncio.Semaphore(1)
    )

    # During the upload the limiter saw the part's real footprint reserved...
    assert active_during_upload >= expected_reservation
    # ...and it was released afterwards.
    assert concurrency.get_active_memory() == 0

    concurrency.set_memory_limit(64)
