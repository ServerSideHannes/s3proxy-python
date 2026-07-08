"""Regression: Scylla SST PUT without x-amz-content-sha256 must not double-buffer.

Production OOM (2026-07-08): request_handler loaded the full ~179MB body for SigV4,
then put.py streamed request.stream() from Starlette's cache — ~350MB per request
while the governor saw ~88MB. This test drives the real PUT streaming handler with
deferred signature (no preloaded body) and asserts peak Python heap stays bounded.
"""

from __future__ import annotations

import hashlib
import tracemalloc
from unittest.mock import AsyncMock, MagicMock

import pytest

from s3proxy import crypto
from s3proxy.errors import S3Error
from s3proxy.handlers.objects.put import PutObjectMixin

MB = 1024 * 1024
SCYLLA_PUT_BYTES = 179 * MB


class _Client:
    def __init__(self):
        self.credentials = MagicMock(access_key="testkey")
        self.abort_multipart_upload = AsyncMock()

    async def create_multipart_upload(self, *a, **k):
        return {"UploadId": "upload-1"}

    async def upload_part(self, bucket, key, upload_id, part_number, body):
        sent = bytes(body)
        return {"ETag": hashlib.md5(sent).hexdigest()}

    async def complete_multipart_upload(self, *a, **k):
        return {}


class _StreamRequest:
    """Simulates a large PUT after deferred signature (body never preloaded)."""

    def __init__(self, total: int, chunk: int = 64 * 1024):
        self._total = total
        self._chunk = chunk
        self.headers = {
            "content-type": "application/octet-stream",
            "content-length": str(total),
        }
        self.url = MagicMock()
        self.url.path = "/bucket/backup/sst/me-big-Data.db"
        self.state = MagicMock(spec=[])
        self.state.s3proxy_deferred_sig = True
        self.app = MagicMock()

    async def is_disconnected(self) -> bool:
        return False

    async def stream(self):
        for i in range(0, self._total, self._chunk):
            yield b"s" * min(self._chunk, self._total - i)


def _handler() -> PutObjectMixin:
    h = PutObjectMixin.__new__(PutObjectMixin)
    h.keyring = MagicMock()
    h.keyring.key_for.return_value = ("kid1", b"0" * 32)
    return h


@pytest.mark.asyncio
async def test_scylla_sized_deferred_put_peak_is_bounded_not_double_body():
    """Peak must stay near streaming_upload_peak, not 2x Content-Length."""
    request = _StreamRequest(SCYLLA_PUT_BYTES)
    client = _Client()

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "s3proxy.handlers.objects.put.verify_deferred_payload_hash",
            lambda *a, **k: None,
        )
        mp.setattr(
            "s3proxy.handlers.objects.put.save_multipart_metadata",
            AsyncMock(),
        )
        tracemalloc.start()
        base = tracemalloc.get_traced_memory()[0]
        await _handler()._put_streaming(
            request,
            client,
            "bucket",
            "key",
            "application/octet-stream",
            decode_chunked=False,
            expected_sha256=None,
        )
        peak = tracemalloc.get_traced_memory()[1]
        tracemalloc.stop()

    measured_mb = (peak - base) / MB
    honest_peak_mb = crypto.streaming_upload_peak(SCYLLA_PUT_BYTES) / MB
    double_body_mb = 2 * SCYLLA_PUT_BYTES / MB

    # Old bug held ~2x body (~358MB) for this payload; streaming peak is ~56MB.
    assert measured_mb < honest_peak_mb * 1.5, (
        f"peak {measured_mb:.1f}MB exceeds 1.5x streaming_upload_peak "
        f"({honest_peak_mb:.1f}MB) — likely double-buffer regression"
    )
    assert measured_mb < double_body_mb / 4, (
        f"peak {measured_mb:.1f}MB is too close to old 2x-body failure ({double_body_mb:.0f}MB)"
    )


@pytest.mark.asyncio
async def test_deferred_sig_failure_aborts_multipart():
    """Invalid signature after streaming must not complete the upload."""
    request = _StreamRequest(4 * MB)
    request.app.state.verifier = MagicMock()
    client = _Client()

    def _bad_verify(*a, **k):
        raise S3Error.signature_does_not_match("Signature mismatch")

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "s3proxy.handlers.objects.put.verify_deferred_payload_hash",
            _bad_verify,
        )
        with pytest.raises(S3Error):
            await _handler()._put_streaming(
                request,
                client,
                "bucket",
                "key",
                "application/octet-stream",
                decode_chunked=False,
                expected_sha256=None,
            )

    client.abort_multipart_upload.assert_awaited_once()
