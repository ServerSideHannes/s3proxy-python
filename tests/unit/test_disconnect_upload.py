"""Client disconnect during streaming upload must abort and stop reading."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from starlette.requests import ClientDisconnect

from s3proxy.disconnect import ClientDisconnectError
from s3proxy.handlers.objects.put import PutObjectMixin

MB = 1024 * 1024


class _Client:
    def __init__(self):
        self.credentials = MagicMock(access_key="testkey")
        self.abort_multipart_upload = AsyncMock()

    async def create_multipart_upload(self, *a, **k):
        return {"UploadId": "upload-1"}

    async def upload_part(self, bucket, key, upload_id, part_number, body):
        return {"ETag": '"etag"'}


class _DisconnectRequest:
    def __init__(self, total: int):
        self.headers = {"content-type": "application/octet-stream"}
        self.state = MagicMock(spec=[])
        self.app = MagicMock()
        self._total = total
        self._sent = 0

    async def stream(self):
        chunk = 64 * 1024
        while self._sent < self._total:
            self._sent += chunk
            if self._sent >= 12 * MB:
                raise ClientDisconnect()
            yield b"x" * min(chunk, self._total - (self._sent - chunk))


def _handler() -> PutObjectMixin:
    h = PutObjectMixin.__new__(PutObjectMixin)
    h.keyring = MagicMock()
    h.keyring.key_for.return_value = ("kid1", b"0" * 32)
    return h


@pytest.mark.asyncio
async def test_put_streaming_aborts_multipart_on_client_disconnect():
    request = _DisconnectRequest(12 * MB)
    client = _Client()

    with pytest.raises(ClientDisconnectError):
        await _handler()._put_streaming(
            request,
            client,
            "bucket",
            "key",
            "application/octet-stream",
            decode_chunked=False,
            expected_sha256=None,
        )

    client.abort_multipart_upload.assert_awaited_once_with("bucket", "key", "upload-1")
