"""Failure-path and backend round-trip regressions for the shared services."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from cryptography.exceptions import InvalidTag
from starlette.requests import ClientDisconnect

from s3proxy import crypto
from s3proxy.client import SigV4Verifier
from s3proxy.client.pool import S3ClientPool
from s3proxy.errors import S3Error
from s3proxy.state import MultipartMetadata, PartMetadata
from s3proxy.streaming.authenticated import decrypt_to_file
from s3proxy.streaming.chunked import decode_aws_chunked_stream
from s3proxy.streaming.frames import plaintext_frames
from s3proxy.streaming.response import OwnedStreamingResponse
from tests.conftest import MockS3Response
from tests.unit.test_generation_writes import request


async def test_pool_reuses_and_isolates_credentials(settings, credentials, monkeypatch):
    instances = []

    class Client:
        def __init__(self, *args):
            self.closed = False
            instances.append(self)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            self.closed = True

    monkeypatch.setattr("s3proxy.client.pool.S3Client", Client)
    pool = S3ClientPool(settings, max_clients=1)
    async with pool.acquire(credentials) as first:
        async with pool.acquire(credentials) as second:
            assert first is second
        other = SimpleNamespace(access_key="other", secret_key="secret", region="us-east-1")
        with pytest.raises(S3Error):
            async with pool.acquire(other):
                pass
        closing = asyncio.create_task(pool.close())
        await asyncio.sleep(0)
        assert not closing.done()
        assert not first.closed
    await asyncio.wait_for(closing, 1)
    assert first.closed and len(instances) == 1


async def test_response_closes_resources_when_headers_fail():
    cleanup = AsyncMock()
    failed = []

    async def body():
        pytest.fail("The body must not start when headers fail")
        yield b""

    async def send(message):
        raise OSError("disconnected")

    response = OwnedStreamingResponse(body(), cleanup=cleanup, on_error=lambda: failed.append(True))
    with pytest.raises(ClientDisconnect):
        await response({"type": "http", "asgi": {"spec_version": "2.4"}}, AsyncMock(), send)
    cleanup.assert_awaited_once()
    assert failed == [True]


async def test_contiguous_frames_use_one_backend_request(mock_s3):
    dek = crypto.generate_dek()
    plaintext = [bytes([n]) * 32768 for n in range(8)]
    ciphertext = [crypto.encrypt(p, dek) for p in plaintext]
    await mock_s3.put_object("bucket", "key", b"".join(ciphertext))
    meta = MultipartMetadata(
        parts=[
            PartMetadata(i, len(p), len(c), "")
            for i, (p, c) in enumerate(zip(plaintext, ciphertext, strict=True), 1)
        ]
    )
    result = b"".join([p async for p in plaintext_frames(mock_s3, "bucket", "key", meta, dek)])
    assert result == b"".join(plaintext)
    assert len([c for c in mock_s3.call_history if c[0] == "get_object"]) == 1


async def test_truncated_range_resumes_at_unpublished_frame(mock_s3, monkeypatch):
    dek = crypto.generate_dek()
    data = [b"a" * 1024, b"b" * 1024]
    seals = [crypto.encrypt(p, dek) for p in data]
    await mock_s3.put_object("bucket", "key", b"".join(seals))
    original = mock_s3.get_object
    calls = []

    async def truncated(bucket, key, byte_range, **kwargs):
        calls.append(byte_range)
        response = await original(bucket, key, byte_range, **kwargs)
        if len(calls) == 1:
            response["Body"] = MockS3Response(seals[0] + seals[1][:5])
        return response

    monkeypatch.setattr(mock_s3, "get_object", truncated)
    monkeypatch.setattr("s3proxy.handlers.base.SOURCE_READ_BACKOFF_SEC", 0)
    meta = MultipartMetadata(
        parts=[
            PartMetadata(i, len(p), len(c), "")
            for i, (p, c) in enumerate(zip(data, seals, strict=True), 1)
        ]
    )
    assert b"".join(
        [p async for p in plaintext_frames(mock_s3, "bucket", "key", meta, dek)]
    ) == b"".join(data)
    assert calls[1].startswith(f"bytes={len(seals[0])}-")


async def test_large_legacy_seal_is_authenticated_before_plaintext(mock_s3):
    dek = crypto.generate_dek()
    data = b"legacy" * (2 * 1024**2)
    ciphertext = crypto.encrypt(data, dek)
    await mock_s3.put_object("bucket", "key", ciphertext)
    meta = MultipartMetadata(parts=[PartMetadata(1, len(data), len(ciphertext), "")])
    assert (
        b"".join([p async for p in plaintext_frames(mock_s3, "bucket", "key", meta, dek)]) == data
    )
    with pytest.raises(InvalidTag):
        await decrypt_to_file(MockS3Response(ciphertext[:-1] + bytes([ciphertext[-1] ^ 1])), dek)


# Public AWS SigV4 test vectors, including the signature on the terminal chunk:
# https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-streaming.html
@pytest.mark.parametrize("tamper", [False, True])
async def test_aws_published_chunk_signature_vector(credentials, tamper):
    signatures = [
        "ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648",
        "0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497",
        "b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9",
    ]
    body = b"".join(
        f"{n:x};chunk-signature={sig}\r\n".encode() + b"a" * n + b"\r\n"
        for n, sig in zip([65536, 1024, 0], signatures, strict=True)
    )
    if tamper:
        body = body.replace(b"aaaa", b"baaa", 1)
    req = request(
        body=body,
        headers={
            "x-amz-content-sha256": "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
            "x-amz-date": "20130524T000000Z",
            "x-amz-decoded-content-length": "66560",
            "authorization": "AWS4-HMAC-SHA256 "
            "Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,"
            "SignedHeaders=host;x-amz-date,"
            "Signature=4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9",
        },
    )
    req.scope["app"] = SimpleNamespace(
        state=SimpleNamespace(
            verifier=SigV4Verifier({credentials.access_key: credentials.secret_key})
        )
    )
    if tamper:
        with pytest.raises(S3Error):
            _ = [part async for part in decode_aws_chunked_stream(req)]
    else:
        assert b"".join([part async for part in decode_aws_chunked_stream(req)]) == b"a" * 66560
