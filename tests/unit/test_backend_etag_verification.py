"""Backend transport integrity: UNSIGNED-PAYLOAD + ciphertext ETag verification.

Payload signing is disabled on the proxy->backend hop (it doubled per-request
upload memory), so the MD5-vs-ETag check in S3Client is the only transport
integrity guarantee there. These tests pin both halves: the client config that
disables signing/checksums, and the verification that replaces them.
"""

import hashlib

import pytest

import s3proxy.client.s3 as client_s3
from s3proxy.client import S3Client
from s3proxy.client.s3 import verify_backend_etag
from s3proxy.errors import BackendIntegrityError, S3Error, raise_for_exception


@pytest.fixture
def s3_client(settings, credentials):
    return S3Client(settings, credentials)


class _StubBackend:
    """Backend stub that returns a fixed or honest (MD5) ETag."""

    def __init__(self, etag: str | None = None):
        self.etag = etag
        self.calls: list[dict] = []

    async def _respond(self, kwargs) -> dict:
        self.calls.append(kwargs)
        if self.etag is not None:
            return {"ETag": self.etag}
        body = kwargs["Body"]
        if not isinstance(body, (bytes, bytearray)):
            body = b"".join([chunk async for chunk in body])
        return {"ETag": f'"{hashlib.md5(body, usedforsecurity=False).hexdigest()}"'}

    async def upload_part(self, **kwargs):
        return await self._respond(kwargs)

    async def put_object(self, **kwargs):
        return await self._respond(kwargs)


class TestBackendClientConfig:
    def test_payload_signing_disabled(self, s3_client):
        assert s3_client._config.s3["payload_signing_enabled"] is False

    def test_flexible_checksums_only_when_required(self, s3_client):
        assert s3_client._config.request_checksum_calculation == "when_required"


class TestUploadPartVerification:
    async def test_matching_etag_passes(self, s3_client):
        s3_client._cached_client = _StubBackend()
        result = await s3_client.upload_part("b", "k", "uid", 1, b"ciphertext-bytes")
        assert result["ETag"].strip('"') == hashlib.md5(b"ciphertext-bytes").hexdigest()

    async def test_mismatched_etag_raises(self, s3_client):
        s3_client._cached_client = _StubBackend(etag='"' + "0" * 32 + '"')
        with pytest.raises(BackendIntegrityError, match="part 3"):
            await s3_client.upload_part("b", "k", "uid", 3, b"ciphertext-bytes")

    async def test_non_md5_etag_skips_verification(self, s3_client, monkeypatch):
        monkeypatch.setattr(client_s3, "_etag_not_md5_logged", False)
        s3_client._cached_client = _StubBackend(etag='"abc123-2"')
        result = await s3_client.upload_part("b", "k", "uid", 1, b"whatever")
        assert result["ETag"] == '"abc123-2"'

    async def test_streaming_body_verified_via_running_md5(self, s3_client):
        payload = b"frame-one" * 1000

        class _StreamBody:
            def __len__(self):
                return len(payload)

            def __aiter__(self):
                async def gen():
                    yield payload

                return gen()

            def ciphertext_md5_hexdigest(self):
                return hashlib.md5(payload, usedforsecurity=False).hexdigest()

        s3_client._cached_client = _StubBackend()
        result = await s3_client.upload_part("b", "k", "uid", 1, _StreamBody())
        assert result["ETag"].strip('"') == hashlib.md5(payload).hexdigest()

        class _CorruptStreamBody(_StreamBody):
            def ciphertext_md5_hexdigest(self):
                return "f" * 32

        with pytest.raises(BackendIntegrityError):
            await s3_client.upload_part("b", "k", "uid", 2, _CorruptStreamBody())


class TestPutObjectVerification:
    async def test_matching_etag_passes(self, s3_client):
        s3_client._cached_client = _StubBackend()
        await s3_client.put_object("b", "k", b"object-bytes")

    async def test_mismatched_etag_raises(self, s3_client):
        s3_client._cached_client = _StubBackend(etag='"' + "0" * 32 + '"')
        with pytest.raises(BackendIntegrityError):
            await s3_client.put_object("b", "k", b"object-bytes")


class TestVerifyBackendEtag:
    def test_none_expected_md5_is_noop(self):
        verify_backend_etag("upload_part", "b", "k", '"' + "0" * 32 + '"', None)

    def test_missing_etag_skips(self):
        verify_backend_etag("upload_part", "b", "k", None, "f" * 32)

    def test_uppercase_or_quoted_etag_normalized(self):
        md5 = hashlib.md5(b"x", usedforsecurity=False).hexdigest()
        verify_backend_etag("upload_part", "b", "k", f'"{md5}"', md5)

    def test_maps_to_retryable_internal_error(self):
        with pytest.raises(S3Error) as exc_info:
            raise_for_exception(BackendIntegrityError("etag mismatch"))
        assert exc_info.value.status_code == 500
        assert exc_info.value.code == "InternalError"
