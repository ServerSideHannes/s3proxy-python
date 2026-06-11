"""End-to-end tests for per-access-key encryption keys.

Drives the real handler against the in-memory MockS3Client to verify:
- objects are encrypted under the calling login's key,
- the access key is stored on the object (kid) and used for decryption,
- a different login decrypts correctly as long as its KEK is configured,
- the access key never leaks back to clients as user metadata.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from s3proxy.client import S3Credentials
from s3proxy.config import Settings
from s3proxy.handlers import S3ProxyHandler
from s3proxy.state import MultipartStateManager

ACME = "AKIA-ACME"
GLOBEX = "AKIA-GLOBEX"


@pytest.fixture
def settings():
    return Settings(
        host="http://localhost:9000",
        no_tls=True,
        credentials=[
            {"access_key": ACME, "secret_key": "acme-sec", "kek": "acme-kek"},
            {"access_key": GLOBEX, "secret_key": "globex-sec", "kek": "globex-kek"},
        ],
    )


def _creds(access_key: str) -> S3Credentials:
    return S3Credentials(access_key=access_key, secret_key="x", region="us-east-1")


def _handler(settings, mock_s3, access_key):
    handler = S3ProxyHandler(settings, settings.credentials_store, MultipartStateManager())
    mock_s3.credentials = _creds(access_key)
    handler._client = MagicMock(return_value=mock_s3)
    return handler


def _put_request(path: str, body: bytes):
    req = MagicMock()
    req.url.path = path
    req.headers = {"content-length": str(len(body)), "content-type": "text/plain"}
    req.body = AsyncMock(return_value=body)
    return req


def _get_request(path: str):
    req = MagicMock()
    req.url.path = path
    req.headers = {}
    return req


async def _read(response) -> bytes:
    if hasattr(response, "body_iterator"):
        return b"".join([c async for c in response.body_iterator])
    return response.body


class TestPerCredentialRoundtrip:
    @pytest.mark.asyncio
    async def test_object_encrypted_under_calling_login(self, settings, mock_s3):
        handler = _handler(settings, mock_s3, ACME)
        await mock_s3.create_bucket("data")

        body = b"acme payload"
        await handler.handle_put_object(_put_request("/data/report.txt", body), _creds(ACME))

        obj = mock_s3.objects[mock_s3._key("data", "report.txt")]
        assert obj["Metadata"][settings.kidtag_name] == ACME

        resp = await handler.handle_get_object(_get_request("/data/report.txt"), _creds(ACME))
        assert await _read(resp) == body

    @pytest.mark.asyncio
    async def test_different_logins_get_different_keys(self, settings, mock_s3):
        # ACME writes one object, GLOBEX writes another - same bucket, different keys.
        await mock_s3.create_bucket("shared")

        h_acme = _handler(settings, mock_s3, ACME)
        await h_acme.handle_put_object(_put_request("/shared/a.txt", b"A"), _creds(ACME))

        h_globex = _handler(settings, mock_s3, GLOBEX)
        await h_globex.handle_put_object(_put_request("/shared/g.txt", b"G"), _creds(GLOBEX))

        assert (
            mock_s3.objects[mock_s3._key("shared", "a.txt")]["Metadata"][settings.kidtag_name]
            == ACME
        )
        assert (
            mock_s3.objects[mock_s3._key("shared", "g.txt")]["Metadata"][settings.kidtag_name]
            == GLOBEX
        )

    @pytest.mark.asyncio
    async def test_decrypt_uses_stored_kid_not_caller(self, settings, mock_s3):
        """ACME writes; a different login (GLOBEX) reads. Decryption uses the
        stored ACME kid, so it works as long as ACME's KEK is configured."""
        await mock_s3.create_bucket("shared")
        body = b"written by acme"

        h_acme = _handler(settings, mock_s3, ACME)
        await h_acme.handle_put_object(_put_request("/shared/f.txt", body), _creds(ACME))

        # GLOBEX reads the same object (its handler shares the same keyring).
        h_globex = _handler(settings, mock_s3, GLOBEX)
        resp = await h_globex.handle_get_object(_get_request("/shared/f.txt"), _creds(GLOBEX))
        assert await _read(resp) == body

    @pytest.mark.asyncio
    async def test_unknown_access_key_rejected_on_write(self, settings, mock_s3):
        handler = _handler(settings, mock_s3, "AKIA-NOT-CONFIGURED")
        await mock_s3.create_bucket("data")
        with pytest.raises(KeyError):
            await handler.handle_put_object(
                _put_request("/data/x.txt", b"data"), _creds("AKIA-NOT-CONFIGURED")
            )


class TestCopyPerCredential:
    @pytest.mark.asyncio
    async def test_copy_object_decrypts_source_kid_reencrypts_dest(self, settings, mock_s3):
        """CopyObject must decrypt the source via its stored kid and re-encrypt
        under the calling credential. Regression: the single-object source path
        previously dropped the kid and crashed on decrypt."""
        await mock_s3.create_bucket("data")
        body = b"copy me across credentials"

        # ACME writes the source.
        h_acme = _handler(settings, mock_s3, ACME)
        await h_acme.handle_put_object(_put_request("/data/src.txt", body), _creds(ACME))

        # GLOBEX copies it -> re-encrypted under GLOBEX's kid.
        h_globex = _handler(settings, mock_s3, GLOBEX)
        copy_req = MagicMock()
        copy_req.url.path = "/data/dst.txt"
        copy_req.headers = {"x-amz-copy-source": "/data/src.txt"}
        await h_globex.handle_copy_object(copy_req, _creds(GLOBEX))

        assert (
            mock_s3.objects[mock_s3._key("data", "dst.txt")]["Metadata"][settings.kidtag_name]
            == GLOBEX
        )

        resp = await h_globex.handle_get_object(_get_request("/data/dst.txt"), _creds(GLOBEX))
        assert await _read(resp) == body


class TestMultipartPerCredential:
    @pytest.mark.asyncio
    async def test_streaming_upload_roundtrip(self, settings, mock_s3):
        handler = _handler(settings, mock_s3, ACME)
        await mock_s3.create_bucket("data")

        body = b"m" * (12 * 1024 * 1024)  # > MAX_BUFFER_SIZE -> multiple parts

        req = MagicMock()
        req.headers = {}

        async def stream():
            for i in range(0, len(body), 1024 * 1024):
                yield body[i : i + 1024 * 1024]

        req.stream = stream

        await handler._put_streaming(req, mock_s3, "data", "big.bin", "application/octet-stream")

        from s3proxy.state import load_multipart_metadata

        meta = await load_multipart_metadata(mock_s3, "data", "big.bin")
        assert meta is not None
        assert meta.kid == ACME

        resp = await handler.handle_get_object(_get_request("/data/big.bin"), _creds(ACME))
        assert await _read(resp) == body


class TestKidDoesNotLeak:
    @pytest.mark.asyncio
    async def test_get_response_has_no_kid_header(self, settings, mock_s3):
        handler = _handler(settings, mock_s3, ACME)
        await mock_s3.create_bucket("data")

        await handler.handle_put_object(_put_request("/data/leak.txt", b"data"), _creds(ACME))
        resp = await handler.handle_get_object(_get_request("/data/leak.txt"), _creds(ACME))

        leaked = [h for h in resp.headers if "isec" in h.lower()]
        assert not leaked, f"internal tags leaked to client: {leaked}"
