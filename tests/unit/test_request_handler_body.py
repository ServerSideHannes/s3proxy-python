"""Request gate: large unsigned-header bodies stream once; sig verified after hash."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import Request

from s3proxy.errors import S3Error
from s3proxy.request_handler import (
    _defer_signature_for_body,
    _handle_proxy_request_impl,
    handle_proxy_request,
)

MB = 1024 * 1024


class TestDeferSignatureForBody:
    def test_defers_large_put_without_payload_hash(self):
        assert _defer_signature_for_body({}, 179 * MB, {}) is True

    def test_no_defer_with_unsigned_payload(self):
        assert (
            _defer_signature_for_body({"x-amz-content-sha256": "UNSIGNED-PAYLOAD"}, 179 * MB, {})
            is False
        )

    def test_no_defer_with_explicit_hash(self):
        assert _defer_signature_for_body({"x-amz-content-sha256": "abc"}, 179 * MB, {}) is False

    def test_no_defer_small_put_without_header(self):
        assert _defer_signature_for_body({}, 4 * MB, {}) is False

    def test_no_defer_presigned_large_put(self):
        query = {
            "X-Amz-Algorithm": ["AWS4-HMAC-SHA256"],
            "X-Amz-Signature": ["abc123"],
        }
        assert _defer_signature_for_body({}, 179 * MB, query) is False


def _make_request(
    *,
    method: str = "PUT",
    content_length: int,
    content_sha: str | None = None,
    query: str = "",
    presigned: bool = False,
) -> MagicMock:
    request = MagicMock(spec=Request)
    request.method = method
    request.url = MagicMock()
    request.url.path = "/bucket/scylla-backup/sst/big-Data.db"
    if presigned:
        request.url.query = (
            "X-Amz-Algorithm=AWS4-HMAC-SHA256"
            "&X-Amz-Credential=key/20260708/us-east-1/s3/aws4_request"
            "&X-Amz-Date=20260708T150228Z"
            "&X-Amz-Expires=900"
            "&X-Amz-SignedHeaders=content-md5%3Bcontent-type%3Bhost"
            "&X-Amz-Signature=deadbeef"
        )
    else:
        request.url.query = query
    headers: dict[str, str] = {
        "content-length": str(content_length),
        "content-type": "application/octet-stream",
    }
    if not presigned:
        headers["authorization"] = (
            "AWS4-HMAC-SHA256 Credential=x, SignedHeaders=host, Signature=sig"
        )
        headers["x-amz-date"] = "20260708T080000Z"
    if content_sha is not None:
        headers["x-amz-content-sha256"] = content_sha
    request.headers = headers
    request.scope = {"raw_path": request.url.path.encode()}
    request.state = MagicMock()
    request.body = AsyncMock(return_value=b"")
    request.stream = MagicMock()
    return request


@pytest.mark.asyncio
async def test_large_put_without_header_defers_signature():
    request = _make_request(content_length=179 * MB)
    verifier = MagicMock()
    verifier.prepare_header_auth = MagicMock(return_value=(MagicMock(), ""))

    with patch("s3proxy.request_handler.RequestDispatcher") as dispatcher_cls:
        dispatcher_cls.return_value.dispatch = AsyncMock(return_value=None)
        await _handle_proxy_request_impl(request, MagicMock(), verifier)

    request.body.assert_not_awaited()
    assert request.state.s3proxy_deferred_sig is True
    verifier.prepare_header_auth.assert_called_once()
    verifier.verify.assert_not_called()


@pytest.mark.asyncio
async def test_large_presigned_put_verifies_without_body_preload():
    """Scylla Manager uses presigned PUTs; must not enter deferred header-auth path."""
    request = _make_request(content_length=179 * MB, presigned=True)
    verifier = MagicMock()
    verifier.verify = MagicMock(return_value=(True, MagicMock(), ""))

    with patch("s3proxy.request_handler.RequestDispatcher") as dispatcher_cls:
        dispatcher_cls.return_value.dispatch = AsyncMock(return_value=None)
        await _handle_proxy_request_impl(request, MagicMock(), verifier)

    request.body.assert_not_awaited()
    verifier.verify.assert_called_once()
    verifier.prepare_header_auth.assert_not_called()


@pytest.mark.asyncio
async def test_large_put_unsigned_payload_verifies_immediately():
    request = _make_request(content_length=179 * MB, content_sha="UNSIGNED-PAYLOAD")
    verifier = MagicMock()
    verifier.verify = MagicMock(return_value=(True, MagicMock(), ""))

    with patch("s3proxy.request_handler.RequestDispatcher") as dispatcher_cls:
        dispatcher_cls.return_value.dispatch = AsyncMock(return_value=None)
        await _handle_proxy_request_impl(request, MagicMock(), verifier)

    request.body.assert_not_awaited()
    verifier.verify.assert_called_once()


@pytest.mark.asyncio
async def test_small_put_without_header_loads_body_once():
    payload = b"x" * (4 * MB)
    request = _make_request(content_length=len(payload))
    request.body = AsyncMock(return_value=payload)

    async def chunks():
        yield payload

    request.stream = MagicMock(side_effect=chunks)
    verifier = MagicMock()
    verifier.verify = MagicMock(return_value=(True, MagicMock(), ""))

    with patch("s3proxy.request_handler.RequestDispatcher") as dispatcher_cls:
        dispatcher_cls.return_value.dispatch = AsyncMock(return_value=None)
        await _handle_proxy_request_impl(request, MagicMock(), verifier)

    request.stream.assert_called_once()
    request.body.assert_not_awaited()
    assert request.state.s3proxy_preloaded_body == payload
    verifier.verify.assert_called_once()


@pytest.mark.asyncio
async def test_deferred_prepare_failure_raises_access_denied():
    request = _make_request(content_length=179 * MB)
    verifier = MagicMock()
    verifier.prepare_header_auth = MagicMock(return_value=(None, "Unknown access key"))

    with pytest.raises(S3Error) as exc:
        await _handle_proxy_request_impl(request, MagicMock(), verifier)
    assert exc.value.status_code == 403
    request.body.assert_not_awaited()


@pytest.mark.asyncio
async def test_impl_error_releases_memory_reservation():
    import s3proxy.concurrency as concurrency_module
    import s3proxy.request_handler as request_handler_module

    request = MagicMock(spec=Request)
    request.method = "PUT"
    request.url = MagicMock()
    request.url.path = "/bucket/key"
    request.url.query = ""
    request.headers = {"content-length": str(179 * MB)}
    request.scope = {"raw_path": b"/bucket/key"}
    request.client = MagicMock()
    request.client.host = "10.0.0.1"
    request.body = AsyncMock()

    with (
        patch.object(concurrency_module, "try_acquire_memory", new_callable=AsyncMock) as acquire,
        patch.object(concurrency_module, "release_memory", new_callable=AsyncMock) as release,
        patch.object(
            request_handler_module, "_handle_proxy_request_impl", new_callable=AsyncMock
        ) as impl,
    ):
        acquire.return_value = 32 * MB
        impl.side_effect = S3Error.access_denied("bad sig")
        with pytest.raises(S3Error):
            await handle_proxy_request(request, MagicMock(), MagicMock())

    release.assert_awaited_once_with(32 * MB)
