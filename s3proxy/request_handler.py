"""Request handling with signature verification and concurrency control."""

from __future__ import annotations

import hashlib
import os
import time
from urllib.parse import parse_qs

import structlog
from botocore.exceptions import ClientError
from fastapi import HTTPException, Request
from fastapi.responses import PlainTextResponse, StreamingResponse
from structlog.stdlib import BoundLogger

from . import concurrency, crypto
from .client import ParsedRequest, SigV4Verifier
from .dashboard import record_request
from .errors import S3Error, raise_for_client_error, raise_for_exception
from .handlers import S3ProxyHandler
from .keyring import UnknownKidError
from .metrics import (
    REQUEST_COUNT,
    REQUEST_DURATION,
    REQUESTS_IN_FLIGHT,
    get_operation_name,
)
from .request_context import bind_request, clear_request, get_request_context
from .routing import RequestDispatcher
from .signature import verify_payload_hash
from .streaming.response import OwnedStreamingResponse

pod_name = os.environ.get("HOSTNAME", "unknown")
logger: BoundLogger = structlog.get_logger(__name__).bind(pod=pod_name)


async def _record_request_safely(*args):
    try:
        await record_request(*args)
    except Exception as error:
        logger.warning("REQUEST_METRICS_FAILED", error=str(error))


def _is_dashboard_path(request: Request, path: str) -> bool:
    """True if the path targets the dashboard (so it's excluded from stats).

    The dashboard router is mounted at settings.dashboard_path; requests there (including a
    bare prefix with no trailing slash that misses the router and falls through to
    the S3 catch-all) must not be recorded as proxied S3 traffic.
    """
    settings = getattr(request.app.state, "settings", None)
    if settings is None or getattr(settings, "dashboard_ui", False) is not True:
        return False
    prefix = getattr(settings, "dashboard_path", "")
    if not isinstance(prefix, str) or not prefix:
        return False
    prefix = prefix.rstrip("/")
    return path == prefix or path.startswith(prefix + "/")


async def _release_after_stream(iterator, reserved: int):
    """Wrap a streaming body so its memory reservation is released only after the
    stream is fully sent (or the client disconnects), not when the handler returns.
    """
    try:
        async for chunk in iterator:
            yield chunk
    finally:
        await concurrency.release_memory(reserved)


def _is_presigned_request(query: dict[str, list[str]]) -> bool:
    """Presigned URLs sign UNSIGNED-PAYLOAD; body is not needed for verification."""
    return "X-Amz-Signature" in query or "Signature" in query


def _needs_body_for_signature(headers: dict[str, str], query: dict[str, list[str]]) -> bool:
    """Body is needed only when x-amz-content-sha256 is absent (header auth).

    The verifier uses that header as the payload hash verbatim and only rehashes
    the body as a fallback when it is missing. Presigned URLs always use
    UNSIGNED-PAYLOAD in the canonical request, so the body is never required.
    """
    if _is_presigned_request(query):
        return False
    return headers.get("x-amz-content-sha256", "") == ""


def _parse_content_length(headers: dict[str, str]) -> int:
    try:
        return int(headers.get("content-length", "0"))
    except ValueError:
        return 0


def _defer_signature_for_body(
    headers: dict[str, str], content_length: int, query: dict[str, list[str]]
) -> bool:
    """Large header-auth bodies without x-amz-content-sha256 are hashed while streaming."""
    return _needs_body_for_signature(headers, query) and content_length > crypto.MAX_BUFFER_SIZE


def _signature_path(request: Request) -> str:
    raw_path = request.scope.get("raw_path")
    if raw_path:
        sig_path = raw_path.decode("utf-8", errors="replace")
        if "?" in sig_path:
            sig_path = sig_path.split("?", 1)[0]
        return sig_path
    return request.url.path


async def handle_proxy_request(
    request: Request,
    handler: S3ProxyHandler,
    verifier: SigV4Verifier,
) -> PlainTextResponse | None:
    """Parse, verify, and route incoming proxy request.

    This is the main entry point for all proxied S3 requests. It:
    1. Acquires memory reservation for concurrency control
    2. Verifies the request signature
    3. Routes to the appropriate handler
    4. Releases memory on completion

    Args:
        request: The incoming FastAPI request.
        handler: The S3ProxyHandler instance.
        verifier: The signature verification instance.

    Returns:
        The response from the handler.

    Raises:
        S3Error: For authentication failures or S3-compatible errors.
    """
    # Track metrics
    method = request.method
    path = request.url.path
    query = str(request.url.query)
    operation = get_operation_name(method, path, query)
    start_time = time.perf_counter()
    status_code = 200

    REQUESTS_IN_FLIGHT.labels(method=method).inc()

    bind_request(method=method, path=path, query=query, content_length=None)

    # Check memory limit BEFORE reading body data - reject if at capacity
    reserved_memory = 0
    needs_limit = method in ("PUT", "POST", "GET") and not request.headers.get("x-amz-copy-source")
    memory_limit = concurrency.get_memory_limit()

    response = None
    try:
        if memory_limit > 0 and needs_limit:
            try:
                content_length = int(request.headers.get("content-length", "0"))
            except ValueError:
                content_length = 0
            bind_request(method=method, path=path, query=query, content_length=content_length)
            memory_needed = concurrency.estimate_memory_footprint(method, content_length)
            if method in ("PUT", "POST") and "content-length" not in request.headers:
                memory_needed = 4 * crypto.MAX_BUFFER_SIZE

            logger.info(
                "REQUEST_ARRIVED - attempting to acquire memory",
                memory_needed_mb=round(memory_needed / 1024 / 1024, 2),
                active_mb=round(concurrency.get_active_memory() / 1024 / 1024, 2),
                limit_mb=round(memory_limit / 1024 / 1024, 2),
                method=method,
                path=path,
                content_length=content_length,
            )
            reserved_memory = await concurrency.try_acquire_memory(memory_needed)
            logger.info(
                "MEMORY_RESERVED",
                reserved_mb=round(reserved_memory / 1024 / 1024, 2),
                active_mb=round(concurrency.get_active_memory() / 1024 / 1024, 2),
                limit_mb=round(memory_limit / 1024 / 1024, 2),
                method=method,
                path=path,
            )

        response = await _handle_proxy_request_impl(request, handler, verifier)
        if response is not None:
            status_code = response.status_code
        # A StreamingResponse sends its body AFTER this handler returns. Releasing
        # the reservation in the finally below frees it before a byte is sent,
        # leaving the stream ungoverned -- and each concurrent stream holds ~one
        # decrypted frame in the transport send buffer, so a flood of concurrent
        # GETs accumulates frames and OOMs the pod while the limiter reads ~budget.
        # Hold the reservation for the whole stream lifetime so the limiter bounds
        # how many streaming GETs run at once (admission control).
        if isinstance(response, StreamingResponse):
            original = response
            reserved = reserved_memory
            reserved_memory = 0
            stream_status = [status_code]
            cleaned = False

            async def stream():
                try:
                    async for chunk in original.body_iterator:
                        yield chunk
                except BaseException:
                    stream_status[0] = 500
                    raise
                finally:
                    try:
                        if hasattr(original.body_iterator, "aclose"):
                            await original.body_iterator.aclose()
                    finally:
                        await cleanup()

            async def cleanup():
                nonlocal cleaned
                if cleaned:
                    return
                cleaned = True
                try:
                    if isinstance(original, OwnedStreamingResponse) and original.cleanup:
                        await original.cleanup()
                finally:
                    await concurrency.release_memory(reserved)
                    if not _is_dashboard_path(request, path):
                        await _record_request_safely(
                            method,
                            path,
                            operation,
                            stream_status[0],
                            time.perf_counter() - start_time,
                            int(original.headers.get("content-length", "0")),
                            request.client.host if request.client else "",
                        )
                    REQUESTS_IN_FLIGHT.labels(method=method).dec()
                    REQUEST_COUNT.labels(
                        method=method, operation=operation, status=stream_status[0]
                    ).inc()
                    REQUEST_DURATION.labels(method=method, operation=operation).observe(
                        time.perf_counter() - start_time
                    )

            response = OwnedStreamingResponse(
                stream(),
                status_code=original.status_code,
                headers=dict(original.headers),
                background=original.background,
                cleanup=cleanup,
                on_error=lambda: stream_status.__setitem__(0, 500),
            )
        return response
    except HTTPException as e:
        status_code = e.status_code
        if isinstance(e, S3Error):
            logger.warning(
                "REQUEST_S3_ERROR",
                status_code=e.status_code,
                code=e.code,
                message=e.message,
                active_mb=round(concurrency.get_active_memory() / 1024 / 1024, 2),
                **get_request_context(),
            )
        else:
            logger.warning(
                "REQUEST_HTTP_ERROR",
                status_code=e.status_code,
                detail=e.detail,
                active_mb=round(concurrency.get_active_memory() / 1024 / 1024, 2),
                **get_request_context(),
            )
        raise
    except Exception as e:
        status_code = 500
        logger.error(
            "REQUEST_UNHANDLED_EXCEPTION",
            error_type=type(e).__name__,
            error=str(e),
            active_mb=round(concurrency.get_active_memory() / 1024 / 1024, 2),
            **get_request_context(),
            exc_info=True,
        )
        raise
    finally:
        clear_request()
        # Record metrics
        duration = time.perf_counter() - start_time
        if not isinstance(response, StreamingResponse):
            REQUESTS_IN_FLIGHT.labels(method=method).dec()
            REQUEST_COUNT.labels(method=method, operation=operation, status=status_code).inc()
            REQUEST_DURATION.labels(method=method, operation=operation).observe(duration)

        try:
            if method == "GET" and response is not None:
                size = int(response.headers.get("content-length", "0"))
            else:
                size = int(request.headers.get("content-length", "0"))
        except ValueError:
            size = 0
        client_ip = request.client.host if request.client else ""
        # Don't record dashboard requests in the dashboard's own stats. A bare
        # "/dashboard" (no trailing slash) doesn't match the mounted dashboard router and
        # falls through to this S3 catch-all, where it would otherwise be logged
        # as a phantom "dashboard" bucket.
        if not isinstance(response, StreamingResponse) and not _is_dashboard_path(request, path):
            await _record_request_safely(
                method, path, operation, status_code, duration, size, client_ip
            )

        if reserved_memory > 0:
            await concurrency.release_memory(reserved_memory)
            logger.info(
                "MEMORY_RELEASED",
                released_mb=round(reserved_memory / 1024 / 1024, 2),
                active_mb=round(concurrency.get_active_memory() / 1024 / 1024, 2),
                limit_mb=round(memory_limit / 1024 / 1024, 2),
                method=method,
                path=path,
            )


async def _handle_proxy_request_impl(
    request: Request,
    handler: S3ProxyHandler,
    verifier: SigV4Verifier,
) -> PlainTextResponse | None:
    """Internal implementation of handle_proxy_request (protected by memory limit)."""
    headers = {k.lower(): v for k, v in request.headers.items()}
    query = parse_qs(str(request.url.query), keep_blank_values=True)

    content_length = _parse_content_length(headers)
    data_write = (
        request.method == "PUT"
        and not headers.get("x-amz-copy-source")
        and not any(k in query for k in ("tagging", "acl", "lifecycle", "policy", "cors"))
        and "/" in request.url.path.strip("/")
    )
    defer_sig = data_write and _defer_signature_for_body(
        headers,
        content_length if "content-length" in headers else crypto.MAX_BUFFER_SIZE + 1,
        query,
    )

    needs_body = request.method in ("PUT", "POST") and _needs_body_for_signature(headers, query)
    body = b""
    if needs_body and not defer_sig:
        chunks = bytearray()
        async for chunk in request.stream():
            chunks.extend(chunk)
            if len(chunks) > crypto.MAX_BUFFER_SIZE:
                raise S3Error.invalid_request("Control request body exceeds 8 MiB")
        body = bytes(chunks)
        request._body = body
        if body:
            request.state.s3proxy_preloaded_body = body
            logger.debug(
                "body_loaded",
                content_length=content_length,
                body_size=len(body),
                method=request.method,
                path=request.url.path,
            )

    parsed = ParsedRequest(
        method=request.method,
        bucket="",
        key="",
        query_params=query,
        headers=headers,
        body=body,
    )

    sig_path = _signature_path(request)
    if defer_sig:
        auth_header = headers.get("authorization", "")
        if not auth_header.startswith("AWS4-HMAC-SHA256"):
            raise S3Error.access_denied("No AWS signature found")
        verified_creds, error = verifier.prepare_header_auth(parsed, auth_header)
        if not verified_creds:
            raise S3Error.access_denied(error or "Access Denied")
        if error:
            raise S3Error.access_denied(error)
        request.state.s3proxy_deferred_sig = True
        request.state.s3proxy_sig_path = sig_path
        logger.debug(
            "signature_deferred",
            content_length=content_length,
            method=request.method,
            path=request.url.path,
        )
    else:
        valid, verified_creds, error = verifier.verify(parsed, sig_path)
        if not valid or not verified_creds:
            if error and "signature" in error.lower():
                raise S3Error.signature_does_not_match(error)
            raise S3Error.access_denied(error or "No credentials")

    if request.method in ("PUT", "POST", "DELETE") and not data_write:
        if headers.get("x-amz-content-sha256", "").startswith("STREAMING-"):
            raise S3Error.invalid_request("Streaming encoding is only supported for data uploads")
        chunks = bytearray()
        async for chunk in request.stream():
            chunks.extend(chunk)
            if len(chunks) > crypto.MAX_BUFFER_SIZE:
                raise S3Error.invalid_request("Control request body exceeds 8 MiB")
        request._body = bytes(chunks)
        verify_payload_hash(request, hashlib.sha256(chunks).hexdigest())

    dispatcher = RequestDispatcher(handler)
    try:
        return await dispatcher.dispatch(request, verified_creds)
    except HTTPException, S3Error:
        raise
    except UnknownKidError as e:
        logger.warning("Cannot decrypt object: key not configured", kid=e.kid)
        raise S3Error.key_not_configured(e.kid) from None
    except ClientError as e:
        logger.error("Request failed with ClientError", error=str(e), exc_info=True)
        raise_for_client_error(e)
    except Exception as e:
        logger.error("Request failed", error=str(e), exc_info=True)
        raise_for_exception(e)
