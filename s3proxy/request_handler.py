"""Request handling with signature verification and concurrency control."""

from __future__ import annotations

import os
import time
from urllib.parse import parse_qs

import structlog
from botocore.exceptions import ClientError
from fastapi import HTTPException, Request
from fastapi.responses import PlainTextResponse
from structlog.stdlib import BoundLogger

from . import concurrency, crypto
from .admin.collectors import record_request
from .errors import S3Error, raise_for_client_error, raise_for_exception
from .handlers import S3ProxyHandler
from .metrics import (
    REQUEST_COUNT,
    REQUEST_DURATION,
    REQUESTS_IN_FLIGHT,
    get_operation_name,
)
from .routing import RequestDispatcher
from .s3client import ParsedRequest, SigV4Verifier

pod_name = os.environ.get("HOSTNAME", "unknown")
logger: BoundLogger = structlog.get_logger(__name__).bind(pod=pod_name)

# Signature verification constants
UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD"
STREAMING_PAYLOAD_PREFIX = "STREAMING-"


def _needs_body_for_signature(headers: dict[str, str], max_size: int) -> bool:
    """Check if body is needed for signature verification.

    Returns False for unsigned payloads, streaming signatures, or large bodies.
    """
    content_sha = headers.get("x-amz-content-sha256", "")
    if content_sha == UNSIGNED_PAYLOAD or content_sha.startswith(STREAMING_PAYLOAD_PREFIX):
        return False

    content_length = headers.get("content-length", "0")
    try:
        if int(content_length) > max_size:
            return False
    except ValueError:
        pass

    return True


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

    # Check memory limit BEFORE reading body data - reject if at capacity
    reserved_memory = 0
    needs_limit = method in ("PUT", "POST", "GET")
    memory_limit = concurrency.get_memory_limit()

    if memory_limit > 0 and needs_limit:
        try:
            content_length = int(request.headers.get("content-length", "0"))
        except ValueError:
            content_length = 0
        memory_needed = concurrency.estimate_memory_footprint(method, content_length)

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

    try:
        response = await _handle_proxy_request_impl(request, handler, verifier)
        if response is not None:
            status_code = response.status_code
        return response
    except HTTPException as e:
        status_code = e.status_code
        raise
    except Exception:
        status_code = 500
        raise
    finally:
        # Record metrics
        duration = time.perf_counter() - start_time
        REQUESTS_IN_FLIGHT.labels(method=method).dec()
        REQUEST_COUNT.labels(method=method, operation=operation, status=status_code).inc()
        REQUEST_DURATION.labels(method=method, operation=operation).observe(duration)
        record_request(
            method, path, operation, status_code, duration,
            int(request.headers.get("content-length", "0") or "0"),
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

    needs_body = request.method in ("PUT", "POST") and _needs_body_for_signature(
        headers, crypto.STREAMING_THRESHOLD
    )
    content_length = headers.get("content-length", "0")
    body = await request.body() if needs_body else b""
    if needs_body and len(body) > 0:
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

    raw_path = request.scope.get("raw_path")
    if raw_path:
        sig_path = raw_path.decode("utf-8", errors="replace")
        if "?" in sig_path:
            sig_path = sig_path.split("?", 1)[0]
    else:
        sig_path = request.url.path
    valid, verified_creds, error = verifier.verify(parsed, sig_path)
    if not valid or not verified_creds:
        if error and "signature" in error.lower():
            raise S3Error.signature_does_not_match(error)
        raise S3Error.access_denied(error or "No credentials")

    dispatcher = RequestDispatcher(handler)
    try:
        return await dispatcher.dispatch(request, verified_creds)
    except HTTPException, S3Error:
        raise
    except ClientError as e:
        logger.error("Request failed with ClientError", error=str(e), exc_info=True)
        raise_for_client_error(e)
    except Exception as e:
        logger.error("Request failed", error=str(e), exc_info=True)
        raise_for_exception(e)
