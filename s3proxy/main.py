"""Main entry point for S3Proxy server."""

import argparse
import asyncio
import logging
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import parse_qs

import structlog
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import PlainTextResponse

from .config import Settings
from .handlers import S3ProxyHandler
from .handlers.base import close_http_client
from .multipart import MultipartStateManager, close_redis, init_redis
from .s3client import ParsedRequest, S3ClientPool, S3Credentials, SigV4Verifier

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

# Configure stdlib logging for structlog
logging.basicConfig(format="%(message)s", stream=sys.stdout, level=logging.INFO)

structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
)

logger = structlog.get_logger()

# ============================================================================
# Constants - Replace magic strings
# ============================================================================
QUERY_UPLOADS = "uploads"
QUERY_UPLOAD_ID = "uploadId"
QUERY_PART_NUMBER = "partNumber"
QUERY_LIST_TYPE = "list-type"
QUERY_LOCATION = "location"
QUERY_DELETE = "delete"
QUERY_TAGGING = "tagging"

# Headers
HEADER_COPY_SOURCE = "x-amz-copy-source"

# HTTP methods
METHOD_GET = "GET"
METHOD_PUT = "PUT"
METHOD_POST = "POST"
METHOD_DELETE = "DELETE"
METHOD_HEAD = "HEAD"

# Content hash values that don't require body for signature verification
UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD"
STREAMING_PAYLOAD_PREFIX = "STREAMING-"


# ============================================================================
# Helper Functions
# ============================================================================
def load_credentials() -> dict[str, str]:
    """Load AWS credentials from environment variables.

    Returns:
        Dictionary mapping access_key -> secret_key
    """
    credentials_store: dict[str, str] = {}
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    if access_key and secret_key:
        credentials_store[access_key] = secret_key
    return credentials_store


def _is_bucket_only_path(path: str) -> bool:
    """Check if path is bucket-only (no object key)."""
    stripped = path.strip("/")
    return "/" not in stripped and bool(stripped)


def _needs_body_for_signature(headers: dict[str, str]) -> bool:
    """Check if body is needed for signature verification.

    Returns False if x-amz-content-sha256 indicates unsigned/streaming payload.
    """
    content_sha = headers.get("x-amz-content-sha256", "")
    return content_sha != UNSIGNED_PAYLOAD and not content_sha.startswith(STREAMING_PAYLOAD_PREFIX)


# ============================================================================
# Lifespan Management
# ============================================================================
def create_lifespan(settings: Settings) -> "AsyncIterator[None]":
    """Create lifespan context manager for FastAPI app.

    Args:
        settings: Application settings

    Returns:
        Async context manager for app lifespan
    """
    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> "AsyncIterator[None]":
        logger.info("Starting", endpoint=settings.s3_endpoint, port=settings.port)
        # Initialize Redis connection
        await init_redis(settings.redis_url)
        yield
        # Close Redis connection
        await close_redis()
        # Close all S3 client pools
        for pool in list(S3ClientPool._instances.values()):
            await pool.close()
        S3ClientPool._instances.clear()
        # Close shared HTTP client
        await close_http_client()
        logger.info("Shutting down")

    return lifespan


# ============================================================================
# Request Handling
# ============================================================================
async def handle_proxy_request(
    request: Request,
    handler: S3ProxyHandler,
    verifier: SigV4Verifier,
) -> "PlainTextResponse | None":
    """Parse, verify, and route incoming proxy request.

    Args:
        request: FastAPI request object
        handler: S3 proxy handler
        verifier: SigV4 signature verifier

    Returns:
        Response from handler or raises HTTPException
    """
    # Parse request
    headers = {k.lower(): v for k, v in request.headers.items()}
    query = parse_qs(str(request.url.query), keep_blank_values=True)

    # Only read body if needed for signature verification
    # For UNSIGNED-PAYLOAD or streaming signatures, we can skip this
    # and let the handler stream the body directly
    needs_body = request.method in (METHOD_PUT, METHOD_POST) and _needs_body_for_signature(headers)
    body = await request.body() if needs_body else b""

    parsed = ParsedRequest(
        method=request.method,
        bucket="",
        key="",
        query_params=query,
        headers=headers,
        body=body,
    )

    # Verify signature
    valid, verified_creds, error = verifier.verify(parsed, request.url.path)
    if not valid or not verified_creds:
        raise HTTPException(403, error or "No credentials")

    # Route to handler
    try:
        return await route_request(request, verified_creds, handler)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Request failed", error=str(e), exc_info=True)
        raise HTTPException(500, str(e)) from e


async def route_request(
    request: Request,
    creds: S3Credentials,
    handler: S3ProxyHandler,
) -> "PlainTextResponse":
    """Route request to appropriate handler.

    Uses early returns pattern for cleaner control flow.
    """
    method = request.method
    query = str(request.url.query)
    path = request.url.path
    headers = {k.lower(): v for k, v in request.headers.items()}

    # Root path - list buckets
    if path.strip("/") == "":
        return await handler.handle_list_buckets(request, creds)

    # Batch delete operation (POST /?delete) - check before other bucket ops
    if QUERY_DELETE in query and method == METHOD_POST:
        return await handler.handle_delete_objects(request, creds)

    # List multipart uploads (GET /?uploads without uploadId)
    if QUERY_UPLOADS in query and QUERY_UPLOAD_ID not in query and method == METHOD_GET:
        return await handler.handle_list_multipart_uploads(request, creds)

    # Create multipart upload (POST /?uploads)
    if QUERY_UPLOADS in query and method == METHOD_POST:
        return await handler.handle_create_multipart_upload(request, creds)

    # Multipart part operations (uploadId in query)
    if QUERY_UPLOAD_ID in query:
        return await _handle_multipart_operation(request, creds, handler, method, query, headers)

    # Bucket-only operations
    if _is_bucket_only_path(path):
        result = await _handle_bucket_operation(request, creds, handler, method, query)
        if result is not None:
            return result

    # List objects (bucket-only GET or explicit list-type)
    if _is_bucket_only_path(path) and method == METHOD_GET:
        # V2 uses list-type=2, V1 uses no list-type or list-type=1
        query_params = parse_qs(query, keep_blank_values=True)
        list_type = query_params.get("list-type", ["1"])[0]
        if list_type == "2":
            return await handler.handle_list_objects(request, creds)
        return await handler.handle_list_objects_v1(request, creds)

    # Copy object (PUT with x-amz-copy-source header)
    if method == METHOD_PUT and HEADER_COPY_SOURCE in headers:
        return await handler.handle_copy_object(request, creds)

    # Standard object operations
    return await _handle_object_operation(request, creds, handler, method, query)


async def _handle_multipart_operation(
    request: Request,
    creds: S3Credentials,
    handler: S3ProxyHandler,
    method: str,
    query: str,
    headers: dict[str, str],
) -> "PlainTextResponse":
    """Handle multipart upload operations."""
    # ListParts: GET with uploadId but no partNumber
    if method == METHOD_GET and QUERY_PART_NUMBER not in query:
        return await handler.handle_list_parts(request, creds)
    if method == METHOD_PUT:
        # UploadPartCopy: PUT with uploadId and x-amz-copy-source
        if HEADER_COPY_SOURCE in headers:
            return await handler.handle_upload_part_copy(request, creds)
        return await handler.handle_upload_part(request, creds)
    if method == METHOD_POST:
        return await handler.handle_complete_multipart_upload(request, creds)
    if method == METHOD_DELETE:
        return await handler.handle_abort_multipart_upload(request, creds)
    return await handler.forward_request(request, creds)


async def _handle_bucket_operation(
    request: Request,
    creds: S3Credentials,
    handler: S3ProxyHandler,
    method: str,
    query: str,
) -> "PlainTextResponse | None":
    """Handle bucket-level operations.

    Returns None if operation should fall through to object handling.
    """
    # GetBucketLocation: GET /?location
    if QUERY_LOCATION in query and method == METHOD_GET:
        return await handler.handle_get_bucket_location(request, creds)

    # Forward other bucket queries like ?versioning to S3
    skip_queries = (QUERY_LIST_TYPE, QUERY_DELETE, QUERY_UPLOADS, QUERY_LOCATION)
    if query and not any(q in query for q in skip_queries):
        return await handler.forward_request(request, creds)

    # Bucket management operations (no query string)
    if not query:
        if method == METHOD_PUT:
            return await handler.handle_create_bucket(request, creds)
        if method == METHOD_DELETE:
            return await handler.handle_delete_bucket(request, creds)
        if method == METHOD_HEAD:
            return await handler.handle_head_bucket(request, creds)

    return None


async def _handle_object_operation(
    request: Request,
    creds: S3Credentials,
    handler: S3ProxyHandler,
    method: str,
    query: str,
) -> "PlainTextResponse":
    """Handle standard object operations."""
    # Object tagging operations
    if QUERY_TAGGING in query:
        if method == METHOD_GET:
            return await handler.handle_get_object_tagging(request, creds)
        if method == METHOD_PUT:
            return await handler.handle_put_object_tagging(request, creds)
        if method == METHOD_DELETE:
            return await handler.handle_delete_object_tagging(request, creds)

    if method == METHOD_GET:
        return await handler.handle_get_object(request, creds)
    if method == METHOD_PUT:
        return await handler.handle_put_object(request, creds)
    if method == METHOD_HEAD:
        return await handler.handle_head_object(request, creds)
    if method == METHOD_DELETE:
        return await handler.handle_delete_object(request, creds)
    return await handler.forward_request(request, creds)


# ============================================================================
# Throttling Middleware
# ============================================================================
def throttle(app: FastAPI, max_requests: int):
    """Wrap app with throttling middleware.

    Limits concurrent requests to max_requests. When limit is reached,
    additional requests wait in queue instead of being rejected.
    This provides memory-bounded execution with graceful backpressure.
    """
    semaphore = asyncio.Semaphore(max_requests)

    async def middleware(scope, receive, send):
        if scope["type"] != "http":
            return await app(scope, receive, send)

        # Wait for slot to become available (queues requests)
        await semaphore.acquire()

        try:
            await app(scope, receive, send)
        finally:
            semaphore.release()

    return middleware


# ============================================================================
# Application Factory
# ============================================================================
def create_app(settings: Settings | None = None) -> FastAPI:
    """Create FastAPI application."""
    settings = settings or Settings()

    # Load credentials and initialize components
    credentials_store = load_credentials()
    multipart_manager = MultipartStateManager(
        ttl_seconds=settings.redis_upload_ttl_seconds,
    )
    verifier = SigV4Verifier(credentials_store)
    handler = S3ProxyHandler(settings, credentials_store, multipart_manager)

    # Create app with lifespan
    lifespan = create_lifespan(settings)
    app = FastAPI(title="S3Proxy", lifespan=lifespan, docs_url=None, redoc_url=None)

    # Health check endpoints
    @app.get("/healthz")
    @app.get("/readyz")
    async def health():
        return PlainTextResponse("ok")

    # Main proxy endpoint
    @app.api_route(
        "/{path:path}",
        methods=[METHOD_GET, METHOD_PUT, METHOD_POST, METHOD_DELETE, METHOD_HEAD],
    )
    async def proxy(request: Request, path: str):  # noqa: ARG001
        return await handle_proxy_request(request, handler, verifier)

    # Add throttling if configured
    if settings.throttling_requests_max > 0:
        app = throttle(app, settings.throttling_requests_max)

    return app


# ============================================================================
# CLI Entry Point
# ============================================================================
def main():
    """CLI entry point."""
    # Try to use uvloop for better performance
    try:
        import uvloop
        uvloop.install()
        logger.info("Using uvloop for improved performance")
    except ImportError:
        pass

    parser = argparse.ArgumentParser(description="S3Proxy - Transparent S3 encryption")
    parser.add_argument("--ip", default="0.0.0.0", help="Bind address")
    parser.add_argument("--port", type=int, default=4433, help="Listen port")
    parser.add_argument("--no-tls", action="store_true", help="Disable TLS")
    parser.add_argument("--cert-path", default="/etc/s3proxy/certs", help="Cert directory")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    parser.add_argument("--log-level", default="INFO", help="Log level")
    args = parser.parse_args()

    # Set environment from CLI args
    os.environ.setdefault("S3PROXY_IP", args.ip)
    os.environ.setdefault("S3PROXY_PORT", str(args.port))
    os.environ.setdefault("S3PROXY_NO_TLS", str(args.no_tls).lower())
    os.environ.setdefault("S3PROXY_CERT_PATH", args.cert_path)
    os.environ.setdefault("S3PROXY_REGION", args.region)
    os.environ.setdefault("S3PROXY_LOG_LEVEL", args.log_level)

    if not os.environ.get("S3PROXY_ENCRYPT_KEY"):
        sys.exit("Error: S3PROXY_ENCRYPT_KEY environment variable required")

    settings = Settings()
    app = create_app(settings)

    # Uvicorn config
    config = {
        "app": app,
        "host": settings.ip,
        "port": settings.port,
        "log_level": settings.log_level.lower(),
    }

    # TLS setup
    if not settings.no_tls:
        cert_path = Path(settings.cert_path)
        cert_file = cert_path / "s3proxy.crt"
        key_file = cert_path / "s3proxy.key"
        if cert_file.exists() and key_file.exists():
            config["ssl_certfile"] = str(cert_file)
            config["ssl_keyfile"] = str(key_file)
        else:
            print(f"Warning: No certs at {cert_path}, running without TLS", file=sys.stderr)

    uvicorn.run(**config)


# Module-level app instance for uvicorn workers
app = create_app()


if __name__ == "__main__":
    main()
