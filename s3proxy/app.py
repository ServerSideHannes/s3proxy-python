"""FastAPI application factory and configuration."""

from __future__ import annotations

import logging
import os
import sys
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from xml.sax.saxutils import escape as xml_escape

import structlog
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from structlog.stdlib import BoundLogger

from .config import Settings
from .errors import S3Error, get_s3_error_code
from .handlers import S3ProxyHandler
from .handlers.base import close_http_client
from .request_handler import handle_proxy_request
from .s3client import SigV4Verifier
from .state import MultipartStateManager, close_redis, create_state_store, init_redis

# Configure logging
logging.basicConfig(format="%(message)s", stream=sys.stdout, level=logging.INFO)

structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
)

pod_name = os.environ.get("HOSTNAME", "unknown")
logger: BoundLogger = structlog.get_logger(__name__).bind(pod=pod_name)


def load_credentials() -> dict[str, str]:
    """Load AWS credentials from environment variables."""
    credentials_store: dict[str, str] = {}
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    if access_key and secret_key:
        credentials_store[access_key] = secret_key
    return credentials_store


def create_lifespan(settings: Settings, credentials_store: dict[str, str]) -> AsyncIterator[None]:
    """Create lifespan context manager for FastAPI app.

    Args:
        settings: Application settings.
        credentials_store: Credentials for signature verification.

    Returns:
        A lifespan context manager for FastAPI.
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        logger.info("Starting", endpoint=settings.s3_endpoint, port=settings.port)

        # Initialize Redis FIRST, then create manager with correct store
        await init_redis(settings.redis_url or None, settings.redis_password or None)
        store = create_state_store()
        multipart_manager = MultipartStateManager(
            store=store,
            ttl_seconds=settings.redis_upload_ttl_seconds,
        )

        # Create handler and verifier with properly initialized manager
        verifier = SigV4Verifier(credentials_store)
        handler = S3ProxyHandler(settings, credentials_store, multipart_manager)

        # Store in app.state for route access
        app.state.settings = settings
        app.state.handler = handler
        app.state.verifier = verifier
        app.state.start_time = time.monotonic()

        yield

        await close_redis()
        await close_http_client()
        logger.info("Shutting down")

    return lifespan


def create_app(settings: Settings | None = None) -> FastAPI:
    """Create and configure FastAPI application.

    Args:
        settings: Optional settings instance. If not provided, creates from environment.

    Returns:
        Configured FastAPI application instance.
    """
    settings = settings or Settings()
    credentials_store = load_credentials()

    lifespan = create_lifespan(settings, credentials_store)
    app = FastAPI(title="S3Proxy", lifespan=lifespan, docs_url=None, redoc_url=None)

    _register_exception_handlers(app)

    if settings.admin_ui:
        from .admin import create_admin_router

        app.include_router(
            create_admin_router(settings, credentials_store),
            prefix=settings.admin_path,
        )

    _register_routes(app)

    return app


def _register_exception_handlers(app: FastAPI) -> None:
    """Register exception handlers for S3-compatible error responses."""

    @app.exception_handler(HTTPException)
    async def s3_exception_handler(request: Request, exc: HTTPException):
        """Return S3-compatible error response with request ID.

        Non-S3 exceptions that carry their own headers (e.g. admin auth 401 with
        WWW-Authenticate) are passed through so browsers can prompt for credentials.
        """
        if not isinstance(exc, S3Error) and getattr(exc, "headers", None):
            from fastapi.responses import JSONResponse

            return JSONResponse(
                status_code=exc.status_code,
                content={"detail": exc.detail},
                headers=exc.headers,
            )

        request_id = str(uuid.uuid4()).replace("-", "").upper()[:16]

        if isinstance(exc, S3Error):
            error_code = exc.code
            message = exc.message
        else:
            error_code = get_s3_error_code(exc.status_code, exc.detail)
            message = exc.detail or "Unknown error"

        error_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>{xml_escape(error_code)}</Code>
    <Message>{xml_escape(str(message))}</Message>
    <RequestId>{request_id}</RequestId>
</Error>"""
        return Response(
            content=error_xml,
            status_code=exc.status_code,
            media_type="application/xml",
            headers={
                "x-amz-request-id": request_id,
                "x-amz-id-2": request_id,
            },
        )


def _register_routes(app: FastAPI) -> None:
    """Register health check and proxy routes."""

    @app.get("/healthz")
    @app.get("/readyz")
    async def health():
        return PlainTextResponse("ok")

    @app.get("/metrics")
    async def metrics():
        return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.api_route(
        "/{path:path}",
        methods=["GET", "PUT", "POST", "DELETE", "HEAD"],
    )
    async def proxy(request: Request, path: str):  # noqa: ARG001 - required by FastAPI for {path:path}
        return await handle_proxy_request(
            request, request.app.state.handler, request.app.state.verifier
        )


# Default app instance for ASGI servers (uvicorn, gunicorn)
app = create_app()
