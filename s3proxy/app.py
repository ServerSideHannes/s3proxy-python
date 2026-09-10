"""FastAPI application factory and configuration."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import sys
import time
import tracemalloc
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from xml.sax.saxutils import escape as xml_escape

import structlog
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from structlog.stdlib import BoundLogger

from . import concurrency
from .client import SigV4Verifier
from .config import Settings
from .errors import S3Error, get_s3_error_code
from .handlers import S3ProxyHandler
from .handlers.base import close_http_client
from .request_context import get_request_context
from .request_handler import handle_proxy_request
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


class _HealthProbeAccessFilter(logging.Filter):
    """Drop uvicorn access-log lines for liveness/readiness probes.

    Probes hit these endpoints every few seconds on every pod; logging each one
    drowns out real request activity.
    """

    _PATHS = {"/healthz", "/readyz"}

    def filter(self, record: logging.LogRecord) -> bool:
        # uvicorn.access record args: (client, method, path, http_version, status)
        args = record.args
        return not (isinstance(args, tuple) and len(args) >= 3 and args[2] in self._PATHS)


_health_probe_filter = _HealthProbeAccessFilter()


def _silence_health_probe_access_logs() -> None:
    access_logger = logging.getLogger("uvicorn.access")
    if _health_probe_filter not in access_logger.filters:
        access_logger.addFilter(_health_probe_filter)


def _rss_mb() -> float | None:
    """Process resident set size in MB from /proc (Linux). None elsewhere."""
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024  # kB -> MB
    except OSError:
        return None
    return None


def _dump_tracemalloc(limit: int = 20) -> None:
    """Log real RSS vs tracked Python heap + the top live allocations by call site.

    Diagnostic only (memory debug mode). The whole point is the gap: RSS is what
    the kernel OOM-kills on, while tracemalloc only sees Python allocations. A
    large rss-minus-tracked gap means the memory is C-level (uvicorn/httptools
    socket buffers, openssl, allocator retention) -- NOT something a call site in
    the top list will explain. A small gap means it IS Python, and the top list
    names the exact line. Logging both each interval settles which world we're in.
    """
    if not tracemalloc.is_tracing():
        return
    snap = tracemalloc.take_snapshot()
    stats = snap.statistics("lineno")
    tracked_mb = sum(s.size for s in stats) / 1024 / 1024
    rss = _rss_mb()
    governed_mb = concurrency.get_active_memory() / 1024 / 1024
    logger.warning(
        "MEMORY_DEBUG",
        rss_mb=round(rss, 1) if rss is not None else None,
        tracked_mb=round(tracked_mb, 1),
        untracked_mb=round(rss - tracked_mb, 1) if rss is not None else None,
        governed_active_mb=round(governed_mb, 1),
        shown=limit,
    )
    for i, st in enumerate(stats[:limit], 1):
        fr = st.traceback[0]
        logger.warning(
            "MEMORY_DEBUG_TOP",
            rank=i,
            size_mb=round(st.size / 1024 / 1024, 2),
            count=st.count,
            loc=f"{fr.filename}:{fr.lineno}",
        )


async def _periodic_tracemalloc(interval: int) -> None:
    while True:
        await asyncio.sleep(interval)
        _dump_tracemalloc()


def _maybe_start_tracemalloc() -> asyncio.Task | None:
    """Enable memory debug mode (RSS + tracemalloc heap dumps) when requested.

    Gated by S3PROXY_MEMORY_DEBUG (alias: S3PROXY_TRACEMALLOC). No-op with zero
    overhead when unset. Used for one-pod, time-boxed profiling: dumps every
    S3PROXY_MEMORY_DEBUG_INTERVAL secs and on SIGUSR1.
    """
    if not (os.environ.get("S3PROXY_MEMORY_DEBUG") or os.environ.get("S3PROXY_TRACEMALLOC")):
        return None
    frames = int(os.environ.get("S3PROXY_MEMORY_DEBUG_FRAMES", "4"))
    interval = int(os.environ.get("S3PROXY_MEMORY_DEBUG_INTERVAL", "15"))
    tracemalloc.start(frames)
    logger.warning("MEMORY_DEBUG_ENABLED", frames=frames, interval_sec=interval, rss_mb=_rss_mb())
    with contextlib.suppress(NotImplementedError, RuntimeError):
        asyncio.get_running_loop().add_signal_handler(signal.SIGUSR1, _dump_tracemalloc)
    return asyncio.create_task(_periodic_tracemalloc(interval))


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
        _silence_health_probe_access_logs()
        loop = asyncio.get_running_loop()
        logger.info(
            "Starting",
            endpoint=settings.s3_endpoint,
            port=settings.port,
            event_loop=f"{type(loop).__module__}.{type(loop).__qualname__}",
        )

        # Initialize Redis FIRST, then create manager with correct store
        await init_redis(settings.redis_url or None, settings.redis_password or None)
        store = create_state_store()
        multipart_manager = MultipartStateManager(
            store=store,
            ttl_seconds=settings.redis_upload_ttl_seconds,
        )

        # Dashboard stats store — Redis-backed (cluster-wide) when Redis is
        # configured, else per-pod in-memory. Mirrors create_state_store().
        from .dashboard.stats_store import create_stats_store, set_store

        stats_store = create_stats_store(settings)
        set_store(stats_store)  # used by the synchronous record_request path
        await stats_store.start()  # background flush loop (Redis store only; no-op for memory)

        if settings.dashboard_ui:
            from .dashboard.auth import RedisSessionStore
            from .state.redis import get_redis

            app.state.session_store = RedisSessionStore(get_redis())

        # Create handler and verifier with properly initialized manager
        verifier = SigV4Verifier(credentials_store)
        handler = S3ProxyHandler(settings, credentials_store, multipart_manager)

        # Store in app.state for route access
        app.state.settings = settings
        app.state.handler = handler
        app.state.verifier = verifier
        app.state.stats_store = stats_store
        app.state.start_time = time.monotonic()

        tracemalloc_task = _maybe_start_tracemalloc()

        try:
            yield
        finally:
            if tracemalloc_task is not None:
                tracemalloc_task.cancel()
            await stats_store.aclose()  # flush buffered samples before Redis closes
            await close_redis()
            await close_http_client()
            await handler.client_pool.close()
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
    credentials_store = settings.credentials_store

    lifespan = create_lifespan(settings, credentials_store)
    app = FastAPI(title="S3Proxy", lifespan=lifespan, docs_url=None, redoc_url=None)

    _register_exception_handlers(app)

    if settings.dashboard_ui:
        from .dashboard import create_dashboard_router

        app.include_router(
            create_dashboard_router(settings, credentials_store),
            prefix=settings.dashboard_path,
        )

    _register_routes(app)

    return app


def _register_exception_handlers(app: FastAPI) -> None:
    """Register exception handlers for S3-compatible error responses."""

    @app.exception_handler(HTTPException)
    async def s3_exception_handler(request: Request, exc: HTTPException):
        """Return S3-compatible error response with request ID.

        Non-S3 exceptions that carry their own headers (e.g. dashboard auth 401 with
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

        logger.warning(
            "S3_ERROR_RESPONSE",
            status_code=exc.status_code,
            error_code=error_code,
            message=str(message),
            **get_request_context(),
        )

        error_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>{xml_escape(error_code)}</Code>
    <Message>{xml_escape(str(message))}</Message>
    <RequestId>{request_id}</RequestId>
</Error>"""
        headers = {
            "x-amz-request-id": request_id,
            "x-amz-id-2": request_id,
        }
        # A small request rejected before its body was read can leave the body
        # bytes unconsumed on the keep-alive connection, and the client's next
        # request on it gets a raw uvicorn 400 (e.g. AbortMultipartUpload after
        # a rejected UploadPart). Drain small bodies so the connection stays
        # clean. Large bodies are left alone: uvicorn discards them after the
        # response, and closing the connection instead resets clients that
        # finish sending before they read (presigned PUT uploaders).
        if request.method in ("PUT", "POST"):
            try:
                content_length = int(request.headers.get("content-length", "0"))
            except ValueError:
                content_length = 0
            if 0 < content_length <= concurrency.MAX_BUFFER_SIZE:
                with contextlib.suppress(Exception):
                    await request.body()
        return Response(
            content=error_xml,
            status_code=exc.status_code,
            media_type="application/xml",
            headers=headers,
        )


def _register_routes(app: FastAPI) -> None:
    """Register health check and proxy routes."""

    @app.get("/healthz")
    @app.get("/readyz")
    async def health():
        return PlainTextResponse("ok")

    @app.get("/favicon.ico")
    async def favicon() -> Response:
        # Silence browser favicon probes so they don't fall through to the
        # S3 catch-all and pollute the dashboard activity feed as a "bucket".
        return Response(status_code=204)

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
