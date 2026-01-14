"""Base handler with shared utilities."""

import asyncio
import re

import httpx
from fastapi import HTTPException, Request, Response

from ..config import Settings
from ..multipart import MultipartStateManager
from ..s3client import S3Client, S3Credentials

PATH_RE = re.compile(r"^/([^/]+)/(.+)$")
BUCKET_RE = re.compile(r"^/([^/]+)/?$")  # Handles /bucket and /bucket/

# Shared httpx client for connection reuse
_http_client: httpx.AsyncClient | None = None
_http_client_lock = asyncio.Lock()


async def get_http_client() -> httpx.AsyncClient:
    """Get or create shared httpx client with optimized connection pooling."""
    global _http_client
    if _http_client is None:
        async with _http_client_lock:
            if _http_client is None:
                _http_client = httpx.AsyncClient(
                    timeout=httpx.Timeout(60.0, connect=10.0),
                    limits=httpx.Limits(
                        max_connections=200,
                        max_keepalive_connections=100,
                        keepalive_expiry=30.0,
                    ),
                    http2=True,  # Enable HTTP/2 for better multiplexing
                )
    return _http_client


async def close_http_client() -> None:
    """Close shared httpx client."""
    global _http_client
    if _http_client is not None:
        async with _http_client_lock:
            if _http_client is not None:
                await _http_client.aclose()
                _http_client = None


class BaseHandler:
    """Base class with shared utilities for S3 handlers."""

    def __init__(
        self,
        settings: Settings,
        credentials_store: dict[str, str],
        multipart_manager: MultipartStateManager,
    ):
        self.settings = settings
        self.credentials_store = credentials_store
        self.multipart_manager = multipart_manager

    def _client(self, creds: S3Credentials) -> S3Client:
        return S3Client(self.settings, creds)

    def _parse_path(self, path: str) -> tuple[str, str]:
        """Parse /bucket/key from path."""
        if m := PATH_RE.match(path):
            return m.group(1), m.group(2)
        raise HTTPException(400, "Invalid path")

    def _parse_bucket(self, path: str) -> str:
        """Parse bucket name from path."""
        if m := BUCKET_RE.match(path):
            return m.group(1)
        if m := PATH_RE.match(path):
            return m.group(1)
        raise HTTPException(400, "Invalid path")

    def _parse_range(self, header: str, size: int) -> tuple[int, int]:
        """Parse HTTP Range header."""
        if not header.startswith("bytes="):
            raise HTTPException(400, "Invalid range")
        spec = header[6:]
        if spec.startswith("-"):
            start = max(0, size - int(spec[1:]))
            end = size - 1
        elif spec.endswith("-"):
            start = int(spec[:-1])
            end = size - 1
        else:
            parts = spec.split("-")
            start, end = int(parts[0]), min(int(parts[1]), size - 1)
        if start > end or start >= size:
            raise HTTPException(416, "Range not satisfiable")
        return start, end

    async def forward_request(self, request: Request, creds: S3Credentials) -> Response:
        """Forward unhandled requests to S3.

        Note: Body is already cached by handle_proxy_request for signature verification,
        so we use the cached body here.
        """
        url = f"{self.settings.s3_endpoint}{request.url.path}"
        if request.url.query:
            url += f"?{request.url.query}"

        http = await get_http_client()
        resp = await http.request(
            request.method,
            url,
            headers=dict(request.headers),
            content=await request.body() if request.method in ("PUT", "POST") else None,
        )
        return Response(resp.content, resp.status_code, dict(resp.headers))
