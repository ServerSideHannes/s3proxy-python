"""Base handler with shared utilities."""

import asyncio
import base64
import os
import re
from collections.abc import AsyncIterator
from datetime import datetime
from typing import NoReturn
from urllib.parse import parse_qs, unquote

import aiohttp
import httpx
import structlog
from botocore.exceptions import (
    ClientError,
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
    IncompleteReadError,
    ReadTimeoutError,
    ResponseStreamingError,
)
from fastapi import Request, Response
from structlog.stdlib import BoundLogger

from .. import crypto
from ..client import S3Client, S3Credentials
from ..config import Settings
from ..errors import S3Error, raise_for_client_error
from ..state import MultipartStateManager
from ..state.complete_lock import CompleteUploadLock, create_complete_upload_lock
from ..utils import etag_matches, parse_http_date

logger: BoundLogger = structlog.get_logger(__name__)

PATH_RE = re.compile(r"^/([^/]+)/(.+)$")
BUCKET_RE = re.compile(r"^/([^/]+)/?$")  # Handles /bucket and /bucket/

# botocore's retry layer only covers a request up to the response headers; a
# body that dies mid-read (backend drops a long-lived connection, observed as
# ClientPayloadError at a fixed offset after minutes of sequential frame GETs)
# surfaces here with no retry and used to fail whole UploadPartCopy parts.
# Re-issuing the same ranged GET on a fresh connection is always safe.
SOURCE_READ_ATTEMPTS = int(os.environ.get("S3PROXY_SOURCE_READ_ATTEMPTS", "4"))
SOURCE_READ_BACKOFF_SEC = float(os.environ.get("S3PROXY_SOURCE_READ_BACKOFF", "0.5"))

_RETRYABLE_S3_ERROR_CODES = frozenset(
    {"InternalError", "SlowDown", "RequestTimeout", "ServiceUnavailable", "BadGateway"}
    | {"500", "502", "503"}
)

_RETRYABLE_TRANSPORT_ERRORS = (
    aiohttp.ClientPayloadError,
    aiohttp.ClientConnectionError,
    TimeoutError,
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
    IncompleteReadError,
    ReadTimeoutError,
    ResponseStreamingError,
)


def is_retryable_source_error(exc: BaseException) -> bool:
    """Transient transport/backend failures where re-issuing the request is safe."""
    if isinstance(exc, _RETRYABLE_TRANSPORT_ERRORS):
        return True
    if isinstance(exc, ClientError):
        code = str(exc.response.get("Error", {}).get("Code", ""))
        return code in _RETRYABLE_S3_ERROR_CODES
    return False


async def read_source_bytes(
    client: S3Client,
    bucket: str,
    key: str,
    range_header: str | None = None,
) -> bytes:
    """GET an object (or range) and read the full body, retrying truncated reads."""
    for attempt in range(1, SOURCE_READ_ATTEMPTS + 1):
        try:
            resp = await client.get_object(bucket, key, range_header=range_header)
            async with resp["Body"] as body:
                return await body.read()
        except Exception as exc:
            if not is_retryable_source_error(exc) or attempt == SOURCE_READ_ATTEMPTS:
                raise
            logger.warning(
                "SOURCE_READ_RETRY",
                bucket=bucket,
                key=key,
                range=range_header,
                attempt=attempt,
                error=str(exc),
            )
            await asyncio.sleep(SOURCE_READ_BACKOFF_SEC * (2 ** (attempt - 1)))
    raise AssertionError("unreachable")


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
        complete_upload_lock: CompleteUploadLock | None = None,
    ):
        self.settings = settings
        self.credentials_store = credentials_store
        self.multipart_manager = multipart_manager
        self.complete_upload_lock = complete_upload_lock or create_complete_upload_lock()
        self.keyring = settings.keyring

    def _client(self, creds: S3Credentials) -> S3Client:
        return S3Client(self.settings, creds)

    def _parse_path(self, path: str) -> tuple[str, str]:
        if m := PATH_RE.match(path):
            return m.group(1), m.group(2)
        raise S3Error.invalid_argument("Invalid path")

    def _parse_bucket(self, path: str) -> str:
        if m := BUCKET_RE.match(path):
            return m.group(1)
        if m := PATH_RE.match(path):
            return m.group(1)
        raise S3Error.invalid_argument("Invalid path")

    def _raise_s3_error(self, e: ClientError, bucket: str, key: str | None = None) -> NoReturn:
        """Raise appropriate S3Error for ClientError. Always raises."""
        raise_for_client_error(e, bucket, key)

    def _raise_bucket_error(self, e: ClientError, bucket: str) -> NoReturn:
        """Raise appropriate S3Error for bucket operations. Always raises."""
        raise_for_client_error(e, bucket)

    def _internal_meta_keys(self) -> set[str]:
        """Metadata keys owned by s3proxy that must never leak to clients."""
        return {
            self.settings.dektag_name.lower(),
            self.settings.kidtag_name.lower(),
            "client-etag",
            "plaintext-size",
        }

    def _parse_range(self, header: str, size: int) -> tuple[int, int]:
        if not header.startswith("bytes="):
            raise S3Error.invalid_range("Invalid range header format")
        spec = header[6:]
        try:
            if spec.startswith("-"):
                start = max(0, size - int(spec[1:]))
                end = size - 1
            elif spec.endswith("-"):
                start = int(spec[:-1])
                end = size - 1
            else:
                parts = spec.split("-")
                start, end = int(parts[0]), min(int(parts[1]), size - 1)
        except (ValueError, IndexError) as err:
            raise S3Error.invalid_range("Invalid range header format") from err
        if start > end or start >= size:
            raise S3Error.invalid_range("Range not satisfiable")
        return start, end

    def _parse_copy_source_range(
        self, range_header: str | None, total_size: int
    ) -> tuple[int, int]:
        if not range_header:
            return 0, total_size - 1
        range_str = range_header.replace("bytes=", "")
        try:
            start, end = map(int, range_str.split("-"))
        except (ValueError, TypeError) as err:
            raise S3Error.invalid_range("Invalid copy source range format") from err
        if start > end or start >= total_size:
            raise S3Error.invalid_range("Range not satisfiable")
        return start, min(end, total_size - 1)

    def _get_effective_etag(self, metadata: dict, fallback_etag: str) -> str:
        """Return client-etag for encrypted objects, S3 etag otherwise."""
        return metadata.get("client-etag", fallback_etag.strip('"'))

    def _get_plaintext_size(self, metadata: dict, fallback_size: int) -> int:
        size = metadata.get("plaintext-size", fallback_size)
        return int(size) if isinstance(size, str) else size

    def _parse_copy_source(self, copy_source: str) -> tuple[str, str]:
        """Parse x-amz-copy-source, returning (bucket, key)."""
        copy_source = unquote(copy_source).lstrip("/")
        if "/" not in copy_source:
            raise S3Error.invalid_argument("Invalid x-amz-copy-source format")
        return copy_source.split("/", 1)

    def _extract_multipart_params(self, request: Request) -> tuple[str, int]:
        query = parse_qs(request.url.query)
        upload_id = query.get("uploadId", [""])[0]
        part_num = int(query.get("partNumber", ["0"])[0])
        return upload_id, part_num

    def _extract_conditional_headers(
        self, request: Request
    ) -> tuple[str | None, str | None, str | None, str | None]:
        return (
            request.headers.get("if-match"),
            request.headers.get("if-none-match"),
            request.headers.get("if-modified-since"),
            request.headers.get("if-unmodified-since"),
        )

    async def _safe_abort(self, client: S3Client, bucket: str, key: str, upload_id: str) -> None:
        try:
            await client.abort_multipart_upload(bucket, key, upload_id)
            logger.info(
                "MULTIPART_ABORTED", bucket=bucket, key=key, upload_id=upload_id[:20] + "..."
            )
        except Exception as e:
            logger.warning(
                "MULTIPART_ABORT_FAILED",
                bucket=bucket,
                key=key,
                upload_id=upload_id[:20] + "...",
                error=str(e),
            )

    # --- Conditional headers ---

    def _check_conditional_headers(
        self,
        etag: str,
        last_modified_dt: datetime | None,
        last_modified_str: str | None,
        if_match: str | None,
        if_none_match: str | None,
        if_modified_since: str | None,
        if_unmodified_since: str | None,
    ) -> Response | None:
        """Return 304/412 Response if condition fails, None otherwise."""
        # If-Match: Return 412 if ETag doesn't match
        if if_match and not etag_matches(etag, if_match):
            raise S3Error.precondition_failed("If-Match")

        # If-Unmodified-Since: Return 412 if modified after the date
        if if_unmodified_since and last_modified_dt:
            since_dt = parse_http_date(if_unmodified_since)
            if since_dt and last_modified_dt > since_dt:
                raise S3Error.precondition_failed("If-Unmodified-Since")

        # If-None-Match: Return 304 if ETag matches
        if if_none_match and etag_matches(etag, if_none_match):
            headers = {"ETag": f'"{etag}"'}
            if last_modified_str:
                headers["Last-Modified"] = last_modified_str
            return Response(status_code=304, headers=headers)

        # If-Modified-Since: Return 304 if not modified since the date
        if if_modified_since and last_modified_dt:
            since_dt = parse_http_date(if_modified_since)
            if since_dt and last_modified_dt <= since_dt:
                headers = {"ETag": f'"{etag}"'}
                if last_modified_str:
                    headers["Last-Modified"] = last_modified_str
                return Response(status_code=304, headers=headers)

        return None

    async def _download_encrypted_single(
        self, client: S3Client, bucket: str, key: str, wrapped_dek_b64: str, kid: str = ""
    ) -> bytes:
        ciphertext = await read_source_bytes(client, bucket, key)
        wrapped_dek = base64.b64decode(wrapped_dek_b64)
        return crypto.decrypt_object(ciphertext, wrapped_dek, self.keyring.key_by_id(kid))

    async def _iter_multipart_plaintext(
        self,
        client: S3Client,
        bucket: str,
        key: str,
        meta,
        dek: bytes,
        range_start: int | None = None,
        range_end: int | None = None,
    ) -> AsyncIterator[bytes]:
        """Yield decrypted plaintext for a multipart-encrypted object, one frame
        at a time.

        A client part may expand into several internal parts (separate S3 parts),
        and each internal part is itself a sequence of independent AES-GCM frames
        (nonce || ciphertext || tag). Every frame must be decrypted individually —
        decrypting a whole internal part or client part as a single seal fails with
        InvalidTag whenever it holds more than one frame. See crypto.FRAME_* and the
        GET reader (_stream_internal_parts) which uses the same layout.

        Fetching per frame also bounds peak memory to O(frame) instead of a whole
        ~50MB client part. Frames outside the requested plaintext range are skipped
        (no fetch); frames that partially overlap are trimmed before yielding.
        """
        sorted_parts = sorted(meta.parts, key=lambda p: p.part_number)
        pt_offset = 0
        ct_offset = 0

        for part in sorted_parts:
            if part.internal_parts:
                segments = [
                    (ip.plaintext_size, ip.ciphertext_size)
                    for ip in sorted(part.internal_parts, key=lambda p: p.internal_part_number)
                ]
            else:
                segments = [(part.plaintext_size, part.ciphertext_size)]

            for seg_pt_size, seg_ct_size in segments:
                for fsize in crypto.ciphertext_frame_byte_sizes(seg_pt_size, seg_ct_size):
                    frame_pt_size = fsize - crypto.ENCRYPTION_OVERHEAD
                    frame_pt_end = pt_offset + frame_pt_size - 1

                    in_range = range_start is None or (
                        frame_pt_end >= range_start and pt_offset <= range_end
                    )

                    if in_range:
                        ct_end = ct_offset + fsize - 1
                        ciphertext = await read_source_bytes(
                            client, bucket, key, f"bytes={ct_offset}-{ct_end}"
                        )
                        plaintext = crypto.decrypt(ciphertext, dek)

                        if range_start is not None:
                            trim_start = max(0, range_start - pt_offset)
                            trim_end = min(frame_pt_size, range_end - pt_offset + 1)
                            plaintext = plaintext[trim_start:trim_end]

                        yield plaintext

                    pt_offset += frame_pt_size
                    ct_offset += fsize

    async def _download_encrypted_multipart(
        self,
        client: S3Client,
        bucket: str,
        key: str,
        meta,
        range_start: int | None = None,
        range_end: int | None = None,
    ) -> bytes:
        """Download and decrypt multipart encrypted object, optionally with range."""
        dek = crypto.unwrap_key(meta.wrapped_dek, self.keyring.key_by_id(meta.kid))
        chunks = [
            chunk
            async for chunk in self._iter_multipart_plaintext(
                client, bucket, key, meta, dek, range_start, range_end
            )
        ]
        return b"".join(chunks)

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

        # Filter out hop-by-hop headers and Content-Length/Content-Encoding
        # httpx decompresses gzip responses, so Content-Length from upstream
        # won't match the decompressed body we're returning
        # Let Starlette calculate correct Content-Length from actual body
        excluded_headers = {
            "content-length",
            "content-encoding",
            "transfer-encoding",
            "connection",
        }
        filtered_headers = {
            k: v for k, v in resp.headers.items() if k.lower() not in excluded_headers
        }
        return Response(resp.content, resp.status_code, filtered_headers)
