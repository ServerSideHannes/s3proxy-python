"""Detect client disconnect during long uploads to stop reading and release memory."""

from __future__ import annotations

import inspect

from fastapi import Request

from .errors import S3Error

# Poll is_disconnected() every 8MB so we don't await on every 64KB chunk.
CHECK_INTERVAL_BYTES = 8 * 1024 * 1024


class ClientDisconnected(S3Error):
    """Client closed the connection mid-upload."""

    @classmethod
    def raised(cls) -> ClientDisconnected:
        return cls(400, "BadRequest", "Client disconnected")


async def _client_disconnected(request: Request) -> bool:
    result = request.is_disconnected()
    if inspect.isawaitable(result):
        return bool(await result)
    return bool(result)


async def track_chunk(request: Request, chunk_len: int, bytes_since_check: int) -> int:
    """Accumulate streamed bytes; raise if the client has disconnected."""
    bytes_since_check += chunk_len
    if bytes_since_check < CHECK_INTERVAL_BYTES:
        return bytes_since_check
    if await _client_disconnected(request):
        raise ClientDisconnected.raised()
    return 0
