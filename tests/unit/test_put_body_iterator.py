"""PUT handler must not double-buffer a body already loaded for signature."""

import pytest
from unittest.mock import AsyncMock, MagicMock

from s3proxy.handlers.objects.put import _iter_request_body

MB = 1024 * 1024


@pytest.mark.asyncio
async def test_iter_request_body_uses_preloaded_chunks():
    request = MagicMock()
    request.state = MagicMock()
    request.state.s3proxy_preloaded_body = b"x" * (2 * MB)
    request.stream = AsyncMock()

    chunks = [c async for c in _iter_request_body(request, decode_chunked=False)]

    assert sum(len(c) for c in chunks) == 2 * MB
    request.stream.assert_not_called()


@pytest.mark.asyncio
async def test_iter_request_body_streams_when_not_preloaded():
    request = MagicMock()
    request.state = MagicMock(spec=[])  # no s3proxy_preloaded_body

    async def _stream():
        yield b"ab"
        yield b"cd"

    request.stream = MagicMock(return_value=_stream())

    chunks = [c async for c in _iter_request_body(request, decode_chunked=False)]

    assert b"".join(chunks) == b"abcd"
