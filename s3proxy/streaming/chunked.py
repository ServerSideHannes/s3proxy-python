"""Strict aws-chunked framing with incremental reads and SigV4 chunk validation."""

import hashlib
import hmac
from collections.abc import AsyncIterator, Iterator

from fastapi import Request

from ..errors import S3Error

STREAM_CHUNK_SIZE = 64 * 1024
_MAX_CHUNK_HEADER_SIZE = 4096
_MAX_CHUNK_SIZE = 64 * 1024 * 1024


class ChunkDecoder:
    """A bounded framing state machine; EOF is valid only after the terminal chunk."""

    def __init__(self, validate=None):
        self.buffer = bytearray()
        self.remaining = None
        self.terminal = False
        self.done = False
        self.validate = validate
        self.digest = None
        self.signature = ""

    def feed(self, data: bytes) -> Iterator[bytes]:
        self.buffer.extend(data)
        while True:
            if self.done:
                if self.buffer:
                    raise ValueError("Unexpected bytes after terminal chunk")
                return
            if self.remaining is None:
                end = self.buffer.find(b"\r\n")
                if end < 0:
                    if len(self.buffer) > _MAX_CHUNK_HEADER_SIZE:
                        raise ValueError("Chunk header too large")
                    return
                if end > _MAX_CHUNK_HEADER_SIZE:
                    raise ValueError("Chunk header too large")
                header = bytes(self.buffer[:end])
                del self.buffer[: end + 2]
                size, *extensions = header.split(b";")
                if not size or any(c not in b"0123456789abcdefABCDEF" for c in size):
                    raise ValueError("Invalid chunk size")
                self.remaining = int(size, 16)
                if self.remaining > _MAX_CHUNK_SIZE:
                    raise ValueError("Chunk size exceeds limit")
                self.terminal = self.remaining == 0
                self.digest = hashlib.sha256()
                self.signature = ""
                for extension in extensions:
                    if extension.startswith(b"chunk-signature=") and not self.signature:
                        self.signature = extension.split(b"=", 1)[1].decode("ascii")
                    else:
                        raise ValueError("Unsupported chunk extension")
            if self.remaining:
                if not self.buffer:
                    return
                count = min(self.remaining, len(self.buffer), STREAM_CHUNK_SIZE)
                chunk = bytes(self.buffer[:count])
                del self.buffer[:count]
                self.remaining -= count
                self.digest.update(chunk)
                yield chunk
                continue
            if len(self.buffer) < 2:
                return
            if self.buffer[:2] != b"\r\n":
                raise ValueError("Missing chunk data terminator")
            del self.buffer[:2]
            if self.validate:
                self.validate(self.signature, self.digest.hexdigest())
            elif self.signature:
                raise ValueError("Signed chunks require signature verification")
            if self.terminal:
                self.done = True
            self.remaining = None

    def finish(self):
        if not self.done or self.buffer:
            raise ValueError("Truncated aws-chunked body")


def decode_aws_chunked(body: bytes) -> bytes:
    decoder = ChunkDecoder()
    result = b"".join(decoder.feed(body))
    decoder.finish()
    return result


def _chunk_validator(request: Request):
    mode = request.headers.get("x-amz-content-sha256", "")
    if mode.startswith("STREAMING-") and mode != "STREAMING-AWS4-HMAC-SHA256-PAYLOAD":
        raise S3Error.invalid_request("Unsupported streaming signature/trailer format")
    if mode != "STREAMING-AWS4-HMAC-SHA256-PAYLOAD":
        return None
    from ..client import ParsedRequest
    from ..client.verifier import _derive_signing_key

    verifier = request.app.state.verifier
    parsed = ParsedRequest(
        method=request.method,
        bucket="",
        key="",
        query_params={},
        headers=dict(request.headers),
        body=b"",
    )
    auth = verifier._parse_header_auth(parsed, request.headers.get("authorization", ""))
    if auth.error or auth.credentials is None:
        raise S3Error.signature_does_not_match("Invalid streaming authorization")
    signing_key = _derive_signing_key(
        auth.credentials.secret_key, auth.date_stamp, auth.region, auth.service
    )
    scope = f"{auth.date_stamp}/{auth.region}/{auth.service}/aws4_request"
    previous = auth.signature

    def validate(signature, digest):
        nonlocal previous
        message = "\n".join(
            [
                "AWS4-HMAC-SHA256-PAYLOAD",
                auth.amz_date,
                scope,
                previous,
                hashlib.sha256(b"").hexdigest(),
                digest,
            ]
        )
        expected = hmac.new(signing_key, message.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, signature):
            raise S3Error.signature_does_not_match("Invalid streaming chunk signature")
        previous = signature

    return validate


async def decode_aws_chunked_stream(request: Request) -> AsyncIterator[bytes]:
    decoder = ChunkDecoder(_chunk_validator(request))
    decoded = 0
    try:
        async for raw in request.stream():
            # Bound parser buffering even if the ASGI server delivers a large block.
            for offset in range(0, len(raw), STREAM_CHUNK_SIZE):
                for chunk in decoder.feed(raw[offset : offset + STREAM_CHUNK_SIZE]):
                    decoded += len(chunk)
                    yield chunk
        decoder.finish()
        expected = request.headers.get("x-amz-decoded-content-length")
        if expected is not None and int(expected) != decoded:
            raise ValueError("Decoded content length mismatch")
    except (ValueError, UnicodeError) as error:
        raise S3Error.bad_request(str(error)) from error


def chunked(data: bytes, size: int) -> Iterator[tuple[int, bytes]]:
    for i in range(0, len(data), size):
        yield i // size + 1, data[i : i + size]
