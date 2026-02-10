"""AWS chunked encoding utilities for streaming SigV4.

This module handles the aws-chunked transfer encoding used by
AWS SDK v4 streaming uploads.

Format: <hex-size>;chunk-signature=<sig>\r\n<data>\r\n...0;chunk-signature=<sig>\r\n
"""

from collections.abc import AsyncIterator, Iterator

from fastapi import Request

# Streaming chunk size for reads/writes
STREAM_CHUNK_SIZE = 64 * 1024  # 64KB chunks for streaming

# Safety limits for chunked decoding
_MAX_CHUNK_HEADER_SIZE = 4096  # Max header line (hex size + signature)
_MAX_CHUNK_SIZE = 64 * 1024 * 1024  # 64 MB max per chunk
_MAX_BUFFER_SIZE = 66 * 1024 * 1024  # Slightly above max chunk to hold chunk + framing


def _parse_chunk_size(header: bytes) -> int:
    """Parse and validate chunk size from header bytes."""
    size_str = header.split(b";")[0].strip()
    if not size_str:
        raise ValueError("Empty chunk size")
    chunk_size = int(size_str, 16)
    if chunk_size < 0:
        raise ValueError(f"Negative chunk size: {chunk_size}")
    if chunk_size > _MAX_CHUNK_SIZE:
        raise ValueError(f"Chunk size {chunk_size} exceeds maximum {_MAX_CHUNK_SIZE}")
    return chunk_size


def decode_aws_chunked(body: bytes) -> bytes:
    """Decode aws-chunked transfer encoding from buffered body.

    Args:
        body: Complete body with aws-chunked encoding

    Returns:
        Decoded bytes without chunk headers

    Raises:
        ValueError: If chunked encoding is malformed or truncated.
    """
    result = bytearray()
    pos = 0
    while pos < len(body):
        header_end = body.find(b"\r\n", pos)
        if header_end == -1:
            raise ValueError("Truncated chunk: missing header terminator")
        header = body[pos:header_end]
        chunk_size = _parse_chunk_size(header)
        if chunk_size == 0:
            break
        data_start = header_end + 2
        data_end = data_start + chunk_size
        if data_end > len(body):
            raise ValueError(
                f"Truncated chunk: expected {chunk_size} bytes, "
                f"only {len(body) - data_start} available"
            )
        result.extend(body[data_start:data_end])
        pos = data_end + 2
    return bytes(result)


async def decode_aws_chunked_stream(
    request: Request,
) -> AsyncIterator[bytes]:
    """Decode aws-chunked encoding from streaming request.

    Yields decoded data chunks without buffering entire body.
    Memory-efficient for large uploads.

    Args:
        request: FastAPI request with aws-chunked body

    Yields:
        Decoded data chunks

    Raises:
        ValueError: If buffer exceeds safety limits or encoding is malformed.
    """
    buffer = bytearray()

    async for raw_chunk in request.stream():
        buffer.extend(raw_chunk)

        if len(buffer) > _MAX_BUFFER_SIZE:
            raise ValueError(
                f"Chunked decode buffer ({len(buffer)} bytes) exceeds "
                f"maximum ({_MAX_BUFFER_SIZE} bytes)"
            )

        while True:
            header_end = buffer.find(b"\r\n")
            if header_end == -1:
                if len(buffer) > _MAX_CHUNK_HEADER_SIZE:
                    raise ValueError(f"Chunk header exceeds {_MAX_CHUNK_HEADER_SIZE} bytes")
                break

            header = buffer[:header_end]
            chunk_size = _parse_chunk_size(header)

            if chunk_size == 0:
                return

            data_start = header_end + 2
            data_end = data_start + chunk_size
            trailing_end = data_end + 2

            if len(buffer) < trailing_end:
                break

            yield bytes(buffer[data_start:data_end])
            del buffer[:trailing_end]


def chunked(data: bytes, size: int) -> Iterator[tuple[int, bytes]]:
    """Split data into numbered chunks for multipart upload.

    Args:
        data: Data to split
        size: Chunk size in bytes

    Yields:
        (part_number, chunk) tuples starting from part 1
    """
    for i in range(0, len(data), size):
        yield i // size + 1, data[i : i + size]
