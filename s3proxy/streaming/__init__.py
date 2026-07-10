"""Streaming utilities for S3 operations.

This package provides AWS chunked encoding/decoding utilities.
"""

from .chunked import (
    STREAM_CHUNK_SIZE,
    chunked,
    decode_aws_chunked,
    decode_aws_chunked_stream,
)
from .framed_body import FramedStreamBody, SourceExhaustedError

__all__ = [
    "STREAM_CHUNK_SIZE",
    "FramedStreamBody",
    "SourceExhaustedError",
    "chunked",
    "decode_aws_chunked",
    "decode_aws_chunked_stream",
]
