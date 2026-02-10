"""Backward compatibility - re-exports from s3proxy.client.

This module is deprecated. Import from s3proxy.client instead:
    from s3proxy.client import S3Client, S3Credentials, SigV4Verifier, ParsedRequest
"""

from .client import (
    CLOCK_SKEW_TOLERANCE,
    ParsedRequest,
    S3Client,
    S3Credentials,
    SigV4Verifier,
    _derive_signing_key,
    get_shared_session,
)

__all__ = [
    "CLOCK_SKEW_TOLERANCE",
    "ParsedRequest",
    "S3Client",
    "S3Credentials",
    "SigV4Verifier",
    "_derive_signing_key",
    "get_shared_session",
]
