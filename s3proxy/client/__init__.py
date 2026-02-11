"""S3 client layer - credentials, verification, and API wrapper."""

from .s3 import S3Client, get_shared_session
from .types import ParsedRequest, S3Credentials
from .verifier import CLOCK_SKEW_TOLERANCE, SigV4Verifier, _derive_signing_key

__all__ = [
    "CLOCK_SKEW_TOLERANCE",
    "ParsedRequest",
    "S3Client",
    "S3Credentials",
    "SigV4Verifier",
    "_derive_signing_key",
    "get_shared_session",
]
