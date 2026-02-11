"""Data types for S3 client operations."""

from dataclasses import dataclass, field


@dataclass(slots=True)
class S3Credentials:
    """AWS credentials extracted from request."""

    access_key: str
    secret_key: str
    region: str
    service: str = "s3"


@dataclass(slots=True)
class ParsedRequest:
    """Parsed S3 request information."""

    method: str
    bucket: str
    key: str
    query_params: dict[str, list[str]] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes = b""
    is_presigned: bool = False
