"""Data models for multipart upload state management."""

from dataclasses import dataclass, field
from datetime import UTC, datetime


@dataclass(slots=True)
class InternalPartMetadata:
    """Metadata for an internal encrypted sub-part.

    When a client uploads a large part, S3Proxy splits it into smaller
    internal parts for streaming. This tracks each sub-part.
    """

    internal_part_number: int  # S3 part number (sequential across all client parts)
    plaintext_size: int
    ciphertext_size: int
    etag: str  # S3 ETag for this internal part


@dataclass(slots=True)
class PartMetadata:
    """Metadata for a client's part (may contain multiple internal parts).

    Tracks the mapping between what the client uploaded (one part) and
    what's stored in S3 (potentially multiple internal parts).
    """

    part_number: int  # Client's part number
    plaintext_size: int  # Total plaintext size of this client part
    ciphertext_size: int  # Total ciphertext size (sum of internal parts)
    etag: str  # Synthetic ETag returned to client (MD5 of plaintext)
    md5: str = ""
    # Internal sub-parts for streaming uploads
    internal_parts: list[InternalPartMetadata] = field(default_factory=list)


@dataclass(slots=True)
class MultipartUploadState:
    """State for an active multipart upload.

    Tracks the DEK (data encryption key) and all parts uploaded so far.
    Stored in Redis for HA deployments or in-memory for single-instance.
    """

    dek: bytes
    bucket: str
    key: str
    upload_id: str
    parts: dict[int, PartMetadata] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    total_plaintext_size: int = 0
    next_internal_part_number: int = 1  # Next S3 part number to use


@dataclass(slots=True)
class MultipartMetadata:
    """Stored metadata for a completed multipart object.

    Saved to S3 alongside the encrypted object to enable decryption
    on subsequent GET requests.
    """

    version: int = 1
    part_count: int = 0
    total_plaintext_size: int = 0
    parts: list[PartMetadata] = field(default_factory=list)
    wrapped_dek: bytes = b""


class StateMissingError(Exception):
    """Raised when upload state is missing from Redis during add_part."""

    pass
