"""Multipart upload state management.

This package provides:
- Data models for multipart uploads (PartMetadata, MultipartMetadata, etc.)
- Pluggable storage backends (Redis or in-memory)
- State manager for active uploads
- Metadata persistence to S3
- Recovery logic for lost Redis state
"""

# State manager and storage
from .manager import MAX_INTERNAL_PARTS_PER_CLIENT, MultipartStateManager

# Metadata encoding/S3 persistence
from .metadata import (
    INTERNAL_PREFIX,
    META_SUFFIX_LEGACY,
    calculate_part_range,
    decode_multipart_metadata,
    delete_multipart_metadata,
    delete_upload_state,
    encode_multipart_metadata,
    load_multipart_metadata,
    load_upload_state,
    persist_upload_state,
    save_multipart_metadata,
)
from .models import (
    InternalPartMetadata,
    MultipartMetadata,
    MultipartUploadState,
    PartMetadata,
    StateMissingError,
)

# Recovery
from .recovery import reconstruct_upload_state_from_s3

# Redis client
from .redis import (
    close_redis,
    create_state_store,
    get_redis,
    init_redis,
    is_using_redis,
)

# Serialization utilities
from .serialization import json_dumps, json_loads
from .storage import MemoryStateStore, RedisStateStore, StateStore

__all__ = [
    # Models
    "InternalPartMetadata",
    "MultipartMetadata",
    "MultipartUploadState",
    "PartMetadata",
    "StateMissingError",
    # Storage backends
    "MemoryStateStore",
    "RedisStateStore",
    "StateStore",
    # Redis client management
    "close_redis",
    "create_state_store",
    "get_redis",
    "init_redis",
    "is_using_redis",
    # Manager
    "MAX_INTERNAL_PARTS_PER_CLIENT",
    "MultipartStateManager",
    # Metadata
    "INTERNAL_PREFIX",
    "META_SUFFIX_LEGACY",
    "calculate_part_range",
    "decode_multipart_metadata",
    "delete_multipart_metadata",
    "delete_upload_state",
    "encode_multipart_metadata",
    "load_multipart_metadata",
    "load_upload_state",
    "persist_upload_state",
    "save_multipart_metadata",
    # Recovery
    "reconstruct_upload_state_from_s3",
    # Serialization
    "json_dumps",
    "json_loads",
]
