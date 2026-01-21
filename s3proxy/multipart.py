"""Multipart upload state management."""

import base64
import contextlib
import gzip
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import redis.asyncio as redis
import structlog

if TYPE_CHECKING:
    from redis.asyncio import Redis

try:
    import orjson

    def json_dumps(obj: dict) -> bytes:
        """Serialize object to JSON bytes using orjson."""
        return orjson.dumps(obj)

    def json_loads(data: bytes) -> dict:
        """Deserialize JSON bytes using orjson."""
        return orjson.loads(data)

except ImportError:
    import json

    def json_dumps(obj: dict) -> bytes:
        """Serialize object to JSON bytes using stdlib json."""
        return json.dumps(obj, separators=(",", ":")).encode()

    def json_loads(data: bytes) -> dict:
        """Deserialize JSON bytes using stdlib json."""
        return json.loads(data)

from . import crypto
from .s3client import S3Client

logger = structlog.get_logger()

# Internal prefix for all s3proxy metadata (hidden from list operations)
INTERNAL_PREFIX = ".s3proxy-internal/"

# Legacy suffix for backwards compatibility detection
META_SUFFIX_LEGACY = ".s3proxy-meta"

# Redis key prefix for upload state
REDIS_KEY_PREFIX = "s3proxy:upload:"

# Module-level Redis client (initialized by init_redis)
_redis_client: "Redis | None" = None

# Flag to track if we're using Redis or in-memory storage
_use_redis: bool = False


async def init_redis(redis_url: str | None) -> "Redis | None":
    """Initialize Redis connection pool if URL is provided.

    Args:
        redis_url: Redis URL or None/empty to use in-memory storage

    Returns:
        Redis client if connected, None if using in-memory storage
    """
    global _redis_client, _use_redis

    if not redis_url:
        logger.info("Redis URL not configured, using in-memory storage (single-instance mode)")
        _use_redis = False
        return None

    _redis_client = redis.from_url(redis_url, decode_responses=False)
    # Test connection
    await _redis_client.ping()
    _use_redis = True
    logger.info("Redis connected (HA mode)", url=redis_url)
    return _redis_client


async def close_redis() -> None:
    """Close Redis connection."""
    global _redis_client
    if _redis_client:
        await _redis_client.aclose()
        _redis_client = None
        logger.info("Redis connection closed")


def get_redis() -> "Redis":
    """Get Redis client (must be initialized first)."""
    if _redis_client is None:
        raise RuntimeError("Redis not initialized. Call init_redis() first.")
    return _redis_client


def is_using_redis() -> bool:
    """Check if we're using Redis or in-memory storage."""
    return _use_redis


@dataclass(slots=True)
class PartMetadata:
    """Metadata for an encrypted part."""

    part_number: int
    plaintext_size: int
    ciphertext_size: int
    etag: str
    md5: str = ""


@dataclass(slots=True)
class MultipartUploadState:
    """State for an active multipart upload."""

    dek: bytes
    bucket: str
    key: str
    upload_id: str
    parts: dict[int, PartMetadata] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    total_plaintext_size: int = 0


@dataclass(slots=True)
class MultipartMetadata:
    """Stored metadata for a completed multipart object."""

    version: int = 1
    part_count: int = 0
    total_plaintext_size: int = 0
    parts: list[PartMetadata] = field(default_factory=list)
    wrapped_dek: bytes = b""


def _serialize_upload_state(state: MultipartUploadState) -> bytes:
    """Serialize upload state to JSON bytes for Redis."""
    data = {
        "dek": base64.b64encode(state.dek).decode(),
        "bucket": state.bucket,
        "key": state.key,
        "upload_id": state.upload_id,
        "created_at": state.created_at.isoformat(),
        "total_plaintext_size": state.total_plaintext_size,
        "parts": {
            str(pn): {
                "part_number": p.part_number,
                "plaintext_size": p.plaintext_size,
                "ciphertext_size": p.ciphertext_size,
                "etag": p.etag,
                "md5": p.md5,
            }
            for pn, p in state.parts.items()
        },
    }
    return json_dumps(data)


def _deserialize_upload_state(data: bytes) -> MultipartUploadState:
    """Deserialize upload state from Redis JSON bytes."""
    obj = json_loads(data)
    parts = {
        int(pn): PartMetadata(
            part_number=p["part_number"],
            plaintext_size=p["plaintext_size"],
            ciphertext_size=p["ciphertext_size"],
            etag=p["etag"],
            md5=p.get("md5", ""),
        )
        for pn, p in obj.get("parts", {}).items()
    }
    return MultipartUploadState(
        dek=base64.b64decode(obj["dek"]),
        bucket=obj["bucket"],
        key=obj["key"],
        upload_id=obj["upload_id"],
        parts=parts,
        created_at=datetime.fromisoformat(obj["created_at"]),
        total_plaintext_size=obj.get("total_plaintext_size", 0),
    )


class MultipartStateManager:
    """Manages multipart upload state in Redis or in-memory.

    Uses Redis when configured (for HA/multi-instance deployments).
    Falls back to in-memory storage for single-instance deployments.
    """

    def __init__(self, ttl_seconds: int = 86400):
        """Initialize state manager.

        Args:
            ttl_seconds: TTL for upload state in Redis (default 24h)
        """
        self._ttl = ttl_seconds
        # In-memory storage for single-instance mode
        self._memory_store: dict[str, MultipartUploadState] = {}

    def _storage_key(self, bucket: str, key: str, upload_id: str) -> str:
        """Generate storage key for upload state."""
        return f"{REDIS_KEY_PREFIX}{bucket}:{key}:{upload_id}"

    async def create_upload(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        dek: bytes,
    ) -> MultipartUploadState:
        """Create new upload state."""
        state = MultipartUploadState(
            dek=dek,
            bucket=bucket,
            key=key,
            upload_id=upload_id,
        )

        sk = self._storage_key(bucket, key, upload_id)

        if is_using_redis():
            redis_client = get_redis()
            await redis_client.set(sk, _serialize_upload_state(state), ex=self._ttl)
        else:
            self._memory_store[sk] = state

        return state

    async def get_upload(
        self, bucket: str, key: str, upload_id: str
    ) -> MultipartUploadState | None:
        """Get upload state."""
        sk = self._storage_key(bucket, key, upload_id)

        if is_using_redis():
            redis_client = get_redis()
            data = await redis_client.get(sk)
            if data is None:
                return None
            return _deserialize_upload_state(data)
        else:
            return self._memory_store.get(sk)

    async def add_part(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        part: PartMetadata,
    ) -> None:
        """Add part to upload state."""
        sk = self._storage_key(bucket, key, upload_id)

        if is_using_redis():
            redis_client = get_redis()
            # Use WATCH/MULTI for atomic update
            async with redis_client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(sk)
                    data = await redis_client.get(sk)
                    if data is None:
                        await pipe.unwatch()
                        return

                    state = _deserialize_upload_state(data)
                    state.parts[part.part_number] = part
                    state.total_plaintext_size += part.plaintext_size

                    pipe.multi()
                    pipe.set(sk, _serialize_upload_state(state), ex=self._ttl)
                    await pipe.execute()
                except redis.WatchError:
                    # Retry on concurrent modification
                    logger.warning("Redis watch error, retrying add_part", key=sk)
                    await self.add_part(bucket, key, upload_id, part)
        else:
            state = self._memory_store.get(sk)
            if state is not None:
                state.parts[part.part_number] = part
                state.total_plaintext_size += part.plaintext_size

    async def complete_upload(
        self, bucket: str, key: str, upload_id: str
    ) -> MultipartUploadState | None:
        """Remove and return upload state on completion."""
        sk = self._storage_key(bucket, key, upload_id)

        if is_using_redis():
            redis_client = get_redis()
            # Get and delete atomically
            async with redis_client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(sk)
                    data = await redis_client.get(sk)
                    if data is None:
                        await pipe.unwatch()
                        return None

                    state = _deserialize_upload_state(data)
                    pipe.multi()
                    pipe.delete(sk)
                    await pipe.execute()
                    return state
                except redis.WatchError:
                    logger.warning("Redis watch error, retrying complete_upload", key=sk)
                    return await self.complete_upload(bucket, key, upload_id)
        else:
            return self._memory_store.pop(sk, None)

    async def abort_upload(self, bucket: str, key: str, upload_id: str) -> None:
        """Remove upload state on abort."""
        sk = self._storage_key(bucket, key, upload_id)

        if is_using_redis():
            redis_client = get_redis()
            await redis_client.delete(sk)
        else:
            self._memory_store.pop(sk, None)


def encode_multipart_metadata(meta: MultipartMetadata) -> str:
    """Encode metadata to base64-compressed JSON."""
    data = {
        "v": meta.version,
        "pc": meta.part_count,
        "ts": meta.total_plaintext_size,
        "dek": base64.b64encode(meta.wrapped_dek).decode(),
        "parts": [
            {
                "pn": p.part_number,
                "ps": p.plaintext_size,
                "cs": p.ciphertext_size,
                "etag": p.etag,
                "md5": p.md5,
            }
            for p in meta.parts
        ],
    }

    json_bytes = json_dumps(data)
    compressed = gzip.compress(json_bytes)
    return base64.b64encode(compressed).decode()


def decode_multipart_metadata(encoded: str) -> MultipartMetadata:
    """Decode metadata from base64-compressed JSON."""
    compressed = base64.b64decode(encoded)
    json_bytes = gzip.decompress(compressed)
    data = json_loads(json_bytes)

    return MultipartMetadata(
        version=data.get("v", 1),
        part_count=data.get("pc", 0),
        total_plaintext_size=data.get("ts", 0),
        wrapped_dek=base64.b64decode(data.get("dek", "")),
        parts=[
            PartMetadata(
                part_number=p["pn"],
                plaintext_size=p["ps"],
                ciphertext_size=p["cs"],
                etag=p.get("etag", ""),
                md5=p.get("md5", ""),
            )
            for p in data.get("parts", [])
        ],
    )


def _internal_upload_key(key: str, upload_id: str) -> str:
    """Get internal key for upload state."""
    return f"{INTERNAL_PREFIX}{key}.upload-{upload_id}"


def _internal_meta_key(key: str) -> str:
    """Get internal key for multipart metadata."""
    return f"{INTERNAL_PREFIX}{key}.meta"


async def persist_upload_state(
    s3_client: S3Client,
    bucket: str,
    key: str,
    upload_id: str,
    wrapped_dek: bytes,
) -> None:
    """Persist DEK to S3 during upload."""
    state_key = _internal_upload_key(key, upload_id)
    data = {"dek": base64.b64encode(wrapped_dek).decode()}

    await s3_client.put_object(
        bucket=bucket,
        key=state_key,
        body=json_dumps(data),
        content_type="application/json",
    )


async def load_upload_state(
    s3_client: S3Client,
    bucket: str,
    key: str,
    upload_id: str,
    kek: bytes,
) -> bytes | None:
    """Load DEK from S3 for resumed upload."""
    state_key = _internal_upload_key(key, upload_id)

    try:
        response = await s3_client.get_object(bucket, state_key)
        body = await response["Body"].read()
        data = json_loads(body)
        wrapped_dek = base64.b64decode(data["dek"])
        return crypto.unwrap_key(wrapped_dek, kek)
    except Exception as e:
        logger.warning("Failed to load upload state", key=state_key, error=str(e))
        return None


async def delete_upload_state(
    s3_client: S3Client,
    bucket: str,
    key: str,
    upload_id: str,
) -> None:
    """Delete persisted upload state."""
    state_key = _internal_upload_key(key, upload_id)
    with contextlib.suppress(Exception):
        await s3_client.delete_object(bucket, state_key)


async def save_multipart_metadata(
    s3_client: S3Client,
    bucket: str,
    key: str,
    meta: MultipartMetadata,
) -> None:
    """Save multipart metadata to S3."""
    meta_key = _internal_meta_key(key)
    encoded = encode_multipart_metadata(meta)

    await s3_client.put_object(
        bucket=bucket,
        key=meta_key,
        body=encoded.encode(),
        content_type="application/octet-stream",
    )


async def load_multipart_metadata(
    s3_client: S3Client,
    bucket: str,
    key: str,
) -> MultipartMetadata | None:
    """Load multipart metadata from S3.

    Checks the new internal prefix first, then falls back to legacy location.
    """
    # Try new location first
    meta_key = _internal_meta_key(key)
    try:
        response = await s3_client.get_object(bucket, meta_key)
        body = await response["Body"].read()
        encoded = body.decode()
        return decode_multipart_metadata(encoded)
    except Exception:
        pass

    # Fall back to legacy location for backwards compatibility
    legacy_key = f"{key}{META_SUFFIX_LEGACY}"
    try:
        response = await s3_client.get_object(bucket, legacy_key)
        body = await response["Body"].read()
        encoded = body.decode()
        return decode_multipart_metadata(encoded)
    except Exception:
        return None


async def delete_multipart_metadata(
    s3_client: S3Client,
    bucket: str,
    key: str,
) -> None:
    """Delete multipart metadata from S3 (both new and legacy locations)."""
    # Delete from new location
    meta_key = _internal_meta_key(key)
    with contextlib.suppress(Exception):
        await s3_client.delete_object(bucket, meta_key)

    # Also delete legacy location if it exists
    legacy_key = f"{key}{META_SUFFIX_LEGACY}"
    with contextlib.suppress(Exception):
        await s3_client.delete_object(bucket, legacy_key)


def calculate_part_range(
    parts: list[PartMetadata],
    start_byte: int,
    end_byte: int | None,
) -> list[tuple[int, int, int]]:
    """Calculate which parts are needed for a byte range.

    Returns list of (part_number, part_start_offset, part_end_offset)
    """
    result = []
    current_offset = 0

    for part in sorted(parts, key=lambda p: p.part_number):
        part_start = current_offset
        part_end = current_offset + part.plaintext_size - 1

        # Check if this part overlaps with requested range
        if end_byte is not None:
            if part_start > end_byte:
                break
            if part_end >= start_byte:
                # Calculate offsets within the part
                offset_start = max(0, start_byte - part_start)
                offset_end = min(part.plaintext_size - 1, end_byte - part_start)
                result.append((part.part_number, offset_start, offset_end))
        else:
            # Open-ended range
            if part_end >= start_byte:
                offset_start = max(0, start_byte - part_start)
                offset_end = part.plaintext_size - 1
                result.append((part.part_number, offset_start, offset_end))

        current_offset += part.plaintext_size

    return result
