"""Multipart upload state management."""

import asyncio
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

# Metadata suffix for multipart uploads
META_SUFFIX = ".s3proxy-meta"
UPLOAD_STATE_SUFFIX = ".s3proxy-upload-"

# Redis key prefix for upload state
REDIS_KEY_PREFIX = "s3proxy:upload:"

# Module-level Redis client (initialized by init_redis)
_redis_client: "Redis | None" = None


async def init_redis(redis_url: str) -> "Redis":
    """Initialize Redis connection pool."""
    global _redis_client
    _redis_client = redis.from_url(redis_url, decode_responses=False)
    # Test connection
    await _redis_client.ping()
    logger.info("Redis connected", url=redis_url)
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
    """Manages multipart upload state in Redis."""

    def __init__(self, max_concurrent: int = 10, ttl_seconds: int = 86400):
        """Initialize state manager.

        Args:
            max_concurrent: Max concurrent uploads (per-pod limit)
            ttl_seconds: TTL for upload state in Redis (default 24h)
        """
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._ttl = ttl_seconds

    def _redis_key(self, bucket: str, key: str, upload_id: str) -> str:
        """Generate Redis key for upload state."""
        return f"{REDIS_KEY_PREFIX}{bucket}:{key}:{upload_id}"

    async def create_upload(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        dek: bytes,
    ) -> MultipartUploadState:
        """Create new upload state in Redis."""
        state = MultipartUploadState(
            dek=dek,
            bucket=bucket,
            key=key,
            upload_id=upload_id,
        )

        redis_client = get_redis()
        rk = self._redis_key(bucket, key, upload_id)
        await redis_client.set(rk, _serialize_upload_state(state), ex=self._ttl)

        return state

    async def get_upload(
        self, bucket: str, key: str, upload_id: str
    ) -> MultipartUploadState | None:
        """Get upload state from Redis."""
        redis_client = get_redis()
        rk = self._redis_key(bucket, key, upload_id)
        data = await redis_client.get(rk)
        if data is None:
            return None
        return _deserialize_upload_state(data)

    async def add_part(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        part: PartMetadata,
    ) -> None:
        """Add part to upload state in Redis."""
        redis_client = get_redis()
        rk = self._redis_key(bucket, key, upload_id)

        # Use WATCH/MULTI for atomic update
        async with redis_client.pipeline(transaction=True) as pipe:
            try:
                await pipe.watch(rk)
                data = await redis_client.get(rk)
                if data is None:
                    await pipe.unwatch()
                    return

                state = _deserialize_upload_state(data)
                state.parts[part.part_number] = part
                state.total_plaintext_size += part.plaintext_size

                pipe.multi()
                pipe.set(rk, _serialize_upload_state(state), ex=self._ttl)
                await pipe.execute()
            except redis.WatchError:
                # Retry on concurrent modification
                logger.warning("Redis watch error, retrying add_part", key=rk)
                await self.add_part(bucket, key, upload_id, part)

    async def complete_upload(
        self, bucket: str, key: str, upload_id: str
    ) -> MultipartUploadState | None:
        """Remove and return upload state from Redis on completion."""
        redis_client = get_redis()
        rk = self._redis_key(bucket, key, upload_id)

        # Get and delete atomically
        async with redis_client.pipeline(transaction=True) as pipe:
            try:
                await pipe.watch(rk)
                data = await redis_client.get(rk)
                if data is None:
                    await pipe.unwatch()
                    return None

                state = _deserialize_upload_state(data)
                pipe.multi()
                pipe.delete(rk)
                await pipe.execute()
                return state
            except redis.WatchError:
                logger.warning("Redis watch error, retrying complete_upload", key=rk)
                return await self.complete_upload(bucket, key, upload_id)

    async def abort_upload(self, bucket: str, key: str, upload_id: str) -> None:
        """Remove upload state from Redis on abort."""
        redis_client = get_redis()
        rk = self._redis_key(bucket, key, upload_id)
        await redis_client.delete(rk)

    async def acquire_slot(self) -> None:
        """Acquire an upload slot (per-pod limit)."""
        await self._semaphore.acquire()

    def release_slot(self) -> None:
        """Release an upload slot."""
        self._semaphore.release()


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


async def persist_upload_state(
    s3_client: S3Client,
    bucket: str,
    key: str,
    upload_id: str,
    wrapped_dek: bytes,
) -> None:
    """Persist DEK to S3 during upload."""
    state_key = f"{key}{UPLOAD_STATE_SUFFIX}{upload_id}"
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
    state_key = f"{key}{UPLOAD_STATE_SUFFIX}{upload_id}"

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
    state_key = f"{key}{UPLOAD_STATE_SUFFIX}{upload_id}"
    with contextlib.suppress(Exception):
        await s3_client.delete_object(bucket, state_key)


async def save_multipart_metadata(
    s3_client: S3Client,
    bucket: str,
    key: str,
    meta: MultipartMetadata,
) -> None:
    """Save multipart metadata to S3."""
    meta_key = f"{key}{META_SUFFIX}"
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
    """Load multipart metadata from S3."""
    meta_key = f"{key}{META_SUFFIX}"

    try:
        response = await s3_client.get_object(bucket, meta_key)
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
    """Delete multipart metadata from S3."""
    meta_key = f"{key}{META_SUFFIX}"
    with contextlib.suppress(Exception):
        await s3_client.delete_object(bucket, meta_key)


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
