"""Multipart metadata encoding/decoding and S3 persistence."""

import base64
import gzip

import structlog
from structlog.stdlib import BoundLogger

from .models import InternalPartMetadata, MultipartMetadata, PartMetadata
from .serialization import json_dumps, json_loads

logger: BoundLogger = structlog.get_logger(__name__)

# Internal prefix for all s3proxy metadata (hidden from list operations)
INTERNAL_PREFIX = ".s3proxy-internal/"

# Legacy suffix for backwards compatibility detection
META_SUFFIX_LEGACY = ".s3proxy-meta"


def _internal_upload_key(key: str, upload_id: str) -> str:
    """Get internal key for upload state."""
    return f"{INTERNAL_PREFIX}{key}.upload-{upload_id}"


def _internal_meta_key(key: str) -> str:
    """Get internal key for multipart metadata."""
    return f"{INTERNAL_PREFIX}{key}.meta"


def encode_multipart_metadata(meta: MultipartMetadata) -> str:
    """Encode metadata to base64-compressed JSON.

    Uses gzip compression for efficient storage in S3.
    """
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
                "ip": [
                    {
                        "ipn": ip.internal_part_number,
                        "ps": ip.plaintext_size,
                        "cs": ip.ciphertext_size,
                        "etag": ip.etag,
                    }
                    for ip in p.internal_parts
                ]
                if p.internal_parts
                else [],
            }
            for p in meta.parts
        ],
    }

    json_bytes = json_dumps(data)
    compressed = gzip.compress(json_bytes)
    return base64.b64encode(compressed).decode()


# Maximum decompressed metadata size (10 MB) — prevents gzip bombs
MAX_METADATA_SIZE = 10 * 1024 * 1024


def _safe_gzip_decompress(data: bytes, max_size: int = MAX_METADATA_SIZE) -> bytes:
    """Decompress gzip data with a size limit to prevent decompression bombs."""
    with gzip.GzipFile(fileobj=__import__("io").BytesIO(data)) as f:
        result = f.read(max_size + 1)
    if len(result) > max_size:
        raise ValueError(f"Decompressed metadata exceeds {max_size} bytes limit")
    return result


def decode_multipart_metadata(encoded: str) -> MultipartMetadata:
    """Decode metadata from base64-compressed JSON."""
    compressed = base64.b64decode(encoded)
    json_bytes = _safe_gzip_decompress(compressed)
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
                internal_parts=[
                    InternalPartMetadata(
                        internal_part_number=ip["ipn"],
                        plaintext_size=ip["ps"],
                        ciphertext_size=ip["cs"],
                        etag=ip["etag"],
                    )
                    for ip in p.get("ip", [])
                ],
            )
            for p in data.get("parts", [])
        ],
    )


async def persist_upload_state(
    s3_client,
    bucket: str,
    key: str,
    upload_id: str,
    wrapped_dek: bytes,
) -> None:
    """Persist DEK to S3 during upload (fallback for Redis failures)."""
    state_key = _internal_upload_key(key, upload_id)
    data = {"dek": base64.b64encode(wrapped_dek).decode()}

    logger.info(
        "PERSIST_UPLOAD_STATE",
        bucket=bucket,
        key=key,
        upload_id=upload_id[:20] + "..." if len(upload_id) > 20 else upload_id,
        state_key=state_key[:60] + "..." if len(state_key) > 60 else state_key,
    )

    await s3_client.put_object(
        bucket=bucket,
        key=state_key,
        body=json_dumps(data),
        content_type="application/json",
    )

    logger.debug(
        "UPLOAD_STATE_PERSISTED",
        bucket=bucket,
        key=key,
        upload_id=upload_id[:20] + "...",
    )


async def load_upload_state(
    s3_client,
    bucket: str,
    key: str,
    upload_id: str,
    kek: bytes,
) -> bytes | None:
    """Load DEK from S3 for resumed upload.

    Returns the unwrapped DEK, or None if not found.
    """
    from .. import crypto

    state_key = _internal_upload_key(key, upload_id)

    logger.info(
        "LOAD_UPLOAD_STATE",
        bucket=bucket,
        key=key,
        upload_id=upload_id[:20] + "..." if len(upload_id) > 20 else upload_id,
        state_key=state_key[:60] + "..." if len(state_key) > 60 else state_key,
    )

    try:
        response = await s3_client.get_object(bucket, state_key)
        body = await response["Body"].read()
        data = json_loads(body)
        wrapped_dek = base64.b64decode(data["dek"])

        logger.info(
            "UPLOAD_STATE_LOADED",
            bucket=bucket,
            key=key,
            upload_id=upload_id[:20] + "...",
        )
        return crypto.unwrap_key(wrapped_dek, kek)

    except Exception as e:
        logger.warning(
            "LOAD_UPLOAD_STATE_FAILED",
            bucket=bucket,
            key=key,
            upload_id=upload_id[:20] + "...",
            error=str(e),
            error_type=type(e).__name__,
        )
        return None


async def delete_upload_state(
    s3_client,
    bucket: str,
    key: str,
    upload_id: str,
) -> None:
    """Delete persisted upload state from S3."""
    state_key = _internal_upload_key(key, upload_id)

    try:
        await s3_client.delete_object(bucket, state_key)
        logger.debug(
            "UPLOAD_STATE_DELETED_S3",
            bucket=bucket,
            key=key,
            upload_id=upload_id[:20] + "...",
        )
    except Exception as e:
        logger.warning(
            "DELETE_UPLOAD_STATE_FAILED",
            bucket=bucket,
            key=key,
            upload_id=upload_id[:20] + "...",
            error=str(e),
        )


async def save_multipart_metadata(
    s3_client,
    bucket: str,
    key: str,
    meta: MultipartMetadata,
) -> None:
    """Save multipart metadata to S3."""
    meta_key = _internal_meta_key(key)
    encoded = encode_multipart_metadata(meta)

    logger.info(
        "SAVE_METADATA",
        bucket=bucket,
        key=key,
        meta_key=meta_key,
        part_count=meta.part_count,
        total_size_mb=f"{meta.total_plaintext_size / 1024 / 1024:.2f}MB",
        encoded_size=len(encoded),
    )

    try:
        await s3_client.put_object(
            bucket=bucket,
            key=meta_key,
            body=encoded.encode(),
            content_type="application/octet-stream",
        )
        logger.debug("METADATA_SAVED", bucket=bucket, key=key, meta_key=meta_key)
    except Exception as e:
        logger.error(
            "SAVE_METADATA_FAILED",
            bucket=bucket,
            key=key,
            meta_key=meta_key,
            error=str(e),
        )
        raise


async def load_multipart_metadata(
    s3_client,
    bucket: str,
    key: str,
) -> MultipartMetadata | None:
    """Load multipart metadata from S3.

    Checks the new internal prefix first, then falls back to legacy location.
    """
    # Try new location first
    meta_key = _internal_meta_key(key)
    logger.debug("LOAD_METADATA", bucket=bucket, key=key, meta_key=meta_key)

    try:
        response = await s3_client.get_object(bucket, meta_key)
        body = await response["Body"].read()
        encoded = body.decode()
        meta = decode_multipart_metadata(encoded)

        logger.info(
            "METADATA_LOADED",
            bucket=bucket,
            key=key,
            meta_key=meta_key,
            part_count=meta.part_count,
            total_size=meta.total_plaintext_size,
        )
        return meta

    except Exception as e:
        logger.debug(
            "METADATA_NOT_AT_NEW_LOCATION",
            bucket=bucket,
            key=key,
            error=str(e),
        )

    # Fall back to legacy location
    legacy_key = f"{key}{META_SUFFIX_LEGACY}"
    try:
        response = await s3_client.get_object(bucket, legacy_key)
        body = await response["Body"].read()
        encoded = body.decode()
        meta = decode_multipart_metadata(encoded)

        logger.info(
            "METADATA_LOADED_LEGACY",
            bucket=bucket,
            key=key,
            legacy_key=legacy_key,
            part_count=meta.part_count,
        )
        return meta

    except Exception as e:
        logger.debug(
            "NO_MULTIPART_METADATA",
            bucket=bucket,
            key=key,
            error=str(e),
        )
        return None


async def delete_multipart_metadata(
    s3_client,
    bucket: str,
    key: str,
) -> None:
    """Delete multipart metadata from S3 (both new and legacy locations)."""
    import asyncio

    meta_key = _internal_meta_key(key)
    legacy_key = f"{key}{META_SUFFIX_LEGACY}"

    async def safe_delete(k: str, location: str) -> None:
        try:
            await s3_client.delete_object(bucket, k)
            logger.debug("METADATA_DELETED", bucket=bucket, key=key, location=location)
        except Exception as e:
            logger.debug(
                "DELETE_METADATA_FAILED",
                bucket=bucket,
                key=key,
                location=location,
                error=str(e),
            )

    # Delete both locations in parallel
    await asyncio.gather(
        safe_delete(meta_key, "new"),
        safe_delete(legacy_key, "legacy"),
    )


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
