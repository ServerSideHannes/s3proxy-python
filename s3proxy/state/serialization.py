"""JSON serialization for upload state."""

import base64
from datetime import datetime

import orjson
import structlog
from structlog.stdlib import BoundLogger

from .models import (
    InternalPartMetadata,
    MultipartUploadState,
    PartMetadata,
)

logger: BoundLogger = structlog.get_logger(__name__)


def json_dumps(obj: dict) -> bytes:
    return orjson.dumps(obj)


def json_loads(data: bytes) -> dict:
    return orjson.loads(data)


def serialize_upload_state(state: MultipartUploadState) -> bytes:
    """Serialize upload state to JSON bytes for Redis."""
    part_numbers = sorted(state.parts.keys())

    data = {
        "dek": base64.b64encode(state.dek).decode(),
        "bucket": state.bucket,
        "key": state.key,
        "upload_id": state.upload_id,
        "created_at": state.created_at.isoformat(),
        "total_plaintext_size": state.total_plaintext_size,
        "next_internal_part_number": state.next_internal_part_number,
        "parts": {
            str(pn): {
                "part_number": p.part_number,
                "plaintext_size": p.plaintext_size,
                "ciphertext_size": p.ciphertext_size,
                "etag": p.etag,
                "md5": p.md5,
                "internal_parts": [
                    {
                        "internal_part_number": ip.internal_part_number,
                        "plaintext_size": ip.plaintext_size,
                        "ciphertext_size": ip.ciphertext_size,
                        "etag": ip.etag,
                    }
                    for ip in p.internal_parts
                ],
            }
            for pn, p in state.parts.items()
        },
    }

    logger.debug(
        "SERIALIZE_STATE",
        bucket=state.bucket,
        key=state.key,
        upload_id=state.upload_id,
        part_count=len(part_numbers),
        part_numbers=part_numbers,
        next_internal=state.next_internal_part_number,
    )

    return json_dumps(data)


def deserialize_upload_state(data: bytes) -> MultipartUploadState | None:
    """Deserialize upload state from Redis JSON bytes.

    Returns None if data is corrupted or missing required fields.
    """
    try:
        obj = json_loads(data)
    except (ValueError, TypeError) as e:
        logger.error("DESERIALIZE_FAILED: invalid JSON", error=str(e))
        return None

    # Validate required fields
    required_fields = ["dek", "bucket", "key", "upload_id", "created_at"]
    if not all(f in obj for f in required_fields):
        logger.error(
            "DESERIALIZE_FAILED: missing fields",
            present=list(obj.keys()),
            required=required_fields,
        )
        return None

    redis_part_keys = sorted([int(k) for k in obj.get("parts", {})])
    logger.debug(
        "DESERIALIZE_STATE",
        bucket=obj.get("bucket"),
        key=obj.get("key"),
        upload_id=obj.get("upload_id"),
        part_count=len(redis_part_keys),
        part_numbers=redis_part_keys,
        next_internal=obj.get("next_internal_part_number", 1),
    )

    try:
        parts = {
            int(pn): PartMetadata(
                part_number=p["part_number"],
                plaintext_size=p["plaintext_size"],
                ciphertext_size=p["ciphertext_size"],
                etag=p["etag"],
                md5=p.get("md5", ""),
                internal_parts=[
                    InternalPartMetadata(
                        internal_part_number=ip["internal_part_number"],
                        plaintext_size=ip["plaintext_size"],
                        ciphertext_size=ip["ciphertext_size"],
                        etag=ip["etag"],
                    )
                    for ip in p.get("internal_parts", [])
                ],
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
            next_internal_part_number=obj.get("next_internal_part_number", 1),
        )
    except (KeyError, TypeError, ValueError) as e:
        logger.error(
            "DESERIALIZE_FAILED: bad data",
            bucket=obj.get("bucket"),
            key=obj.get("key"),
            error=str(e),
        )
        return None
