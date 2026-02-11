"""Recovery logic for multipart upload state from S3."""

from collections import defaultdict
from datetime import UTC, datetime

import structlog
from structlog.stdlib import BoundLogger

from .. import crypto
from .manager import MAX_INTERNAL_PARTS_PER_CLIENT
from .metadata import load_upload_state
from .models import InternalPartMetadata, MultipartUploadState, PartMetadata

logger: BoundLogger = structlog.get_logger(__name__)


def _internal_to_client_part(internal_part_number: int) -> int:
    """Convert internal part number to client part number.

    Internal parts are allocated in ranges:
    - Client part 1: internal parts 1-20
    - Client part 2: internal parts 21-40
    - etc.
    """
    return ((internal_part_number - 1) // MAX_INTERNAL_PARTS_PER_CLIENT) + 1


async def reconstruct_upload_state_from_s3(
    s3_client,
    bucket: str,
    key: str,
    upload_id: str,
    kek: bytes,
) -> MultipartUploadState | None:
    """Reconstruct upload state from S3 when Redis state is lost.

    This is a fallback recovery mechanism that:
    1. Loads the DEK from S3 metadata
    2. Lists all uploaded parts from S3
    3. Reconstructs part metadata from the S3 response

    Note: Internal part mapping cannot be perfectly reconstructed without
    the original metadata. Each S3 part is treated as a client part.
    """
    logger.info(
        "RECONSTRUCT_STATE_START",
        bucket=bucket,
        key=key,
        upload_id=upload_id[:20] + "..." if len(upload_id) > 20 else upload_id,
    )

    # Step 1: Load DEK from S3 metadata
    dek = await load_upload_state(s3_client, bucket, key, upload_id, kek)
    if not dek:
        logger.warning(
            "RECONSTRUCT_FAILED_NO_DEK",
            bucket=bucket,
            key=key,
            upload_id=upload_id,
        )
        return None

    # Step 2: List all uploaded parts from S3
    try:
        parts_response = await s3_client.list_parts(bucket, key, upload_id, max_parts=10000)
    except Exception as e:
        logger.error(
            "RECONSTRUCT_LIST_PARTS_FAILED",
            bucket=bucket,
            key=key,
            upload_id=upload_id,
            error=str(e),
        )
        return None

    # Step 3: Group S3 parts by client part number
    s3_parts = parts_response.get("Parts", [])
    logger.debug(
        "RECONSTRUCT_PARTS",
        bucket=bucket,
        key=key,
        upload_id=upload_id,
        s3_parts_count=len(s3_parts),
    )

    # Group internal parts by their client part number
    client_parts: dict[int, list[dict]] = defaultdict(list)
    max_internal_part_number = 0

    for s3_part in s3_parts:
        internal_part_number = s3_part["PartNumber"]
        client_part_number = _internal_to_client_part(internal_part_number)
        client_parts[client_part_number].append(s3_part)
        max_internal_part_number = max(max_internal_part_number, internal_part_number)

    logger.debug(
        "RECONSTRUCT_CLIENT_PARTS",
        bucket=bucket,
        key=key,
        client_parts=sorted(client_parts.keys()),
        internal_to_client_mapping={
            sp["PartNumber"]: _internal_to_client_part(sp["PartNumber"]) for sp in s3_parts
        },
    )

    # Build PartMetadata for each client part
    parts_dict: dict[int, PartMetadata] = {}
    total_plaintext_size = 0

    for client_part_num, internal_s3_parts in client_parts.items():
        # Sort internal parts by part number
        internal_s3_parts.sort(key=lambda p: p["PartNumber"])

        internal_parts_meta = []
        part_plaintext_size = 0
        part_ciphertext_size = 0

        for s3_part in internal_s3_parts:
            internal_num = s3_part["PartNumber"]
            size = s3_part["Size"]
            etag = s3_part["ETag"].strip('"')
            plaintext_size = crypto.plaintext_size(size)

            internal_parts_meta.append(
                InternalPartMetadata(
                    internal_part_number=internal_num,
                    plaintext_size=plaintext_size,
                    ciphertext_size=size,
                    etag=etag,
                )
            )
            part_plaintext_size += plaintext_size
            part_ciphertext_size += size

        # Use first internal part's etag as the client part etag
        # (In normal operation, client etag is MD5 of plaintext, which we can't compute)
        first_etag = internal_s3_parts[0]["ETag"].strip('"') if internal_s3_parts else ""

        part_meta = PartMetadata(
            part_number=client_part_num,
            plaintext_size=part_plaintext_size,
            ciphertext_size=part_ciphertext_size,
            etag=first_etag,
            md5="",  # Not available from ListParts
            internal_parts=internal_parts_meta,
        )

        parts_dict[client_part_num] = part_meta
        total_plaintext_size += part_plaintext_size

    # Step 4: Create reconstructed state
    state = MultipartUploadState(
        bucket=bucket,
        key=key,
        upload_id=upload_id,
        dek=dek,
        parts=parts_dict,
        total_plaintext_size=total_plaintext_size,
        next_internal_part_number=max_internal_part_number + 1,
        created_at=datetime.now(UTC),
    )

    logger.info(
        "RECONSTRUCT_STATE_SUCCESS",
        bucket=bucket,
        key=key,
        upload_id=upload_id[:20] + "..." if len(upload_id) > 20 else upload_id,
        parts_recovered=len(parts_dict),
        part_numbers=sorted(parts_dict.keys()),
        total_plaintext_size=total_plaintext_size,
        next_internal=state.next_internal_part_number,
    )

    return state
