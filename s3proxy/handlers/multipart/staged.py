"""Generation-bound multipart writes with immutable, verified part attempts.

Each client part is encrypted into a private temporary object. Only a validated,
completed attempt is published in upload state. Final assembly uses server-side
copies in client order, so replacements cannot overwrite accepted backend bytes
and arbitrary arrival order never changes the plaintext order.
"""

import contextlib
import hashlib
import math
import uuid
from collections.abc import AsyncIterator
from urllib.parse import quote

from fastapi import Request, Response

from ... import crypto, xml_responses
from ...errors import S3Error
from ...signature import verify_payload_hash
from ...state import InternalPartMetadata, MultipartMetadata, PartMetadata
from ...state.metadata import (
    INTERNAL_PREFIX,
    save_multipart_metadata,
)


async def stage_part(
    handler,
    request: Request,
    client,
    state,
    part_number: int,
    source: AsyncIterator[bytes],
    *,
    verify: bool = True,
) -> PartMetadata:
    if not 1 <= part_number <= 10000:
        raise S3Error.invalid_part("PartNumber must be between 1 and 10000")
    # Local import avoids mixing the legacy pipeline's implementation into ours.
    from .upload_part import _PlaintextReader

    state = await handler.multipart_manager.begin_write(state.bucket, state.key, state.upload_id)
    stage_key = f"{INTERNAL_PREFIX}attempts/{state.generation}/{uuid.uuid4().hex}"
    created = await client.create_multipart_upload(state.bucket, stage_key)
    stage_id = created["UploadId"]
    md5 = hashlib.md5(usedforsecurity=False)
    sha = hashlib.sha256()
    reader = _PlaintextReader(source)
    parts = []
    uploaded = []
    total = 0
    publishing = False
    try:
        while data := await reader.read(crypto.FRAME_PLAINTEXT_SIZE):
            md5.update(data)
            sha.update(data)
            total += len(data)
            if total > 5 * 1024**3:
                raise S3Error.invalid_argument("Client parts cannot exceed 5 GiB")
            ciphertext = crypto.encrypt(data, state.dek)
            number = len(parts) + 1
            response = await handler._upload_part_with_retry(
                client,
                state.bucket,
                stage_key,
                stage_id,
                number,
                ciphertext,
                client_part_num=part_number,
            )
            parts.append(
                InternalPartMetadata(
                    number, len(data), len(ciphertext), response["ETag"].strip('"')
                )
            )
            uploaded.append({"PartNumber": number, "ETag": response["ETag"]})
            del data, ciphertext
        if verify:
            verify_payload_hash(request, sha.hexdigest())
            expected = request.headers.get("content-length")
            chunked = "aws-chunked" in request.headers.get(
                "content-encoding", ""
            ) or request.headers.get("x-amz-content-sha256", "").startswith("STREAMING-")
            if expected is not None and not chunked and int(expected) != total:
                raise S3Error.bad_request("Content-Length does not match uploaded body")
        if not parts:
            ciphertext = crypto.encrypt(b"", state.dek)
            response = await client.upload_part(state.bucket, stage_key, stage_id, 1, ciphertext)
            parts.append(InternalPartMetadata(1, 0, len(ciphertext), response["ETag"].strip('"')))
            uploaded.append({"PartNumber": 1, "ETag": response["ETag"]})
        await client.complete_multipart_upload(state.bucket, stage_key, stage_id, uploaded)
        part = PartMetadata(
            part_number,
            total,
            sum(p.ciphertext_size for p in parts),
            md5.hexdigest(),
            md5.hexdigest(),
            internal_parts=parts,
            staging_key=stage_key,
        )
        # The old attempt remains immutable. Replaced attempts are cleaned by a
        # bucket lifecycle rule; deleting here could race a Complete snapshot.
        publishing = True
        await handler.multipart_manager.add_part(state.bucket, state.key, state.upload_id, part)
        return part
    except BaseException:
        with contextlib.suppress(Exception):
            await handler._safe_abort(client, state.bucket, stage_key, stage_id)
        # A cancelled Redis write may already have published the reference.
        if not publishing:
            with contextlib.suppress(Exception):
                await client.delete_object(state.bucket, stage_key)
        raise
    finally:
        close = getattr(source, "aclose", None)
        if close is not None:
            await close()


def select_parts(handler, body: bytes, state) -> list[PartMetadata]:
    requested = handler._parse_client_parts(body)
    numbers = [p["PartNumber"] for p in requested]
    if not numbers or numbers != sorted(set(numbers)):
        raise S3Error.invalid_part("Parts must be unique and ordered")
    parts = []
    for item in requested:
        part = state.parts.get(item["PartNumber"])
        if part is None or part.md5 != item["ETag"].strip('"') or not part.staging_key:
            raise S3Error.invalid_part("Part or ETag does not match an accepted upload")
        parts.append(part)
    if any(p.plaintext_size < crypto.MIN_PART_SIZE for p in parts[:-1]):
        raise S3Error.entity_too_small("All client parts except the last must be at least 5 MiB")
    return parts


async def complete_staged(handler, request, client, state) -> Response:
    parts = select_parts(handler, await request.body(), state)
    # One backend copy for almost all legal client parts. Split ciphertext just
    # above S3's 5 GiB CopyPart limit into balanced ranges, never a tiny tail.
    max_copy = 5 * 1024**3
    counts = [math.ceil(p.ciphertext_size / max_copy) for p in parts]
    if sum(counts) > 10000:
        raise S3Error.invalid_request("Encrypted upload exceeds S3's 10000 backend part limit")
    copies = []
    for part, count in zip(parts, counts, strict=True):
        size = math.ceil(part.ciphertext_size / count)
        for start in range(0, part.ciphertext_size, size):
            number = len(copies) + 1
            end = min(start + size, part.ciphertext_size) - 1
            response = await copy_with_retry(
                client,
                state.bucket,
                state.key,
                state.upload_id,
                number,
                f"{state.bucket}/{quote(part.staging_key, safe='/')}",
                f"bytes={start}-{end}",
            )
            copies.append({"PartNumber": number, "ETag": response["CopyPartResult"]["ETag"]})
    # ETag is consistent across Complete, HEAD, GET and LIST; no size-only hash.
    etag = (
        hashlib.md5(
            b"".join(bytes.fromhex(p.md5) for p in parts), usedforsecurity=False
        ).hexdigest()
        + f"-{len(parts)}"
    )
    wrapped = crypto.wrap_key(state.dek, handler.keyring.key_by_id(state.kid))
    meta = MultipartMetadata(
        version=3,
        generation=state.generation,
        upload_id=state.upload_id,
        upload_bucket=state.bucket,
        upload_key=state.key,
        client_etag=etag,
        parts=parts,
        part_count=len(parts),
        total_plaintext_size=sum(p.plaintext_size for p in parts),
        wrapped_dek=wrapped,
        kid=state.kid,
    )
    # The object's immutable generation pointer becomes visible only at Complete.
    # Persist its full decryption map first; failed Complete is safely retryable.
    await save_multipart_metadata(client, state.bucket, state.key, meta)
    await handler._complete_multipart_upload_with_retry(
        client, state.bucket, state.key, state.upload_id, copies, parts
    )
    location = f"{handler.settings.s3_endpoint}/{state.bucket}/{state.key}"
    return Response(
        content=xml_responses.complete_multipart(location, state.bucket, state.key, etag),
        media_type="application/xml",
    )


async def copy_with_retry(
    client, bucket, key, upload_id, number, source, byte_range, *, if_match=None
):
    import asyncio

    from ..base import SOURCE_READ_ATTEMPTS, SOURCE_READ_BACKOFF_SEC, is_retryable_source_error

    for attempt in range(SOURCE_READ_ATTEMPTS):
        try:
            return await client.upload_part_copy(
                bucket,
                key,
                upload_id,
                number,
                source,
                byte_range,
                **({"copy_source_if_match": if_match} if if_match else {}),
            )
        except Exception as error:
            if attempt + 1 == SOURCE_READ_ATTEMPTS or not is_retryable_source_error(error):
                raise
            await asyncio.sleep(SOURCE_READ_BACKOFF_SEC * 2**attempt)


async def cleanup_attempts(handler, client, state):
    """Best-effort terminal cleanup; lifecycle expiry covers crashes and late writers."""
    try:
        token = None
        while True:
            page = await client.list_objects_v2(
                state.bucket,
                prefix=f"{INTERNAL_PREFIX}attempts/{state.generation}/",
                continuation_token=token,
            )
            keys = [o["Key"] for o in page.get("Contents", [])]
            if keys:
                await client.delete_objects(state.bucket, [{"Key": key} for key in keys])
            if not page.get("IsTruncated"):
                break
            token = page["NextContinuationToken"]
    except Exception as error:
        import structlog

        structlog.get_logger(__name__).warning("STAGING_CLEANUP_FAILED", error=str(error))


async def stage_ciphertext_copy(handler, client, state, part_number, source, head, segments, etag):
    """Snapshot a whole encrypted source without changing its ciphertext/nonces."""
    if not 1 <= part_number <= 10000 or sum(p.plaintext_size for p in segments) > 5 * 1024**3:
        raise S3Error.invalid_argument("Invalid part number or source exceeds 5 GiB")
    stage_key = f"{INTERNAL_PREFIX}attempts/{state.generation}/{uuid.uuid4().hex}"
    upload = await client.create_multipart_upload(state.bucket, stage_key)
    stage_id = upload["UploadId"]
    publishing = False
    try:
        total = head["ContentLength"]
        count = max(1, math.ceil(total / (5 * 1024**3)))
        step = math.ceil(total / count)
        copied = []
        for start in range(0, total, step):
            number = len(copied) + 1
            result = await copy_with_retry(
                client,
                state.bucket,
                stage_key,
                stage_id,
                number,
                source,
                f"bytes={start}-{min(start + step, total) - 1}",
                if_match=head["ETag"],
            )
            copied.append({"PartNumber": number, "ETag": result["CopyPartResult"]["ETag"]})
        await client.complete_multipart_upload(state.bucket, stage_key, stage_id, copied)
        # S3 ETags are opaque; keep a 128-bit token for multipart ETag composition.
        if len(etag) != 32 or any(c not in "0123456789abcdef" for c in etag):
            etag = hashlib.md5(etag.encode(), usedforsecurity=False).hexdigest()
        part = PartMetadata(
            part_number,
            sum(p.plaintext_size for p in segments),
            total,
            etag,
            etag,
            internal_parts=[
                InternalPartMetadata(i, p.plaintext_size, p.ciphertext_size, "")
                for i, p in enumerate(segments, 1)
            ],
            staging_key=stage_key,
        )
        publishing = True
        await handler.multipart_manager.add_part(state.bucket, state.key, state.upload_id, part)
        return part
    except BaseException:
        with contextlib.suppress(Exception):
            await handler._safe_abort(client, state.bucket, stage_key, stage_id)
        if not publishing:
            with contextlib.suppress(Exception):
                await client.delete_object(state.bucket, stage_key)
        raise
