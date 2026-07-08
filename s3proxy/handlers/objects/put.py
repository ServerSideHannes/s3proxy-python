"""PUT object operations with encryption support."""

import base64
import hashlib
from collections.abc import AsyncIterator
from typing import Any

import structlog
from fastapi import Request, Response
from starlette.requests import ClientDisconnect
from structlog.stdlib import BoundLogger

from ... import crypto
from ...client import S3Client, S3Credentials
from ...disconnect import ClientDisconnectError
from ...errors import S3Error
from ...signature import verify_deferred_payload_hash
from ...state import (
    MultipartMetadata,
    PartMetadata,
    save_multipart_metadata,
)
from ...streaming import decode_aws_chunked, decode_aws_chunked_stream
from ...utils import etag_matches
from ..base import BaseHandler

logger: BoundLogger = structlog.get_logger(__name__)

_STREAM_CHUNK = 64 * 1024


async def _iter_request_body(request: Request, decode_chunked: bool) -> AsyncIterator[bytes]:
    """Single-pass body iterator: preloaded small body, else live stream.

    Avoids reading request.stream() after request.body() pinned the payload in
    Starlette's cache (double buffer on large uploads).
    """
    if decode_chunked:
        async for chunk in decode_aws_chunked_stream(request):
            yield chunk
        return
    preloaded = getattr(request.state, "s3proxy_preloaded_body", None)
    if isinstance(preloaded, (bytes, bytearray)) and len(preloaded) > 0:
        for offset in range(0, len(preloaded), _STREAM_CHUNK):
            yield preloaded[offset : offset + _STREAM_CHUNK]
        return
    async for chunk in request.stream():
        yield chunk


class PutObjectMixin(BaseHandler):
    async def handle_put_object(self, request: Request, creds: S3Credentials) -> Response:
        bucket, key = self._parse_path(request.url.path)
        async with self._client(creds) as client:
            # Check If-None-Match header (prevents overwriting existing objects)
            if_none_match = request.headers.get("if-none-match")
            if if_none_match:
                try:
                    head_resp = await client.head_object(bucket, key)
                    # Object exists - check if etag matches
                    if if_none_match.strip() == "*":
                        # * means fail if object exists at all
                        raise S3Error.precondition_failed(
                            "At least one of the pre-conditions you specified did not hold"
                        )
                    # Check specific etag match
                    metadata = head_resp.get("Metadata", {})
                    existing_etag = self._get_effective_etag(metadata, head_resp.get("ETag", ""))
                    if etag_matches(existing_etag, if_none_match):
                        raise S3Error.precondition_failed(
                            "At least one of the pre-conditions you specified did not hold"
                        )
                except S3Error:
                    raise
                except Exception:
                    # Object doesn't exist - proceed with upload
                    pass
            content_type = request.headers.get("content-type", "application/octet-stream")
            content_sha = request.headers.get("x-amz-content-sha256", "")
            content_encoding = request.headers.get("content-encoding", "")
            cache_control = request.headers.get("cache-control")
            expires = request.headers.get("expires")
            tagging = request.headers.get("x-amz-tagging")

            try:
                content_length = int(request.headers.get("content-length", "0"))
            except ValueError:
                content_length = 0
            is_unsigned = content_sha == "UNSIGNED-PAYLOAD"
            is_streaming_sig = content_sha.startswith("STREAMING-")
            needs_chunked_decode = "aws-chunked" in content_encoding or is_streaming_sig

            # Stream large uploads to avoid buffering
            if is_unsigned or is_streaming_sig or content_length > crypto.MAX_BUFFER_SIZE:
                logger.debug(
                    "PUT_STREAMING",
                    bucket=bucket,
                    key=key,
                    content_length=content_length,
                    content_length_mb=round(content_length / 1024 / 1024, 2),
                    is_unsigned=is_unsigned,
                    is_streaming_sig=is_streaming_sig,
                )
                is_verifiable = content_sha and not (is_unsigned or is_streaming_sig)
                expected_sha = content_sha if is_verifiable else None
                return await self._put_streaming(
                    request,
                    client,
                    bucket,
                    key,
                    content_type,
                    needs_chunked_decode,
                    expected_sha,
                    cache_control=cache_control,
                    expires=expires,
                    tagging=tagging,
                )

            # Buffer small signed uploads
            return await self._put_buffered(
                request,
                client,
                bucket,
                key,
                content_type,
                content_length,
                needs_chunked_decode,
                cache_control=cache_control,
                expires=expires,
                tagging=tagging,
            )

    async def _put_buffered(
        self,
        request: Request,
        client: S3Client,
        bucket: str,
        key: str,
        content_type: str,
        content_length: int,
        needs_chunked_decode: bool,
        cache_control: str | None = None,
        expires: str | None = None,
        tagging: str | None = None,
    ) -> Response:
        logger.debug(
            "PUT_BUFFERED",
            bucket=bucket,
            key=key,
            content_length=content_length,
            content_length_mb=round(content_length / 1024 / 1024, 2),
        )

        preloaded = getattr(request.state, "s3proxy_preloaded_body", None)
        if isinstance(preloaded, (bytes, bytearray)) and len(preloaded) > 0:
            body = bytes(preloaded)
        else:
            body = await request.body()
        if needs_chunked_decode:
            body = decode_aws_chunked(body)

        kid, kek = self.keyring.key_for(client.credentials.access_key)
        encrypted = crypto.encrypt_object(body, kek)
        logger.debug(
            "PUT_ENCRYPTED",
            bucket=bucket,
            key=key,
            kid=kid,
            plaintext_mb=round(len(body) / 1024 / 1024, 2),
            ciphertext_mb=round(len(encrypted.ciphertext) / 1024 / 1024, 2),
        )
        etag = hashlib.md5(body, usedforsecurity=False).hexdigest()

        await client.put_object(
            bucket,
            key,
            encrypted.ciphertext,
            metadata={
                self.settings.dektag_name: base64.b64encode(encrypted.wrapped_dek).decode(),
                self.settings.kidtag_name: kid,
                "client-etag": etag,
                "plaintext-size": str(len(body)),
            },
            content_type=content_type,
            cache_control=cache_control,
            expires=expires,
            tagging=tagging,
        )
        return Response(headers={"ETag": f'"{etag}"'})

    async def _put_streaming(
        self,
        request: Request,
        client: S3Client,
        bucket: str,
        key: str,
        content_type: str,
        decode_chunked: bool = False,
        expected_sha256: str | None = None,
        cache_control: str | None = None,
        expires: str | None = None,
        tagging: str | None = None,
    ) -> Response:
        kid, kek = self.keyring.key_for(client.credentials.access_key)
        dek = crypto.generate_dek()
        wrapped_dek = crypto.wrap_key(dek, kek)

        resp = await client.create_multipart_upload(
            bucket,
            key,
            content_type=content_type,
            cache_control=cache_control,
            expires=expires,
            tagging=tagging,
        )
        upload_id = resp["UploadId"]

        parts_meta: list[PartMetadata] = []
        parts_complete: list[dict[str, Any]] = []
        total_plaintext_size = 0
        part_num = 0
        md5_hash = hashlib.md5(usedforsecurity=False)
        deferred_sig = getattr(request.state, "s3proxy_deferred_sig", False) is True
        sha256_hash = hashlib.sha256() if (expected_sha256 or deferred_sig) else None
        buffer = bytearray()

        async def upload_part(data: bytes) -> None:
            nonlocal part_num
            part_num += 1
            nonce = crypto.derive_part_nonce(upload_id, part_num)
            data_len = len(data)
            data_md5 = hashlib.md5(data, usedforsecurity=False).hexdigest()
            ciphertext = crypto.encrypt(data, dek, nonce)
            cipher_len = len(ciphertext)
            del data  # Free memory

            part_resp = await client.upload_part(bucket, key, upload_id, part_num, ciphertext)
            etag = part_resp["ETag"].strip('"')

            logger.debug(
                "PUT_STREAMING_PART",
                bucket=bucket,
                key=key,
                upload_id=upload_id,
                part_number=part_num,
                plaintext_mb=round(data_len / 1024 / 1024, 2),
                ciphertext_mb=round(cipher_len / 1024 / 1024, 2),
            )

            parts_meta.append(PartMetadata(part_num, data_len, cipher_len, etag, data_md5))
            parts_complete.append({"PartNumber": part_num, "ETag": part_resp["ETag"]})

        try:
            logger.info(
                "PUT_STREAMING_START",
                bucket=bucket,
                key=key,
                upload_id=upload_id,
                decode_chunked=decode_chunked,
                verify_sha256=expected_sha256 is not None,
            )

            stream_source = _iter_request_body(request, decode_chunked)

            async for chunk in stream_source:
                buffer.extend(chunk)
                md5_hash.update(chunk)
                if sha256_hash:
                    sha256_hash.update(chunk)
                total_plaintext_size += len(chunk)

                # Upload when buffer reaches threshold
                while len(buffer) >= crypto.MAX_BUFFER_SIZE:
                    part_data = bytes(buffer[: crypto.MAX_BUFFER_SIZE])
                    del buffer[: crypto.MAX_BUFFER_SIZE]
                    await upload_part(part_data)

            # Upload remaining buffer
            if buffer:
                part_data = bytes(buffer)
                buffer.clear()
                await upload_part(part_data)

            # Verify SHA256 if provided, or deferred SigV4 after streaming hash
            if deferred_sig:
                try:
                    verify_deferred_payload_hash(
                        request, request.app.state.verifier, sha256_hash.hexdigest()
                    )
                except S3Error:
                    await client.abort_multipart_upload(bucket, key, upload_id)
                    raise
            elif expected_sha256 is not None:
                computed_sha256 = sha256_hash.hexdigest()
                if computed_sha256 != expected_sha256:
                    logger.error(
                        "PUT_SHA256_MISMATCH",
                        bucket=bucket,
                        key=key,
                        upload_id=upload_id,
                        expected=expected_sha256,
                        computed=computed_sha256,
                    )
                    await client.abort_multipart_upload(bucket, key, upload_id)
                    raise S3Error.signature_does_not_match(
                        f"SHA256 mismatch: {computed_sha256} != {expected_sha256}"
                    )

            # Complete upload
            await client.complete_multipart_upload(bucket, key, upload_id, parts_complete)
            await save_multipart_metadata(
                client,
                bucket,
                key,
                MultipartMetadata(
                    version=2,
                    part_count=len(parts_meta),
                    total_plaintext_size=total_plaintext_size,
                    parts=parts_meta,
                    wrapped_dek=wrapped_dek,
                    kid=kid,
                ),
            )

            etag = md5_hash.hexdigest()
            logger.info(
                "PUT_STREAMING_COMPLETE",
                bucket=bucket,
                key=key,
                upload_id=upload_id,
                part_count=len(parts_meta),
                total_mb=round(total_plaintext_size / 1024 / 1024, 2),
            )
            return Response(headers={"ETag": f'"{etag}"'})

        except ClientDisconnectError, ClientDisconnect:
            await self._safe_abort(client, bucket, key, upload_id)
            raise ClientDisconnectError.raised() from None
        except S3Error:
            raise
        except Exception as e:
            logger.error(
                "PUT_STREAMING_FAILED",
                bucket=bucket,
                key=key,
                upload_id=upload_id,
                error_type=type(e).__name__,
                error=str(e),
            )
            await self._safe_abort(client, bucket, key, upload_id)
            raise S3Error.internal_error(str(e)) from e
