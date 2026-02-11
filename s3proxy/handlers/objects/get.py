"""GET object operations with encryption support."""

import base64
from collections.abc import AsyncIterator
from itertools import accumulate
from typing import Any

import structlog
from botocore.exceptions import ClientError
from fastapi import Request, Response
from fastapi.responses import StreamingResponse
from structlog.stdlib import BoundLogger

from ... import concurrency, crypto
from ...concurrency import MAX_BUFFER_SIZE
from ...errors import S3Error
from ...s3client import S3Client, S3Credentials
from ...state import (
    MultipartMetadata,
    calculate_part_range,
    load_multipart_metadata,
)
from ...streaming import STREAM_CHUNK_SIZE
from ...utils import format_http_date
from ..base import BaseHandler

logger: BoundLogger = structlog.get_logger(__name__)


def _format_expires(expires: Any) -> str:
    return format_http_date(expires) if hasattr(expires, "strftime") else str(expires)


class GetObjectMixin(BaseHandler):
    async def handle_get_object(self, request: Request, creds: S3Credentials) -> Response:
        bucket, key = self._parse_path(request.url.path)
        async with self._client(creds) as client:
            range_header = request.headers.get("range")
            if_match, if_none_match, if_modified_since, if_unmodified_since = (
                self._extract_conditional_headers(request)
            )

            try:
                head_resp = await client.head_object(bucket, key)
                last_modified = format_http_date(head_resp.get("LastModified"))
                last_modified_dt = head_resp.get("LastModified")

                # Get the effective ETag (client-etag for encrypted, S3 etag otherwise)
                metadata = head_resp.get("Metadata", {})
                effective_etag = self._get_effective_etag(metadata, head_resp.get("ETag", ""))

                # Check conditional headers (inherited from BaseHandler)
                cond_response = self._check_conditional_headers(
                    effective_etag,
                    last_modified_dt,
                    last_modified,
                    if_match,
                    if_none_match,
                    if_modified_since,
                    if_unmodified_since,
                )
                if cond_response:
                    return cond_response

                if meta := await load_multipart_metadata(client, bucket, key):
                    response = await self._get_multipart(
                        client, bucket, key, meta, range_header, last_modified, creds
                    )
                else:
                    response = await self._get_single(
                        client, bucket, key, range_header, head_resp, last_modified
                    )

                # Add ETag header
                response.headers["ETag"] = f'"{effective_etag}"'

                # Add user metadata (x-amz-meta-*), excluding internal keys
                internal_keys = {
                    self.settings.dektag_name.lower(),
                    "client-etag",
                    "plaintext-size",
                }
                for k, v in metadata.items():
                    if k.lower() not in internal_keys:
                        response.headers[f"x-amz-meta-{k}"] = v

                return response
            except ClientError as e:
                self._raise_s3_error(e, bucket, key)

    async def _get_single(
        self,
        client: S3Client,
        bucket: str,
        key: str,
        range_header: str | None,
        head_resp: dict,
        last_modified: str | None,
    ) -> Response:
        metadata = head_resp.get("Metadata", {})
        wrapped_dek_b64 = metadata.get(self.settings.dektag_name)

        if not wrapped_dek_b64:
            # Unencrypted - stream directly from S3
            return await self._stream_unencrypted(
                client, bucket, key, range_header, head_resp, last_modified
            )

        # Encrypted single-object - decrypt in memory
        return await self._decrypt_single_object(
            client, bucket, key, range_header, head_resp, last_modified, wrapped_dek_b64
        )

    async def _stream_unencrypted(
        self,
        client: S3Client,
        bucket: str,
        key: str,
        range_header: str | None,
        head_resp: dict,
        last_modified: str | None,
    ) -> Response:
        logger.info("GET_UNENCRYPTED", bucket=bucket, key=key)
        resp = await client.get_object(bucket, key, range_header=range_header)
        s3_body = resp["Body"]

        headers = self._build_response_headers(resp, last_modified)

        async def stream_s3_body() -> AsyncIterator[bytes]:
            async with s3_body:
                while chunk := await s3_body.read(STREAM_CHUNK_SIZE):
                    yield chunk

        if "ContentRange" in resp:
            headers["Content-Range"] = resp["ContentRange"]
            return StreamingResponse(stream_s3_body(), status_code=206, headers=headers)
        return StreamingResponse(stream_s3_body(), headers=headers)

    async def _decrypt_single_object(
        self,
        client: S3Client,
        bucket: str,
        key: str,
        range_header: str | None,
        head_resp: dict,
        last_modified: str | None,
        wrapped_dek_b64: str,
    ) -> Response:
        logger.info("GET_ENCRYPTED_SINGLE", bucket=bucket, key=key)
        resp = await client.get_object(bucket, key)
        content_length = resp.get("ContentLength", 0)

        # Encrypted decrypts buffer ciphertext + plaintext simultaneously.
        # Acquire additional memory beyond the initial MAX_BUFFER_SIZE reservation.
        additional = max(0, content_length * 2 - MAX_BUFFER_SIZE)
        extra_reserved = 0
        try:
            if additional > 0:
                extra_reserved = await concurrency.try_acquire_memory(additional)

            wrapped_dek = base64.b64decode(wrapped_dek_b64)
            async with resp["Body"] as body:
                ciphertext = await body.read()
            plaintext = crypto.decrypt_object(ciphertext, wrapped_dek, self.settings.kek)
            del ciphertext

            content_type = head_resp.get("ContentType", "application/octet-stream")
            cache_control = head_resp.get("CacheControl")
            expires = head_resp.get("Expires")

            if range_header:
                start, end = self._parse_range(range_header, len(plaintext))
                headers = self._build_headers(
                    content_type=content_type,
                    content_length=end - start + 1,
                    last_modified=last_modified,
                    cache_control=cache_control,
                    expires=expires,
                )
                headers["Content-Range"] = f"bytes {start}-{end}/{len(plaintext)}"
                return Response(
                    content=plaintext[start : end + 1], status_code=206, headers=headers
                )

            headers = self._build_headers(
                content_type=content_type,
                content_length=len(plaintext),
                last_modified=last_modified,
                cache_control=cache_control,
                expires=expires,
            )
            return Response(content=plaintext, headers=headers)
        finally:
            if extra_reserved > 0:
                await concurrency.release_memory(extra_reserved)

    async def _get_multipart(
        self,
        client: S3Client,
        bucket: str,
        key: str,
        meta: MultipartMetadata,
        range_header: str | None,
        last_modified: str | None,
        creds: S3Credentials,
    ) -> Response:
        dek = crypto.unwrap_key(meta.wrapped_dek, self.settings.kek)
        total = meta.total_plaintext_size
        start, end = self._parse_range(range_header, total) if range_header else (0, total - 1)
        parts = calculate_part_range(meta.parts, start, end)

        # Build lookup: part_number -> (part_metadata, ciphertext_offset)
        sorted_parts = sorted(meta.parts, key=lambda p: p.part_number)
        offsets = [0, *accumulate(p.ciphertext_size for p in sorted_parts)]
        part_info = {p.part_number: (p, offsets[i]) for i, p in enumerate(sorted_parts)}

        # Get actual object size and content type
        actual_size, content_type, cache_control, expires_val = await self._get_object_info(
            client, bucket, key, meta
        )

        # Create stream generator
        stream = self._create_multipart_stream(
            creds, bucket, key, parts, part_info, dek, actual_size, start, end
        )

        # Build response
        length = sum(e - s + 1 for _, s, e in parts)
        headers = self._build_headers(
            content_type=content_type,
            content_length=length,
            last_modified=last_modified,
            cache_control=cache_control,
            expires=expires_val,
        )
        if range_header:
            headers["Content-Range"] = f"bytes {start}-{end}/{total}"
            return StreamingResponse(stream, status_code=206, headers=headers)
        return StreamingResponse(stream, headers=headers)

    async def _get_object_info(
        self, client: S3Client, bucket: str, key: str, meta: MultipartMetadata
    ) -> tuple[int | None, str, str | None, str | None]:
        try:
            head_resp = await client.head_object(bucket, key)
            actual_size = head_resp.get("ContentLength", 0)
            content_type = head_resp.get("ContentType", "application/octet-stream")
            cache_control = head_resp.get("CacheControl")
            expires_val = head_resp.get("Expires")
            logger.debug(
                "GET_MULTIPART_INFO",
                bucket=bucket,
                key=key,
                plaintext_total=meta.total_plaintext_size,
                actual_object_size=actual_size,
                part_count=len(meta.parts),
            )
            return actual_size, content_type, cache_control, expires_val
        except Exception as e:
            logger.warning("GET_MULTIPART_INFO_FAILED", bucket=bucket, key=key, error=str(e))
            return None, "application/octet-stream", None, None

    async def _create_multipart_stream(
        self,
        creds: S3Credentials,
        bucket: str,
        key: str,
        parts: list,
        part_info: dict,
        dek: bytes,
        actual_size: int | None,
        start: int,
        end: int,
    ) -> AsyncIterator[bytes]:
        async with self._client(creds) as stream_client:
            for _, (part_num, off_start, off_end) in enumerate(parts):
                part_meta, ct_start = part_info[part_num]

                if part_meta.internal_parts:
                    async for chunk in self._stream_internal_parts(
                        stream_client,
                        bucket,
                        key,
                        part_num,
                        part_meta,
                        ct_start,
                        off_start,
                        off_end,
                        dek,
                        actual_size,
                    ):
                        yield chunk
                else:
                    chunk = await self._fetch_and_decrypt_part(
                        stream_client,
                        bucket,
                        key,
                        part_num,
                        part_meta,
                        ct_start,
                        off_start,
                        off_end,
                        dek,
                        actual_size,
                    )
                    yield chunk

    async def _stream_internal_parts(
        self,
        client: S3Client,
        bucket: str,
        key: str,
        part_num: int,
        part_meta,
        ct_start: int,
        off_start: int,
        off_end: int,
        dek: bytes,
        actual_size: int | None,
    ) -> AsyncIterator[bytes]:
        logger.debug(
            "GET_INTERNAL_PARTS",
            bucket=bucket,
            key=key,
            part_number=part_num,
            internal_part_count=len(part_meta.internal_parts),
        )

        ct_offset = ct_start
        pt_offset = 0

        for ip in sorted(part_meta.internal_parts, key=lambda p: p.internal_part_number):
            pt_end = pt_offset + ip.plaintext_size - 1

            # Skip parts before our range
            if pt_end < off_start:
                ct_offset += ip.ciphertext_size
                pt_offset += ip.plaintext_size
                continue
            # Stop after our range
            if pt_offset > off_end:
                break

            ct_end = ct_offset + ip.ciphertext_size - 1
            self._validate_ciphertext_range(
                bucket, key, part_num, ip.internal_part_number, ct_end, actual_size
            )

            chunk = await self._fetch_internal_part(
                client, bucket, key, part_num, ip, ct_offset, ct_end, dek
            )

            # Slice to requested range within this part
            slice_start = max(0, off_start - pt_offset)
            slice_end = min(ip.plaintext_size, off_end - pt_offset + 1)
            yield chunk[slice_start:slice_end]

            ct_offset += ip.ciphertext_size
            pt_offset += ip.plaintext_size

    def _validate_ciphertext_range(
        self,
        bucket: str,
        key: str,
        part_num: int,
        internal_part_num: int,
        ct_end: int,
        actual_size: int | None,
    ) -> None:
        if actual_size is not None and ct_end >= actual_size:
            logger.error(
                "GET_METADATA_MISMATCH",
                bucket=bucket,
                key=key,
                part_number=part_num,
                internal_part_number=internal_part_num,
                ct_end=ct_end,
                actual_object_size=actual_size,
            )
            raise S3Error.internal_error(
                f"Metadata corruption: part {part_num} internal part {internal_part_num} "
                f"expects byte {ct_end} but object size is {actual_size}"
            )

    async def _fetch_internal_part(
        self,
        client: S3Client,
        bucket: str,
        key: str,
        part_num: int,
        internal_part,
        ct_start: int,
        ct_end: int,
        dek: bytes,
    ) -> bytes:
        expected_size = ct_end - ct_start + 1
        additional = max(0, expected_size * 2 - MAX_BUFFER_SIZE)
        extra_reserved = 0
        try:
            if additional > 0:
                extra_reserved = await concurrency.try_acquire_memory(additional)

            resp = await client.get_object(bucket, key, f"bytes={ct_start}-{ct_end}")
            async with resp["Body"] as body:
                ciphertext = await body.read()

            if len(ciphertext) < crypto.ENCRYPTION_OVERHEAD or len(ciphertext) != expected_size:
                logger.error(
                    "GET_CIPHERTEXT_SIZE_MISMATCH",
                    bucket=bucket,
                    key=key,
                    part_number=part_num,
                    internal_part_number=internal_part.internal_part_number,
                    expected_size=expected_size,
                    actual_size=len(ciphertext),
                )
                raise S3Error.internal_error(
                    f"Metadata corruption: part {part_num} "
                    f"internal part {internal_part.internal_part_number} "
                    f"expected {expected_size} bytes, got {len(ciphertext)}"
                )

            return crypto.decrypt(ciphertext, dek)

        except ClientError as e:
            if e.response["Error"]["Code"] == "InvalidRange":
                logger.error(
                    "GET_INVALID_RANGE",
                    bucket=bucket,
                    key=key,
                    part_number=part_num,
                    internal_part_number=internal_part.internal_part_number,
                    requested_range=f"{ct_start}-{ct_end}",
                )
                raise S3Error.internal_error(
                    f"Metadata corruption: part {part_num} "
                    f"internal part {internal_part.internal_part_number} "
                    f"range {ct_start}-{ct_end} invalid"
                ) from e
            raise
        finally:
            if extra_reserved > 0:
                await concurrency.release_memory(extra_reserved)

    async def _fetch_and_decrypt_part(
        self,
        client: S3Client,
        bucket: str,
        key: str,
        part_num: int,
        part_meta,
        ct_start: int,
        off_start: int,
        off_end: int,
        dek: bytes,
        actual_size: int | None,
    ) -> bytes:
        ct_end = ct_start + part_meta.ciphertext_size - 1

        logger.debug(
            "GET_PART",
            bucket=bucket,
            key=key,
            part_number=part_num,
            ct_range=f"{ct_start}-{ct_end}",
        )

        self._validate_ciphertext_range(bucket, key, part_num, 0, ct_end, actual_size)

        part_size = part_meta.ciphertext_size
        additional = max(0, part_size * 2 - MAX_BUFFER_SIZE)
        extra_reserved = 0
        try:
            if additional > 0:
                extra_reserved = await concurrency.try_acquire_memory(additional)

            resp = await client.get_object(bucket, key, f"bytes={ct_start}-{ct_end}")
            async with resp["Body"] as body:
                ciphertext = await body.read()
            decrypted = crypto.decrypt(ciphertext, dek)
            return decrypted[off_start : off_end + 1]
        finally:
            if extra_reserved > 0:
                await concurrency.release_memory(extra_reserved)

    def _build_response_headers(self, resp: dict, last_modified: str | None) -> dict[str, str]:
        return self._build_headers(
            content_length=resp.get("ContentLength"),
            content_type=resp.get("ContentType", "application/octet-stream"),
            last_modified=last_modified,
            cache_control=resp.get("CacheControl"),
            expires=resp.get("Expires"),
        )

    def _build_headers(
        self,
        content_type: str,
        content_length: int | None = None,
        last_modified: str | None = None,
        cache_control: str | None = None,
        expires: Any = None,
    ) -> dict[str, str]:
        headers: dict[str, str] = {"Content-Type": content_type}
        if content_length is not None:
            headers["Content-Length"] = str(content_length)
        if last_modified:
            headers["Last-Modified"] = last_modified
        if cache_control:
            headers["Cache-Control"] = cache_control
        if expires:
            headers["Expires"] = _format_expires(expires)
        return headers
