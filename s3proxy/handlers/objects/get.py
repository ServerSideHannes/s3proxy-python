"""GET object operations with encryption support."""

import asyncio
import base64
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import structlog
from botocore.exceptions import ClientError
from fastapi import Request, Response
from structlog.stdlib import BoundLogger

from ... import crypto
from ...client import S3Client, S3Credentials
from ...errors import S3Error
from ...state import (
    MultipartMetadata,
    calculate_part_range,
)
from ...streaming import STREAM_CHUNK_SIZE
from ...streaming.response import OwnedStreamingResponse
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
                descriptor = await self._resolve_object(client, bucket, key, head_resp)
                mp_meta = descriptor.multipart
                effective_etag = descriptor.etag

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

                if (meta := mp_meta) is not None:
                    response = await self._get_multipart(
                        client, bucket, key, meta, range_header, last_modified, creds, head_resp
                    )
                else:
                    response = await self._get_single(
                        client, bucket, key, range_header, head_resp, last_modified
                    )

                # Add ETag header
                response.headers["ETag"] = f'"{effective_etag}"'

                # Add user metadata (x-amz-meta-*), excluding internal keys
                internal_keys = self._internal_meta_keys()
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

        # Encrypted single-object - decrypt in memory using the kid that wrapped it
        kid = metadata.get(self.settings.kidtag_name, "")
        return await self._decrypt_single_object(
            client, bucket, key, range_header, head_resp, last_modified, wrapped_dek_b64, kid
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
        lease = self._client(client.credentials)
        stream_client = await lease.__aenter__()
        try:
            resp = await stream_client.get_object(bucket, key, range_header=range_header)
        except BaseException:
            await lease.__aexit__(None, None, None)
            raise
        s3_body = resp["Body"]
        headers = self._build_response_headers(resp, last_modified)

        async def stream_s3_body():
            async with s3_body:
                while chunk := await s3_body.read(STREAM_CHUNK_SIZE):
                    yield chunk

        async def cleanup():
            try:
                await s3_body.__aexit__(None, None, None)
            finally:
                await lease.__aexit__(None, None, None)

        if "ContentRange" in resp:
            headers["Content-Range"] = resp["ContentRange"]
        return OwnedStreamingResponse(
            stream_s3_body(),
            headers=headers,
            status_code=206 if "ContentRange" in resp else 200,
            cleanup=cleanup,
        )

    async def _decrypt_single_object(
        self,
        client: S3Client,
        bucket: str,
        key: str,
        range_header: str | None,
        head_resp: dict,
        last_modified: str | None,
        wrapped_dek_b64: str,
        kid: str = "",
    ) -> Response:
        logger.info("GET_ENCRYPTED_SINGLE", bucket=bucket, key=key)
        resp = await client.get_object(bucket, key, if_match=head_resp.get("ETag"))

        wrapped_dek = base64.b64decode(wrapped_dek_b64)
        dek = crypto.unwrap_key(wrapped_dek, self.keyring.key_by_id(kid))
        from ...streaming.authenticated import decrypt_to_file, file_range

        # Authenticate to bounded disk storage before emitting any plaintext. This
        # also handles old single-envelope objects larger than the memory budget.
        spool, length = await decrypt_to_file(resp["Body"], dek)
        try:
            start, end = (
                self._parse_range(range_header, length) if range_header else (0, length - 1)
            )
            headers = self._build_headers(
                head_resp.get("ContentType", "application/octet-stream"),
                end - start + 1,
                last_modified,
                head_resp.get("CacheControl"),
                head_resp.get("Expires"),
            )
            if range_header:
                headers["Content-Range"] = f"bytes {start}-{end}/{length}"

            async def cleanup():
                spool.close()

            return OwnedStreamingResponse(
                file_range(spool, start, end),
                headers=headers,
                cleanup=cleanup,
                status_code=206 if range_header else 200,
            )
        except BaseException:
            spool.close()
            raise

    async def _get_multipart(
        self,
        client: S3Client,
        bucket: str,
        key: str,
        meta: MultipartMetadata,
        range_header: str | None,
        last_modified: str | None,
        creds: S3Credentials,
        head_resp: dict | None = None,
    ) -> Response:
        dek = crypto.unwrap_key(meta.wrapped_dek, self.keyring.key_by_id(meta.kid))
        total = meta.total_plaintext_size
        start, end = self._parse_range(range_header, total) if range_header else (0, total - 1)
        parts = calculate_part_range(meta.parts, start, end)

        # Get actual object size and content type
        head_resp = head_resp or await client.head_object(bucket, key)
        content_type = head_resp.get("ContentType", "application/octet-stream")
        cache_control = head_resp.get("CacheControl")
        expires_val = head_resp.get("Expires")

        # Create stream generator
        async def stream():
            from ...streaming.frames import plaintext_frames

            async with (
                self._client(creds) as stream_client,
                contextlib.aclosing(
                    plaintext_frames(
                        stream_client,
                        bucket,
                        key,
                        meta,
                        dek,
                        start if range_header else None,
                        end if range_header else None,
                        if_match=head_resp.get("ETag"),
                        ciphertext_size=head_resp.get("ContentLength"),
                    )
                ) as plaintext,
            ):
                try:
                    async for chunk in plaintext:
                        yield chunk
                except ClientError as error:
                    self._raise_s3_error(error, bucket, key)

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
            return OwnedStreamingResponse(stream(), status_code=206, headers=headers)
        if total == 0:
            async for _ in stream():
                pass
            return Response(headers=headers)
        return OwnedStreamingResponse(stream(), headers=headers)

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
                    # aclosing() so a client disconnect deterministically tears down
                    # the prefetch lookahead (cancelling its in-flight fetch and
                    # releasing its memory reservation) rather than waiting on GC.
                    async with contextlib.aclosing(
                        self._stream_internal_parts(
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
                    ) as parts_stream:
                        async for chunk in parts_stream:
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

        # Resolve which internal parts intersect the requested range FIRST, with
        # absolute ciphertext bounds and the plaintext slice to emit. The prefetch
        # below only ever looks ahead within this filtered list, so it never
        # fetches a part the range would skip.
        # Frame-level fetches keep memory O(frame) so 64MB internal parts stay
        # within a 64MB pod budget (whole-part decrypt would reserve ~2× part).
        needed: list[tuple[int, int, int, int, int, int]] = []
        ct_offset = ct_start
        pt_offset = 0
        for ip in sorted(part_meta.internal_parts, key=lambda p: p.internal_part_number):
            pt_end = pt_offset + ip.plaintext_size - 1
            if pt_end < off_start:  # entirely before the range
                ct_offset += ip.ciphertext_size
                pt_offset += ip.plaintext_size
                continue
            if pt_offset > off_end:  # entirely after the range
                break

            frame_sizes = crypto.ciphertext_frame_byte_sizes(ip.plaintext_size, ip.ciphertext_size)
            frame_pt_offset = 0
            frame_ct_offset = 0
            for fsize in frame_sizes:
                fpt_size = fsize - crypto.ENCRYPTION_OVERHEAD
                frame_global_start = pt_offset + frame_pt_offset
                frame_global_end = frame_global_start + fpt_size - 1
                if frame_global_end < off_start:
                    frame_pt_offset += fpt_size
                    frame_ct_offset += fsize
                    continue
                if frame_global_start > off_end:
                    break

                abs_ct_start = ct_offset + frame_ct_offset
                abs_ct_end = abs_ct_start + fsize - 1
                self._validate_ciphertext_range(
                    bucket, key, part_num, ip.internal_part_number, abs_ct_end, actual_size
                )
                slice_start = max(0, off_start - frame_global_start)
                slice_end = min(fpt_size, off_end - frame_global_start + 1)
                needed.append(
                    (
                        ip.internal_part_number,
                        abs_ct_start,
                        abs_ct_end,
                        fsize,
                        slice_start,
                        slice_end,
                    )
                )
                frame_pt_offset += fpt_size
                frame_ct_offset += fsize

            ct_offset += ip.ciphertext_size
            pt_offset += ip.plaintext_size

        def fetch(item: tuple[int, int, int, int, int, int]) -> Awaitable[bytes]:
            ipn, c_start, c_end, fsize, _, _ = item
            return self._fetch_and_decrypt_frame(
                client, bucket, key, part_num, ipn, c_start, c_end, fsize, dek
            )

        # aclosing() guarantees the prefetch generator's finally (which cancels an
        # in-flight lookahead and releases its memory reservation) runs when this
        # stream is torn down — e.g. on client disconnect.
        async with contextlib.aclosing(self._stream_parts_with_prefetch(needed, fetch)) as stream:
            async for item, chunk in stream:
                *_, slice_start, slice_end = item
                yield chunk[slice_start:slice_end]

    async def _stream_parts_with_prefetch(
        self,
        items: list,
        fetch: Callable[[Any], Awaitable[bytes]],
    ) -> AsyncIterator[tuple[Any, bytes]]:
        """Yield ``(item, fetched_bytes)`` with single-part lookahead (double buffer).

        While the caller consumes item N, item N+1 is fetched concurrently as a
        task — overlapping backend fetch+decrypt with the client send so each part
        boundary no longer serializes a full backend TTFB behind sending the
        previous part. Lookahead depth is fixed at 1: at most one extra fetch is in
        flight, holding at most one extra memory reservation (acquired inside
        ``fetch``), so the memory ceiling is unchanged. On early exit / client
        disconnect the pending prefetch is cancelled and ``fetch``'s own ``finally``
        releases its reservation.
        """
        if not items:
            return
        next_task = asyncio.create_task(fetch(items[0]))
        try:
            for i, item in enumerate(items):
                chunk = await next_task
                if i + 1 < len(items):
                    next_task = asyncio.create_task(fetch(items[i + 1]))
                yield item, chunk
        finally:
            next_task.cancel()
            # Drain the cancelled/failed lookahead so its reservation is released.
            # Any exception here is from the abandoned task; the exception that
            # triggered cleanup (if any) re-propagates after this block.
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await next_task

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

    async def _fetch_and_decrypt_frame(
        self,
        client: S3Client,
        bucket: str,
        key: str,
        part_num: int,
        internal_part_num: int,
        ct_start: int,
        ct_end: int,
        frame_ciphertext_size: int,
        dek: bytes,
    ) -> bytes:
        # No per-frame memory reservation here: concurrent streaming GETs are bounded
        # at admission (the request-level reservation is held for the whole stream
        # lifetime), so the working set is O(concurrent streams), not O(frames). A
        # nested per-frame acquire would deadlock against that held reservation.
        expected_size = frame_ciphertext_size
        try:
            resp = await client.get_object(bucket, key, f"bytes={ct_start}-{ct_end}")
            async with resp["Body"] as body:
                ciphertext = await body.read()

            if len(ciphertext) < crypto.ENCRYPTION_OVERHEAD or len(ciphertext) != expected_size:
                logger.error(
                    "GET_CIPHERTEXT_SIZE_MISMATCH",
                    bucket=bucket,
                    key=key,
                    part_number=part_num,
                    internal_part_number=internal_part_num,
                    expected_size=expected_size,
                    actual_size=len(ciphertext),
                )
                raise S3Error.internal_error(
                    f"Metadata corruption: part {part_num} "
                    f"internal part {internal_part_num} "
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
                    internal_part_number=internal_part_num,
                    requested_range=f"{ct_start}-{ct_end}",
                )
                raise S3Error.internal_error(
                    f"Metadata corruption: part {part_num} "
                    f"internal part {internal_part_num} "
                    f"range {ct_start}-{ct_end} invalid"
                ) from e
            raise

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

        # See _fetch_and_decrypt_frame: stream concurrency is bounded at admission,
        # so no per-frame reservation is taken here.
        resp = await client.get_object(bucket, key, f"bytes={ct_start}-{ct_end}")
        async with resp["Body"] as body:
            ciphertext = await body.read()
        decrypted = crypto.decrypt(ciphertext, dek)
        return decrypted[off_start : off_end + 1]

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
