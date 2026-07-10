"""UploadPartCopy handler for multipart uploads."""

import asyncio
import base64
import contextlib
import hashlib
import math
import os
import time
from collections.abc import AsyncIterator, Callable, Coroutine
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import quote

import structlog
from botocore.exceptions import ClientError
from fastapi import Request, Response
from fastapi.responses import StreamingResponse
from structlog.stdlib import BoundLogger

from ... import concurrency, crypto, xml_responses
from ...client import S3Client, S3Credentials
from ...errors import S3Error, raise_for_client_error, raise_for_exception
from ...state import (
    InternalPartMetadata,
    MultipartMetadata,
    MultipartUploadState,
    PartMetadata,
    load_multipart_metadata,
    load_upload_state,
    persist_upload_state,
)
from ...streaming import FramedStreamBody
from ...utils import format_iso8601
from ..base import BaseHandler
from .upload_part import _PlaintextReader

logger: BoundLogger = structlog.get_logger(__name__)

# Cap concurrent streaming copy pipelines per pod. Without this, HAProxy maxconn
# can land many UploadPartCopy requests on one pod; each holds ciphertext and
# read buffers outside the governor if reservation is released before upload.
MAX_PARALLEL_COPY_PIPELINES = int(os.environ.get("S3PROXY_MAX_PARALLEL_COPIES", "2"))
_copy_pipeline_semaphore = asyncio.Semaphore(MAX_PARALLEL_COPY_PIPELINES)

# A multi-GB UploadPartCopy can take longer than the client's idle timeout
# (rclone in scylla-manager-agent gives up after 5 minutes with no response
# bytes). Like AWS S3, commit to 200 OK and trickle whitespace while the copy
# runs, then send CopyPartResult -- or an <Error> document -- as the body.
COPY_KEEPALIVE_INTERVAL = float(os.environ.get("S3PROXY_COPY_KEEPALIVE_INTERVAL", "5"))

# Backend UploadPartCopy calls per passthrough copy run concurrently; a 4.7GB
# Scylla part is ~570 x 8.33MB segments and sequential calls alone can exceed
# the client timeout.
PASSTHROUGH_SEGMENT_CONCURRENCY = int(
    os.environ.get("S3PROXY_PASSTHROUGH_SEGMENT_CONCURRENCY", "8")
)

# Scylla/rclone manifest part 1 is ~4.7GB; only defer a sub-5MB hybrid tail when
# part 1 is large enough that a client part 2 will follow (avoids EntityTooSmall).
HYBRID_TAIL_DEFER_MIN_CLIENT_PART = 1024 * 1024 * 1024  # 1 GiB

# Streamed internal-part bodies cannot be replayed by botocore's retry layer,
# so the copy pump retries them itself by reopening the source at the failed
# part's offset.
COPY_INTERNAL_PART_ATTEMPTS = 3


def reset_copy_pipeline_semaphore(limit: int | None = None) -> None:
    """Reset the global copy pipeline semaphore (testing only)."""
    global _copy_pipeline_semaphore
    _copy_pipeline_semaphore = asyncio.Semaphore(
        limit if limit is not None else MAX_PARALLEL_COPY_PIPELINES
    )


@dataclass(frozen=True, slots=True)
class _CiphertextSegment:
    """One contiguous ciphertext span in a completed encrypted object."""

    plaintext_size: int
    ciphertext_size: int
    ct_offset: int


@dataclass(frozen=True, slots=True)
class _PlaintextRangeSplit:
    """Passthrough-safe prefix segments plus optional plaintext tail to re-encrypt."""

    passthrough_segments: tuple[_CiphertextSegment, ...]
    streaming_tail: tuple[int, int] | None  # inclusive plaintext [start, end]


class CopyPartMixin(BaseHandler):
    async def handle_upload_part_copy(self, request: Request, creds: S3Credentials) -> Response:
        bucket, key = self._parse_path(request.url.path)
        async with self._client(creds) as client:
            upload_id, part_num = self._extract_multipart_params(request)
            copy_source = request.headers.get("x-amz-copy-source", "")
            raw_copy_source_range = request.headers.get("x-amz-copy-source-range")
            src_bucket, src_key = self._parse_copy_source(copy_source)

            state = await self.multipart_manager.get_upload(bucket, key, upload_id)
            if not state:
                state_data = await load_upload_state(client, bucket, key, upload_id)
                if not state_data:
                    raise S3Error.no_such_upload(upload_id)
                wrapped_dek, kid = state_data
                dek = crypto.unwrap_key(wrapped_dek, self.keyring.key_by_id(kid))
                state = await self.multipart_manager.create_upload(bucket, key, upload_id, dek, kid)

            try:
                head_resp = await client.head_object(src_bucket, src_key)
            except Exception as e:
                logger.error(
                    "UPLOAD_PART_COPY_HEAD_FAILED",
                    bucket=bucket,
                    key=key,
                    client_part=part_num,
                    src_bucket=src_bucket,
                    src_key=src_key,
                    error_type=type(e).__name__,
                    error=str(e),
                )
                raise S3Error.no_such_key(src_key) from e

            src_metadata = head_resp.get("Metadata", {})
            src_wrapped_dek = src_metadata.get(self.settings.dektag_name)
            src_multipart_meta = await load_multipart_metadata(client, src_bucket, src_key)

            total_plaintext = self._copy_plaintext_size(
                head_resp, None, src_wrapped_dek, src_multipart_meta
            )
            copy_source_range = self._normalize_copy_source_range(
                raw_copy_source_range, total_plaintext, head_resp, src_wrapped_dek
            )
            plaintext_size = self._copy_plaintext_size(
                head_resp, copy_source_range, src_wrapped_dek, src_multipart_meta
            )

            passthrough_block = self._passthrough_block_reason(
                copy_source_range,
                raw_copy_source_range,
                src_wrapped_dek,
                src_multipart_meta,
                src_metadata,
                creds,
                plaintext_size,
                head_resp,
            )
            route = (
                "passthrough"
                if passthrough_block is None
                else ("streaming" if plaintext_size > crypto.STREAMING_THRESHOLD else "simple")
            )
            self._log_upload_part_copy_route(
                bucket=bucket,
                key=key,
                part_num=part_num,
                raw_copy_source_range=raw_copy_source_range,
                normalized_copy_source_range=copy_source_range,
                metadata_total_plaintext=total_plaintext,
                range_plaintext_size=plaintext_size,
                route=route,
                passthrough_blocked_reason=passthrough_block,
            )

        # Validation and routing are done with real HTTP status semantics; the
        # copy work itself runs while the response streams keepalive whitespace,
        # so a copy that outlives the client's idle timeout still succeeds.
        async def run_copy() -> bytes:
            async with self._client(creds) as work_client:
                if plaintext_size <= crypto.STREAMING_THRESHOLD:
                    # Small copies buffer the whole object + re-encrypt it; gate
                    # them by the limiter too (they carry no body, so the
                    # request-level reservation was ~nothing and a small-object
                    # flood ran unbounded).
                    if passthrough_block is None:
                        resp = await self._gated_passthrough_copy_part(
                            work_client,
                            bucket,
                            key,
                            upload_id,
                            part_num,
                            state,
                            src_bucket,
                            src_key,
                            copy_source,
                            head_resp,
                            src_metadata,
                            src_wrapped_dek,
                            src_multipart_meta,
                            plaintext_size,
                            copy_source_range,
                        )
                        return resp.body
                    peak = crypto.copy_pipeline_peak(plaintext_size)
                    async with concurrency.reserve_copy_memory(peak):
                        resp = await self._simple_copy_part(
                            work_client,
                            bucket,
                            key,
                            upload_id,
                            part_num,
                            state,
                            src_bucket,
                            src_key,
                            copy_source_range,
                            head_resp,
                            src_metadata,
                            src_wrapped_dek,
                            src_multipart_meta,
                        )
                        return resp.body
                if passthrough_block is None:
                    resp = await self._gated_passthrough_copy_part(
                        work_client,
                        bucket,
                        key,
                        upload_id,
                        part_num,
                        state,
                        src_bucket,
                        src_key,
                        copy_source,
                        head_resp,
                        src_metadata,
                        src_wrapped_dek,
                        src_multipart_meta,
                        plaintext_size,
                        copy_source_range,
                    )
                    return resp.body
                resp = await self._streaming_copy_part(
                    work_client,
                    bucket,
                    key,
                    upload_id,
                    part_num,
                    state,
                    src_bucket,
                    src_key,
                    copy_source_range,
                    src_wrapped_dek,
                    src_multipart_meta,
                    head_resp,
                    src_metadata,
                    plaintext_size,
                )
                return resp.body

        return StreamingResponse(
            self._keepalive_copy_stream(run_copy(), bucket=bucket, key=key, part_num=part_num),
            media_type="application/xml",
        )

    async def _keepalive_copy_stream(
        self,
        work: Coroutine[None, None, bytes],
        *,
        bucket: str,
        key: str,
        part_num: int,
    ) -> AsyncIterator[bytes]:
        """Run the copy while trickling whitespace, then emit the result XML.

        Cancels the copy if the client disconnects, so an abandoned request does
        not keep burning memory budget and backend bandwidth as a zombie.
        """
        task = asyncio.create_task(work)
        start = time.monotonic()
        keepalives = 0
        try:
            while True:
                done, _ = await asyncio.wait({task}, timeout=COPY_KEEPALIVE_INTERVAL)
                if done:
                    break
                keepalives += 1
                yield b" "
            try:
                body = task.result()
            except Exception as e:
                code, message = self._copy_failure_code_message(e, bucket, key)
                logger.error(
                    "UPLOAD_PART_COPY_FAILED_AFTER_200",
                    bucket=bucket,
                    key=key,
                    part_num=part_num,
                    error_code=code,
                    error=message,
                    keepalives=keepalives,
                    elapsed_sec=f"{time.monotonic() - start:.2f}s",
                )
                yield xml_responses.error_document(code, message).encode()
                return
            if keepalives:
                logger.info(
                    "UPLOAD_PART_COPY_KEEPALIVE",
                    bucket=bucket,
                    key=key,
                    part_num=part_num,
                    keepalives=keepalives,
                    elapsed_sec=f"{time.monotonic() - start:.2f}s",
                )
            yield xml_responses.without_xml_declaration(body.decode()).encode()
        finally:
            if not task.done():
                task.cancel()
                with contextlib.suppress(BaseException):
                    await task
                logger.warning(
                    "UPLOAD_PART_COPY_CLIENT_GONE",
                    bucket=bucket,
                    key=key,
                    part_num=part_num,
                    keepalives=keepalives,
                    elapsed_sec=f"{time.monotonic() - start:.2f}s",
                )

    def _copy_failure_code_message(self, exc: Exception, bucket: str, key: str) -> tuple[str, str]:
        """Map a copy failure to an S3 error code/message for the 200-body Error."""
        if isinstance(exc, S3Error):
            return exc.code, exc.message
        try:
            if isinstance(exc, ClientError):
                raise_for_client_error(exc, bucket, key)
            raise_for_exception(exc)
        except S3Error as mapped:
            return mapped.code, mapped.message

    def _copy_plaintext_size(
        self,
        head_resp: dict,
        copy_source_range: str | None,
        src_wrapped_dek: str | None,
        src_multipart_meta,
    ) -> int:
        """Return the number of plaintext bytes that will be copied."""
        if src_multipart_meta:
            total = src_multipart_meta.total_plaintext_size
        elif src_wrapped_dek:
            plaintext_size_str = head_resp.get("Metadata", {}).get("plaintext-size")
            if plaintext_size_str:
                total = int(plaintext_size_str)
            else:
                total = crypto.plaintext_size(head_resp.get("ContentLength", 0))
        else:
            total = head_resp.get("ContentLength", 0)

        if not copy_source_range:
            return total
        start, end = self._parse_copy_source_range(copy_source_range, total)
        return end - start + 1

    def _parse_raw_copy_source_range(self, copy_source_range: str) -> tuple[int, int]:
        """Parse x-amz-copy-source-range without clamping end to object size."""
        range_str = copy_source_range.replace("bytes=", "")
        try:
            start, end = map(int, range_str.split("-"))
        except (ValueError, TypeError) as err:
            raise S3Error.invalid_range("Invalid copy source range format") from err
        if start > end:
            raise S3Error.invalid_range("Range not satisfiable")
        return start, end

    def _head_plaintext_size(self, head_resp: dict, src_wrapped_dek: str | None) -> int | None:
        if not src_wrapped_dek:
            return None
        size_str = head_resp.get("Metadata", {}).get("plaintext-size")
        if not size_str:
            return None
        return int(size_str)

    def _normalize_copy_source_range(
        self,
        copy_source_range: str | None,
        total_plaintext_size: int,
        head_resp: dict,
        src_wrapped_dek: str | None,
    ) -> str | None:
        """Treat a range spanning the entire object as 'copy whole object'.

        Scylla Manager often sends ``bytes=0-(size-1)`` on manifest UploadPartCopy.
        When that matches metadata or HEAD plaintext-size, clear the range so routing
        treats it as a whole-object copy. Ranges shorter than metadata total (prod
        manifest shape) are left intact and handled by range-aware passthrough.
        """
        if not copy_source_range:
            return None
        raw_start, raw_end = self._parse_raw_copy_source_range(copy_source_range)
        end = self._parse_copy_source_range(copy_source_range, total_plaintext_size)[1]
        if raw_start == 0 and end == total_plaintext_size - 1:
            return None
        head_plaintext = self._head_plaintext_size(head_resp, src_wrapped_dek)
        if raw_start == 0 and head_plaintext and raw_end >= head_plaintext - 1:
            return None
        return copy_source_range

    def _split_plaintext_range_on_segments(
        self,
        segments: list[_CiphertextSegment],
        range_start: int,
        range_end: int,
    ) -> _PlaintextRangeSplit | None:
        """Split a plaintext range into ciphertext-passthrough prefix + optional streaming tail.

        Scylla manifest ranges often end mid internal frame (~8.33MB from 50MB uploads).
        Copy every fully covered segment server-side and re-encrypt only the trailing suffix.
        """
        if range_start > range_end:
            return _PlaintextRangeSplit((), None)
        selected: list[_CiphertextSegment] = []
        pt_offset = 0
        streaming_tail: tuple[int, int] | None = None
        for seg in segments:
            seg_start = pt_offset
            seg_end = pt_offset + seg.plaintext_size - 1
            if seg_end < range_start:
                pt_offset += seg.plaintext_size
                continue
            if seg_start > range_end:
                break
            if seg_start < range_start:
                return None
            if seg_end > range_end:
                streaming_tail = (seg_start, range_end)
                break
            selected.append(seg)
            pt_offset += seg.plaintext_size
        if streaming_tail is None and pt_offset <= range_end:
            return None
        return _PlaintextRangeSplit(tuple(selected), streaming_tail)

    def _source_plaintext_end(self, segments: list[_CiphertextSegment]) -> int:
        if not segments:
            return -1
        return sum(seg.plaintext_size for seg in segments) - 1

    def _should_defer_hybrid_tail(
        self,
        streaming_tail: tuple[int, int] | None,
        range_end: int,
        all_segments: list[_CiphertextSegment],
        *,
        client_part_plaintext_size: int,
        part_num: int,
    ) -> bool:
        """Defer a sub-5MB hybrid tail when a large client part 1 precedes part 2.

        S3 requires every internal part except the last to be >= 5MB. Scylla
        manifest part 1 (~4.7GB) often ends mid internal frame (~1MB tail)
        before part 2. Small single-part partial copies still upload the tail
        immediately so the client part is self-contained.
        """
        if streaming_tail is None or part_num != 1:
            return False
        if range_end >= self._source_plaintext_end(all_segments):
            return False
        tail_bytes = streaming_tail[1] - streaming_tail[0] + 1
        if tail_bytes >= crypto.MIN_PART_SIZE:
            return False
        defer = client_part_plaintext_size >= HYBRID_TAIL_DEFER_MIN_CLIENT_PART
        logger.info(
            "HYBRID_TAIL_DEFER_DECISION",
            defer=defer,
            part_num=part_num,
            tail_bytes=tail_bytes,
            client_part_plaintext_mb=f"{client_part_plaintext_size / 1024 / 1024:.2f}MB",
            range_end=range_end,
            source_end=self._source_plaintext_end(all_segments),
            min_client_part_mb=f"{HYBRID_TAIL_DEFER_MIN_CLIENT_PART / 1024 / 1024:.0f}MB",
        )
        return defer

    def _segments_for_plaintext_range(
        self,
        segments: list[_CiphertextSegment],
        range_start: int,
        range_end: int,
    ) -> list[_CiphertextSegment] | None:
        """Segments fully inside [range_start, range_end], or None if range splits a segment."""
        split = self._split_plaintext_range_on_segments(segments, range_start, range_end)
        if split is None:
            return None
        if split.streaming_tail is not None:
            return None
        return list(split.passthrough_segments)

    def _passthrough_block_reason(
        self,
        copy_source_range: str | None,
        raw_copy_source_range: str | None,
        src_wrapped_dek: str | None,
        src_multipart_meta: MultipartMetadata | None,
        src_metadata: dict,
        creds: S3Credentials,
        plaintext_size: int,
        head_resp: dict,
    ) -> str | None:
        """None when server-side ciphertext copy is allowed; else a short reason code."""
        if not src_wrapped_dek and not src_multipart_meta:
            return "unencrypted_source"
        if not src_multipart_meta and plaintext_size > crypto.STREAMING_THRESHOLD:
            return "large_put_object_no_sidecar"
        if src_multipart_meta:
            src_kid = src_multipart_meta.kid
        else:
            src_kid = src_metadata.get(self.settings.kidtag_name, "")
        if src_kid and src_kid != creds.access_key:
            return "needs_rekey"

        if not copy_source_range:
            return None

        # Small ranged copies stay on streaming re-encrypt (tests + partial object copies).
        if plaintext_size <= crypto.STREAMING_THRESHOLD:
            return "small_ranged_copy"

        if not src_multipart_meta:
            return "ranged_copy_no_multipart_meta"

        total = src_multipart_meta.total_plaintext_size
        range_start, range_end = self._parse_copy_source_range(copy_source_range, total)
        if range_start != 0:
            return "ranged_copy_nonzero_start"

        segments = self._source_ciphertext_segments(
            src_multipart_meta, head_resp, src_wrapped_dek, src_metadata
        )
        split = self._split_plaintext_range_on_segments(segments, range_start, range_end)
        if split is None:
            return "ranged_copy_segment_misaligned"
        if not split.passthrough_segments and not split.streaming_tail:
            return "ranged_copy_empty_segments"
        if not split.passthrough_segments and split.streaming_tail:
            tail_bytes = split.streaming_tail[1] - split.streaming_tail[0] + 1
            if tail_bytes <= crypto.STREAMING_THRESHOLD:
                return "small_ranged_copy"
            return "ranged_copy_segment_misaligned"
        return None

    def _log_upload_part_copy_route(
        self,
        *,
        bucket: str,
        key: str,
        part_num: int,
        raw_copy_source_range: str | None,
        normalized_copy_source_range: str | None,
        metadata_total_plaintext: int,
        range_plaintext_size: int,
        route: str,
        passthrough_blocked_reason: str | None,
    ) -> None:
        logger.info(
            "UPLOAD_PART_COPY_ROUTE",
            bucket=bucket,
            key=key,
            part_num=part_num,
            raw_copy_source_range=raw_copy_source_range,
            normalized_copy_source_range=normalized_copy_source_range,
            metadata_total_plaintext_mb=f"{metadata_total_plaintext / 1024 / 1024:.2f}MB",
            range_plaintext_mb=f"{range_plaintext_size / 1024 / 1024:.2f}MB",
            route=route,
            passthrough_blocked_reason=passthrough_blocked_reason,
        )

    def _can_passthrough_part_copy(
        self,
        copy_source_range: str | None,
        src_wrapped_dek: str | None,
        src_multipart_meta: MultipartMetadata | None,
        src_metadata: dict,
        creds: S3Credentials,
        plaintext_size: int,
    ) -> bool:
        """True when a native server-side copy preserves decryptability without re-encrypt."""
        return (
            self._passthrough_block_reason(
                copy_source_range,
                copy_source_range,
                src_wrapped_dek,
                src_multipart_meta,
                src_metadata,
                creds,
                plaintext_size,
                {},
            )
            is None
        )

    def _resolve_source_dek(
        self,
        src_multipart_meta: MultipartMetadata | None,
        src_wrapped_dek: str | None,
        src_metadata: dict,
        creds: S3Credentials,
    ) -> tuple[bytes, str]:
        if src_multipart_meta:
            kid = src_multipart_meta.kid
            dek = crypto.unwrap_key(src_multipart_meta.wrapped_dek, self.keyring.key_by_id(kid))
            return dek, kid
        kid = src_metadata.get(self.settings.kidtag_name, "")
        wrapped = base64.b64decode(src_wrapped_dek or "")
        if kid:
            kek = self.keyring.key_by_id(kid)
        else:
            kid, kek = self.keyring.key_for(creds.access_key)
        return crypto.unwrap_key(wrapped, kek), kid

    def _source_ciphertext_segments(
        self,
        src_multipart_meta: MultipartMetadata | None,
        head_resp: dict,
        src_wrapped_dek: str | None,
        src_metadata: dict,
    ) -> list[_CiphertextSegment]:
        if src_multipart_meta:
            segments: list[_CiphertextSegment] = []
            ct_offset = 0
            for part in sorted(src_multipart_meta.parts, key=lambda p: p.part_number):
                if part.internal_parts:
                    for ip in sorted(part.internal_parts, key=lambda x: x.internal_part_number):
                        segments.append(
                            _CiphertextSegment(ip.plaintext_size, ip.ciphertext_size, ct_offset)
                        )
                        ct_offset += ip.ciphertext_size
                else:
                    segments.append(
                        _CiphertextSegment(part.plaintext_size, part.ciphertext_size, ct_offset)
                    )
                    ct_offset += part.ciphertext_size
            return segments

        ct_size = head_resp.get("ContentLength", 0) or 0
        size_str = src_metadata.get("plaintext-size")
        plaintext_size = int(size_str) if size_str else crypto.plaintext_size(ct_size)
        return [_CiphertextSegment(plaintext_size, ct_size, 0)]

    async def _adopt_upload_dek(
        self,
        client: S3Client,
        bucket: str,
        key: str,
        upload_id: str,
        state: MultipartUploadState,
        dek: bytes,
        kid: str,
        creds: S3Credentials,
    ) -> None:
        if state.dek == dek and state.kid == kid:
            return
        if state.parts and state.dek != dek:
            raise S3Error.invalid_request(
                "Upload already contains parts encrypted with a different key"
            )
        state.dek = dek
        state.kid = kid
        if kid:
            kek = self.keyring.key_by_id(kid)
        else:
            kid, kek = self.keyring.key_for(creds.access_key)
        wrapped = crypto.wrap_key(dek, kek)
        await persist_upload_state(client, bucket, key, upload_id, wrapped, kid)
        await self.multipart_manager.store_reconstructed_state(bucket, key, upload_id, state)

    async def _md5_plaintext_from_copy_source(
        self,
        client: S3Client,
        src_bucket: str,
        src_key: str,
        copy_source_range: str | None,
        src_wrapped_dek: str | None,
        src_multipart_meta: MultipartMetadata | None,
        head_resp: dict,
        src_metadata: dict,
    ):
        """Frame-bounded plaintext MD5 for the UploadPartCopy synthetic ETag."""
        md5 = hashlib.md5(usedforsecurity=False)
        async for chunk in self._iter_copy_source(
            client,
            src_bucket,
            src_key,
            copy_source_range,
            src_wrapped_dek,
            src_multipart_meta,
            head_resp,
            src_metadata,
        ):
            md5.update(chunk)
        return md5

    async def _gated_passthrough_copy_part(
        self,
        client: S3Client,
        bucket: str,
        key: str,
        upload_id: str,
        part_num: int,
        state: MultipartUploadState,
        src_bucket: str,
        src_key: str,
        copy_source: str,
        head_resp: dict,
        src_metadata: dict,
        src_wrapped_dek: str | None,
        src_multipart_meta: MultipartMetadata | None,
        plaintext_size: int,
        copy_source_range: str | None = None,
    ) -> Response:
        """Server-side ciphertext copy; does not use the streaming pipeline semaphore."""
        return await self._passthrough_copy_part(
            client,
            bucket,
            key,
            upload_id,
            part_num,
            state,
            src_bucket,
            src_key,
            copy_source,
            head_resp,
            src_metadata,
            src_wrapped_dek,
            src_multipart_meta,
            plaintext_size,
            copy_source_range,
        )

    async def _passthrough_copy_part(
        self,
        client: S3Client,
        bucket: str,
        key: str,
        upload_id: str,
        part_num: int,
        state: MultipartUploadState,
        src_bucket: str,
        src_key: str,
        copy_source: str,
        head_resp: dict,
        src_metadata: dict,
        src_wrapped_dek: str | None,
        src_multipart_meta: MultipartMetadata | None,
        plaintext_size: int,
        copy_source_range: str | None = None,
    ) -> Response:
        """Server-side copy of encrypted ciphertext; destination adopts the source DEK."""
        src_dek, src_kid = self._resolve_source_dek(
            src_multipart_meta, src_wrapped_dek, src_metadata, client.credentials
        )
        await self._adopt_upload_dek(
            client, bucket, key, upload_id, state, src_dek, src_kid, client.credentials
        )

        segments = self._source_ciphertext_segments(
            src_multipart_meta, head_resp, src_wrapped_dek, src_metadata
        )
        if not segments:
            raise S3Error.invalid_request("Copy source has no ciphertext segments")

        all_segments = segments
        range_end = 0
        if copy_source_range and src_multipart_meta:
            range_start, range_end = self._parse_copy_source_range(
                copy_source_range, src_multipart_meta.total_plaintext_size
            )
            split = self._split_plaintext_range_on_segments(segments, range_start, range_end)
            if split is None:
                raise S3Error.invalid_request("Copy range splits an encrypted segment")
            segments = list(split.passthrough_segments)
            streaming_tail = split.streaming_tail
            defer_tail = self._should_defer_hybrid_tail(
                streaming_tail,
                range_end,
                all_segments,
                client_part_plaintext_size=plaintext_size,
                part_num=part_num,
            )
        else:
            streaming_tail = None
            defer_tail = False

        copy_source_path = copy_source or f"/{src_bucket}/{quote(src_key, safe='/')}"
        tail_mb = (
            f"{(streaming_tail[1] - streaming_tail[0] + 1) / 1024 / 1024:.2f}MB"
            if streaming_tail
            else None
        )

        logger.info(
            "UPLOAD_PART_COPY_PASSTHROUGH",
            bucket=bucket,
            key=key,
            part_num=part_num,
            src_bucket=src_bucket,
            src_key=src_key,
            plaintext_mb=f"{plaintext_size / 1024 / 1024:.2f}MB",
            segments=len(segments),
            streaming_tail_mb=tail_mb,
            defer_tail=defer_tail,
            copy_source_range=copy_source_range,
        )

        has_internal_layout = bool(
            src_multipart_meta and any(p.internal_parts for p in src_multipart_meta.parts)
        )
        if not has_internal_layout and len(segments) == 1:
            # Single-part encrypted object (or legacy multipart without internal parts):
            # copy straight to the client part number.
            seg = segments[0]
            ct_end = seg.ct_offset + seg.ciphertext_size - 1
            copy_range = f"bytes={seg.ct_offset}-{ct_end}"
            part_reserve = crypto.copy_passthrough_segment_peak(seg.plaintext_size)
            async with concurrency.reserve_copy_memory(part_reserve):
                resp = await client.upload_part_copy(
                    bucket,
                    key,
                    upload_id,
                    part_num,
                    copy_source_path,
                    copy_source_range=copy_range,
                )
            md5 = await self._md5_plaintext_from_copy_source(
                client,
                src_bucket,
                src_key,
                copy_source_range,
                src_wrapped_dek,
                src_multipart_meta,
                head_resp,
                src_metadata,
            )
            etag = md5.hexdigest()
            # Store the BACKEND part etag; CompleteMultipartUpload must present
            # it to S3, and the synthetic plaintext etag returned to the client
            # would be rejected there (InvalidPart).
            await self.multipart_manager.add_part(
                bucket,
                key,
                upload_id,
                PartMetadata(
                    part_number=part_num,
                    plaintext_size=seg.plaintext_size,
                    ciphertext_size=seg.ciphertext_size,
                    etag=resp["CopyPartResult"]["ETag"].strip('"'),
                    md5=etag,
                ),
            )
            return Response(
                content=xml_responses.upload_part_copy_result(
                    etag, format_iso8601(datetime.now(UTC))
                ),
                media_type="application/xml",
            )

        tail_internal_slots = 0 if defer_tail else (1 if streaming_tail else 0)
        internal_part_start = await self.multipart_manager.allocate_internal_parts(
            bucket,
            key,
            upload_id,
            len(segments) + tail_internal_slots,
            client_part_number=0,
        )
        logger.info(
            "UPLOAD_PART_COPY_PASSTHROUGH_ALLOC",
            bucket=bucket,
            key=key,
            part_num=part_num,
            passthrough_segments=len(segments),
            tail_internal_slots=tail_internal_slots,
            internal_part_start=internal_part_start,
        )

        start = time.monotonic()
        segment_sem = asyncio.Semaphore(PASSTHROUGH_SEGMENT_CONCURRENCY)
        deferred_tail_bytes: bytes | None = None

        async def copy_segment(idx: int, seg: _CiphertextSegment) -> InternalPartMetadata:
            internal_num = internal_part_start + idx
            ct_end = seg.ct_offset + seg.ciphertext_size - 1
            copy_range = f"bytes={seg.ct_offset}-{ct_end}"
            part_reserve = crypto.copy_passthrough_segment_peak(seg.plaintext_size)
            async with segment_sem, concurrency.reserve_copy_memory(part_reserve):
                resp = await client.upload_part_copy(
                    bucket,
                    key,
                    upload_id,
                    internal_num,
                    copy_source_path,
                    copy_source_range=copy_range,
                )
            return InternalPartMetadata(
                internal_part_number=internal_num,
                plaintext_size=seg.plaintext_size,
                ciphertext_size=seg.ciphertext_size,
                etag=resp["CopyPartResult"]["ETag"].strip('"'),
            )

        async def upload_tail() -> InternalPartMetadata | None:
            nonlocal deferred_tail_bytes
            if not (streaming_tail and src_multipart_meta):
                return None
            tail_start, tail_end = streaming_tail
            tail_plaintext = await self._download_encrypted_multipart(
                client, src_bucket, src_key, src_multipart_meta, tail_start, tail_end
            )
            if defer_tail:
                deferred_tail_bytes = tail_plaintext
                logger.info(
                    "UPLOAD_PART_COPY_PASSTHROUGH_TAIL_DEFERRED",
                    bucket=bucket,
                    key=key,
                    part_num=part_num,
                    tail_plaintext_mb=f"{len(tail_plaintext) / 1024 / 1024:.2f}MB",
                )
                return None
            internal_num = internal_part_start + len(segments)
            tail_ct = crypto.encrypt_frame(tail_plaintext, state.dek, upload_id, internal_num, 0)
            part_reserve = crypto.copy_chunk_peak(len(tail_plaintext))
            async with concurrency.reserve_copy_memory(part_reserve):
                resp = await client.upload_part(bucket, key, upload_id, internal_num, tail_ct)
            logger.info(
                "UPLOAD_PART_COPY_PASSTHROUGH_TAIL",
                bucket=bucket,
                key=key,
                part_num=part_num,
                internal_part=internal_num,
                tail_plaintext_mb=f"{len(tail_plaintext) / 1024 / 1024:.2f}MB",
            )
            return InternalPartMetadata(
                internal_part_number=internal_num,
                plaintext_size=len(tail_plaintext),
                ciphertext_size=len(tail_ct),
                etag=resp["ETag"].strip('"'),
            )

        # The synthetic-ETag MD5 pass reads the whole source; run it alongside
        # the segment copies and the tail instead of serially after them --
        # sequential, the three phases pushed a 4.7GB part past the client's
        # idle timeout.
        try:
            async with asyncio.TaskGroup() as tg:
                seg_tasks = [tg.create_task(copy_segment(i, seg)) for i, seg in enumerate(segments)]
                tail_task = tg.create_task(upload_tail())
                md5_task = tg.create_task(
                    self._md5_plaintext_from_copy_source(
                        client,
                        src_bucket,
                        src_key,
                        copy_source_range,
                        src_wrapped_dek,
                        src_multipart_meta,
                        head_resp,
                        src_metadata,
                    )
                )
        except BaseExceptionGroup as eg:
            exc: BaseException = eg
            while isinstance(exc, BaseExceptionGroup):
                exc = exc.exceptions[0]
            raise exc from eg

        internal_parts = [t.result() for t in seg_tasks]
        if (tail_part := tail_task.result()) is not None:
            internal_parts.append(tail_part)
        if deferred_tail_bytes:
            await self.multipart_manager.set_deferred_copy_tail(
                bucket, key, upload_id, deferred_tail_bytes
            )
        # A deferred tail is NOT part of this client part's stored bytes: it is
        # folded into the next part's stream (take_deferred_copy_tail) or flushed
        # at complete, and its plaintext is counted there. Counting it here too
        # inflated total_plaintext_size by the tail size, so HEAD reported
        # size+tail and rclone failed every >copy-cutoff copy with
        # "corrupted on transfer: sizes differ".
        total_plaintext = sum(p.plaintext_size for p in internal_parts)
        total_ciphertext = sum(p.ciphertext_size for p in internal_parts)
        etag = md5_task.result().hexdigest()
        await self.multipart_manager.add_part(
            bucket,
            key,
            upload_id,
            PartMetadata(
                part_number=part_num,
                plaintext_size=total_plaintext,
                ciphertext_size=total_ciphertext,
                etag=etag,
                md5=etag,
                internal_parts=internal_parts,
            ),
        )

        logger.info(
            "UPLOAD_PART_COPY_PASSTHROUGH_COMPLETE",
            bucket=bucket,
            key=key,
            part_num=part_num,
            internal_parts=len(internal_parts),
            deferred_tail_bytes=len(deferred_tail_bytes) if deferred_tail_bytes else 0,
            plaintext_mb=f"{total_plaintext / 1024 / 1024:.2f}MB",
            copy_source_range=copy_source_range,
            elapsed_sec=f"{time.monotonic() - start:.2f}s",
        )
        return Response(
            content=xml_responses.upload_part_copy_result(etag, format_iso8601(datetime.now(UTC))),
            media_type="application/xml",
        )

    async def _flush_deferred_copy_tail_for_complete(
        self,
        client: S3Client,
        bucket: str,
        key: str,
        upload_id: str,
        state: MultipartUploadState,
    ) -> MultipartUploadState:
        """Upload a leftover deferred hybrid tail as the final internal S3 part."""
        tail = state.deferred_copy_tail
        if not tail:
            return state

        internal_num = state.next_internal_part_number
        tail_ct = crypto.encrypt_frame(tail, state.dek, upload_id, internal_num, 0)
        resp = await client.upload_part(bucket, key, upload_id, internal_num, tail_ct)
        ip = InternalPartMetadata(
            internal_part_number=internal_num,
            plaintext_size=len(tail),
            ciphertext_size=len(tail_ct),
            etag=resp["ETag"].strip('"'),
        )
        last_pn = max(state.parts)
        last_part = state.parts[last_pn]
        last_part.internal_parts.append(ip)
        last_part.plaintext_size += len(tail)
        last_part.ciphertext_size += len(tail_ct)
        state.deferred_copy_tail = b""
        state.next_internal_part_number = internal_num + 1

        logger.info(
            "DEFERRED_COPY_TAIL_FLUSHED_ON_COMPLETE",
            bucket=bucket,
            key=key,
            upload_id=upload_id[:20] + "...",
            client_part=last_pn,
            internal_part=internal_num,
            tail_plaintext_mb=f"{len(tail) / 1024 / 1024:.2f}MB",
        )
        return state

    async def _simple_copy_part(
        self,
        client: S3Client,
        bucket: str,
        key: str,
        upload_id: str,
        part_num: int,
        state: MultipartUploadState,
        src_bucket: str,
        src_key: str,
        copy_source_range: str | None,
        head_resp: dict,
        src_metadata: dict,
        src_wrapped_dek: str | None,
        src_multipart_meta,
    ) -> Response:
        """Encrypt and upload a small copy source as a single S3 part."""
        plaintext = await self._fetch_copy_plaintext(
            client,
            src_bucket,
            src_key,
            copy_source_range,
            head_resp,
            src_metadata,
            src_wrapped_dek,
            src_multipart_meta,
        )
        ciphertext = crypto.encrypt_part(plaintext, state.dek, upload_id, part_num)
        resp = await client.upload_part(bucket, key, upload_id, part_num, ciphertext)
        body_md5 = hashlib.md5(plaintext, usedforsecurity=False).hexdigest()
        await self.multipart_manager.add_part(
            bucket,
            key,
            upload_id,
            PartMetadata(
                part_num,
                len(plaintext),
                len(ciphertext),
                resp["ETag"].strip('"'),
                body_md5,
            ),
        )
        last_modified = format_iso8601(datetime.now(UTC))
        return Response(
            content=xml_responses.upload_part_copy_result(resp["ETag"].strip('"'), last_modified),
            media_type="application/xml",
        )

    async def _fetch_copy_plaintext(
        self,
        client: S3Client,
        src_bucket: str,
        src_key: str,
        copy_source_range: str | None,
        head_resp: dict,
        src_metadata: dict,
        src_wrapped_dek: str | None,
        src_multipart_meta,
    ) -> bytes:
        """Download the full plaintext of a copy source (small sources only)."""
        if not src_wrapped_dek and not src_multipart_meta:
            resp = await client.get_object(src_bucket, src_key, range_header=copy_source_range)
            async with resp["Body"] as body:
                return await body.read()
        elif src_multipart_meta:
            range_start, range_end = self._parse_copy_source_range(
                copy_source_range, src_multipart_meta.total_plaintext_size
            )
            return await self._download_encrypted_multipart(
                client, src_bucket, src_key, src_multipart_meta, range_start, range_end
            )
        else:
            src_kid = src_metadata.get(self.settings.kidtag_name, "")
            full_plaintext = await self._download_encrypted_single(
                client, src_bucket, src_key, src_wrapped_dek, src_kid
            )
            if copy_source_range:
                start, end = self._parse_copy_source_range(copy_source_range, len(full_plaintext))
                return full_plaintext[start : end + 1]
            return full_plaintext

    async def _streaming_copy_part(
        self,
        client: S3Client,
        bucket: str,
        key: str,
        upload_id: str,
        part_num: int,
        state: MultipartUploadState,
        src_bucket: str,
        src_key: str,
        copy_source_range: str | None,
        src_wrapped_dek: str | None,
        src_multipart_meta,
        head_resp: dict,
        src_metadata: dict,
        plaintext_size: int,
    ) -> Response:
        # UploadPartCopy carries no request body, so the request-level limiter
        # reserved ~nothing. Per-internal-part reservations in _pump_copy_chunks
        # gate memory; the pipeline semaphore caps how many copies run at once.
        async with _copy_pipeline_semaphore:
            return await self._streaming_copy_part_inner(
                client,
                bucket,
                key,
                upload_id,
                part_num,
                state,
                src_bucket,
                src_key,
                copy_source_range,
                src_wrapped_dek,
                src_multipart_meta,
                head_resp,
                src_metadata,
                plaintext_size,
            )

    async def _streaming_copy_part_inner(
        self,
        client: S3Client,
        bucket: str,
        key: str,
        upload_id: str,
        part_num: int,
        state: MultipartUploadState,
        src_bucket: str,
        src_key: str,
        copy_source_range: str | None,
        src_wrapped_dek: str | None,
        src_multipart_meta,
        head_resp: dict,
        src_metadata: dict,
        plaintext_size: int,
    ) -> Response:
        """Stream-decrypt the source and encrypt+upload each chunk as an internal S3 part."""
        # Fixed internal part size: the copy peak is O(1) in object size (~90MB)
        # instead of object_size/20 (~535MB for a 4.7GB SSTable, which had to
        # reserve the whole governor budget and deadlocked). A large copy needs
        # many internal parts, so allocate them sequentially (client_part_number=0)
        # rather than from the fixed 20-wide per-client window -- safe because
        # copies are single-part uploads and reassembly is metadata-driven.
        chunk_size = crypto.copy_internal_part_size(plaintext_size)
        estimated_parts = max(1, math.ceil(plaintext_size / chunk_size))

        deferred_tail = await self.multipart_manager.take_deferred_copy_tail(bucket, key, upload_id)
        if deferred_tail:
            logger.info(
                "UPLOAD_PART_COPY_CONSUME_DEFERRED_TAIL",
                bucket=bucket,
                key=key,
                part_num=part_num,
                tail_plaintext_mb=f"{len(deferred_tail) / 1024 / 1024:.2f}MB",
            )
            estimated_parts = max(1, math.ceil((plaintext_size + len(deferred_tail)) / chunk_size))

        internal_part_start = await self.multipart_manager.allocate_internal_parts(
            bucket, key, upload_id, estimated_parts, client_part_number=0
        )

        logger.info(
            "UPLOAD_PART_COPY_STREAMING",
            bucket=bucket,
            key=key,
            part_num=part_num,
            plaintext_mb=f"{plaintext_size / 1024 / 1024:.2f}MB",
            deferred_tail_bytes=len(deferred_tail),
            chunk_size_mb=f"{chunk_size / 1024 / 1024:.2f}MB",
            estimated_parts=estimated_parts,
            internal_part_start=internal_part_start,
        )

        def open_source(skip_bytes: int) -> AsyncIterator[bytes]:
            return self._iter_copy_source(
                client,
                src_bucket,
                src_key,
                copy_source_range,
                src_wrapped_dek,
                src_multipart_meta,
                head_resp,
                src_metadata,
                skip_bytes=skip_bytes,
            )

        # Streamed bodies declare Content-Length up front, so the pump needs the
        # number of bytes the source will actually yield. Scylla manifest sidecar
        # metadata overstates total_plaintext_size relative to the stored
        # segments (prod shape), so clamp the requested range to the segments'
        # real extent — the claimed total would over-promise and abort the copy.
        source_bytes = plaintext_size
        if src_multipart_meta:
            real_total = sum(p.plaintext_size for p in src_multipart_meta.parts)
            if copy_source_range:
                range_start, range_end = self._parse_copy_source_range(
                    copy_source_range, src_multipart_meta.total_plaintext_size
                )
            else:
                range_start, range_end = 0, src_multipart_meta.total_plaintext_size - 1
            range_end = min(range_end, real_total - 1)
            source_bytes = max(0, range_end - range_start + 1)

        internal_parts, total_plaintext, total_ciphertext, md5 = await self._pump_copy_chunks(
            client,
            bucket,
            key,
            upload_id,
            part_num,
            state,
            open_source(0),
            chunk_size,
            internal_part_start,
            leading_plaintext=deferred_tail or b"",
            expected_plaintext=source_bytes + len(deferred_tail),
            reopen_source=open_source,
        )

        etag = md5.hexdigest()
        await self.multipart_manager.add_part(
            bucket,
            key,
            upload_id,
            PartMetadata(
                part_number=part_num,
                plaintext_size=total_plaintext,
                ciphertext_size=total_ciphertext,
                etag=etag,
                md5=etag,
                internal_parts=internal_parts,
            ),
        )

        logger.info(
            "UPLOAD_PART_COPY_STREAMING_COMPLETE",
            bucket=bucket,
            key=key,
            part_num=part_num,
            plaintext_mb=f"{total_plaintext / 1024 / 1024:.2f}MB",
            internal_parts=len(internal_parts),
            first_internal_plaintext_mb=(
                f"{internal_parts[0].plaintext_size / 1024 / 1024:.2f}MB"
                if internal_parts
                else None
            ),
        )
        return Response(
            content=xml_responses.upload_part_copy_result(etag, format_iso8601(datetime.now(UTC))),
            media_type="application/xml",
        )

    async def _iter_copy_source(
        self,
        client: S3Client,
        src_bucket: str,
        src_key: str,
        copy_source_range: str | None,
        src_wrapped_dek: str | None,
        src_multipart_meta,
        head_resp: dict,
        src_metadata: dict,
        skip_bytes: int = 0,
    ) -> AsyncIterator[bytes]:
        """Yield raw plaintext bytes from the copy source, one part/chunk at a time.

        skip_bytes skips that many plaintext bytes from the start of the
        (possibly ranged) source — used to rebuild the stream at an internal
        part boundary when the copy pump retries a failed part.
        """
        if src_multipart_meta:
            total = src_multipart_meta.total_plaintext_size
            if copy_source_range:
                range_start, range_end = self._parse_copy_source_range(copy_source_range, total)
            elif skip_bytes:
                range_start, range_end = 0, total - 1
            else:
                range_start, range_end = None, None
            if skip_bytes:
                range_start += skip_bytes
                if range_start > range_end:
                    return
            dek = crypto.unwrap_key(
                src_multipart_meta.wrapped_dek,
                self.keyring.key_by_id(src_multipart_meta.kid),
            )
            async for chunk in self._iter_multipart_plaintext(
                client, src_bucket, src_key, src_multipart_meta, dek, range_start, range_end
            ):
                yield chunk
        elif src_wrapped_dek:
            src_kid = src_metadata.get(self.settings.kidtag_name, "")
            plaintext = await self._download_encrypted_single(
                client, src_bucket, src_key, src_wrapped_dek, src_kid
            )
            if copy_source_range:
                start, end = self._parse_copy_source_range(copy_source_range, len(plaintext))
                plaintext = plaintext[start : end + 1]
            if skip_bytes:
                plaintext = plaintext[skip_bytes:]
            if plaintext:
                yield plaintext
            return
        else:
            range_header = copy_source_range
            if skip_bytes:
                if copy_source_range:
                    raw_start, raw_end = self._parse_raw_copy_source_range(copy_source_range)
                    range_header = f"bytes={raw_start + skip_bytes}-{raw_end}"
                else:
                    range_header = f"bytes={skip_bytes}-"
            resp = await client.get_object(src_bucket, src_key, range_header=range_header)
            async with resp["Body"] as body:
                # resp["Body"] enters as an aiohttp ClientResponse, whose read()
                # takes no size arg; stream via its StreamReader in bounded chunks
                # (body.read(n) raised TypeError and 500'd every passthrough copy).
                while True:
                    chunk = await body.content.read(crypto.MAX_BUFFER_SIZE)
                    if not chunk:
                        break
                    yield chunk

    async def _pump_copy_chunks(
        self,
        client: S3Client,
        bucket: str,
        key: str,
        upload_id: str,
        part_num: int,
        state: MultipartUploadState,
        src_iter: AsyncIterator[bytes],
        chunk_size: int,
        internal_part_start: int,
        *,
        leading_plaintext: bytes = b"",
        expected_plaintext: int,
        reopen_source: Callable[[int], AsyncIterator[bytes]] | None = None,
    ) -> tuple[list[InternalPartMetadata], int, int, object]:
        """Frame-encrypt the copy source into internal S3 parts, one at a time.

        Each internal part is uploaded as a FramedStreamBody that yields sealed
        frames while the source is being read, so peak memory is O(frame)
        instead of O(chunk_size). The part sizes are derived up front from
        expected_plaintext (source metadata is authoritative); a source that
        ends early fails loudly instead of writing an object with wrong sizes.

        A streamed body cannot be replayed by botocore, so failed internal
        parts are retried here: reopen_source(offset) rebuilds the plaintext
        stream at the failed part's offset (relative to the source, after
        leading_plaintext). The client-part MD5 is committed per attempt from a
        copy() snapshot so retried bytes are never hashed twice.
        """
        reader = _PlaintextReader(src_iter, prefix=leading_plaintext)
        md5 = hashlib.md5(usedforsecurity=False)
        internal_parts: list[InternalPartMetadata] = []
        total_ciphertext = 0
        offset = 0
        index = 0

        while offset < expected_plaintext:
            part_plaintext = min(chunk_size, expected_plaintext - offset)
            ipn = internal_part_start + index
            ct_size = crypto.framed_ciphertext_size(part_plaintext)
            part_reserve = crypto.copy_chunk_peak(part_plaintext)

            for attempt in range(1, COPY_INTERNAL_PART_ATTEMPTS + 1):
                md5_attempt = md5.copy()
                body = FramedStreamBody(
                    reader,
                    part_plaintext,
                    state.dek,
                    upload_id,
                    ipn,
                    plaintext_hashes=(md5_attempt,),
                )
                upload_start = time.monotonic()
                try:
                    async with concurrency.reserve_copy_memory(part_reserve):
                        resp = await client.upload_part(bucket, key, upload_id, ipn, body)
                    md5 = md5_attempt
                    break
                except Exception as e:
                    if attempt == COPY_INTERNAL_PART_ATTEMPTS or reopen_source is None:
                        raise
                    logger.warning(
                        "COPY_INTERNAL_PART_RETRY",
                        bucket=bucket,
                        key=key,
                        client_part=part_num,
                        internal_part=ipn,
                        attempt=attempt,
                        plaintext_offset=offset,
                        error_type=type(e).__name__,
                        error=str(e),
                    )
                    if offset < len(leading_plaintext):
                        reader = _PlaintextReader(
                            reopen_source(0), prefix=leading_plaintext[offset:]
                        )
                    else:
                        reader = _PlaintextReader(reopen_source(offset - len(leading_plaintext)))

            internal_parts.append(
                InternalPartMetadata(
                    internal_part_number=ipn,
                    plaintext_size=part_plaintext,
                    ciphertext_size=ct_size,
                    etag=resp["ETag"].strip('"'),
                )
            )
            logger.info(
                "INTERNAL_PART_UPLOADED",
                bucket=bucket,
                key=key,
                client_part=part_num,
                internal_part=ipn,
                plaintext_mb=f"{part_plaintext / 1024 / 1024:.2f}MB",
                elapsed_sec=f"{time.monotonic() - upload_start:.2f}s",
            )
            offset += part_plaintext
            total_ciphertext += ct_size
            index += 1

        return internal_parts, offset, total_ciphertext, md5
