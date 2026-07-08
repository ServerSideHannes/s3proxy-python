"""UploadPartCopy handler for multipart uploads."""

import asyncio
import base64
import hashlib
import math
import os
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import quote

import structlog
from fastapi import Request, Response
from structlog.stdlib import BoundLogger

from ... import concurrency, crypto, xml_responses
from ...client import S3Client, S3Credentials
from ...errors import S3Error
from ...state import (
    InternalPartMetadata,
    MultipartMetadata,
    MultipartUploadState,
    PartMetadata,
    load_multipart_metadata,
    load_upload_state,
    persist_upload_state,
)
from ...utils import format_iso8601
from ..base import BaseHandler
from .upload_part import _PlaintextReader

logger: BoundLogger = structlog.get_logger(__name__)

# Cap concurrent streaming copy pipelines per pod. Without this, HAProxy maxconn
# can land many UploadPartCopy requests on one pod; each holds ciphertext and
# read buffers outside the governor if reservation is released before upload.
MAX_PARALLEL_COPY_PIPELINES = int(os.environ.get("S3PROXY_MAX_PARALLEL_COPIES", "2"))
_copy_pipeline_semaphore = asyncio.Semaphore(MAX_PARALLEL_COPY_PIPELINES)


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
                raise S3Error.no_such_key(src_key) from e

            src_metadata = head_resp.get("Metadata", {})
            src_wrapped_dek = src_metadata.get(self.settings.dektag_name)
            src_multipart_meta = await load_multipart_metadata(client, src_bucket, src_key)

            total_plaintext = self._copy_plaintext_size(
                head_resp, None, src_wrapped_dek, src_multipart_meta
            )
            copy_source_range = self._normalize_copy_source_range(
                raw_copy_source_range, total_plaintext
            )
            plaintext_size = self._copy_plaintext_size(
                head_resp, copy_source_range, src_wrapped_dek, src_multipart_meta
            )

            if plaintext_size <= crypto.STREAMING_THRESHOLD:
                # Small copies buffer the whole object + re-encrypt it; gate them
                # by the limiter too (they carry no body, so the request-level
                # reservation was ~nothing and a small-object flood ran unbounded).
                if self._can_passthrough_part_copy(
                    copy_source_range,
                    src_wrapped_dek,
                    src_multipart_meta,
                    src_metadata,
                    creds,
                    plaintext_size,
                ):
                    return await self._gated_passthrough_copy_part(
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
                    )
                peak = crypto.copy_pipeline_peak(plaintext_size)
                async with concurrency.reserve_copy_memory(peak):
                    return await self._simple_copy_part(
                        client,
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
            if self._can_passthrough_part_copy(
                copy_source_range,
                src_wrapped_dek,
                src_multipart_meta,
                src_metadata,
                creds,
                plaintext_size,
            ):
                return await self._gated_passthrough_copy_part(
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
                )
            return await self._streaming_copy_part(
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

    def _normalize_copy_source_range(
        self, copy_source_range: str | None, total_plaintext_size: int
    ) -> str | None:
        """Treat a range spanning the entire object as 'copy whole object'.

        Scylla Manager often sends ``bytes=0-(size-1)`` on manifest UploadPartCopy
        even when copying the full SST. That must not force the streaming re-encrypt
        path (which queues behind the copy pipeline and exceeds the 300s client timeout).
        """
        if not copy_source_range:
            return None
        start, end = self._parse_copy_source_range(copy_source_range, total_plaintext_size)
        if start == 0 and end == total_plaintext_size - 1:
            return None
        return copy_source_range

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
        if copy_source_range:
            return False
        if not src_wrapped_dek and not src_multipart_meta:
            return False
        # Large PutObject blobs have no sidecar part map; streaming re-encrypt frames them.
        if not src_multipart_meta and plaintext_size > crypto.STREAMING_THRESHOLD:
            return False
        if src_multipart_meta:
            src_kid = src_multipart_meta.kid
        else:
            src_kid = src_metadata.get(self.settings.kidtag_name, "")
        needs_rekey = bool(src_kid) and src_kid != creds.access_key
        return not needs_rekey

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

        copy_source_path = copy_source or f"/{src_bucket}/{quote(src_key, safe='/')}"

        logger.info(
            "UPLOAD_PART_COPY_PASSTHROUGH",
            bucket=bucket,
            key=key,
            part_num=part_num,
            src_bucket=src_bucket,
            src_key=src_key,
            plaintext_mb=f"{plaintext_size / 1024 / 1024:.2f}MB",
            segments=len(segments),
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
                None,
                src_wrapped_dek,
                src_multipart_meta,
                head_resp,
                src_metadata,
            )
            etag = md5.hexdigest()
            await self.multipart_manager.add_part(
                bucket,
                key,
                upload_id,
                PartMetadata(
                    part_number=part_num,
                    plaintext_size=seg.plaintext_size,
                    ciphertext_size=seg.ciphertext_size,
                    etag=etag,
                    md5=etag,
                ),
            )
            return Response(
                content=xml_responses.upload_part_copy_result(
                    etag, format_iso8601(datetime.now(UTC))
                ),
                media_type="application/xml",
            )

        internal_part_start = await self.multipart_manager.allocate_internal_parts(
            bucket, key, upload_id, len(segments), client_part_number=0
        )

        internal_parts: list[InternalPartMetadata] = []
        total_plaintext = 0
        total_ciphertext = 0

        for i, seg in enumerate(segments):
            internal_num = internal_part_start + i
            ct_end = seg.ct_offset + seg.ciphertext_size - 1
            copy_range = f"bytes={seg.ct_offset}-{ct_end}"
            part_reserve = crypto.copy_passthrough_segment_peak(seg.plaintext_size)
            async with concurrency.reserve_copy_memory(part_reserve):
                resp = await client.upload_part_copy(
                    bucket,
                    key,
                    upload_id,
                    internal_num,
                    copy_source_path,
                    copy_source_range=copy_range,
                )
            etag_part = resp["CopyPartResult"]["ETag"].strip('"')
            internal_parts.append(
                InternalPartMetadata(
                    internal_part_number=internal_num,
                    plaintext_size=seg.plaintext_size,
                    ciphertext_size=seg.ciphertext_size,
                    etag=etag_part,
                )
            )
            total_plaintext += seg.plaintext_size
            total_ciphertext += seg.ciphertext_size

        md5 = await self._md5_plaintext_from_copy_source(
            client,
            src_bucket,
            src_key,
            None,
            src_wrapped_dek,
            src_multipart_meta,
            head_resp,
            src_metadata,
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
            "UPLOAD_PART_COPY_PASSTHROUGH_COMPLETE",
            bucket=bucket,
            key=key,
            part_num=part_num,
            internal_parts=len(internal_parts),
            plaintext_mb=f"{total_plaintext / 1024 / 1024:.2f}MB",
        )
        return Response(
            content=xml_responses.upload_part_copy_result(etag, format_iso8601(datetime.now(UTC))),
            media_type="application/xml",
        )

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

        internal_part_start = await self.multipart_manager.allocate_internal_parts(
            bucket, key, upload_id, estimated_parts, client_part_number=0
        )

        logger.info(
            "UPLOAD_PART_COPY_STREAMING",
            bucket=bucket,
            key=key,
            part_num=part_num,
            plaintext_mb=f"{plaintext_size / 1024 / 1024:.2f}MB",
            chunk_size_mb=f"{chunk_size / 1024 / 1024:.2f}MB",
            estimated_parts=estimated_parts,
        )

        src_iter = self._iter_copy_source(
            client,
            src_bucket,
            src_key,
            copy_source_range,
            src_wrapped_dek,
            src_multipart_meta,
            head_resp,
            src_metadata,
        )

        internal_parts, total_plaintext, total_ciphertext, md5 = await self._pump_copy_chunks(
            client,
            bucket,
            key,
            upload_id,
            part_num,
            state,
            src_iter,
            chunk_size,
            internal_part_start,
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
    ) -> AsyncIterator[bytes]:
        """Yield raw plaintext bytes from the copy source, one part/chunk at a time."""
        if src_multipart_meta:
            total = src_multipart_meta.total_plaintext_size
            if copy_source_range:
                range_start, range_end = self._parse_copy_source_range(copy_source_range, total)
            else:
                range_start, range_end = None, None
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
            yield plaintext
        else:
            resp = await client.get_object(src_bucket, src_key, range_header=copy_source_range)
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
    ) -> tuple[list[InternalPartMetadata], int, int, object]:
        """Frame-encrypt the copy source into internal S3 parts, one at a time.

        Mirrors UploadPartMixin._stream_and_upload_framed: reads FRAME_PLAINTEXT_SIZE
        plaintext frames from the source, encrypts each with encrypt_frame,
        accumulates one internal part's ciphertext, then uploads it before starting
        the next. Memory governor reservation is per internal part (acquire before
        encrypt, hold through upload, release after ciphertext is freed) so RSS
        matches what the limiter tracks and a multi-GB copy does not hold one
        reservation for its entire duration.
        """
        reader = _PlaintextReader(src_iter)
        md5 = hashlib.md5(usedforsecurity=False)
        internal_parts: list[InternalPartMetadata] = []
        total_plaintext = 0
        total_ciphertext = 0
        internal_part_num = internal_part_start

        prefetch: bytes | None = None
        while True:
            if prefetch is None:
                prefetch = await reader.read(min(crypto.FRAME_PLAINTEXT_SIZE, chunk_size))
                if not prefetch:
                    break

            part_reserve = crypto.copy_chunk_peak(chunk_size)
            ciphertext = bytearray()
            part_plaintext = 0
            frame_idx = 0
            async with concurrency.reserve_copy_memory(part_reserve):
                frame_pt = prefetch
                prefetch = None
                while True:
                    md5.update(frame_pt)
                    part_plaintext += len(frame_pt)
                    ciphertext.extend(
                        crypto.encrypt_frame(
                            frame_pt, state.dek, upload_id, internal_part_num, frame_idx
                        )
                    )
                    frame_idx += 1
                    if part_plaintext >= chunk_size:
                        break
                    frame_pt = await reader.read(
                        min(crypto.FRAME_PLAINTEXT_SIZE, chunk_size - part_plaintext)
                    )
                    if not frame_pt:
                        break

                upload_start = time.monotonic()
                resp = await client.upload_part(
                    bucket, key, upload_id, internal_part_num, ciphertext
                )
                del ciphertext

            ct_size = crypto.framed_ciphertext_size(part_plaintext)
            internal_parts.append(
                InternalPartMetadata(
                    internal_part_number=internal_part_num,
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
                internal_part=internal_part_num,
                plaintext_mb=f"{part_plaintext / 1024 / 1024:.2f}MB",
                elapsed_sec=f"{time.monotonic() - upload_start:.2f}s",
            )
            total_plaintext += part_plaintext
            total_ciphertext += ct_size
            internal_part_num += 1

        return internal_parts, total_plaintext, total_ciphertext, md5
