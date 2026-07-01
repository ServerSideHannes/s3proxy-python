"""UploadPartCopy handler for multipart uploads."""

import asyncio
import hashlib
import math
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import structlog
from fastapi import Request, Response
from structlog.stdlib import BoundLogger

from ... import concurrency, crypto, xml_responses
from ...client import S3Client, S3Credentials
from ...errors import S3Error
from ...state import (
    InternalPartMetadata,
    MultipartUploadState,
    PartMetadata,
    load_multipart_metadata,
    load_upload_state,
)
from ...utils import format_iso8601
from ..base import BaseHandler

logger: BoundLogger = structlog.get_logger(__name__)

MAX_PARALLEL_INTERNAL_UPLOADS = 2


class CopyPartMixin(BaseHandler):
    async def handle_upload_part_copy(self, request: Request, creds: S3Credentials) -> Response:
        bucket, key = self._parse_path(request.url.path)
        async with self._client(creds) as client:
            upload_id, part_num = self._extract_multipart_params(request)
            copy_source = request.headers.get("x-amz-copy-source", "")
            copy_source_range = request.headers.get("x-amz-copy-source-range")
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

            plaintext_size = self._copy_plaintext_size(
                head_resp, copy_source_range, src_wrapped_dek, src_multipart_meta
            )

            if plaintext_size <= crypto.STREAMING_THRESHOLD:
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
        # reserved ~nothing -- but this streams the source through decrypt +
        # re-encrypt. Reserve the pipeline peak so concurrent copies are bounded
        # (a dedup flood otherwise runs unbounded and OOMs the pod).
        async with concurrency.reserve_memory(crypto.copy_pipeline_peak(plaintext_size)):
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
        # Cap the pump chunk at MAX_BUFFER_SIZE: calculate_optimal_part_size returns
        # up to 64MB for large sources, which made _pump_copy_chunks buffer a 64MB
        # chunk + copy it + re-encrypt (~150MB/copy) while the limiter only reserved
        # copy_pipeline_peak (~32MB). Under a scylla dedup flood of large SSTables
        # that under-reservation OOMed the pod. 8MB chunks keep the copy truly
        # streaming and matched to the reservation.
        chunk_size = min(crypto.calculate_optimal_part_size(plaintext_size), crypto.MAX_BUFFER_SIZE)
        estimated_parts = max(1, math.ceil(plaintext_size / chunk_size))

        internal_part_start = await self.multipart_manager.allocate_internal_parts(
            bucket, key, upload_id, estimated_parts, client_part_number=part_num
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
        """Buffer plaintext from src_iter into chunk_size pieces and upload as internal parts."""
        buf = bytearray()
        current_internal = internal_part_start
        upload_tasks: dict[int, asyncio.Task] = {}
        semaphore = asyncio.Semaphore(MAX_PARALLEL_INTERNAL_UPLOADS)
        md5 = hashlib.md5(usedforsecurity=False)
        total_plaintext = 0

        async for raw in src_iter:
            buf.extend(raw)
            md5.update(raw)
            total_plaintext += len(raw)

            while len(buf) >= chunk_size:
                data = bytes(buf[:chunk_size])
                del buf[:chunk_size]
                await semaphore.acquire()
                task = asyncio.create_task(
                    self._upload_internal_part_with_semaphore(  # type: ignore[attr-defined]
                        client,
                        bucket,
                        key,
                        upload_id,
                        part_num,
                        state,
                        data,
                        current_internal,
                        semaphore,
                    )
                )
                upload_tasks[current_internal] = task
                current_internal += 1

        if buf:
            await semaphore.acquire()
            data = bytes(buf)
            task = asyncio.create_task(
                self._upload_internal_part_with_semaphore(  # type: ignore[attr-defined]
                    client,
                    bucket,
                    key,
                    upload_id,
                    part_num,
                    state,
                    data,
                    current_internal,
                    semaphore,
                )
            )
            upload_tasks[current_internal] = task

        results = await asyncio.gather(*upload_tasks.values(), return_exceptions=True)
        self._check_upload_results(results, bucket, key, upload_id, part_num)  # type: ignore[attr-defined]

        results_by_part: dict[int, InternalPartMetadata] = {
            r.internal_part_number: r
            for r in results  # type: ignore[union-attr]
        }
        internal_parts = []
        total_ciphertext = 0
        for pn in sorted(results_by_part):
            meta = results_by_part[pn]
            internal_parts.append(meta)
            total_ciphertext += meta.ciphertext_size

        return internal_parts, total_plaintext, total_ciphertext, md5
