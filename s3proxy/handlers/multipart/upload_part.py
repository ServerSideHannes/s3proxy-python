"""UploadPart handler with streaming support."""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections import deque
from collections.abc import AsyncIterator
from typing import NoReturn

import structlog
from botocore.exceptions import ClientError
from fastapi import Request, Response
from structlog.stdlib import BoundLogger

from ... import crypto
from ...errors import S3Error, raise_for_client_error, raise_for_exception
from ...s3client import S3Client, S3Credentials
from ...state import (
    InternalPartMetadata,
    MultipartUploadState,
    PartMetadata,
    StateMissingError,
)
from ...streaming import decode_aws_chunked_stream
from ..base import BaseHandler

logger: BoundLogger = structlog.get_logger(__name__)

# Limit concurrent internal part uploads to bound memory usage
MAX_PARALLEL_INTERNAL_UPLOADS = 2


class UploadPartMixin(BaseHandler):
    async def handle_upload_part(self, request: Request, creds: S3Credentials) -> Response:
        """Memory usage is O(MAX_BUFFER_SIZE) regardless of client part size."""
        bucket, key = self._parse_path(request.url.path)
        async with self._client(creds) as client:
            upload_id, part_num = self._extract_multipart_params(request)

            # Get upload state
            state = await self._get_or_recover_state(client, bucket, key, upload_id, part_num)

            # Parse request info
            content_encoding = request.headers.get("content-encoding", "")
            content_sha = request.headers.get("x-amz-content-sha256", "")
            try:
                content_length = int(request.headers.get("content-length", "0"))
            except ValueError:
                content_length = 0

            upload_start_time = time.monotonic()
            logger.info(
                "UPLOAD_PART_START",
                bucket=bucket,
                key=key,
                upload_id=upload_id[:20] + "...",
                part_number=part_num,
                content_length_mb=f"{content_length / 1024 / 1024:.2f}MB",
            )

            # Determine encoding type
            is_unsigned = content_sha == "UNSIGNED-PAYLOAD"
            is_streaming_sig = content_sha.startswith("STREAMING-")
            needs_chunked_decode = "aws-chunked" in content_encoding or is_streaming_sig
            is_large_signed = (
                not is_unsigned
                and not is_streaming_sig
                and content_length > crypto.STREAMING_THRESHOLD
            )

            # Calculate optimal part size
            optimal_part_size = crypto.calculate_optimal_part_size(content_length)
            estimated_parts = max(1, (content_length + optimal_part_size - 1) // optimal_part_size)

            logger.info(
                "UPLOAD_PART_CONFIG",
                bucket=bucket,
                key=key,
                part_number=part_num,
                optimal_part_size_mb=f"{optimal_part_size / 1024 / 1024:.2f}MB",
                estimated_internal_parts=estimated_parts,
            )

            # Allocate internal part numbers
            internal_part_start = await self.multipart_manager.allocate_internal_parts(
                bucket,
                key,
                upload_id,
                estimated_parts,
                client_part_number=part_num,
            )

            try:
                result = await self._stream_and_upload(
                    request,
                    client,
                    bucket,
                    key,
                    upload_id,
                    part_num,
                    state,
                    content_sha,
                    content_length,
                    is_unsigned,
                    is_streaming_sig,
                    is_large_signed,
                    needs_chunked_decode,
                    optimal_part_size,
                    internal_part_start,
                )

                # Late signature verification for large signed uploads
                if is_large_signed and content_sha and result["computed_sha256"] != content_sha:
                    logger.warning(
                        "UPLOAD_PART_SHA256_MISMATCH",
                        bucket=bucket,
                        key=key,
                        part_num=part_num,
                        expected=content_sha,
                        computed=result["computed_sha256"],
                    )
                    raise S3Error.signature_does_not_match("Signature verification failed")

                upload_duration = time.monotonic() - upload_start_time
                logger.info(
                    "UPLOAD_PART_COMPLETE",
                    bucket=bucket,
                    key=key,
                    part_number=part_num,
                    plaintext_mb=f"{result['total_plaintext_size'] / 1024 / 1024:.2f}MB",
                    internal_parts=result["internal_parts_count"],
                    duration_sec=f"{upload_duration:.2f}",
                )

                return Response(headers={"ETag": f'"{result["client_etag"]}"'})

            except S3Error:
                raise
            except ClientError as e:
                return self._handle_client_error(e, bucket, key, part_num, upload_id)
            except Exception as e:
                return self._handle_generic_error(e, bucket, key, part_num, upload_id)

    async def _get_or_recover_state(
        self, client: S3Client, bucket: str, key: str, upload_id: str, part_num: int
    ) -> MultipartUploadState:
        state = await self.multipart_manager.get_upload(bucket, key, upload_id)
        if not state:
            logger.warning(
                "UPLOAD_PART_STATE_MISSING",
                bucket=bucket,
                key=key,
                upload_id=upload_id[:20] + "...",
            )
            state = await self._recover_upload_state(
                client, bucket, key, upload_id, context="initial lookup"
            )
        return state

    async def _stream_and_upload(
        self,
        request: Request,
        client: S3Client,
        bucket: str,
        key: str,
        upload_id: str,
        part_num: int,
        state: MultipartUploadState,
        content_sha: str,
        content_length: int,
        is_unsigned: bool,
        is_streaming_sig: bool,
        is_large_signed: bool,
        needs_chunked_decode: bool,
        optimal_part_size: int,
        internal_part_start: int,
    ) -> dict[str, str | int]:
        # Initialize state
        buffer_chunks: deque[bytes] = deque()
        buffer_size = 0
        md5_hash = hashlib.md5(usedforsecurity=False)
        sha256_hash = hashlib.sha256()
        total_plaintext_size = 0
        total_ciphertext_size = 0
        internal_parts: list[InternalPartMetadata] = []
        current_internal_part = internal_part_start

        # Get stream source
        stream_source = await self._get_stream_source(
            request,
            is_unsigned,
            is_streaming_sig,
            is_large_signed,
            needs_chunked_decode,
            content_length,
            part_num,
        )

        # Set up parallel upload infrastructure
        upload_tasks: dict[int, asyncio.Task] = {}
        upload_semaphore = asyncio.Semaphore(MAX_PARALLEL_INTERNAL_UPLOADS)

        # Process stream
        async for chunk in stream_source:
            if not chunk:
                continue

            buffer_chunks.append(chunk)
            buffer_size += len(chunk)
            md5_hash.update(chunk)
            sha256_hash.update(chunk)
            total_plaintext_size += len(chunk)

            # Upload when buffer reaches optimal size
            while buffer_size >= optimal_part_size:
                await upload_semaphore.acquire()

                part_data, buffer_size = self._extract_part_data(
                    buffer_chunks, buffer_size, optimal_part_size
                )
                internal_part_num = current_internal_part
                current_internal_part += 1

                task = asyncio.create_task(
                    self._upload_internal_part_with_semaphore(
                        client,
                        bucket,
                        key,
                        upload_id,
                        part_num,
                        state,
                        part_data,
                        internal_part_num,
                        upload_semaphore,
                    )
                )
                upload_tasks[internal_part_num] = task

        # Upload remaining buffer
        if buffer_chunks:
            await upload_semaphore.acquire()
            remaining = b"".join(buffer_chunks)
            buffer_chunks.clear()
            internal_part_num = current_internal_part

            task = asyncio.create_task(
                self._upload_internal_part_with_semaphore(
                    client,
                    bucket,
                    key,
                    upload_id,
                    part_num,
                    state,
                    remaining,
                    internal_part_num,
                    upload_semaphore,
                )
            )
            upload_tasks[internal_part_num] = task

        # Wait for all uploads
        if upload_tasks:
            results = await asyncio.gather(*upload_tasks.values(), return_exceptions=True)
            self._check_upload_results(results, bucket, key, upload_id, part_num)

            # Collect results in order
            part_num_to_result = {r.internal_part_number: r for r in results}
            for pn in sorted(part_num_to_result.keys()):
                meta = part_num_to_result[pn]
                internal_parts.append(meta)
                total_ciphertext_size += meta.ciphertext_size

        # Store part metadata
        client_etag = md5_hash.hexdigest()
        part_meta = PartMetadata(
            part_number=part_num,
            plaintext_size=total_plaintext_size,
            ciphertext_size=total_ciphertext_size,
            etag=client_etag,
            md5=client_etag,
            internal_parts=internal_parts,
        )

        try:
            await self.multipart_manager.add_part(bucket, key, upload_id, part_meta)
        except StateMissingError:
            await self._recover_upload_state(
                client, bucket, key, upload_id, context="after part upload"
            )
            await self.multipart_manager.add_part(bucket, key, upload_id, part_meta)

        return {
            "client_etag": client_etag,
            "total_plaintext_size": total_plaintext_size,
            "total_ciphertext_size": total_ciphertext_size,
            "internal_parts_count": len(internal_parts),
            "computed_sha256": sha256_hash.hexdigest(),
        }

    async def _get_stream_source(
        self,
        request: Request,
        is_unsigned: bool,
        is_streaming_sig: bool,
        is_large_signed: bool,
        needs_chunked_decode: bool,
        content_length: int,
        part_num: int,
    ) -> AsyncIterator[bytes]:
        if needs_chunked_decode:
            logger.debug("STREAM_SOURCE_CHUNKED", part_number=part_num)
            return decode_aws_chunked_stream(request)
        elif is_unsigned or is_streaming_sig or is_large_signed:
            logger.debug(
                "STREAM_SOURCE_DIRECT",
                part_number=part_num,
                is_unsigned=is_unsigned,
                is_large_signed=is_large_signed,
            )
            return request.stream()
        else:
            # Small signed upload - buffer body
            logger.debug(
                "STREAM_SOURCE_BUFFERED",
                part_number=part_num,
                content_length_mb=f"{content_length / 1024 / 1024:.2f}MB",
            )
            body = await request.body()

            async def body_iter():
                yield body

            return body_iter()

    def _extract_part_data(
        self, buffer_chunks: deque[bytes], buffer_size: int, optimal_part_size: int
    ) -> tuple[bytes, int]:
        part_bytes = bytearray()
        bytes_needed = optimal_part_size

        while bytes_needed > 0 and buffer_chunks:
            chunk = buffer_chunks.popleft()
            chunk_len = len(chunk)

            if chunk_len <= bytes_needed:
                part_bytes.extend(chunk)
                bytes_needed -= chunk_len
                buffer_size -= chunk_len
            else:
                part_bytes.extend(chunk[:bytes_needed])
                buffer_chunks.appendleft(chunk[bytes_needed:])
                buffer_size -= bytes_needed
                bytes_needed = 0

        return bytes(part_bytes), buffer_size

    async def _upload_internal_part_with_semaphore(
        self,
        client: S3Client,
        bucket: str,
        key: str,
        upload_id: str,
        client_part_num: int,
        state: MultipartUploadState,
        data: bytes,
        internal_part_num: int,
        semaphore: asyncio.Semaphore,
    ) -> InternalPartMetadata:
        data_size = len(data)
        upload_start = time.monotonic()

        try:
            # Encrypt
            nonce = crypto.derive_part_nonce(upload_id, internal_part_num)
            ciphertext = crypto.encrypt(data, state.dek, nonce)
            plaintext_size = len(data)
            ciphertext_size = len(ciphertext)
            del data  # Free memory

            # Upload
            resp = await client.upload_part(bucket, key, upload_id, internal_part_num, ciphertext)
            etag = resp["ETag"].strip('"')
            del ciphertext  # Free memory

            elapsed = time.monotonic() - upload_start
            logger.info(
                "INTERNAL_PART_UPLOADED",
                bucket=bucket,
                key=key,
                client_part=client_part_num,
                internal_part=internal_part_num,
                plaintext_mb=f"{plaintext_size / 1024 / 1024:.2f}MB",
                elapsed_sec=f"{elapsed:.2f}s",
            )

            return InternalPartMetadata(
                internal_part_number=internal_part_num,
                plaintext_size=plaintext_size,
                ciphertext_size=ciphertext_size,
                etag=etag,
            )
        finally:
            logger.debug(
                "UPLOAD_SLOT_RELEASED",
                internal_part=internal_part_num,
                freed_mb=f"{data_size / 1024 / 1024:.1f}MB",
            )
            semaphore.release()

    def _check_upload_results(
        self,
        results: list[InternalPartMetadata | BaseException],
        bucket: str,
        key: str,
        upload_id: str,
        part_num: int,
    ) -> None:
        for result in results:
            if isinstance(result, Exception):
                exc_name = type(result).__name__
                is_no_such_upload = False

                if isinstance(result, ClientError):
                    error_code = result.response.get("Error", {}).get("Code", "")
                    is_no_such_upload = error_code == "NoSuchUpload"
                elif exc_name == "NoSuchUpload" or "NoSuchUpload" in str(result):
                    is_no_such_upload = True

                if is_no_such_upload:
                    logger.warning(
                        "UPLOAD_ABORTED_BY_CLIENT",
                        bucket=bucket,
                        key=key,
                        upload_id=upload_id,
                    )
                    raise S3Error.no_such_upload(upload_id)
                raise result

    def _handle_client_error(
        self, e: ClientError, bucket: str, key: str, part_num: int, upload_id: str
    ) -> NoReturn:
        logger.error("UPLOAD_PART_CLIENT_ERROR", bucket=bucket, key=key, part_num=part_num)
        raise_for_client_error(e, bucket, key)

    def _handle_generic_error(
        self, e: Exception, bucket: str, key: str, part_num: int, upload_id: str
    ) -> NoReturn:
        logger.error("UPLOAD_PART_ERROR", bucket=bucket, key=key, part_num=part_num)
        raise_for_exception(e)
