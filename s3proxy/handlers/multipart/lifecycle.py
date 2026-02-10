"""Multipart upload lifecycle operations: Create, Complete, Abort."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import xml.etree.ElementTree as ET
from typing import NoReturn

import structlog
from botocore.exceptions import ClientError
from fastapi import Request, Response
from structlog.stdlib import BoundLogger

from ... import crypto, xml_responses
from ...errors import S3Error
from ...s3client import S3Client, S3Credentials
from ...state import (
    InternalPartMetadata,
    MultipartMetadata,
    MultipartUploadState,
    PartMetadata,
    delete_upload_state,
    load_upload_state,
    persist_upload_state,
    save_multipart_metadata,
)
from ...xml_utils import find_elements, get_element_text
from ..base import BaseHandler

logger: BoundLogger = structlog.get_logger(__name__)


class LifecycleMixin(BaseHandler):
    async def _recover_upload_state(
        self, client: S3Client, bucket: str, key: str, upload_id: str, context: str = ""
    ) -> MultipartUploadState:
        from s3proxy.state import reconstruct_upload_state_from_s3

        logger.warning(
            "RECOVER_STATE_FROM_S3",
            bucket=bucket,
            key=key,
            upload_id=upload_id[:20] + "...",
            context=context,
        )

        state = await reconstruct_upload_state_from_s3(
            client, bucket, key, upload_id, self.settings.kek
        )
        if not state:
            raise S3Error.no_such_upload(upload_id)

        await self.multipart_manager.store_reconstructed_state(bucket, key, upload_id, state)
        logger.info(
            "RECOVER_STATE_SUCCESS",
            bucket=bucket,
            key=key,
            upload_id=upload_id[:20] + "...",
            parts_recovered=len(state.parts),
        )
        return state

    async def handle_create_multipart_upload(
        self, request: Request, creds: S3Credentials
    ) -> Response:
        bucket, key = self._parse_path(request.url.path)
        logger.info("CREATE_MULTIPART", bucket=bucket, key=key)

        async with self._client(creds) as client:
            content_type = request.headers.get("content-type", "application/octet-stream")
            tagging = request.headers.get("x-amz-tagging")
            cache_control = request.headers.get("cache-control")
            expires = request.headers.get("expires")

            dek = crypto.generate_dek()
            wrapped_dek = crypto.wrap_key(dek, self.settings.kek)

            # Build metadata (include user's x-amz-meta-*)
            upload_metadata = {
                self.settings.dektag_name: base64.b64encode(wrapped_dek).decode(),
            }
            for hdr, val in request.headers.items():
                if hdr.lower().startswith("x-amz-meta-"):
                    upload_metadata[hdr[11:]] = val

            resp = await client.create_multipart_upload(
                bucket,
                key,
                content_type=content_type,
                metadata=upload_metadata,
                tagging=tagging,
                cache_control=cache_control,
                expires=expires,
            )
            upload_id = resp["UploadId"]

            # Store state in Redis/memory first, then persist to S3 as backup
            await self.multipart_manager.create_upload(bucket, key, upload_id, dek)

            # Persist DEK to S3 as backup - retry once on failure
            for attempt in range(2):
                try:
                    await persist_upload_state(client, bucket, key, upload_id, wrapped_dek)
                    break
                except Exception as e:
                    if attempt == 0:
                        logger.warning(
                            "PERSIST_STATE_RETRY",
                            bucket=bucket,
                            key=key,
                            upload_id=upload_id[:20] + "...",
                            error=str(e),
                        )
                    else:
                        logger.error(
                            "PERSIST_STATE_FAILED",
                            bucket=bucket,
                            key=key,
                            upload_id=upload_id[:20] + "...",
                            error=str(e),
                        )

            logger.info(
                "CREATE_MULTIPART_COMPLETE",
                bucket=bucket,
                key=key,
                upload_id=upload_id[:20] + "...",
            )

            return Response(
                content=xml_responses.initiate_multipart(bucket, key, upload_id),
                media_type="application/xml",
            )

    async def handle_complete_multipart_upload(
        self, request: Request, creds: S3Credentials
    ) -> Response:
        bucket, key = self._parse_path(request.url.path)
        async with self._client(creds) as client:
            upload_id, _ = self._extract_multipart_params(request)

            state = await self.multipart_manager.complete_upload(bucket, key, upload_id)
            if not state:
                state = await self._recover_state_for_complete(client, bucket, key, upload_id)

            # Parse client's part list
            body = await request.body()
            client_parts = self._parse_client_parts(body)

            # Build S3 parts list
            s3_parts, completed_parts, total_plaintext = self._build_s3_parts(
                client_parts, state, bucket, key, upload_id
            )

            logger.info(
                "COMPLETE_MULTIPART",
                bucket=bucket,
                key=key,
                upload_id=upload_id[:20] + "...",
                client_parts=len(completed_parts),
                s3_parts=len(s3_parts),
                total_mb=f"{total_plaintext / 1024 / 1024:.2f}MB",
            )

            # Complete in S3
            try:
                await client.complete_multipart_upload(bucket, key, upload_id, s3_parts)
            except ClientError as e:
                await self._handle_complete_error(
                    e, client, bucket, key, upload_id, s3_parts, completed_parts, total_plaintext
                )

            # Save metadata first, then delete state.
            # Order matters: if metadata save fails, state is preserved
            # so the upload can be retried. Deleting state first would
            # lose the DEK, making the object permanently undecryptable.
            wrapped_dek = crypto.wrap_key(state.dek, self.settings.kek)
            await save_multipart_metadata(
                client,
                bucket,
                key,
                MultipartMetadata(
                    version=1,
                    part_count=len(completed_parts),
                    total_plaintext_size=total_plaintext,
                    parts=completed_parts,
                    wrapped_dek=wrapped_dek,
                ),
            )
            await delete_upload_state(client, bucket, key, upload_id)

            logger.info(
                "COMPLETE_MULTIPART_SUCCESS",
                bucket=bucket,
                key=key,
                upload_id=upload_id[:20] + "...",
                total_parts=len(completed_parts),
                total_mb=f"{total_plaintext / 1024 / 1024:.2f}MB",
            )

            location = f"{self.settings.s3_endpoint}/{bucket}/{key}"
            etag = hashlib.md5(
                str(state.total_plaintext_size).encode(), usedforsecurity=False
            ).hexdigest()

            return Response(
                content=xml_responses.complete_multipart(location, bucket, key, etag),
                media_type="application/xml",
            )

    async def _recover_state_for_complete(
        self, client: S3Client, bucket: str, key: str, upload_id: str
    ) -> MultipartUploadState | None:
        from collections import defaultdict

        from ... import crypto
        from ...state import MAX_INTERNAL_PARTS_PER_CLIENT

        def internal_to_client_part(internal_part_number: int) -> int:
            """Convert internal part number to client part number."""
            return ((internal_part_number - 1) // MAX_INTERNAL_PARTS_PER_CLIENT) + 1

        dek = await load_upload_state(client, bucket, key, upload_id, self.settings.kek)
        if not dek:
            # Check if upload exists in S3 before returning NoSuchUpload
            try:
                await client.list_parts(bucket, key, upload_id, max_parts=1)
                # Upload exists but DEK is missing - internal state corruption
                logger.error(
                    "RECOVER_DEK_MISSING",
                    bucket=bucket,
                    key=key,
                    upload_id=upload_id[:20] + "...",
                    message="Upload exists in S3 but DEK state is missing",
                )
            except Exception:
                # Upload doesn't exist in S3
                pass
            raise S3Error.no_such_upload(upload_id)

        state = await self.multipart_manager.create_upload(bucket, key, upload_id, dek)

        try:
            parts_resp = await client.list_parts(bucket, key, upload_id)

            # Group S3 internal parts by client part number
            client_parts: dict[int, list[dict]] = defaultdict(list)
            for part in parts_resp.get("Parts", []):
                internal_part_num = part.get("PartNumber", 0)
                client_part_num = internal_to_client_part(internal_part_num)
                client_parts[client_part_num].append(part)

            logger.debug(
                "RECOVER_STATE_GROUPING",
                bucket=bucket,
                key=key,
                upload_id=upload_id[:20] + "...",
                s3_parts=len(parts_resp.get("Parts", [])),
                client_parts=sorted(client_parts.keys()),
            )

            # Build PartMetadata for each client part
            for client_part_num, internal_s3_parts in client_parts.items():
                internal_s3_parts.sort(key=lambda p: p.get("PartNumber", 0))

                internal_parts_meta = []
                part_plaintext_size = 0
                part_ciphertext_size = 0

                for s3_part in internal_s3_parts:
                    internal_num = s3_part.get("PartNumber", 0)
                    ciphertext_size = s3_part.get("Size", 0)
                    plaintext_size = crypto.plaintext_size(ciphertext_size)
                    etag = s3_part.get("ETag", "").strip('"')

                    internal_parts_meta.append(
                        InternalPartMetadata(
                            internal_part_number=internal_num,
                            plaintext_size=plaintext_size,
                            ciphertext_size=ciphertext_size,
                            etag=etag,
                        )
                    )
                    part_plaintext_size += plaintext_size
                    part_ciphertext_size += ciphertext_size

                first_etag = internal_s3_parts[0].get("ETag", "").strip('"')

                await self.multipart_manager.add_part(
                    bucket,
                    key,
                    upload_id,
                    PartMetadata(
                        client_part_num,
                        part_plaintext_size,
                        part_ciphertext_size,
                        first_etag,
                        "",
                        internal_parts=internal_parts_meta,
                    ),
                )

            state = await self.multipart_manager.get_upload(bucket, key, upload_id)
        except Exception as e:
            logger.error(
                "RECOVER_STATE_FOR_COMPLETE_FAILED",
                bucket=bucket,
                key=key,
                upload_id=upload_id[:20] + "...",
                error=str(e),
            )
        return state

    def _parse_client_parts(self, body: bytes) -> list[dict]:
        client_parts = []
        root = ET.fromstring(body.decode())
        for part in find_elements(root, "Part"):
            pn_text = get_element_text(part, "PartNumber")
            etag_text = get_element_text(part, "ETag")
            if pn_text and etag_text:
                client_parts.append({"PartNumber": int(pn_text), "ETag": etag_text})
        return client_parts

    def _build_s3_parts(
        self,
        client_parts: list[dict[str, int | str]],
        state: MultipartUploadState,
        bucket: str,
        key: str,
        upload_id: str,
    ) -> tuple[list[dict[str, int | str]], list[PartMetadata], int]:
        s3_parts = []
        completed_parts = []
        total_plaintext = 0
        missing_parts = []

        for cp in sorted(client_parts, key=lambda x: x["PartNumber"]):
            client_part_num = cp["PartNumber"]
            if client_part_num in state.parts:
                part_meta = state.parts[client_part_num]
                completed_parts.append(part_meta)
                total_plaintext += part_meta.plaintext_size

                if part_meta.internal_parts:
                    sorted_internal = sorted(
                        part_meta.internal_parts, key=lambda x: x.internal_part_number
                    )
                    for ip in sorted_internal:
                        etag = f'"{ip.etag}"' if not ip.etag.startswith('"') else ip.etag
                        s3_parts.append(
                            {
                                "PartNumber": ip.internal_part_number,
                                "ETag": etag,
                            }
                        )
                else:
                    s3_parts.append(
                        {
                            "PartNumber": client_part_num,
                            "ETag": cp["ETag"],
                        }
                    )
            else:
                missing_parts.append(client_part_num)

        if missing_parts:
            raise S3Error.invalid_part(f"Parts {missing_parts} were never uploaded")
        if not s3_parts:
            raise S3Error.invalid_part("No valid parts found")

        s3_parts.sort(key=lambda p: p["PartNumber"])
        return s3_parts, completed_parts, total_plaintext

    async def _handle_complete_error(
        self,
        e: ClientError,
        client: S3Client,
        bucket: str,
        key: str,
        upload_id: str,
        s3_parts: list[dict[str, int | str]],
        completed_parts: list[PartMetadata],
        total_plaintext: int,
    ) -> NoReturn:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code == "EntityTooSmall":
            logger.warning(
                "ENTITY_TOO_SMALL",
                bucket=bucket,
                key=key,
                upload_id=upload_id[:20] + "...",
                parts=len(s3_parts),
                total_plaintext=total_plaintext,
            )
            with contextlib.suppress(Exception):
                await client.abort_multipart_upload(bucket, key, upload_id)

            part_sizes = [p.plaintext_size for p in completed_parts]
            raise S3Error.invalid_request(
                f"S3 requires all parts except last >= 5MB. "
                f"Parts have sizes: {part_sizes}. "
                f"Configure client part_size >= 5MB."
            ) from e
        raise

    async def handle_abort_multipart_upload(
        self, request: Request, creds: S3Credentials
    ) -> Response:
        bucket, key = self._parse_path(request.url.path)
        async with self._client(creds) as client:
            upload_id, _ = self._extract_multipart_params(request)

            logger.info(
                "ABORT_MULTIPART",
                bucket=bucket,
                key=key,
                upload_id=upload_id[:20] + "...",
            )

            await asyncio.gather(
                self.multipart_manager.abort_upload(bucket, key, upload_id),
                self._safe_abort(client, bucket, key, upload_id),
                delete_upload_state(client, bucket, key, upload_id),
            )

            return Response(status_code=204)
