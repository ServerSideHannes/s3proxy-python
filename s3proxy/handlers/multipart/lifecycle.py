"""Multipart upload lifecycle operations: Create, Complete, Abort."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import os
import xml.etree.ElementTree as ET
from typing import Any, NoReturn

import structlog
from botocore.exceptions import ClientError
from fastapi import Request, Response
from structlog.stdlib import BoundLogger

from ... import crypto, xml_responses
from ...client import S3Client, S3Credentials
from ...errors import S3Error
from ...state import (
    MultipartMetadata,
    MultipartUploadState,
    PartMetadata,
    delete_upload_state,
    persist_upload_state,
    plaintext_attr_cache,
    save_multipart_metadata,
    synthetic_multipart_etag,
)
from ...xml_utils import find_elements, get_element_text
from ..base import BaseHandler, is_retryable_source_error

logger: BoundLogger = structlog.get_logger(__name__)

# Backend (Hetzner) can stream a 200 for CompleteMultipartUpload and still fail
# server-side mid-response, embedding an InternalError in the body - a documented
# S3-family quirk for this operation specifically. Retrying the same upload_id
# and part list is safe *unless* the prior attempt actually finished assembling
# the object before the response died, in which case the upload_id is already
# invalidated and a retry surfaces NoSuchUpload - see _verify_already_completed.
COMPLETE_RETRY_ATTEMPTS = int(os.environ.get("S3PROXY_COMPLETE_RETRY_ATTEMPTS", "4"))
COMPLETE_RETRY_BACKOFF_SEC = float(os.environ.get("S3PROXY_COMPLETE_RETRY_BACKOFF", "0.5"))


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

        state = await reconstruct_upload_state_from_s3(client, bucket, key, upload_id, self.keyring)
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

            kid, kek = self.keyring.key_for(client.credentials.access_key)
            dek = crypto.generate_dek()
            wrapped_dek = crypto.wrap_key(dek, kek)

            # Build metadata (include user's x-amz-meta-*)
            upload_metadata = {
                self.settings.dektag_name: base64.b64encode(wrapped_dek).decode(),
                self.settings.kidtag_name: kid,
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
            await self.multipart_manager.create_upload(bucket, key, upload_id, dek, kid)

            # Persist DEK to S3 as backup - retry once on failure
            for attempt in range(2):
                try:
                    await persist_upload_state(client, bucket, key, upload_id, wrapped_dek, kid)
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
                state = await self._recover_upload_state(
                    client, bucket, key, upload_id, context="for complete"
                )

            if state.deferred_copy_tail:
                logger.info(
                    "COMPLETE_MULTIPART_DEFERRED_TAIL_PENDING",
                    bucket=bucket,
                    key=key,
                    upload_id=upload_id[:20] + "...",
                    tail_bytes=len(state.deferred_copy_tail),
                )
                state = await self._flush_deferred_copy_tail_for_complete(
                    client, bucket, key, upload_id, state
                )

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
                complete_resp = await self._complete_multipart_upload_with_retry(
                    client, bucket, key, upload_id, s3_parts, completed_parts
                )
            except ClientError as e:
                await self._handle_complete_error(
                    e, client, bucket, key, upload_id, s3_parts, completed_parts, total_plaintext
                )
            else:
                plaintext_attr_cache.put(
                    bucket,
                    key,
                    str(complete_resp.get("ETag", "")).strip('"'),
                    total_plaintext,
                    synthetic_multipart_etag(total_plaintext),
                )

            # Save metadata first, then delete state.
            # Order matters: if metadata save fails, state is preserved
            # so the upload can be retried. Deleting state first would
            # lose the DEK, making the object permanently undecryptable.
            # Prefer the kid recorded when the upload was created; if the state
            # predates it (e.g. older recovered state), fall back to the
            # completing credential's key.
            if state.kid:
                kid, kek = state.kid, self.keyring.key_by_id(state.kid)
            else:
                kid, kek = self.keyring.key_for(creds.access_key)
            wrapped_dek = crypto.wrap_key(state.dek, kek)
            await save_multipart_metadata(
                client,
                bucket,
                key,
                MultipartMetadata(
                    version=2,
                    part_count=len(completed_parts),
                    total_plaintext_size=total_plaintext,
                    parts=completed_parts,
                    wrapped_dek=wrapped_dek,
                    kid=kid,
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
                    internal_plaintext = sum(ip.plaintext_size for ip in part_meta.internal_parts)
                    if internal_plaintext != part_meta.plaintext_size:
                        logger.warning(
                            "PART_PLAINTEXT_MISMATCH",
                            bucket=bucket,
                            key=key,
                            upload_id=upload_id[:20] + "...",
                            client_part=client_part_num,
                            part_plaintext=part_meta.plaintext_size,
                            internal_plaintext=internal_plaintext,
                            drift=part_meta.plaintext_size - internal_plaintext,
                        )
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
                    # Use the stored backend etag, not the client-echoed one:
                    # single-segment passthrough copies return a synthetic
                    # plaintext etag to the client that S3 would reject.
                    etag = (
                        f'"{part_meta.etag}"'
                        if not part_meta.etag.startswith('"')
                        else part_meta.etag
                    )
                    s3_parts.append(
                        {
                            "PartNumber": client_part_num,
                            "ETag": etag,
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

    async def _complete_multipart_upload_with_retry(
        self,
        client: S3Client,
        bucket: str,
        key: str,
        upload_id: str,
        s3_parts: list[dict[str, int | str]],
        completed_parts: list[PartMetadata],
    ) -> dict[str, Any]:
        expected_ciphertext_size = sum(p.ciphertext_size for p in completed_parts)
        last_exc: ClientError | None = None

        for attempt in range(1, COMPLETE_RETRY_ATTEMPTS + 1):
            try:
                return await client.complete_multipart_upload(bucket, key, upload_id, s3_parts)
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "")

                # A previous attempt may have actually finished assembling the
                # object before its response died - the upload_id would then
                # already be invalidated. Confirm before treating this as failed.
                if error_code == "NoSuchUpload" and attempt > 1:
                    verified = await self._verify_already_completed(
                        client, bucket, key, expected_ciphertext_size
                    )
                    if verified is not None:
                        logger.warning(
                            "COMPLETE_MULTIPART_ALREADY_DONE",
                            bucket=bucket,
                            key=key,
                            upload_id=upload_id[:20] + "...",
                            attempt=attempt,
                        )
                        return verified
                    raise

                if not is_retryable_source_error(e) or attempt == COMPLETE_RETRY_ATTEMPTS:
                    raise

                logger.warning(
                    "COMPLETE_MULTIPART_RETRY",
                    bucket=bucket,
                    key=key,
                    upload_id=upload_id[:20] + "...",
                    attempt=attempt,
                    aws_error_code=error_code,
                    error=str(e),
                )
                last_exc = e
                await asyncio.sleep(COMPLETE_RETRY_BACKOFF_SEC * (2 ** (attempt - 1)))

        assert last_exc is not None  # loop always returns or raises before exhausting
        raise last_exc

    async def _verify_already_completed(
        self, client: S3Client, bucket: str, key: str, expected_ciphertext_size: int
    ) -> dict[str, Any] | None:
        """Check whether a retried CompleteMultipartUpload's NoSuchUpload means the
        prior attempt actually succeeded (backend assembled the object, then the
        response delivery died before the client saw it)."""
        try:
            head = await client.head_object(bucket, key)
        except ClientError:
            return None
        if head.get("ContentLength") == expected_ciphertext_size:
            return {"ETag": head.get("ETag", "")}
        return None

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
