"""Miscellaneous object operations: HEAD, DELETE, COPY, Tagging."""

import asyncio
import base64
import hashlib
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from urllib.parse import quote

import structlog
from botocore.exceptions import ClientError
from fastapi import Request, Response
from structlog.stdlib import BoundLogger

from ... import concurrency, crypto, xml_responses
from ...client import S3Client, S3Credentials
from ...errors import S3Error
from ...state import (
    InternalPartMetadata,
    MultipartMetadata,
    PartMetadata,
    delete_multipart_metadata,
    load_multipart_metadata,
    save_multipart_metadata,
)
from ...state.metadata import _internal_meta_key
from ...utils import format_http_date, format_iso8601
from ...xml_utils import find_element, find_elements
from ..base import BaseHandler

logger: BoundLogger = structlog.get_logger(__name__)

# S3 CopyObject is a single server-side operation capped at 5 GiB; larger
# ciphertext objects fall back to the decrypt/re-encrypt streaming path.
# ponytail: 5 GiB ceiling; upgrade path is server-side UploadPartCopy per range.
MAX_SERVER_SIDE_COPY_BYTES = 5 * 1024**3


class MiscObjectMixin(BaseHandler):
    async def handle_head_object(self, request: Request, creds: S3Credentials) -> Response:
        bucket, key = self._parse_path(request.url.path)
        async with self._client(creds) as client:
            if_match, if_none_match, if_modified_since, if_unmodified_since = (
                self._extract_conditional_headers(request)
            )

            try:
                resp = await client.head_object(bucket, key)
                last_modified = format_http_date(resp.get("LastModified"))
                last_modified_dt = resp.get("LastModified")

                # Get the effective ETag (client-etag for encrypted, S3 etag otherwise)
                metadata = resp.get("Metadata", {})
                effective_etag = self._get_effective_etag(metadata, resp.get("ETag", ""))

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

                extra_headers = self._build_head_extra_headers(resp, last_modified)

                if meta := await load_multipart_metadata(client, bucket, key):
                    headers = {
                        "Content-Length": str(meta.total_plaintext_size),
                        "Content-Type": resp.get("ContentType", "application/octet-stream"),
                        "ETag": f'"{
                            hashlib.md5(
                                str(meta.total_plaintext_size).encode(),
                                usedforsecurity=False,
                            ).hexdigest()
                        }"',
                        **extra_headers,
                    }
                    return Response(headers=headers)

                size = self._get_plaintext_size(metadata, resp.get("ContentLength", 0))
                etag = self._get_effective_etag(metadata, resp.get("ETag", ""))

                headers = {
                    "Content-Length": str(size),
                    "Content-Type": resp.get("ContentType", "application/octet-stream"),
                    "ETag": f'"{etag}"',
                    **extra_headers,
                }
                return Response(headers=headers)

            except ClientError as e:
                self._raise_s3_error(e, bucket, key)

    def _build_head_extra_headers(self, resp: dict, last_modified: str | None) -> dict[str, str]:
        extra: dict[str, str] = {}
        if last_modified:
            extra["Last-Modified"] = last_modified
        if "CacheControl" in resp:
            extra["Cache-Control"] = resp["CacheControl"]
        if "Expires" in resp:
            exp = resp["Expires"]
            extra["Expires"] = format_http_date(exp) if hasattr(exp, "strftime") else str(exp)
        if resp.get("TagCount"):
            extra["x-amz-tagging-count"] = str(resp["TagCount"])
        # Include user metadata (x-amz-meta-*) excluding internal s3proxy keys
        metadata = resp.get("Metadata", {})
        internal_keys = self._internal_meta_keys()
        for key, value in metadata.items():
            if key.lower() not in internal_keys:
                extra[f"x-amz-meta-{key}"] = value
        return extra

    async def handle_delete_object(self, request: Request, creds: S3Credentials) -> Response:
        bucket, key = self._parse_path(request.url.path)
        logger.info("DELETE_OBJECT", bucket=bucket, key=key)

        async with self._client(creds) as client:
            try:
                await asyncio.gather(
                    client.delete_object(bucket, key),
                    delete_multipart_metadata(client, bucket, key),
                )
                logger.info("DELETE_OBJECT_COMPLETE", bucket=bucket, key=key)
            except Exception as e:
                logger.error(
                    "DELETE_OBJECT_FAILED",
                    bucket=bucket,
                    key=key,
                    error=str(e),
                    error_type=type(e).__name__,
                )
                raise
            return Response(status_code=204)

    async def handle_copy_object(self, request: Request, creds: S3Credentials) -> Response:
        """Decrypt/re-encrypt for encrypted objects, passthrough otherwise."""
        bucket, key = self._parse_path(request.url.path)
        async with self._client(creds) as client:
            copy_source = request.headers.get("x-amz-copy-source", "")
            content_type = request.headers.get("content-type")
            metadata_directive = request.headers.get("x-amz-metadata-directive", "COPY").upper()

            # Parse copy source using shared helper
            src_bucket, src_key = self._parse_copy_source(copy_source)

            # Check for copy to itself - S3 requires REPLACE directive
            is_same_object = src_bucket == bucket and src_key == key
            if is_same_object and metadata_directive != "REPLACE":
                raise S3Error.invalid_request(
                    "This copy request is illegal because it is trying to copy "
                    "an object to itself without changing the object's metadata, "
                    "storage class, website redirect location or encryption attributes."
                )

            # Collect new metadata if directive is REPLACE
            new_metadata: dict[str, str] | None = None
            if metadata_directive == "REPLACE":
                new_metadata = {}
                for hdr, val in request.headers.items():
                    if hdr.lower().startswith("x-amz-meta-"):
                        new_metadata[hdr[11:]] = val  # Strip x-amz-meta- prefix

            logger.info(
                "COPY_OBJECT",
                src_bucket=src_bucket,
                src_key=src_key,
                dest_bucket=bucket,
                dest_key=key,
                metadata_directive=metadata_directive,
            )

            # Check if source is encrypted
            try:
                head_resp = await client.head_object(src_bucket, src_key)
            except Exception as e:
                logger.warning(
                    "COPY_SOURCE_NOT_FOUND",
                    src_bucket=src_bucket,
                    src_key=src_key,
                    error=str(e),
                )
                raise S3Error.no_such_key(src_key) from e

            src_metadata = head_resp.get("Metadata", {})
            src_wrapped_dek = src_metadata.get(self.settings.dektag_name)
            src_multipart_meta = await load_multipart_metadata(client, src_bucket, src_key)

            if not src_wrapped_dek and not src_multipart_meta:
                # Not encrypted - pass through
                return await self._copy_passthrough(
                    client,
                    bucket,
                    key,
                    copy_source,
                    content_type,
                    src_bucket,
                    src_key,
                    metadata_directive,
                    new_metadata,
                    request,
                )

            # Encrypted source. A plain COPY needs no re-encrypt: the ciphertext
            # is self-describing and key-independent (GCM AAD is None, the DEK is
            # random and stored in metadata / the sidecar, nonces are embedded),
            # so a native server-side CopyObject yields a byte-identical
            # destination and skips the download+re-encrypt+re-upload
            # amplification (Scylla Manager dedup versioning).
            #
            # Re-encrypt only to re-key to a *different* owning credential. A
            # same-kid copy is byte-identical, and a legacy object with no kid
            # cannot be decrypted to re-encrypt anyway (re-encrypt would fail with
            # UnknownKidError) -- both must pass through as-is. REPLACE and
            # objects above the single-op CopyObject limit still re-encrypt.
            if src_multipart_meta:
                src_kid = src_multipart_meta.kid
            else:
                src_kid = head_resp.get("Metadata", {}).get(self.settings.kidtag_name, "")
            needs_rekey = bool(src_kid) and src_kid != client.credentials.access_key
            ciphertext_size = head_resp.get("ContentLength", 0) or 0
            if (
                metadata_directive == "COPY"
                and not needs_rekey
                and ciphertext_size <= MAX_SERVER_SIDE_COPY_BYTES
            ):
                return await self._copy_passthrough_encrypted(
                    client,
                    bucket,
                    key,
                    content_type,
                    copy_source,
                    src_bucket,
                    src_key,
                    head_resp,
                    src_multipart_meta,
                )

            # Encrypted - need to decrypt and re-encrypt
            return await self._copy_encrypted(
                client,
                bucket,
                key,
                content_type,
                src_bucket,
                src_key,
                head_resp,
                src_wrapped_dek,
                src_multipart_meta,
                metadata_directive,
                new_metadata,
            )

    async def _copy_passthrough(
        self,
        client,
        bucket: str,
        key: str,
        copy_source: str,
        content_type: str | None,
        src_bucket: str,
        src_key: str,
        metadata_directive: str,
        new_metadata: dict[str, str] | None,
        request: Request,
    ) -> Response:
        logger.info(
            "COPY_PASSTHROUGH",
            src_bucket=src_bucket,
            src_key=src_key,
            dest_bucket=bucket,
            dest_key=key,
            metadata_directive=metadata_directive,
        )

        # Get tagging directive
        tagging_directive = request.headers.get("x-amz-tagging-directive", "COPY").upper()
        tagging = request.headers.get("x-amz-tagging") if tagging_directive == "REPLACE" else None

        resp = await client.copy_object(
            bucket,
            key,
            copy_source,
            metadata=new_metadata,
            metadata_directive=metadata_directive,
            content_type=content_type,
            tagging_directive=tagging_directive if tagging_directive != "COPY" else None,
            tagging=tagging,
        )
        copy_result = resp.get("CopyObjectResult", {})
        etag = copy_result.get("ETag", "").strip('"')
        last_modified = copy_result.get("LastModified")
        if hasattr(last_modified, "isoformat"):
            last_modified = last_modified.isoformat().replace("+00:00", "Z")
        else:
            last_modified = str(last_modified) if last_modified else ""

        return Response(
            content=xml_responses.copy_object_result(etag, last_modified),
            media_type="application/xml",
        )

    async def _copy_passthrough_encrypted(
        self,
        client: S3Client,
        bucket: str,
        key: str,
        content_type: str | None,
        copy_source: str,
        src_bucket: str,
        src_key: str,
        head_resp: dict,
        src_multipart_meta,
    ) -> Response:
        """Server-side copy of an *encrypted* object, moving no bulk bytes.

        The ciphertext, wrapped DEK and kid are self-describing and not bound to
        the object key, so a native CopyObject with MetadataDirective=COPY yields
        a byte-identical, decryptable destination. This collapses Scylla Manager's
        dedup copies from a full download+re-encrypt+re-upload into a
        metadata-only op and keeps them off the in-flight memory governor.
        """
        logger.info(
            "COPY_PASSTHROUGH_ENCRYPTED",
            src_bucket=src_bucket,
            src_key=src_key,
            dest_bucket=bucket,
            dest_key=key,
            is_multipart=bool(src_multipart_meta),
        )

        resp = await client.copy_object(
            bucket,
            key,
            copy_source,
            metadata_directive="COPY",
            content_type=content_type,
        )

        # Multipart objects keep their part/frame map in a separate sidecar
        # object; the destination needs its own copy or the read path can't
        # reconstruct (and decrypt) it.
        if src_multipart_meta:
            await client.copy_object(
                bucket,
                _internal_meta_key(key),
                f"{src_bucket}/{quote(_internal_meta_key(src_key), safe='/')}",
                metadata_directive="COPY",
            )

        # Encrypted objects report the plaintext md5 (client-etag), not the
        # ciphertext ETag, to match GET/HEAD and the re-encrypt path.
        src_metadata = head_resp.get("Metadata", {})
        result = resp.get("CopyObjectResult", {})
        etag = src_metadata.get("client-etag") or str(result.get("ETag", "")).strip('"')
        last_modified = result.get("LastModified")
        if hasattr(last_modified, "isoformat"):
            last_modified = last_modified.isoformat().replace("+00:00", "Z")
        else:
            last_modified = format_iso8601(datetime.now(UTC))
        return Response(
            content=xml_responses.copy_object_result(etag, last_modified),
            media_type="application/xml",
        )

    async def _copy_encrypted(
        self,
        client: S3Client,
        bucket: str,
        key: str,
        content_type: str | None,
        src_bucket: str,
        src_key: str,
        head_resp: dict,
        src_wrapped_dek: str | None,
        src_multipart_meta,
        metadata_directive: str,
        new_metadata: dict[str, str] | None,
    ) -> Response:
        # Copies decrypt+re-encrypt the source in memory but carry no request
        # body, so the request-level limiter reserved ~nothing. Reserve the copy
        # pipeline peak here so concurrent copies are bounded (a dedup flood would
        # otherwise run unbounded and OOM the pod).
        if src_multipart_meta:
            pt_size = src_multipart_meta.total_plaintext_size
        else:
            _s = head_resp.get("Metadata", {}).get("plaintext-size")
            pt_size = int(_s) if _s else crypto.plaintext_size(head_resp.get("ContentLength", 0))
        async with concurrency.reserve_copy_memory(crypto.copy_pipeline_peak(pt_size)):
            return await self._copy_encrypted_inner(
                client,
                bucket,
                key,
                content_type,
                src_bucket,
                src_key,
                head_resp,
                src_wrapped_dek,
                src_multipart_meta,
                metadata_directive,
                new_metadata,
            )

    async def _copy_encrypted_inner(
        self,
        client: S3Client,
        bucket: str,
        key: str,
        content_type: str | None,
        src_bucket: str,
        src_key: str,
        head_resp: dict,
        src_wrapped_dek: str | None,
        src_multipart_meta,
        metadata_directive: str,
        new_metadata: dict[str, str] | None,
    ) -> Response:
        logger.info(
            "COPY_ENCRYPTED",
            src_bucket=src_bucket,
            src_key=src_key,
            dest_bucket=bucket,
            dest_key=key,
            is_multipart=bool(src_multipart_meta),
            metadata_directive=metadata_directive,
        )

        if src_multipart_meta:
            plaintext_size = src_multipart_meta.total_plaintext_size
        else:
            size_str = head_resp.get("Metadata", {}).get("plaintext-size")
            if size_str:
                plaintext_size = int(size_str)
            else:
                plaintext_size = crypto.plaintext_size(head_resp.get("ContentLength", 0))

        if plaintext_size > crypto.STREAMING_THRESHOLD:
            return await self._copy_encrypted_streaming(
                client,
                bucket,
                key,
                content_type,
                src_bucket,
                src_key,
                head_resp,
                src_wrapped_dek,
                src_multipart_meta,
                metadata_directive,
                new_metadata,
                plaintext_size,
            )

        if src_multipart_meta:
            plaintext = await self._download_encrypted_multipart(
                client, src_bucket, src_key, src_multipart_meta
            )
        else:
            src_kid = head_resp.get("Metadata", {}).get(self.settings.kidtag_name, "")
            plaintext = await self._download_encrypted_single(
                client, src_bucket, src_key, src_wrapped_dek, src_kid
            )

        dest_kid, dest_kek = self.keyring.key_for(client.credentials.access_key)
        encrypted = crypto.encrypt_object(plaintext, dest_kek)
        etag = hashlib.md5(plaintext, usedforsecurity=False).hexdigest()

        dest_metadata = {
            self.settings.dektag_name: base64.b64encode(encrypted.wrapped_dek).decode(),
            self.settings.kidtag_name: dest_kid,
            "client-etag": etag,
            "plaintext-size": str(len(plaintext)),
        }

        if metadata_directive == "REPLACE" and new_metadata is not None:
            dest_metadata.update(new_metadata)
        else:
            src_metadata = head_resp.get("Metadata", {})
            internal_keys = self._internal_meta_keys()
            for meta_key, meta_value in src_metadata.items():
                if meta_key.lower() not in internal_keys:
                    dest_metadata[meta_key] = meta_value

        if metadata_directive == "REPLACE":
            src_cache_control = None
            src_expires = None
        else:
            src_cache_control = head_resp.get("CacheControl")
            src_expires = head_resp.get("Expires")

        await client.put_object(
            bucket,
            key,
            encrypted.ciphertext,
            metadata=dest_metadata,
            content_type=content_type or head_resp.get("ContentType", "application/octet-stream"),
            cache_control=src_cache_control,
            expires=src_expires,
        )

        logger.info(
            "COPY_ENCRYPTED_COMPLETE",
            src_bucket=src_bucket,
            src_key=src_key,
            dest_bucket=bucket,
            dest_key=key,
            plaintext_mb=round(len(plaintext) / 1024 / 1024, 2),
        )

        last_modified = format_iso8601(datetime.now(UTC))
        return Response(
            content=xml_responses.copy_object_result(etag, last_modified),
            media_type="application/xml",
        )

    async def _copy_encrypted_streaming(
        self,
        client: S3Client,
        bucket: str,
        key: str,
        content_type: str | None,
        src_bucket: str,
        src_key: str,
        head_resp: dict,
        src_wrapped_dek: str | None,
        src_multipart_meta,
        metadata_directive: str,
        new_metadata: dict[str, str] | None,
        plaintext_size: int,
    ) -> Response:
        """Re-encrypt a large source object via an internal multipart upload.

        Streams the source plaintext in MAX_BUFFER_SIZE chunks to avoid buffering
        the entire object in memory and to keep individual AES-GCM calls bounded.
        """
        logger.info(
            "COPY_ENCRYPTED_STREAMING",
            src_bucket=src_bucket,
            src_key=src_key,
            dest_bucket=bucket,
            dest_key=key,
            plaintext_mb=f"{plaintext_size / 1024 / 1024:.2f}MB",
        )

        dest_kid, dest_kek = self.keyring.key_for(client.credentials.access_key)
        dek = crypto.generate_dek()
        wrapped_dek = crypto.wrap_key(dek, dest_kek)

        upload_metadata: dict[str, str] = {
            self.settings.dektag_name: base64.b64encode(wrapped_dek).decode(),
            self.settings.kidtag_name: dest_kid,
        }
        if metadata_directive == "REPLACE" and new_metadata is not None:
            upload_metadata.update(new_metadata)
        else:
            src_metadata = head_resp.get("Metadata", {})
            internal_keys = self._internal_meta_keys()
            for meta_key, meta_value in src_metadata.items():
                if meta_key.lower() not in internal_keys:
                    upload_metadata[meta_key] = meta_value

        if metadata_directive == "REPLACE":
            src_cache_control = None
            src_expires = None
        else:
            src_cache_control = head_resp.get("CacheControl")
            src_expires = head_resp.get("Expires")

        resp = await client.create_multipart_upload(
            bucket,
            key,
            content_type=content_type or head_resp.get("ContentType", "application/octet-stream"),
            metadata=upload_metadata,
            cache_control=src_cache_control,
            expires=src_expires,
        )
        upload_id = resp["UploadId"]

        try:
            s3_parts, meta_parts, total_plaintext = await self._stream_copy_to_multipart(
                client,
                bucket,
                key,
                upload_id,
                dek,
                src_bucket,
                src_key,
                src_wrapped_dek,
                src_multipart_meta,
                head_resp,
            )

            await client.complete_multipart_upload(bucket, key, upload_id, s3_parts)
        except Exception:
            await self._safe_abort(client, bucket, key, upload_id)
            raise

        await save_multipart_metadata(
            client,
            bucket,
            key,
            MultipartMetadata(
                version=2,
                part_count=len(meta_parts),
                total_plaintext_size=total_plaintext,
                parts=meta_parts,
                wrapped_dek=wrapped_dek,
                kid=dest_kid,
            ),
        )

        etag = hashlib.md5(str(total_plaintext).encode(), usedforsecurity=False).hexdigest()

        logger.info(
            "COPY_ENCRYPTED_STREAMING_COMPLETE",
            src_bucket=src_bucket,
            src_key=src_key,
            dest_bucket=bucket,
            dest_key=key,
            plaintext_mb=f"{total_plaintext / 1024 / 1024:.2f}MB",
            parts=len(meta_parts),
        )
        last_modified = format_iso8601(datetime.now(UTC))
        return Response(
            content=xml_responses.copy_object_result(etag, last_modified),
            media_type="application/xml",
        )

    async def _stream_copy_to_multipart(
        self,
        client: S3Client,
        bucket: str,
        key: str,
        upload_id: str,
        dek: bytes,
        src_bucket: str,
        src_key: str,
        src_wrapped_dek: str | None,
        src_multipart_meta,
        head_resp: dict,
    ) -> tuple[list[dict], list[PartMetadata], int]:
        """Stream-encrypt plaintext from source into an in-progress S3 multipart upload.

        Returns the S3 parts list (for CompleteMultipartUpload), PartMetadata list
        (for MultipartMetadata sidecar), and total plaintext bytes written.
        """
        s3_parts: list[dict] = []
        meta_parts: list[PartMetadata] = []
        total_plaintext = 0
        part_number = 1

        src_iter = self._iter_object_plaintext(
            client, src_bucket, src_key, src_wrapped_dek, src_multipart_meta, head_resp
        )

        buf = bytearray()
        async for raw in src_iter:
            buf.extend(raw)

            while len(buf) >= crypto.MAX_BUFFER_SIZE:
                chunk = bytes(buf[: crypto.MAX_BUFFER_SIZE])
                del buf[: crypto.MAX_BUFFER_SIZE]
                (
                    part_number,
                    s3_parts,
                    meta_parts,
                    total_plaintext,
                ) = await self._encrypt_and_upload_chunk(
                    client,
                    bucket,
                    key,
                    upload_id,
                    dek,
                    chunk,
                    part_number,
                    s3_parts,
                    meta_parts,
                    total_plaintext,
                )

        if buf:
            chunk = bytes(buf)
            (
                part_number,
                s3_parts,
                meta_parts,
                total_plaintext,
            ) = await self._encrypt_and_upload_chunk(
                client,
                bucket,
                key,
                upload_id,
                dek,
                chunk,
                part_number,
                s3_parts,
                meta_parts,
                total_plaintext,
            )

        return s3_parts, meta_parts, total_plaintext

    async def _encrypt_and_upload_chunk(
        self,
        client: S3Client,
        bucket: str,
        key: str,
        upload_id: str,
        dek: bytes,
        chunk: bytes,
        part_number: int,
        s3_parts: list[dict],
        meta_parts: list[PartMetadata],
        total_plaintext: int,
    ) -> tuple[int, list[dict], list[PartMetadata], int]:
        nonce = crypto.derive_part_nonce(upload_id, part_number)
        ciphertext = crypto.encrypt(chunk, dek, nonce)
        resp = await client.upload_part(bucket, key, upload_id, part_number, ciphertext)
        etag = resp["ETag"].strip('"')
        plaintext_len = len(chunk)
        ciphertext_len = len(ciphertext)
        del ciphertext

        s3_parts.append({"PartNumber": part_number, "ETag": f'"{etag}"'})
        meta_parts.append(
            PartMetadata(
                part_number=part_number,
                plaintext_size=plaintext_len,
                ciphertext_size=ciphertext_len,
                etag=etag,
                md5="",
                internal_parts=[
                    InternalPartMetadata(
                        internal_part_number=part_number,
                        plaintext_size=plaintext_len,
                        ciphertext_size=ciphertext_len,
                        etag=etag,
                    )
                ],
            )
        )
        return part_number + 1, s3_parts, meta_parts, total_plaintext + plaintext_len

    async def _iter_object_plaintext(
        self,
        client: S3Client,
        src_bucket: str,
        src_key: str,
        src_wrapped_dek: str | None,
        src_multipart_meta,
        head_resp: dict,
    ):
        """Yield plaintext bytes from an encrypted source object."""
        if src_multipart_meta:
            dek = crypto.unwrap_key(
                src_multipart_meta.wrapped_dek,
                self.keyring.key_by_id(src_multipart_meta.kid),
            )
            async for chunk in self._iter_multipart_plaintext(
                client, src_bucket, src_key, src_multipart_meta, dek
            ):
                yield chunk
        else:
            src_kid = head_resp.get("Metadata", {}).get(self.settings.kidtag_name, "")
            plaintext = await self._download_encrypted_single(
                client, src_bucket, src_key, src_wrapped_dek, src_kid
            )
            yield plaintext

    async def handle_get_object_tagging(self, request: Request, creds: S3Credentials) -> Response:
        bucket, key = self._parse_path(request.url.path)
        async with self._client(creds) as client:
            try:
                resp = await client.get_object_tagging(bucket, key)
                return Response(
                    content=xml_responses.get_tagging(resp.get("TagSet", [])),
                    media_type="application/xml",
                )
            except ClientError as e:
                self._raise_s3_error(e, bucket, key)

    async def handle_put_object_tagging(self, request: Request, creds: S3Credentials) -> Response:
        bucket, key = self._parse_path(request.url.path)
        async with self._client(creds) as client:
            body = await request.body()
            try:
                root = ET.fromstring(body.decode())
            except ET.ParseError as e:
                raise S3Error.malformed_xml(str(e)) from e

            tags = []
            for tag_elem in find_elements(root, "Tag"):
                key_elem = find_element(tag_elem, "Key")
                value_elem = find_element(tag_elem, "Value")
                if key_elem is not None and key_elem.text:
                    tags.append(
                        {
                            "Key": key_elem.text,
                            "Value": (
                                value_elem.text
                                if value_elem is not None and value_elem.text
                                else ""
                            ),
                        }
                    )

            try:
                await client.put_object_tagging(bucket, key, tags)
                return Response(status_code=200)
            except ClientError as e:
                self._raise_s3_error(e, bucket, key)

    async def handle_delete_object_tagging(
        self, request: Request, creds: S3Credentials
    ) -> Response:
        bucket, key = self._parse_path(request.url.path)
        async with self._client(creds) as client:
            try:
                await client.delete_object_tagging(bucket, key)
                return Response(status_code=204)
            except ClientError as e:
                self._raise_s3_error(e, bucket, key)
