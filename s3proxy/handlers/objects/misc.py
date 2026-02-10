"""Miscellaneous object operations: HEAD, DELETE, COPY, Tagging."""

import asyncio
import base64
import hashlib
import xml.etree.ElementTree as ET
from datetime import UTC, datetime

import structlog
from botocore.exceptions import ClientError
from fastapi import Request, Response
from structlog.stdlib import BoundLogger

from ... import crypto, xml_responses
from ...errors import S3Error
from ...s3client import S3Credentials
from ...state import (
    delete_multipart_metadata,
    load_multipart_metadata,
)
from ...utils import format_http_date, format_iso8601
from ...xml_utils import find_element, find_elements
from ..base import BaseHandler

logger: BoundLogger = structlog.get_logger(__name__)


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
        internal_keys = {self.settings.dektag_name.lower(), "client-etag", "plaintext-size"}
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

    async def _copy_encrypted(
        self,
        client,
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
            plaintext = await self._download_encrypted_multipart(
                client, src_bucket, src_key, src_multipart_meta
            )
        else:
            plaintext = await self._download_encrypted_single(
                client, src_bucket, src_key, src_wrapped_dek
            )

        # Re-encrypt
        encrypted = crypto.encrypt_object(plaintext, self.settings.kek)
        etag = hashlib.md5(plaintext, usedforsecurity=False).hexdigest()

        # Build destination metadata
        dest_metadata = {
            self.settings.dektag_name: base64.b64encode(encrypted.wrapped_dek).decode(),
            "client-etag": etag,
            "plaintext-size": str(len(plaintext)),
        }

        if metadata_directive == "REPLACE" and new_metadata is not None:
            # Use new metadata from request
            dest_metadata.update(new_metadata)
        else:
            # Copy user metadata from source (excluding our internal keys)
            src_metadata = head_resp.get("Metadata", {})
            internal_keys = {self.settings.dektag_name.lower(), "client-etag", "plaintext-size"}
            for meta_key, meta_value in src_metadata.items():
                if meta_key.lower() not in internal_keys:
                    dest_metadata[meta_key] = meta_value

        # Get source headers to preserve (only if not replacing)
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
