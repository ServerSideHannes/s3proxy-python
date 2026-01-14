"""Multipart upload API operations."""

import contextlib
import hashlib
import xml.etree.ElementTree as ET
from urllib.parse import parse_qs

from fastapi import HTTPException, Request, Response

from .. import crypto, xml_responses
from ..multipart import (
    MultipartMetadata,
    PartMetadata,
    delete_upload_state,
    load_upload_state,
    persist_upload_state,
    save_multipart_metadata,
)
from ..s3client import S3Credentials
from .base import BaseHandler
from .objects import decode_aws_chunked, decode_aws_chunked_stream


class MultipartHandlerMixin(BaseHandler):
    """Mixin for multipart upload API operations."""

    async def handle_create_multipart_upload(
        self, request: Request, creds: S3Credentials
    ) -> Response:
        bucket, key = self._parse_path(request.url.path)
        client = self._client(creds)
        content_type = request.headers.get("content-type", "application/octet-stream")

        dek = crypto.generate_dek()
        wrapped_dek = crypto.wrap_key(dek, self.settings.kek)

        resp = await client.create_multipart_upload(bucket, key, content_type=content_type)
        upload_id = resp["UploadId"]

        await self.multipart_manager.create_upload(bucket, key, upload_id, dek)
        await persist_upload_state(client, bucket, key, upload_id, wrapped_dek)

        return Response(
            content=xml_responses.initiate_multipart(bucket, key, upload_id),
            media_type="application/xml",
        )

    async def handle_upload_part(self, request: Request, creds: S3Credentials) -> Response:
        bucket, key = self._parse_path(request.url.path)
        client = self._client(creds)
        query = parse_qs(request.url.query)
        upload_id = query.get("uploadId", [""])[0]
        part_num = int(query.get("partNumber", ["0"])[0])

        state = await self.multipart_manager.get_upload(bucket, key, upload_id)
        if not state:
            dek = await load_upload_state(client, bucket, key, upload_id, self.settings.kek)
            if not dek:
                raise HTTPException(404, "Upload not found")
            state = await self.multipart_manager.create_upload(bucket, key, upload_id, dek)

        content_encoding = request.headers.get("content-encoding", "")
        content_sha = request.headers.get("x-amz-content-sha256", "")

        # Check if we can stream (unsigned or streaming signature)
        is_unsigned = content_sha == "UNSIGNED-PAYLOAD"
        is_streaming_sig = content_sha.startswith("STREAMING-")
        needs_chunked_decode = "aws-chunked" in content_encoding or is_streaming_sig

        if is_unsigned or is_streaming_sig:
            # Stream the part without buffering
            body = bytearray()
            md5_hash = hashlib.md5()

            if needs_chunked_decode:
                async for chunk in decode_aws_chunked_stream(request):
                    body.extend(chunk)
                    md5_hash.update(chunk)
            else:
                async for chunk in request.stream():
                    body.extend(chunk)
                    md5_hash.update(chunk)

            body = bytes(body)
            body_md5 = md5_hash.hexdigest()
        else:
            # Body is already cached by handle_proxy_request for signature verification
            body = await request.body()

            # Decode aws-chunked encoding if present
            if needs_chunked_decode:
                body = decode_aws_chunked(body)

            body_md5 = hashlib.md5(body).hexdigest()

        ciphertext = crypto.encrypt_part(body, state.dek, upload_id, part_num)

        resp = await client.upload_part(bucket, key, upload_id, part_num, ciphertext)

        await self.multipart_manager.add_part(bucket, key, upload_id, PartMetadata(
            part_num, len(body), len(ciphertext),
            resp["ETag"].strip('"'), body_md5
        ))

        return Response(headers={"ETag": resp["ETag"]})

    async def handle_complete_multipart_upload(
        self, request: Request, creds: S3Credentials
    ) -> Response:
        bucket, key = self._parse_path(request.url.path)
        client = self._client(creds)
        query = parse_qs(request.url.query)
        upload_id = query.get("uploadId", [""])[0]

        state = await self.multipart_manager.complete_upload(bucket, key, upload_id)
        if not state:
            raise HTTPException(404, "Upload not found")

        body = await request.body()
        parts = []
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        for part in ET.fromstring(body.decode()).findall(f".//{ns}Part"):
            pn = part.find(f"{ns}PartNumber")
            etag = part.find(f"{ns}ETag")
            if pn is not None and etag is not None:
                parts.append({"PartNumber": int(pn.text or "0"), "ETag": etag.text or ""})

        await client.complete_multipart_upload(bucket, key, upload_id, parts)

        wrapped_dek = crypto.wrap_key(state.dek, self.settings.kek)
        await save_multipart_metadata(client, bucket, key, MultipartMetadata(
            version=1,
            part_count=len(state.parts),
            total_plaintext_size=state.total_plaintext_size,
            parts=list(state.parts.values()),
            wrapped_dek=wrapped_dek,
        ))
        await delete_upload_state(client, bucket, key, upload_id)

        location = f"{self.settings.s3_endpoint}/{bucket}/{key}"
        etag = hashlib.md5(str(state.total_plaintext_size).encode()).hexdigest()

        return Response(
            content=xml_responses.complete_multipart(location, bucket, key, etag),
            media_type="application/xml",
        )

    async def handle_abort_multipart_upload(
        self, request: Request, creds: S3Credentials
    ) -> Response:
        bucket, key = self._parse_path(request.url.path)
        client = self._client(creds)
        query = parse_qs(request.url.query)
        upload_id = query.get("uploadId", [""])[0]

        await self.multipart_manager.abort_upload(bucket, key, upload_id)
        with contextlib.suppress(Exception):
            await client.abort_multipart_upload(bucket, key, upload_id)
        await delete_upload_state(client, bucket, key, upload_id)

        return Response(status_code=204)

    async def handle_list_parts(
        self, request: Request, creds: S3Credentials
    ) -> Response:
        """Handle ListParts request (GET ?uploadId=X without partNumber)."""
        bucket, key = self._parse_path(request.url.path)
        client = self._client(creds)
        query = parse_qs(request.url.query)

        upload_id = query.get("uploadId", [""])[0]
        part_number_marker = query.get("part-number-marker", [""])[0]
        part_number_marker = int(part_number_marker) if part_number_marker else None
        max_parts = int(query.get("max-parts", ["1000"])[0])

        resp = await client.list_parts(
            bucket, key, upload_id, part_number_marker, max_parts
        )

        parts = []
        for part in resp.get("Parts", []):
            last_modified = part.get("LastModified")
            if hasattr(last_modified, "isoformat"):
                last_modified = last_modified.isoformat().replace("+00:00", "Z")
            else:
                last_modified = str(last_modified) if last_modified else ""

            parts.append({
                "PartNumber": part.get("PartNumber", 0),
                "LastModified": last_modified,
                "ETag": part.get("ETag", "").strip('"'),
                "Size": part.get("Size", 0),
            })

        return Response(
            content=xml_responses.list_parts(
                bucket=bucket,
                key=key,
                upload_id=upload_id,
                parts=parts,
                part_number_marker=part_number_marker,
                next_part_number_marker=resp.get("NextPartNumberMarker"),
                max_parts=max_parts,
                is_truncated=resp.get("IsTruncated", False),
                storage_class=resp.get("StorageClass", "STANDARD"),
            ),
            media_type="application/xml",
        )
