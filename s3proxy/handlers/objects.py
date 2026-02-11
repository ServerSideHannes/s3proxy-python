"""Object operations: GET, PUT, HEAD, DELETE."""

import base64
import contextlib
import hashlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from itertools import accumulate
from typing import Any, Iterator

import structlog
from botocore.exceptions import ClientError
from fastapi import HTTPException, Request, Response
from fastapi.responses import StreamingResponse

from .. import crypto
from ..multipart import (
    MultipartMetadata,
    PartMetadata,
    calculate_part_range,
    delete_multipart_metadata,
    load_multipart_metadata,
    save_multipart_metadata,
)
from ..s3client import S3Client, S3Credentials
from .base import BaseHandler

logger = structlog.get_logger()

# Streaming chunk size for reads/writes
STREAM_CHUNK_SIZE = 64 * 1024  # 64KB chunks for streaming


def decode_aws_chunked(body: bytes) -> bytes:
    """Decode aws-chunked transfer encoding used by streaming SigV4.

    Format: <hex-size>;chunk-signature=<sig>\r\n<data>\r\n...0;chunk-signature=<sig>\r\n
    """
    result = bytearray()
    pos = 0
    while pos < len(body):
        # Find end of chunk header
        header_end = body.find(b"\r\n", pos)
        if header_end == -1:
            break
        header = body[pos:header_end]
        # Parse chunk size (before semicolon)
        size_str = header.split(b";")[0]
        try:
            chunk_size = int(size_str, 16)
        except ValueError:
            break
        if chunk_size == 0:
            break
        # Extract chunk data
        data_start = header_end + 2
        data_end = data_start + chunk_size
        if data_end > len(body):
            break
        result.extend(body[data_start:data_end])
        # Move past data and trailing CRLF
        pos = data_end + 2
    return bytes(result)


async def decode_aws_chunked_stream(
    request: Request,
) -> AsyncIterator[bytes]:
    """Decode aws-chunked encoding from streaming request.

    Yields decoded data chunks without buffering entire body.
    Format: <hex-size>;chunk-signature=<sig>\r\n<data>\r\n...
    """
    buffer = bytearray()

    async for raw_chunk in request.stream():
        buffer.extend(raw_chunk)

        # Process complete chunks from buffer
        while True:
            # Find chunk header end
            header_end = buffer.find(b"\r\n")
            if header_end == -1:
                break  # Need more data

            # Parse chunk size
            header = buffer[:header_end]
            size_str = header.split(b";")[0]
            try:
                chunk_size = int(size_str, 16)
            except ValueError:
                break

            # Check for final chunk
            if chunk_size == 0:
                return

            # Check if we have the full chunk
            data_start = header_end + 2
            data_end = data_start + chunk_size
            trailing_end = data_end + 2  # Account for trailing CRLF

            if len(buffer) < trailing_end:
                break  # Need more data

            # Yield the decoded chunk
            yield bytes(buffer[data_start:data_end])

            # Remove processed data from buffer
            del buffer[:trailing_end]


def chunked(data: bytes, size: int) -> Iterator[tuple[int, bytes]]:
    """Yield (part_number, chunk) tuples starting from part 1."""
    for i in range(0, len(data), size):
        yield i // size + 1, data[i:i + size]


def format_http_date(dt: datetime | None) -> str | None:
    """Format datetime as HTTP date string."""
    return dt.strftime("%a, %d %b %Y %H:%M:%S GMT") if dt else None


class ObjectHandlerMixin(BaseHandler):
    """Mixin for object operations."""

    async def handle_get_object(self, request: Request, creds: S3Credentials) -> Response:
        bucket, key = self._parse_path(request.url.path)
        client = self._client(creds)
        range_header = request.headers.get("range")

        try:
            # Get LastModified from object metadata first
            head_resp = await client.head_object(bucket, key)
            last_modified = format_http_date(head_resp.get("LastModified"))

            if meta := await load_multipart_metadata(client, bucket, key):
                return await self._get_multipart(client, bucket, key, meta, range_header, last_modified)
            return await self._get_single(client, bucket, key, range_header, head_resp, last_modified)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                raise HTTPException(404, "Not found") from None
            raise HTTPException(500, str(e)) from e

    async def _get_single(
        self,
        client: S3Client,
        bucket: str,
        key: str,
        range_header: str | None,
        head_resp: dict,
        last_modified: str | None,
    ) -> Response:
        # Check metadata to determine if object is encrypted
        metadata = head_resp.get("Metadata", {})
        wrapped_dek_b64 = metadata.get(self.settings.dektag_name)

        if not wrapped_dek_b64:
            # Unencrypted passthrough - stream directly from S3
            logger.info("Streaming non-encrypted object directly from S3", bucket=bucket, key=key)
            resp = await client.get_object(bucket, key, range_header=range_header)
            s3_body = resp["Body"]

            headers: dict[str, str] = {
                "Content-Type": resp.get("ContentType", "application/octet-stream"),
            }
            if "ContentLength" in resp:
                headers["Content-Length"] = str(resp["ContentLength"])
            if last_modified:
                headers["Last-Modified"] = last_modified

            async def stream_s3_body() -> AsyncIterator[bytes]:
                """Stream S3 body in chunks."""
                async with s3_body:
                    while chunk := await s3_body.read(STREAM_CHUNK_SIZE):
                        yield chunk

            if "ContentRange" in resp:
                headers["Content-Range"] = resp["ContentRange"]
                return StreamingResponse(stream_s3_body(), status_code=206, headers=headers)
            return StreamingResponse(stream_s3_body(), headers=headers)

        # Encrypted object - need full ciphertext to decrypt
        resp = await client.get_object(bucket, key)
        wrapped_dek = base64.b64decode(wrapped_dek_b64)
        ciphertext = await resp["Body"].read()
        plaintext = crypto.decrypt_object(ciphertext, wrapped_dek, self.settings.kek)

        headers: dict[str, str] = {"Content-Length": str(len(plaintext))}
        if last_modified:
            headers["Last-Modified"] = last_modified

        if range_header:
            start, end = self._parse_range(range_header, len(plaintext))
            return Response(
                content=plaintext[start:end + 1],
                status_code=206,
                headers={
                    "Content-Range": f"bytes {start}-{end}/{len(plaintext)}",
                    "Content-Length": str(end - start + 1),
                    **({"Last-Modified": last_modified} if last_modified else {}),
                },
            )
        return Response(content=plaintext, headers=headers)

    async def _get_multipart(
        self,
        client: S3Client,
        bucket: str,
        key: str,
        meta: MultipartMetadata,
        range_header: str | None,
        last_modified: str | None,
    ) -> Response:
        dek = crypto.unwrap_key(meta.wrapped_dek, self.settings.kek)
        total = meta.total_plaintext_size
        start, end = self._parse_range(range_header, total) if range_header else (0, total - 1)
        parts = calculate_part_range(meta.parts, start, end)

        # Build lookup: part_number -> (part_metadata, ciphertext_offset)
        sorted_parts = sorted(meta.parts, key=lambda p: p.part_number)
        offsets = [0, *accumulate(p.ciphertext_size for p in sorted_parts)]
        part_info = {p.part_number: (p, offsets[i]) for i, p in enumerate(sorted_parts)}

        async def stream():
            for part_num, off_start, off_end in parts:
                part_meta, ct_start = part_info[part_num]
                ct_end = ct_start + part_meta.ciphertext_size - 1
                resp = await client.get_object(bucket, key, f"bytes={ct_start}-{ct_end}")
                ciphertext = await resp["Body"].read()
                yield crypto.decrypt(ciphertext, dek)[off_start:off_end + 1]

        length = sum(e - s + 1 for _, s, e in parts)
        headers: dict[str, str] = {"Content-Length": str(length)}
        if last_modified:
            headers["Last-Modified"] = last_modified
        if range_header:
            headers["Content-Range"] = f"bytes {start}-{end}/{total}"
            return StreamingResponse(stream(), status_code=206, headers=headers)
        return StreamingResponse(stream(), headers=headers)

    async def handle_put_object(self, request: Request, creds: S3Credentials) -> Response:
        bucket, key = self._parse_path(request.url.path)
        client = self._client(creds)
        content_type = request.headers.get("content-type", "application/octet-stream")
        content_sha = request.headers.get("x-amz-content-sha256", "")
        content_encoding = request.headers.get("content-encoding", "")

        # Check if we can stream (body wasn't read for signature verification)
        is_unsigned = content_sha == "UNSIGNED-PAYLOAD"
        is_streaming_sig = content_sha.startswith("STREAMING-")
        needs_chunked_decode = "aws-chunked" in content_encoding or is_streaming_sig

        # For unsigned or streaming signatures, use streaming upload to avoid buffering
        if is_unsigned or is_streaming_sig:
            return await self._put_streaming(
                request, client, bucket, key, content_type, needs_chunked_decode
            )

        # Body was already cached by handle_proxy_request for signature verification
        body = await request.body()

        # Decode aws-chunked encoding if present (shouldn't happen with above check)
        if needs_chunked_decode:
            body = decode_aws_chunked(body)

        # Reject if exceeds max upload size
        if len(body) > self.settings.max_upload_size_bytes:
            raise HTTPException(413, f"Max upload size: {self.settings.max_upload_size_mb}MB")

        # Auto-use multipart for files >16MB to split encryption into parts
        if len(body) > crypto.PART_SIZE:
            return await self._put_multipart(client, bucket, key, body, content_type)

        encrypted = crypto.encrypt_object(body, self.settings.kek)
        etag = hashlib.md5(body).hexdigest()

        await client.put_object(
            bucket, key, encrypted.ciphertext,
            metadata={
                self.settings.dektag_name: base64.b64encode(encrypted.wrapped_dek).decode(),
                "client-etag": etag,
                "plaintext-size": str(len(body)),
            },
            content_type=content_type,
        )
        return Response(headers={"ETag": f'"{etag}"'})

    async def _put_multipart(
        self, client: S3Client, bucket: str, key: str, body: bytes, content_type: str
    ) -> Response:
        dek = crypto.generate_dek()
        wrapped_dek = crypto.wrap_key(dek, self.settings.kek)

        resp = await client.create_multipart_upload(bucket, key, content_type=content_type)
        upload_id = resp["UploadId"]

        parts_meta: list[PartMetadata] = []
        parts_complete: list[dict[str, Any]] = []

        try:
            for part_num, chunk in chunked(body, crypto.PART_SIZE):
                nonce = crypto.derive_part_nonce(upload_id, part_num)
                ciphertext = crypto.encrypt(chunk, dek, nonce)

                part_resp = await client.upload_part(bucket, key, upload_id, part_num, ciphertext)
                etag = part_resp["ETag"].strip('"')

                parts_meta.append(PartMetadata(
                    part_num, len(chunk), len(ciphertext), etag, hashlib.md5(chunk).hexdigest()
                ))
                parts_complete.append({"PartNumber": part_num, "ETag": part_resp["ETag"]})

            await client.complete_multipart_upload(bucket, key, upload_id, parts_complete)
            await save_multipart_metadata(client, bucket, key, MultipartMetadata(
                version=1,
                part_count=len(parts_meta),
                total_plaintext_size=len(body),
                parts=parts_meta,
                wrapped_dek=wrapped_dek,
            ))

            return Response(headers={"ETag": f'"{hashlib.md5(body).hexdigest()}"'})
        except Exception as e:
            with contextlib.suppress(Exception):
                await client.abort_multipart_upload(bucket, key, upload_id)
            raise HTTPException(500, str(e)) from e

    async def _put_streaming(
        self,
        request: Request,
        client: S3Client,
        bucket: str,
        key: str,
        content_type: str,
        decode_chunked: bool = False,
    ) -> Response:
        """Stream upload without buffering entire body in memory.

        Reads chunks from request stream, encrypts, and uploads as multipart.
        Memory usage is bounded by crypto.PART_SIZE (default 16MB).

        Args:
            decode_chunked: If True, decode aws-chunked encoding on-the-fly
        """
        dek = crypto.generate_dek()
        wrapped_dek = crypto.wrap_key(dek, self.settings.kek)

        resp = await client.create_multipart_upload(bucket, key, content_type=content_type)
        upload_id = resp["UploadId"]

        parts_meta: list[PartMetadata] = []
        parts_complete: list[dict[str, Any]] = []
        total_plaintext_size = 0
        part_num = 0
        md5_hash = hashlib.md5()

        # Buffer for accumulating chunks up to PART_SIZE
        buffer = bytearray()

        async def upload_part(data: bytes) -> None:
            """Encrypt and upload a part."""
            nonlocal part_num
            part_num += 1
            nonce = crypto.derive_part_nonce(upload_id, part_num)
            ciphertext = crypto.encrypt(data, dek, nonce)

            part_resp = await client.upload_part(bucket, key, upload_id, part_num, ciphertext)
            etag = part_resp["ETag"].strip('"')

            parts_meta.append(PartMetadata(
                part_num, len(data), len(ciphertext), etag, hashlib.md5(data).hexdigest()
            ))
            parts_complete.append({"PartNumber": part_num, "ETag": part_resp["ETag"]})

        try:
            # Choose stream source based on encoding
            if decode_chunked:
                stream_source = decode_aws_chunked_stream(request)
            else:
                stream_source = request.stream()

            # Stream chunks from request
            async for chunk in stream_source:
                buffer.extend(chunk)
                md5_hash.update(chunk)
                total_plaintext_size += len(chunk)

                # Upload when buffer reaches PART_SIZE
                # Process immediately without intermediate variable to reduce memory
                while len(buffer) >= crypto.PART_SIZE:
                    # Extract, upload, then clear - minimizes peak memory
                    await upload_part(bytes(buffer[:crypto.PART_SIZE]))
                    del buffer[:crypto.PART_SIZE]

            # Upload remaining data
            if buffer:
                await upload_part(bytes(buffer))

            # Complete multipart upload
            await client.complete_multipart_upload(bucket, key, upload_id, parts_complete)
            await save_multipart_metadata(client, bucket, key, MultipartMetadata(
                version=1,
                part_count=len(parts_meta),
                total_plaintext_size=total_plaintext_size,
                parts=parts_meta,
                wrapped_dek=wrapped_dek,
            ))

            return Response(headers={"ETag": f'"{md5_hash.hexdigest()}"'})

        except Exception as e:
            with contextlib.suppress(Exception):
                await client.abort_multipart_upload(bucket, key, upload_id)
            raise HTTPException(500, str(e)) from e

    async def handle_head_object(self, request: Request, creds: S3Credentials) -> Response:
        bucket, key = self._parse_path(request.url.path)
        client = self._client(creds)

        try:
            resp = await client.head_object(bucket, key)
            last_modified = format_http_date(resp.get("LastModified"))

            if meta := await load_multipart_metadata(client, bucket, key):
                return Response(headers={
                    "Content-Length": str(meta.total_plaintext_size),
                    "Content-Type": resp.get("ContentType", "application/octet-stream"),
                    "ETag": f'"{hashlib.md5(str(meta.total_plaintext_size).encode()).hexdigest()}"',
                    **({"Last-Modified": last_modified} if last_modified else {}),
                })

            metadata = resp.get("Metadata", {})
            size = metadata.get("plaintext-size", resp.get("ContentLength", 0))
            etag = metadata.get("client-etag", resp.get("ETag", "").strip('"'))

            return Response(headers={
                "Content-Length": str(size),
                "Content-Type": resp.get("ContentType", "application/octet-stream"),
                "ETag": f'"{etag}"',
                **({"Last-Modified": last_modified} if last_modified else {}),
            })

        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                raise HTTPException(404, "Not found") from None
            raise HTTPException(500, str(e)) from e

    async def handle_delete_object(self, request: Request, creds: S3Credentials) -> Response:
        bucket, key = self._parse_path(request.url.path)
        client = self._client(creds)
        await client.delete_object(bucket, key)
        await delete_multipart_metadata(client, bucket, key)
        return Response(status_code=204)

    async def handle_copy_object(self, request: Request, creds: S3Credentials) -> Response:
        """Handle CopyObject request (PUT with x-amz-copy-source header).

        This copies an object server-side. For encrypted objects, we need to:
        1. Download and decrypt the source
        2. Re-encrypt with destination DEK
        3. Upload the re-encrypted data

        For non-encrypted objects, we can pass through to backend S3.
        """
        from .. import xml_responses

        bucket, key = self._parse_path(request.url.path)
        client = self._client(creds)

        copy_source = request.headers.get("x-amz-copy-source", "")
        metadata_directive = request.headers.get("x-amz-metadata-directive", "COPY")
        content_type = request.headers.get("content-type")

        # Parse copy source: can be "bucket/key" or "/bucket/key" or URL-encoded
        from urllib.parse import unquote
        copy_source = unquote(copy_source).lstrip("/")
        if "/" not in copy_source:
            from fastapi import HTTPException
            raise HTTPException(400, "Invalid x-amz-copy-source format")

        src_bucket, src_key = copy_source.split("/", 1)

        # Check if source is encrypted
        try:
            head_resp = await client.head_object(src_bucket, src_key)
        except Exception as e:
            from fastapi import HTTPException
            raise HTTPException(404, f"Source object not found: {e}") from e

        src_metadata = head_resp.get("Metadata", {})
        src_wrapped_dek = src_metadata.get(self.settings.dektag_name)

        # Check for multipart metadata
        src_multipart_meta = await load_multipart_metadata(client, src_bucket, src_key)

        if not src_wrapped_dek and not src_multipart_meta:
            # Source is not encrypted - pass through to backend
            resp = await client.copy_object(
                bucket, key, copy_source, content_type=content_type
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

        # Source is encrypted - need to decrypt and re-encrypt
        if src_multipart_meta:
            # Multipart encrypted source - download all parts and decrypt
            dek = crypto.unwrap_key(src_multipart_meta.wrapped_dek, self.settings.kek)
            sorted_parts = sorted(src_multipart_meta.parts, key=lambda p: p.part_number)

            plaintext_chunks = []
            ct_offset = 0
            for part in sorted_parts:
                ct_end = ct_offset + part.ciphertext_size - 1
                resp = await client.get_object(src_bucket, src_key, f"bytes={ct_offset}-{ct_end}")
                ciphertext = await resp["Body"].read()
                plaintext_chunks.append(crypto.decrypt(ciphertext, dek))
                ct_offset = ct_end + 1

            plaintext = b"".join(plaintext_chunks)
        else:
            # Single-part encrypted source
            resp = await client.get_object(src_bucket, src_key)
            ciphertext = await resp["Body"].read()
            wrapped_dek = base64.b64decode(src_wrapped_dek)
            plaintext = crypto.decrypt_object(ciphertext, wrapped_dek, self.settings.kek)

        # Re-encrypt with new DEK for destination
        encrypted = crypto.encrypt_object(plaintext, self.settings.kek)
        etag = hashlib.md5(plaintext).hexdigest()

        # Determine metadata for destination
        dest_metadata = {
            self.settings.dektag_name: base64.b64encode(encrypted.wrapped_dek).decode(),
            "client-etag": etag,
            "plaintext-size": str(len(plaintext)),
        }

        await client.put_object(
            bucket, key, encrypted.ciphertext,
            metadata=dest_metadata,
            content_type=content_type or head_resp.get("ContentType", "application/octet-stream"),
        )

        # Return CopyObjectResult
        last_modified = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z")

        return Response(
            content=xml_responses.copy_object_result(etag, last_modified),
            media_type="application/xml",
        )

    async def handle_get_object_tagging(
        self, request: Request, creds: S3Credentials
    ) -> Response:
        """Handle GetObjectTagging request (GET /bucket/key?tagging)."""
        from .. import xml_responses

        bucket, key = self._parse_path(request.url.path)
        client = self._client(creds)

        try:
            resp = await client.get_object_tagging(bucket, key)
            return Response(
                content=xml_responses.get_tagging(resp.get("TagSet", [])),
                media_type="application/xml",
            )
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                raise HTTPException(404, "Not found") from None
            raise HTTPException(500, str(e)) from e

    async def handle_put_object_tagging(
        self, request: Request, creds: S3Credentials
    ) -> Response:
        """Handle PutObjectTagging request (PUT /bucket/key?tagging)."""
        import xml.etree.ElementTree as ET

        bucket, key = self._parse_path(request.url.path)
        client = self._client(creds)

        # Parse the XML body
        body = await request.body()
        try:
            root = ET.fromstring(body.decode())
        except ET.ParseError as e:
            raise HTTPException(400, f"Invalid XML: {e}") from e

        # Extract tags
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        tags = []
        for tag_elem in root.findall(f".//{ns}Tag") or root.findall(".//Tag"):
            key_elem = tag_elem.find(f"{ns}Key") or tag_elem.find("Key")
            value_elem = tag_elem.find(f"{ns}Value") or tag_elem.find("Value")
            if key_elem is not None and key_elem.text:
                tags.append({
                    "Key": key_elem.text,
                    "Value": value_elem.text if value_elem is not None and value_elem.text else "",
                })

        try:
            await client.put_object_tagging(bucket, key, tags)
            return Response(status_code=200)
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                raise HTTPException(404, "Not found") from None
            raise HTTPException(500, str(e)) from e

    async def handle_delete_object_tagging(
        self, request: Request, creds: S3Credentials
    ) -> Response:
        """Handle DeleteObjectTagging request (DELETE /bucket/key?tagging)."""
        bucket, key = self._parse_path(request.url.path)
        client = self._client(creds)

        try:
            await client.delete_object_tagging(bucket, key)
            return Response(status_code=204)
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                raise HTTPException(404, "Not found") from None
            raise HTTPException(500, str(e)) from e
