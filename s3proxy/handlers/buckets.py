"""Bucket operations and list objects."""

import asyncio
import contextlib
import uuid
import xml.etree.ElementTree as ET
from urllib.parse import parse_qs

import structlog
from botocore.exceptions import ClientError
from fastapi import Request, Response
from structlog.stdlib import BoundLogger

from .. import xml_responses
from ..errors import S3Error
from ..s3client import S3Credentials
from ..state import INTERNAL_PREFIX, META_SUFFIX_LEGACY, delete_multipart_metadata
from ..xml_utils import find_element, find_elements
from .base import BaseHandler

logger: BoundLogger = structlog.get_logger()


def _strip_minio_cache_suffix(value: str | None) -> str | None:
    """Strip MinIO cache metadata suffix from marker/token values.

    MinIO adds internal cache metadata to markers like:
    'prefix/[minio_cache:v2,return:]' -> 'prefix/'

    Returns the cleaned value or None if the result would be empty.
    """
    if not value:
        return value
    if "[minio_cache:" in value:
        stripped = value.split("[minio_cache:")[0]
        # Return None instead of empty string to indicate no valid marker
        return stripped if stripped else None
    return value


class BucketHandlerMixin(BaseHandler):
    async def handle_list_buckets(self, request: Request, creds: S3Credentials) -> Response:
        async with self._client(creds) as client:
            try:
                resp = await client.list_buckets()
            except ClientError as e:
                raise S3Error.internal_error(str(e)) from e
            return Response(
                content=xml_responses.list_buckets(
                    resp.get("Owner", {}),
                    resp.get("Buckets", []),
                ),
                media_type="application/xml",
            )

    async def handle_list_objects(self, request: Request, creds: S3Credentials) -> Response:
        bucket = self._parse_bucket(request.url.path)
        async with self._client(creds) as client:
            query = parse_qs(request.url.query, keep_blank_values=True)
            prefix = query.get("prefix", [""])[0]
            # Empty string continuation-token should be echoed back, None means not provided
            token = query.get("continuation-token", [None])[0]
            delimiter = query.get("delimiter", [""])[0] or None
            max_keys = int(query.get("max-keys", ["1000"])[0])
            start_after = query.get("start-after", [""])[0] or None
            encoding_type = query.get("encoding-type", [""])[0] or None
            fetch_owner = query.get("fetch-owner", ["false"])[0].lower() == "true"

            # Don't pass empty string token to backend - only pass actual tokens
            backend_token = token if token else None

            try:
                resp = await client.list_objects_v2(
                    bucket, prefix, backend_token, max_keys, delimiter, start_after
                )
            except ClientError as e:
                self._raise_bucket_error(e, bucket)

            objects = await self._process_list_objects(client, bucket, resp.get("Contents", []))

            # Extract common prefixes, filtering internal ones and stripping cache suffix
            common_prefixes = []
            for cp in resp.get("CommonPrefixes", []):
                cp_prefix = cp["Prefix"]
                if cp_prefix.startswith(INTERNAL_PREFIX):
                    continue
                stripped = _strip_minio_cache_suffix(cp_prefix)
                if stripped is not None:
                    common_prefixes.append(stripped)

            # V2 continuation tokens are opaque and must be passed back unchanged
            # Don't strip MinIO cache suffix - it's needed for pagination to work
            next_token = resp.get("NextContinuationToken")

            return Response(
                content=xml_responses.list_objects(
                    bucket,
                    prefix,
                    max_keys,
                    resp.get("IsTruncated", False),
                    next_token,
                    objects,
                    delimiter,
                    common_prefixes,
                    continuation_token=token,
                    start_after=start_after,
                    encoding_type=encoding_type,
                    fetch_owner=fetch_owner,
                ),
                media_type="application/xml",
            )

    async def handle_list_objects_v1(self, request: Request, creds: S3Credentials) -> Response:
        bucket = self._parse_bucket(request.url.path)
        async with self._client(creds) as client:
            query = parse_qs(request.url.query)
            prefix = query.get("prefix", [""])[0]
            marker = query.get("marker", [""])[0] or None
            delimiter = query.get("delimiter", [""])[0] or None
            max_keys = int(query.get("max-keys", ["1000"])[0])
            encoding_type = query.get("encoding-type", [""])[0] or None

            try:
                resp = await client.list_objects_v1(bucket, prefix, marker, delimiter, max_keys)
            except ClientError as e:
                self._raise_bucket_error(e, bucket)

            objects = await self._process_list_objects(client, bucket, resp.get("Contents", []))

            # Extract common prefixes, filtering internal ones and stripping cache suffix
            common_prefixes = []
            for cp in resp.get("CommonPrefixes", []):
                cp_prefix = cp["Prefix"]
                if cp_prefix.startswith(INTERNAL_PREFIX):
                    continue
                stripped = _strip_minio_cache_suffix(cp_prefix)
                if stripped is not None:
                    common_prefixes.append(stripped)

            # V1 uses NextMarker (or last key if truncated and no delimiter)
            next_marker = _strip_minio_cache_suffix(resp.get("NextMarker"))
            if resp.get("IsTruncated") and not next_marker:
                # Fallback: use last object key or last common prefix
                if objects:
                    next_marker = objects[-1]["key"]
                elif common_prefixes:
                    next_marker = common_prefixes[-1]

            return Response(
                content=xml_responses.list_objects_v1(
                    bucket,
                    prefix,
                    marker,
                    delimiter,
                    max_keys,
                    resp.get("IsTruncated", False),
                    next_marker,
                    objects,
                    common_prefixes,
                    encoding_type=encoding_type,
                ),
                media_type="application/xml",
            )

    def _is_internal_key(self, key: str) -> bool:
        return (
            key.startswith(INTERNAL_PREFIX)
            or key.endswith(META_SUFFIX_LEGACY)
            or ".s3proxy-upload-" in key
        )

    async def _process_list_objects(self, client, bucket: str, contents: list[dict]) -> list[dict]:
        objects = []
        for obj in contents:
            if self._is_internal_key(obj["Key"]):
                continue
            try:
                head = await client.head_object(bucket, obj["Key"])
                meta = head.get("Metadata", {})
                size = self._get_plaintext_size(meta, obj.get("Size", 0))
                etag = self._get_effective_etag(meta, obj.get("ETag", ""))
            except Exception:
                size, etag = obj.get("Size", 0), obj.get("ETag", "").strip('"')

            objects.append(
                {
                    "key": obj["Key"],
                    "last_modified": obj["LastModified"].isoformat(),
                    "etag": etag,
                    "size": size,
                    "storage_class": obj.get("StorageClass", "STANDARD"),
                }
            )
        return objects

    async def handle_create_bucket(self, request: Request, creds: S3Credentials) -> Response:
        bucket = self._parse_bucket(request.url.path)
        async with self._client(creds) as client:
            try:
                await client.create_bucket(bucket)
                return Response(status_code=200)
            except ClientError as e:
                code = e.response["Error"]["Code"]
                if code == "BucketAlreadyOwnedByYou":
                    return Response(status_code=200)
                if code == "BucketAlreadyExists":
                    raise S3Error.bucket_already_exists(bucket) from e
                if code == "InvalidBucketName":
                    raise S3Error.invalid_bucket_name(bucket) from e
                raise S3Error.bad_request(str(e)) from e

    async def handle_delete_bucket(self, request: Request, creds: S3Credentials) -> Response:
        bucket = self._parse_bucket(request.url.path)
        async with self._client(creds) as client:
            try:
                await client.delete_bucket(bucket)
                return Response(status_code=204)
            except ClientError as e:
                code = e.response["Error"]["Code"]
                if code in ("NoSuchBucket", "404"):
                    raise S3Error.no_such_bucket(bucket) from None
                if code == "BucketNotEmpty":
                    raise S3Error.bucket_not_empty(bucket) from None
                raise S3Error.internal_error(str(e)) from e

    async def handle_head_bucket(self, request: Request, creds: S3Credentials) -> Response:
        bucket = self._parse_bucket(request.url.path)
        async with self._client(creds) as client:
            try:
                await client.head_bucket(bucket)
                return Response(status_code=200)
            except ClientError as e:
                self._raise_bucket_error(e, bucket)

    async def handle_get_bucket_location(self, request: Request, creds: S3Credentials) -> Response:
        bucket = self._parse_bucket(request.url.path)
        async with self._client(creds) as client:
            try:
                resp = await client.get_bucket_location(bucket)
                # AWS returns None for us-east-1
                location = resp.get("LocationConstraint")
                return Response(
                    content=xml_responses.location_constraint(location),
                    media_type="application/xml",
                )
            except ClientError as e:
                self._raise_bucket_error(e, bucket)

    async def handle_list_multipart_uploads(
        self, request: Request, creds: S3Credentials
    ) -> Response:
        bucket = self._parse_bucket(request.url.path)
        async with self._client(creds) as client:
            query = parse_qs(request.url.query)

            prefix = query.get("prefix", [""])[0] or None
            key_marker = query.get("key-marker", [""])[0] or None
            upload_id_marker = query.get("upload-id-marker", [""])[0] or None
            max_uploads = int(query.get("max-uploads", ["1000"])[0])

            try:
                resp = await client.list_multipart_uploads(
                    bucket, prefix, key_marker, upload_id_marker, max_uploads
                )
            except ClientError as e:
                self._raise_bucket_error(e, bucket)

            uploads = []
            for upload in resp.get("Uploads", []):
                # Filter out internal s3proxy metadata uploads
                key = upload.get("Key", "")
                if self._is_internal_key(key):
                    continue
                uploads.append(
                    {
                        "Key": key,
                        "UploadId": upload.get("UploadId", ""),
                        "Initiated": upload.get("Initiated", "").isoformat()
                        if hasattr(upload.get("Initiated"), "isoformat")
                        else str(upload.get("Initiated", "")),
                        "StorageClass": upload.get("StorageClass", "STANDARD"),
                    }
                )

            return Response(
                content=xml_responses.list_multipart_uploads(
                    bucket=bucket,
                    uploads=uploads,
                    key_marker=key_marker,
                    upload_id_marker=upload_id_marker,
                    next_key_marker=resp.get("NextKeyMarker"),
                    next_upload_id_marker=resp.get("NextUploadIdMarker"),
                    max_uploads=max_uploads,
                    is_truncated=resp.get("IsTruncated", False),
                    prefix=prefix,
                ),
                media_type="application/xml",
            )

    async def handle_delete_objects(self, request: Request, creds: S3Credentials) -> Response:
        request_id = str(uuid.uuid4()).replace("-", "").upper()[:16]
        bucket = self._parse_bucket(request.url.path)
        async with self._client(creds) as client:
            # Parse the XML body
            body = await request.body()
            if not body:
                logger.warning("DeleteObjects request with empty body", bucket=bucket)
                raise S3Error.malformed_xml("Empty request body")

            try:
                root = ET.fromstring(body.decode("utf-8"))
            except (ET.ParseError, UnicodeDecodeError) as e:
                logger.warning("DeleteObjects XML parse error", bucket=bucket, error=str(e))
                raise S3Error.malformed_xml(str(e)) from e

            # Extract objects to delete
            objects_to_delete = []
            for obj_elem in find_elements(root, "Object"):
                key_elem = find_element(obj_elem, "Key")
                if key_elem is not None and key_elem.text:
                    obj_dict = {"Key": key_elem.text}
                    version_elem = find_element(obj_elem, "VersionId")
                    if version_elem is not None and version_elem.text:
                        obj_dict["VersionId"] = version_elem.text
                    objects_to_delete.append(obj_dict)

            if not objects_to_delete:
                # Log the raw XML for debugging
                logger.warning(
                    "DeleteObjects with no objects parsed",
                    bucket=bucket,
                    raw_xml=body.decode("utf-8")[:500],
                    request_id=request_id,
                )
                raise S3Error.malformed_xml("No objects specified for deletion")

            # Check for Quiet mode
            quiet_elem = find_element(root, "Quiet")
            quiet = quiet_elem is not None and quiet_elem.text and quiet_elem.text.lower() == "true"

            # Perform batch delete
            deleted = []
            errors = []

            try:
                resp = await client.delete_objects(bucket, objects_to_delete, quiet)

                # Process response
                deleted_items = resp.get("Deleted", [])
                for d in deleted_items:
                    deleted.append(
                        {
                            "Key": d.get("Key", ""),
                            "VersionId": d.get("VersionId", ""),
                        }
                    )

                # Clean up multipart metadata for all deleted objects in parallel
                if deleted_items:

                    async def safe_delete_metadata(key: str) -> None:
                        with contextlib.suppress(Exception):
                            await delete_multipart_metadata(client, bucket, key)

                    await asyncio.gather(
                        *[safe_delete_metadata(d.get("Key", "")) for d in deleted_items]
                    )

                for e in resp.get("Errors", []):
                    errors.append(
                        {
                            "Key": e.get("Key", ""),
                            "Code": e.get("Code", "InternalError"),
                            "Message": e.get("Message", ""),
                            "VersionId": e.get("VersionId", ""),
                        }
                    )

            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "InternalError")
                error_msg = e.response.get("Error", {}).get("Message", str(e))
                logger.error(
                    "DeleteObjects S3 error",
                    bucket=bucket,
                    error_code=error_code,
                    error_message=error_msg,
                    request_id=request_id,
                )
                # If the entire operation fails, report all objects as errors
                for obj in objects_to_delete:
                    errors.append(
                        {
                            "Key": obj["Key"],
                            "Code": error_code,
                            "Message": error_msg,
                            "VersionId": obj.get("VersionId", ""),
                        }
                    )

            except Exception as e:
                # Catch any other unexpected errors
                logger.error(
                    "DeleteObjects unexpected error",
                    bucket=bucket,
                    error=str(e),
                    request_id=request_id,
                    exc_info=True,
                )
                for obj in objects_to_delete:
                    errors.append(
                        {
                            "Key": obj["Key"],
                            "Code": "InternalError",
                            "Message": str(e),
                            "VersionId": obj.get("VersionId", ""),
                        }
                    )

            return Response(
                content=xml_responses.delete_objects_result(deleted, errors, quiet),
                media_type="application/xml",
                headers={
                    "x-amz-request-id": request_id,
                    "x-amz-id-2": request_id,
                },
            )
