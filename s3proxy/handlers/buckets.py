"""Bucket operations and list objects."""

import xml.etree.ElementTree as ET
from urllib.parse import parse_qs

from botocore.exceptions import ClientError
from fastapi import HTTPException, Request, Response

from .. import xml_responses
from ..multipart import INTERNAL_PREFIX, META_SUFFIX_LEGACY, delete_multipart_metadata
from ..s3client import S3Credentials
from .base import BaseHandler


class BucketHandlerMixin(BaseHandler):
    """Mixin for bucket operations."""

    async def handle_list_buckets(self, request: Request, creds: S3Credentials) -> Response:
        """Handle ListBuckets request (GET /)."""
        client = self._client(creds)
        resp = await client.list_buckets()
        return Response(
            content=xml_responses.list_buckets(
                resp.get("Owner", {}),
                resp.get("Buckets", []),
            ),
            media_type="application/xml",
        )

    async def handle_list_objects(self, request: Request, creds: S3Credentials) -> Response:
        """Handle ListObjectsV2 request (GET /bucket?list-type=2)."""
        bucket = self._parse_bucket(request.url.path)
        client = self._client(creds)
        query = parse_qs(request.url.query)
        prefix = query.get("prefix", [""])[0]
        token = query.get("continuation-token", [""])[0] or None
        max_keys = int(query.get("max-keys", ["1000"])[0])

        resp = await client.list_objects_v2(bucket, prefix, token, max_keys)

        objects = await self._process_list_objects(client, bucket, resp.get("Contents", []))

        return Response(
            content=xml_responses.list_objects(
                bucket, prefix, max_keys,
                resp.get("IsTruncated", False),
                resp.get("NextContinuationToken"),
                objects,
            ),
            media_type="application/xml",
        )

    async def handle_list_objects_v1(self, request: Request, creds: S3Credentials) -> Response:
        """Handle ListObjects V1 request (GET /bucket without list-type=2)."""
        bucket = self._parse_bucket(request.url.path)
        client = self._client(creds)
        query = parse_qs(request.url.query)
        prefix = query.get("prefix", [""])[0]
        marker = query.get("marker", [""])[0] or None
        delimiter = query.get("delimiter", [""])[0] or None
        max_keys = int(query.get("max-keys", ["1000"])[0])

        resp = await client.list_objects_v1(bucket, prefix, marker, delimiter, max_keys)

        objects = await self._process_list_objects(client, bucket, resp.get("Contents", []))

        # Extract common prefixes, filtering out internal prefixes
        common_prefixes = [
            cp["Prefix"] for cp in resp.get("CommonPrefixes", [])
            if not cp["Prefix"].startswith(INTERNAL_PREFIX)
        ]

        # V1 uses NextMarker (or last key if truncated and no delimiter)
        next_marker = resp.get("NextMarker")
        if resp.get("IsTruncated") and not next_marker and objects:
            next_marker = objects[-1]["key"]

        return Response(
            content=xml_responses.list_objects_v1(
                bucket, prefix, marker, delimiter, max_keys,
                resp.get("IsTruncated", False),
                next_marker,
                objects,
                common_prefixes,
            ),
            media_type="application/xml",
        )

    def _is_internal_key(self, key: str) -> bool:
        """Check if key is an internal s3proxy key that should be hidden."""
        return (
            key.startswith(INTERNAL_PREFIX)
            or key.endswith(META_SUFFIX_LEGACY)
            or ".s3proxy-upload-" in key
        )

    async def _process_list_objects(
        self, client, bucket: str, contents: list[dict]
    ) -> list[dict]:
        """Process list objects response, filtering internal objects and fetching metadata."""
        objects = []
        for obj in contents:
            if self._is_internal_key(obj["Key"]):
                continue
            try:
                head = await client.head_object(bucket, obj["Key"])
                meta = head.get("Metadata", {})
                size = meta.get("plaintext-size", obj.get("Size", 0))
                etag = meta.get("client-etag", obj.get("ETag", "").strip('"'))
            except Exception:
                size, etag = obj.get("Size", 0), obj.get("ETag", "").strip('"')

            objects.append({
                "key": obj["Key"],
                "last_modified": obj["LastModified"].isoformat(),
                "etag": etag,
                "size": size,
                "storage_class": obj.get("StorageClass", "STANDARD"),
            })
        return objects

    async def handle_create_bucket(self, request: Request, creds: S3Credentials) -> Response:
        bucket = self._parse_bucket(request.url.path)
        client = self._client(creds)
        try:
            await client.create_bucket(bucket)
            return Response(status_code=200)
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code == "BucketAlreadyOwnedByYou":
                return Response(status_code=200)
            raise HTTPException(400, str(e)) from e

    async def handle_delete_bucket(self, request: Request, creds: S3Credentials) -> Response:
        bucket = self._parse_bucket(request.url.path)
        client = self._client(creds)
        await client.delete_bucket(bucket)
        return Response(status_code=204)

    async def handle_head_bucket(self, request: Request, creds: S3Credentials) -> Response:
        bucket = self._parse_bucket(request.url.path)
        client = self._client(creds)
        try:
            await client.head_bucket(bucket)
            return Response(status_code=200)
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchBucket", "404"):
                raise HTTPException(404, "Bucket not found") from None
            raise HTTPException(500, str(e)) from e

    async def handle_get_bucket_location(
        self, request: Request, creds: S3Credentials
    ) -> Response:
        """Handle GetBucketLocation request."""
        bucket = self._parse_bucket(request.url.path)
        client = self._client(creds)
        try:
            resp = await client.get_bucket_location(bucket)
            # AWS returns None for us-east-1
            location = resp.get("LocationConstraint")
            return Response(
                content=xml_responses.location_constraint(location),
                media_type="application/xml",
            )
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchBucket", "404"):
                raise HTTPException(404, "Bucket not found") from None
            raise HTTPException(500, str(e)) from e

    async def handle_list_multipart_uploads(
        self, request: Request, creds: S3Credentials
    ) -> Response:
        """Handle ListMultipartUploads request (GET /?uploads)."""
        bucket = self._parse_bucket(request.url.path)
        client = self._client(creds)
        query = parse_qs(request.url.query)

        prefix = query.get("prefix", [""])[0] or None
        key_marker = query.get("key-marker", [""])[0] or None
        upload_id_marker = query.get("upload-id-marker", [""])[0] or None
        max_uploads = int(query.get("max-uploads", ["1000"])[0])

        resp = await client.list_multipart_uploads(
            bucket, prefix, key_marker, upload_id_marker, max_uploads
        )

        uploads = []
        for upload in resp.get("Uploads", []):
            # Filter out internal s3proxy metadata uploads
            key = upload.get("Key", "")
            if key.endswith(META_SUFFIX) or ".s3proxy-upload-" in key:
                continue
            uploads.append({
                "Key": key,
                "UploadId": upload.get("UploadId", ""),
                "Initiated": upload.get("Initiated", "").isoformat()
                if hasattr(upload.get("Initiated"), "isoformat")
                else str(upload.get("Initiated", "")),
                "StorageClass": upload.get("StorageClass", "STANDARD"),
            })

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

    async def handle_delete_objects(
        self, request: Request, creds: S3Credentials
    ) -> Response:
        """Handle DeleteObjects batch delete request (POST /?delete)."""
        bucket = self._parse_bucket(request.url.path)
        client = self._client(creds)

        # Parse the XML body
        body = await request.body()
        try:
            root = ET.fromstring(body.decode())
        except ET.ParseError as e:
            raise HTTPException(400, f"Invalid XML: {e}") from e

        # Extract objects to delete
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        objects_to_delete = []
        for obj_elem in root.findall(f".//{ns}Object") or root.findall(".//Object"):
            key_elem = obj_elem.find(f"{ns}Key") or obj_elem.find("Key")
            if key_elem is not None and key_elem.text:
                obj_dict = {"Key": key_elem.text}
                version_elem = obj_elem.find(f"{ns}VersionId") or obj_elem.find("VersionId")
                if version_elem is not None and version_elem.text:
                    obj_dict["VersionId"] = version_elem.text
                objects_to_delete.append(obj_dict)

        if not objects_to_delete:
            raise HTTPException(400, "No objects specified for deletion")

        # Check for Quiet mode
        quiet_elem = root.find(f"{ns}Quiet") or root.find("Quiet")
        quiet = quiet_elem is not None and quiet_elem.text and quiet_elem.text.lower() == "true"

        # Perform batch delete
        deleted = []
        errors = []

        try:
            resp = await client.delete_objects(bucket, objects_to_delete, quiet)

            # Process response
            for d in resp.get("Deleted", []):
                deleted.append({
                    "Key": d.get("Key", ""),
                    "VersionId": d.get("VersionId", ""),
                })
                # Also clean up any multipart metadata for deleted objects
                try:
                    await delete_multipart_metadata(client, bucket, d.get("Key", ""))
                except Exception:
                    pass  # Ignore metadata cleanup errors

            for e in resp.get("Errors", []):
                errors.append({
                    "Key": e.get("Key", ""),
                    "Code": e.get("Code", "InternalError"),
                    "Message": e.get("Message", ""),
                    "VersionId": e.get("VersionId", ""),
                })

        except ClientError as e:
            # If the entire operation fails, report all objects as errors
            for obj in objects_to_delete:
                errors.append({
                    "Key": obj["Key"],
                    "Code": e.response["Error"]["Code"],
                    "Message": e.response["Error"]["Message"],
                    "VersionId": obj.get("VersionId", ""),
                })

        return Response(
            content=xml_responses.delete_objects_result(deleted, errors, quiet),
            media_type="application/xml",
        )
