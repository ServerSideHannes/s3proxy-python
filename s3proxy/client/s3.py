"""Async S3 client wrapper with memory-efficient session management."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

import aioboto3
import structlog
from botocore.config import Config
from structlog.stdlib import BoundLogger

if TYPE_CHECKING:
    from ..config import Settings
    from .types import S3Credentials

logger: BoundLogger = structlog.get_logger(__name__)

# Shared session to avoid repeated JSON service model loading
# See: https://github.com/boto/boto3/issues/1670
_shared_session: aioboto3.Session | None = None


def get_shared_session() -> aioboto3.Session:
    """Get or create the shared aioboto3 session."""
    global _shared_session
    if _shared_session is None:
        _shared_session = aioboto3.Session()
    return _shared_session


def _add_optional_kwargs(kwargs: dict[str, Any], **optional: Any) -> None:
    """Add non-None optional kwargs to the dict."""
    for key, value in optional.items():
        if value is not None:
            kwargs[key] = value


class S3Client:
    """Async S3 client wrapper with async context manager lifecycle.

    Memory management:
    - Uses a shared aioboto3 Session to avoid repeated JSON model loading
    - Creates fresh clients per request for proper connection cleanup
    - Each session load costs ~30-150MB (botocore service definitions)

    See: https://github.com/boto/boto3/issues/1670
    """

    def __init__(self, settings: Settings, credentials: S3Credentials):
        """Initialize S3 client with credentials."""
        self.settings = settings
        self.credentials = credentials
        self._config = Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 3, "mode": "adaptive"},
            max_pool_connections=100,
            connect_timeout=10,
            read_timeout=60,
        )
        self._cached_client = None
        self._client_context = None

    async def __aenter__(self):
        """Enter async context - create client from shared session."""
        # Use shared session to avoid loading JSON service models repeatedly
        # Each new session costs ~30-150MB for botocore service definitions
        session = get_shared_session()
        self._client_context = session.client(
            "s3",
            endpoint_url=self.settings.s3_endpoint,
            config=self._config,
            aws_access_key_id=self.credentials.access_key,
            aws_secret_access_key=self.credentials.secret_key,
            region_name=self.credentials.region,
        )
        self._cached_client = await self._client_context.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit async context - clean up client."""
        if self._client_context is not None:
            await self._client_context.__aexit__(exc_type, exc_val, exc_tb)
            self._cached_client = None
            self._client_context = None
        logger.debug("Cleaned up S3 client context")

    async def get_object(
        self,
        bucket: str,
        key: str,
        range_header: str | None = None,
        if_match: str | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        if_unmodified_since: str | None = None,
    ) -> dict[str, Any]:
        """Get object from S3."""
        kwargs: dict[str, Any] = {"Bucket": bucket, "Key": key}
        _add_optional_kwargs(
            kwargs,
            Range=range_header,
            IfMatch=if_match,
            IfNoneMatch=if_none_match,
            IfModifiedSince=if_modified_since,
            IfUnmodifiedSince=if_unmodified_since,
        )
        return await self._cached_client.get_object(**kwargs)

    async def put_object(
        self,
        bucket: str,
        key: str,
        body: bytes,
        metadata: dict[str, str] | None = None,
        content_type: str | None = None,
        tagging: str | None = None,
        cache_control: str | None = None,
        expires: str | None = None,
    ) -> dict[str, Any]:
        """Put object to S3."""
        kwargs: dict[str, Any] = {"Bucket": bucket, "Key": key, "Body": body}
        _add_optional_kwargs(
            kwargs,
            Metadata=metadata,
            ContentType=content_type,
            Tagging=tagging,
            CacheControl=cache_control,
            Expires=expires,
        )
        return await self._cached_client.put_object(**kwargs)

    async def head_object(
        self,
        bucket: str,
        key: str,
        if_match: str | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        if_unmodified_since: str | None = None,
    ) -> dict[str, Any]:
        """Get object metadata."""
        kwargs: dict[str, Any] = {"Bucket": bucket, "Key": key}
        _add_optional_kwargs(
            kwargs,
            IfMatch=if_match,
            IfNoneMatch=if_none_match,
            IfModifiedSince=if_modified_since,
            IfUnmodifiedSince=if_unmodified_since,
        )
        return await self._cached_client.head_object(**kwargs)

    async def delete_object(self, bucket: str, key: str) -> dict[str, Any]:
        """Delete object from S3."""
        return await self._cached_client.delete_object(Bucket=bucket, Key=key)

    async def create_multipart_upload(
        self,
        bucket: str,
        key: str,
        metadata: dict[str, str] | None = None,
        content_type: str | None = None,
        tagging: str | None = None,
        cache_control: str | None = None,
        expires: str | None = None,
    ) -> dict[str, Any]:
        """Create multipart upload."""
        kwargs: dict[str, Any] = {"Bucket": bucket, "Key": key}
        _add_optional_kwargs(
            kwargs,
            Metadata=metadata,
            ContentType=content_type,
            Tagging=tagging,
            CacheControl=cache_control,
            Expires=expires,
        )
        return await self._cached_client.create_multipart_upload(**kwargs)

    async def upload_part(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        part_number: int,
        body: bytes,
    ) -> dict[str, Any]:
        """Upload a part."""
        start = time.monotonic()
        result = await self._cached_client.upload_part(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            PartNumber=part_number,
            Body=body,
        )
        duration = time.monotonic() - start
        size_mb = len(body) / 1024 / 1024
        logger.debug(
            "S3 upload_part completed",
            bucket=bucket,
            part_number=part_number,
            size_mb=f"{size_mb:.2f}",
            duration_seconds=f"{duration:.2f}",
            throughput_mbps=f"{size_mb / duration:.2f}" if duration > 0 else "N/A",
        )
        return result

    async def complete_multipart_upload(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        parts: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Complete multipart upload."""
        start = time.monotonic()
        result = await self._cached_client.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
        duration = time.monotonic() - start
        logger.info(
            "S3 complete_multipart_upload completed",
            bucket=bucket,
            key=key,
            parts_count=len(parts),
            duration_seconds=f"{duration:.2f}",
        )
        return result

    async def abort_multipart_upload(self, bucket: str, key: str, upload_id: str) -> dict[str, Any]:
        """Abort multipart upload."""
        return await self._cached_client.abort_multipart_upload(
            Bucket=bucket, Key=key, UploadId=upload_id
        )

    async def list_objects_v2(
        self,
        bucket: str,
        prefix: str | None = None,
        continuation_token: str | None = None,
        max_keys: int = 1000,
        delimiter: str | None = None,
        start_after: str | None = None,
    ) -> dict[str, Any]:
        """List objects in bucket (V2 API)."""
        kwargs: dict[str, Any] = {"Bucket": bucket, "MaxKeys": max_keys}
        _add_optional_kwargs(
            kwargs,
            Prefix=prefix,
            ContinuationToken=continuation_token,
            Delimiter=delimiter,
            StartAfter=start_after,
        )
        return await self._cached_client.list_objects_v2(**kwargs)

    async def create_bucket(self, bucket: str) -> dict[str, Any]:
        """Create a bucket."""
        return await self._cached_client.create_bucket(Bucket=bucket)

    async def delete_bucket(self, bucket: str) -> dict[str, Any]:
        """Delete a bucket."""
        return await self._cached_client.delete_bucket(Bucket=bucket)

    async def head_bucket(self, bucket: str) -> dict[str, Any]:
        """Check if bucket exists."""
        return await self._cached_client.head_bucket(Bucket=bucket)

    async def get_bucket_location(self, bucket: str) -> dict[str, Any]:
        """Get bucket location/region."""
        return await self._cached_client.get_bucket_location(Bucket=bucket)

    async def copy_object(
        self,
        bucket: str,
        key: str,
        copy_source: str,
        metadata: dict[str, str] | None = None,
        metadata_directive: str = "COPY",
        content_type: str | None = None,
        tagging_directive: str | None = None,
        tagging: str | None = None,
    ) -> dict[str, Any]:
        """Copy object within S3."""
        kwargs: dict[str, Any] = {
            "Bucket": bucket,
            "Key": key,
            "CopySource": copy_source,
            "MetadataDirective": metadata_directive,
        }
        if metadata is not None and metadata_directive == "REPLACE":
            kwargs["Metadata"] = metadata
        if content_type:
            kwargs["ContentType"] = content_type
        if tagging_directive:
            kwargs["TaggingDirective"] = tagging_directive
        if tagging and tagging_directive == "REPLACE":
            kwargs["Tagging"] = tagging
        return await self._cached_client.copy_object(**kwargs)

    async def delete_objects(
        self,
        bucket: str,
        objects: list[dict[str, str]],
        quiet: bool = False,
    ) -> dict[str, Any]:
        """Delete multiple objects."""
        return await self._cached_client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": objects, "Quiet": quiet},
        )

    async def list_multipart_uploads(
        self,
        bucket: str,
        prefix: str | None = None,
        key_marker: str | None = None,
        upload_id_marker: str | None = None,
        max_uploads: int = 1000,
    ) -> dict[str, Any]:
        """List in-progress multipart uploads."""
        kwargs: dict[str, Any] = {"Bucket": bucket, "MaxUploads": max_uploads}
        _add_optional_kwargs(
            kwargs, Prefix=prefix, KeyMarker=key_marker, UploadIdMarker=upload_id_marker
        )
        return await self._cached_client.list_multipart_uploads(**kwargs)

    async def list_parts(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        part_number_marker: int | None = None,
        max_parts: int = 1000,
    ) -> dict[str, Any]:
        """List parts of a multipart upload."""
        kwargs: dict[str, Any] = {
            "Bucket": bucket,
            "Key": key,
            "UploadId": upload_id,
            "MaxParts": max_parts,
        }
        _add_optional_kwargs(kwargs, PartNumberMarker=part_number_marker)
        return await self._cached_client.list_parts(**kwargs)

    async def list_buckets(self) -> dict[str, Any]:
        """List all buckets owned by the authenticated user."""
        return await self._cached_client.list_buckets()

    async def list_objects_v1(
        self,
        bucket: str,
        prefix: str | None = None,
        marker: str | None = None,
        delimiter: str | None = None,
        max_keys: int = 1000,
    ) -> dict[str, Any]:
        """List objects in bucket (V1 API)."""
        kwargs: dict[str, Any] = {"Bucket": bucket, "MaxKeys": max_keys}
        _add_optional_kwargs(kwargs, Prefix=prefix, Marker=marker, Delimiter=delimiter)
        return await self._cached_client.list_objects(**kwargs)

    async def get_object_tagging(self, bucket: str, key: str) -> dict[str, Any]:
        """Get object tags."""
        return await self._cached_client.get_object_tagging(Bucket=bucket, Key=key)

    async def put_object_tagging(
        self, bucket: str, key: str, tags: list[dict[str, str]]
    ) -> dict[str, Any]:
        """Set object tags."""
        return await self._cached_client.put_object_tagging(
            Bucket=bucket, Key=key, Tagging={"TagSet": tags}
        )

    async def delete_object_tagging(self, bucket: str, key: str) -> dict[str, Any]:
        """Delete object tags."""
        return await self._cached_client.delete_object_tagging(Bucket=bucket, Key=key)

    async def upload_part_copy(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        part_number: int,
        copy_source: str,
        copy_source_range: str | None = None,
    ) -> dict[str, Any]:
        """Copy a part from another object."""
        kwargs: dict[str, Any] = {
            "Bucket": bucket,
            "Key": key,
            "UploadId": upload_id,
            "PartNumber": part_number,
            "CopySource": copy_source,
        }
        _add_optional_kwargs(kwargs, CopySourceRange=copy_source_range)
        return await self._cached_client.upload_part_copy(**kwargs)
