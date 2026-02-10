"""Shared pytest fixtures for S3Proxy tests."""

import asyncio
import hashlib
import os
from datetime import UTC, datetime
from typing import Any
from unittest.mock import patch

import fakeredis.aioredis
import pytest

# Set required environment variables before importing s3proxy modules
os.environ.setdefault("S3PROXY_ENCRYPT_KEY", "test-encryption-key-for-pytest")
os.environ.setdefault("S3PROXY_HOST", "http://localhost:9000")

from s3proxy.config import Settings
from s3proxy.s3client import S3Client, S3Credentials
from s3proxy.state import MultipartStateManager
from s3proxy.state import redis as state_redis

# ============================================================================
# Redis Fixtures
# ============================================================================


@pytest.fixture(autouse=True)
async def mock_redis():
    """Set up fake Redis for all tests."""
    fake_redis = fakeredis.aioredis.FakeRedis(decode_responses=False)
    original_client = state_redis._redis_client
    state_redis._redis_client = fake_redis
    yield fake_redis
    await fake_redis.aclose()
    state_redis._redis_client = original_client


# ============================================================================
# Settings Fixtures
# ============================================================================


@pytest.fixture
def settings():
    """Create test settings with encryption key."""
    return Settings(
        host="http://localhost:9000",
        encrypt_key="test-encryption-key-32bytes!!!!",
        region="us-east-1",
        no_tls=True,
        port=4433,
    )


@pytest.fixture
def kek(settings):
    """Get the Key Encryption Key derived from settings."""
    return settings.kek


# ============================================================================
# Credentials Fixtures
# ============================================================================


@pytest.fixture
def credentials():
    """Create test AWS credentials."""
    return S3Credentials(
        access_key="AKIAIOSFODNN7EXAMPLE",
        secret_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        region="us-east-1",
    )


@pytest.fixture
def mock_credentials_env():
    """Set up mock AWS credentials in environment."""
    with patch.dict(
        os.environ,
        {
            "AWS_ACCESS_KEY_ID": "AKIAIOSFODNN7EXAMPLE",
            "AWS_SECRET_ACCESS_KEY": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        },
    ):
        yield


@pytest.fixture
def credentials_store():
    """Create a credentials store dict."""
    return {
        "AKIAIOSFODNN7EXAMPLE": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    }


# ============================================================================
# Mock S3 Client Fixtures
# ============================================================================


class MockS3Response:
    """Mock S3 response object."""

    def __init__(self, data: bytes):
        self.data = data
        self._read = False

    async def read(self):
        """Read response body."""
        return self.data

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        return None


class MockS3Client:
    """Mock S3 client for testing without real S3 backend."""

    def __init__(self):
        self.objects: dict[str, dict[str, Any]] = {}  # bucket/key -> {body, metadata, ...}
        self.buckets: dict[str, dict] = {}
        self.multipart_uploads: dict[str, dict] = {}  # upload_id -> {bucket, key, parts}
        self.call_history: list[tuple[str, dict]] = []

    async def __aenter__(self):
        """Async context manager entry - returns self."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - no cleanup needed for mock."""
        return None

    def _key(self, bucket: str, key: str) -> str:
        return f"{bucket}/{key}"

    async def put_object(
        self,
        bucket: str,
        key: str,
        body: bytes,
        metadata: dict[str, str] | None = None,
        content_type: str = "application/octet-stream",
        tagging: str | None = None,
        cache_control: str | None = None,
        expires: str | None = None,
    ) -> dict:
        """Store an object."""
        self.call_history.append(("put_object", {"bucket": bucket, "key": key}))
        self.objects[self._key(bucket, key)] = {
            "Body": body,
            "Metadata": metadata or {},
            "ContentType": content_type,
            "ContentLength": len(body),
            "ETag": hashlib.md5(body).hexdigest(),
            "LastModified": datetime.now(UTC),
            "CacheControl": cache_control,
            "Expires": expires,
            "Tagging": tagging,
        }
        return {"ETag": f'"{hashlib.md5(body).hexdigest()}"'}

    async def get_object(self, bucket: str, key: str, range_header: str | None = None) -> dict:
        """Retrieve an object."""
        self.call_history.append(
            ("get_object", {"bucket": bucket, "key": key, "range": range_header})
        )
        obj_key = self._key(bucket, key)
        if obj_key not in self.objects:
            raise self._not_found_error(key)

        obj = self.objects[obj_key]
        body = obj["Body"]

        if range_header:
            # Parse range header: bytes=start-end
            range_spec = range_header.replace("bytes=", "")
            start, end = range_spec.split("-")
            start = int(start)
            end = int(end) if end else len(body) - 1
            body = body[start : end + 1]

        return {
            "Body": MockS3Response(body),
            "Metadata": obj["Metadata"],
            "ContentType": obj["ContentType"],
            "ContentLength": len(body),
            "ETag": obj["ETag"],
            "LastModified": obj["LastModified"],
        }

    async def head_object(self, bucket: str, key: str) -> dict:
        """Get object metadata."""
        self.call_history.append(("head_object", {"bucket": bucket, "key": key}))
        obj_key = self._key(bucket, key)
        if obj_key not in self.objects:
            raise self._not_found_error(key)

        obj = self.objects[obj_key]
        return {
            "Metadata": obj["Metadata"],
            "ContentType": obj["ContentType"],
            "ContentLength": obj["ContentLength"],
            "ETag": obj["ETag"],
            "LastModified": obj["LastModified"],
        }

    async def delete_object(self, bucket: str, key: str) -> dict:
        """Delete an object."""
        self.call_history.append(("delete_object", {"bucket": bucket, "key": key}))
        obj_key = self._key(bucket, key)
        if obj_key in self.objects:
            del self.objects[obj_key]
        return {}

    async def delete_objects(
        self,
        bucket: str,
        objects: list[dict[str, str]],
        quiet: bool = False,
    ) -> dict:
        """Delete multiple objects."""
        self.call_history.append(("delete_objects", {"bucket": bucket, "objects": objects}))
        deleted = []
        errors = []

        for obj in objects:
            key = obj["Key"]
            obj_key = self._key(bucket, key)
            if obj_key in self.objects:
                del self.objects[obj_key]
                deleted.append({"Key": key})
            else:
                # S3 doesn't error on missing keys in batch delete
                deleted.append({"Key": key})

        return {"Deleted": deleted, "Errors": errors}

    async def list_objects_v2(
        self,
        bucket: str,
        prefix: str = "",
        continuation_token: str | None = None,
        max_keys: int = 1000,
        delimiter: str | None = None,
    ) -> dict:
        """List objects in bucket."""
        self.call_history.append(
            ("list_objects_v2", {"bucket": bucket, "prefix": prefix, "delimiter": delimiter})
        )
        contents = []
        common_prefixes = set()

        for obj_key, obj in sorted(self.objects.items()):
            b, k = obj_key.split("/", 1)
            if b != bucket or not k.startswith(prefix):
                continue

            # Handle delimiter for grouping
            if delimiter:
                suffix = k[len(prefix) :]
                if delimiter in suffix:
                    common_prefix = prefix + suffix[: suffix.index(delimiter) + len(delimiter)]
                    common_prefixes.add(common_prefix)
                    continue

            contents.append(
                {
                    "Key": k,
                    "Size": obj["ContentLength"],
                    "ETag": obj["ETag"],
                    "LastModified": obj["LastModified"],
                    "StorageClass": "STANDARD",
                }
            )

        is_truncated = len(contents) > max_keys
        result = {
            "Contents": contents[:max_keys],
            "IsTruncated": is_truncated,
            "KeyCount": min(len(contents), max_keys),
        }

        if common_prefixes:
            result["CommonPrefixes"] = [{"Prefix": p} for p in sorted(common_prefixes)]

        return result

    async def copy_object(
        self,
        bucket: str,
        key: str,
        copy_source: str,
        metadata: dict[str, str] | None = None,
        metadata_directive: str = "COPY",
        content_type: str | None = None,
    ) -> dict:
        """Copy an object."""
        self.call_history.append(
            ("copy_object", {"bucket": bucket, "key": key, "source": copy_source})
        )

        # Parse source
        source = copy_source.lstrip("/")
        src_bucket, src_key = source.split("/", 1)
        src_obj_key = self._key(src_bucket, src_key)

        if src_obj_key not in self.objects:
            raise self._not_found_error(src_key)

        src_obj = self.objects[src_obj_key]

        # Copy to destination
        dest_metadata = metadata if metadata_directive == "REPLACE" else src_obj["Metadata"]
        dest_content_type = content_type or src_obj["ContentType"]

        self.objects[self._key(bucket, key)] = {
            "Body": src_obj["Body"],
            "Metadata": dest_metadata,
            "ContentType": dest_content_type,
            "ContentLength": src_obj["ContentLength"],
            "ETag": src_obj["ETag"],
            "LastModified": datetime.now(UTC),
        }

        return {
            "CopyObjectResult": {
                "ETag": src_obj["ETag"],
                "LastModified": datetime.now(UTC),
            }
        }

    async def create_bucket(self, bucket: str) -> dict:
        """Create a bucket."""
        self.call_history.append(("create_bucket", {"bucket": bucket}))
        self.buckets[bucket] = {"CreationDate": datetime.now(UTC)}
        return {}

    async def delete_bucket(self, bucket: str) -> dict:
        """Delete a bucket."""
        self.call_history.append(("delete_bucket", {"bucket": bucket}))
        if bucket in self.buckets:
            del self.buckets[bucket]
        return {}

    async def head_bucket(self, bucket: str) -> dict:
        """Check if bucket exists."""
        self.call_history.append(("head_bucket", {"bucket": bucket}))
        if bucket not in self.buckets:
            raise self._bucket_not_found_error(bucket)
        return {}

    async def get_bucket_location(self, bucket: str) -> dict:
        """Get bucket location."""
        self.call_history.append(("get_bucket_location", {"bucket": bucket}))
        if bucket not in self.buckets:
            raise self._bucket_not_found_error(bucket)
        return {"LocationConstraint": "us-east-1"}

    # Multipart upload methods
    async def create_multipart_upload(self, bucket: str, key: str, **kwargs) -> dict:
        """Initiate multipart upload."""
        self.call_history.append(("create_multipart_upload", {"bucket": bucket, "key": key}))
        upload_id = f"upload-{len(self.multipart_uploads)}-{key}"
        self.multipart_uploads[upload_id] = {
            "Bucket": bucket,
            "Key": key,
            "Parts": {},
            "Initiated": datetime.now(UTC),
        }
        return {"UploadId": upload_id}

    async def upload_part(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        part_number: int,
        body: bytes,
    ) -> dict:
        """Upload a part."""
        self.call_history.append(
            ("upload_part", {"bucket": bucket, "key": key, "part": part_number})
        )
        if upload_id not in self.multipart_uploads:
            raise self._not_found_error(f"upload {upload_id}")

        etag = hashlib.md5(body).hexdigest()
        self.multipart_uploads[upload_id]["Parts"][part_number] = {
            "Body": body,
            "ETag": etag,
            "Size": len(body),
            "LastModified": datetime.now(UTC),
        }
        return {"ETag": f'"{etag}"'}

    async def complete_multipart_upload(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        parts: list[dict],
    ) -> dict:
        """Complete multipart upload."""
        self.call_history.append(("complete_multipart_upload", {"bucket": bucket, "key": key}))
        if upload_id not in self.multipart_uploads:
            raise self._not_found_error(f"upload {upload_id}")

        upload = self.multipart_uploads[upload_id]

        # Concatenate parts
        body = b""
        for part_info in sorted(parts, key=lambda p: p["PartNumber"]):
            part_num = part_info["PartNumber"]
            if part_num in upload["Parts"]:
                body += upload["Parts"][part_num]["Body"]

        # Store complete object
        etag = hashlib.md5(body).hexdigest()
        self.objects[self._key(bucket, key)] = {
            "Body": body,
            "Metadata": {},
            "ContentType": "application/octet-stream",
            "ContentLength": len(body),
            "ETag": etag,
            "LastModified": datetime.now(UTC),
        }

        del self.multipart_uploads[upload_id]
        return {"ETag": f'"{etag}"'}

    async def abort_multipart_upload(self, bucket: str, key: str, upload_id: str) -> dict:
        """Abort multipart upload."""
        self.call_history.append(("abort_multipart_upload", {"bucket": bucket, "key": key}))
        if upload_id in self.multipart_uploads:
            del self.multipart_uploads[upload_id]
        return {}

    async def list_multipart_uploads(
        self,
        bucket: str,
        prefix: str | None = None,
        key_marker: str | None = None,
        upload_id_marker: str | None = None,
        max_uploads: int = 1000,
    ) -> dict:
        """List multipart uploads."""
        self.call_history.append(("list_multipart_uploads", {"bucket": bucket}))
        uploads = []
        for upload_id, upload in self.multipart_uploads.items():
            if upload["Bucket"] == bucket:
                key = upload["Key"]
                if prefix and not key.startswith(prefix):
                    continue
                uploads.append(
                    {
                        "Key": key,
                        "UploadId": upload_id,
                        "Initiated": upload["Initiated"],
                        "StorageClass": "STANDARD",
                    }
                )

        return {
            "Uploads": uploads[:max_uploads],
            "IsTruncated": len(uploads) > max_uploads,
        }

    async def list_parts(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        part_number_marker: int | None = None,
        max_parts: int = 1000,
    ) -> dict:
        """List parts of a multipart upload."""
        self.call_history.append(
            ("list_parts", {"bucket": bucket, "key": key, "upload_id": upload_id})
        )
        if upload_id not in self.multipart_uploads:
            raise self._not_found_error(f"upload {upload_id}")

        upload = self.multipart_uploads[upload_id]
        parts = []
        for part_num, part in sorted(upload["Parts"].items()):
            if part_number_marker and part_num <= part_number_marker:
                continue
            parts.append(
                {
                    "PartNumber": part_num,
                    "ETag": part["ETag"],
                    "Size": part["Size"],
                    "LastModified": part["LastModified"],
                }
            )

        return {
            "Parts": parts[:max_parts],
            "IsTruncated": len(parts) > max_parts,
            "StorageClass": "STANDARD",
        }

    async def list_buckets(self) -> dict:
        """List all buckets."""
        self.call_history.append(("list_buckets", {}))
        buckets = [
            {"Name": name, "CreationDate": info["CreationDate"]}
            for name, info in self.buckets.items()
        ]
        return {
            "Owner": {"ID": "owner-id-123", "DisplayName": "test-owner"},
            "Buckets": buckets,
        }

    async def list_objects_v1(
        self,
        bucket: str,
        prefix: str | None = None,
        marker: str | None = None,
        delimiter: str | None = None,
        max_keys: int = 1000,
    ) -> dict:
        """List objects in bucket using V1 API."""
        self.call_history.append(
            ("list_objects_v1", {"bucket": bucket, "prefix": prefix, "marker": marker})
        )
        contents = []
        common_prefixes = set()
        prefix = prefix or ""

        for obj_key, obj in sorted(self.objects.items()):
            b, k = obj_key.split("/", 1)
            if b != bucket or not k.startswith(prefix):
                continue
            if marker and k <= marker:
                continue

            # Handle delimiter for grouping
            if delimiter:
                suffix = k[len(prefix) :]
                if delimiter in suffix:
                    common_prefix = prefix + suffix[: suffix.index(delimiter) + len(delimiter)]
                    common_prefixes.add(common_prefix)
                    continue

            contents.append(
                {
                    "Key": k,
                    "Size": obj["ContentLength"],
                    "ETag": obj["ETag"],
                    "LastModified": obj["LastModified"],
                    "StorageClass": "STANDARD",
                }
            )

        is_truncated = len(contents) > max_keys
        contents = contents[:max_keys]
        next_marker = contents[-1]["Key"] if is_truncated and contents else None

        return {
            "Contents": contents,
            "CommonPrefixes": [{"Prefix": cp} for cp in sorted(common_prefixes)],
            "IsTruncated": is_truncated,
            "NextMarker": next_marker,
        }

    async def get_object_tagging(self, bucket: str, key: str) -> dict:
        """Get object tags."""
        self.call_history.append(("get_object_tagging", {"bucket": bucket, "key": key}))
        obj_key = self._key(bucket, key)
        if obj_key not in self.objects:
            raise self._not_found_error(key)

        obj = self.objects[obj_key]
        return {"TagSet": obj.get("Tags", [])}

    async def put_object_tagging(self, bucket: str, key: str, tags: list[dict[str, str]]) -> dict:
        """Set object tags."""
        self.call_history.append(
            ("put_object_tagging", {"bucket": bucket, "key": key, "tags": tags})
        )
        obj_key = self._key(bucket, key)
        if obj_key not in self.objects:
            raise self._not_found_error(key)

        self.objects[obj_key]["Tags"] = tags
        return {}

    async def delete_object_tagging(self, bucket: str, key: str) -> dict:
        """Delete object tags."""
        self.call_history.append(("delete_object_tagging", {"bucket": bucket, "key": key}))
        obj_key = self._key(bucket, key)
        if obj_key not in self.objects:
            raise self._not_found_error(key)

        self.objects[obj_key]["Tags"] = []
        return {}

    async def upload_part_copy(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        part_number: int,
        copy_source: str,
        copy_source_range: str | None = None,
    ) -> dict:
        """Copy a part from another object."""
        self.call_history.append(
            (
                "upload_part_copy",
                {
                    "bucket": bucket,
                    "key": key,
                    "upload_id": upload_id,
                    "part_number": part_number,
                    "copy_source": copy_source,
                },
            )
        )
        if upload_id not in self.multipart_uploads:
            raise self._not_found_error(f"upload {upload_id}")

        # Parse source
        source = copy_source.lstrip("/")
        src_bucket, src_key = source.split("/", 1)
        src_obj_key = self._key(src_bucket, src_key)

        if src_obj_key not in self.objects:
            raise self._not_found_error(src_key)

        src_obj = self.objects[src_obj_key]
        body = src_obj["Body"]

        # Handle range if specified
        if copy_source_range:
            range_spec = copy_source_range.replace("bytes=", "")
            start, end = range_spec.split("-")
            start = int(start)
            end = int(end)
            body = body[start : end + 1]

        etag = hashlib.md5(body).hexdigest()
        self.multipart_uploads[upload_id]["Parts"][part_number] = {
            "Body": body,
            "ETag": etag,
            "Size": len(body),
            "LastModified": datetime.now(UTC),
        }
        return {
            "CopyPartResult": {
                "ETag": f'"{etag}"',
                "LastModified": datetime.now(UTC),
            }
        }

    def _not_found_error(self, key: str):
        """Create a NoSuchKey error."""
        error = Exception(f"NoSuchKey: {key}")
        error.response = {"Error": {"Code": "NoSuchKey", "Message": f"Key not found: {key}"}}
        return error

    def _bucket_not_found_error(self, bucket: str):
        """Create a NoSuchBucket error."""
        error = Exception(f"NoSuchBucket: {bucket}")
        error.response = {
            "Error": {"Code": "NoSuchBucket", "Message": f"Bucket not found: {bucket}"}
        }
        return error


@pytest.fixture
def mock_s3():
    """Create a mock S3 client."""
    return MockS3Client()


@pytest.fixture
def mock_s3_client(mock_s3, settings, credentials):
    """Create an S3Client with mocked boto3."""

    class PatchedS3Client(S3Client):
        def __init__(self, mock):
            self._mock = mock
            self.settings = settings
            self.credentials = credentials

        async def _client(self):
            return self._mock

        # Delegate all methods to mock
        async def put_object(self, *args, **kwargs):
            return await self._mock.put_object(*args, **kwargs)

        async def get_object(self, *args, **kwargs):
            return await self._mock.get_object(*args, **kwargs)

        async def head_object(self, *args, **kwargs):
            return await self._mock.head_object(*args, **kwargs)

        async def delete_object(self, *args, **kwargs):
            return await self._mock.delete_object(*args, **kwargs)

        async def delete_objects(self, *args, **kwargs):
            return await self._mock.delete_objects(*args, **kwargs)

        async def list_objects_v2(self, *args, **kwargs):
            return await self._mock.list_objects_v2(*args, **kwargs)

        async def copy_object(self, *args, **kwargs):
            return await self._mock.copy_object(*args, **kwargs)

        async def create_bucket(self, *args, **kwargs):
            return await self._mock.create_bucket(*args, **kwargs)

        async def delete_bucket(self, *args, **kwargs):
            return await self._mock.delete_bucket(*args, **kwargs)

        async def head_bucket(self, *args, **kwargs):
            return await self._mock.head_bucket(*args, **kwargs)

        async def get_bucket_location(self, *args, **kwargs):
            return await self._mock.get_bucket_location(*args, **kwargs)

        async def create_multipart_upload(self, *args, **kwargs):
            return await self._mock.create_multipart_upload(*args, **kwargs)

        async def upload_part(self, *args, **kwargs):
            return await self._mock.upload_part(*args, **kwargs)

        async def complete_multipart_upload(self, *args, **kwargs):
            return await self._mock.complete_multipart_upload(*args, **kwargs)

        async def abort_multipart_upload(self, *args, **kwargs):
            return await self._mock.abort_multipart_upload(*args, **kwargs)

        async def list_multipart_uploads(self, *args, **kwargs):
            return await self._mock.list_multipart_uploads(*args, **kwargs)

        async def list_parts(self, *args, **kwargs):
            return await self._mock.list_parts(*args, **kwargs)

        async def list_buckets(self, *args, **kwargs):
            return await self._mock.list_buckets(*args, **kwargs)

        async def list_objects_v1(self, *args, **kwargs):
            return await self._mock.list_objects_v1(*args, **kwargs)

        async def get_object_tagging(self, *args, **kwargs):
            return await self._mock.get_object_tagging(*args, **kwargs)

        async def put_object_tagging(self, *args, **kwargs):
            return await self._mock.put_object_tagging(*args, **kwargs)

        async def delete_object_tagging(self, *args, **kwargs):
            return await self._mock.delete_object_tagging(*args, **kwargs)

        async def upload_part_copy(self, *args, **kwargs):
            return await self._mock.upload_part_copy(*args, **kwargs)

    return PatchedS3Client(mock_s3)


# ============================================================================
# Handler Fixtures
# ============================================================================


@pytest.fixture
def multipart_manager():
    """Create a multipart state manager."""
    return MultipartStateManager()


@pytest.fixture
def manager():
    """Alias for multipart_manager (used by many test files)."""
    return MultipartStateManager()


@pytest.fixture
def handler(settings, manager):
    """Create MultipartHandlerMixin instance for testing."""
    from s3proxy.handlers.multipart import MultipartHandlerMixin

    return MultipartHandlerMixin(settings, {}, manager)


# ============================================================================
# Test Data Fixtures
# ============================================================================


@pytest.fixture
def sample_plaintext():
    """Sample plaintext data for testing."""
    return b"Hello, World! This is test data for S3Proxy encryption testing."


@pytest.fixture
def large_plaintext():
    """Large plaintext data for multipart testing (10MB)."""
    return b"x" * (10 * 1024 * 1024)


@pytest.fixture
def sample_objects():
    """Sample objects for listing tests."""
    return [
        {"key": "file1.txt", "content": b"content1"},
        {"key": "file2.txt", "content": b"content2"},
        {"key": "subdir/file3.txt", "content": b"content3"},
        {"key": "subdir/nested/file4.txt", "content": b"content4"},
    ]


# ============================================================================
# Event Loop Fixture
# ============================================================================


@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()
