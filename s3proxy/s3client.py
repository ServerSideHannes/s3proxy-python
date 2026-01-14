"""Async S3 client wrapper with SigV4 verification."""

import base64
import hashlib
import hmac
import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from typing import Any

import aioboto3
import structlog
from botocore.config import Config

from .config import Settings

logger = structlog.get_logger()

# SigV4 clock skew tolerance
CLOCK_SKEW_TOLERANCE = timedelta(minutes=5)


@lru_cache(maxsize=64)
def _derive_signing_key(secret_key: str, date_stamp: str, region: str, service: str) -> bytes:
    """Derive SigV4 signing key with caching.

    The signing key only depends on (secret_key, date_stamp, region, service) and
    stays the same for an entire day. Caching avoids 4 HMAC operations per request.
    """
    k_date = hmac.new(
        f"AWS4{secret_key}".encode(), date_stamp.encode(), hashlib.sha256
    ).digest()
    k_region = hmac.new(k_date, region.encode(), hashlib.sha256).digest()
    k_service = hmac.new(k_region, service.encode(), hashlib.sha256).digest()
    return hmac.new(k_service, b"aws4_request", hashlib.sha256).digest()


@dataclass(slots=True)
class S3Credentials:
    """AWS credentials extracted from request."""

    access_key: str
    secret_key: str
    region: str
    service: str = "s3"


@dataclass(slots=True)
class ParsedRequest:
    """Parsed S3 request information."""

    method: str
    bucket: str
    key: str
    query_params: dict[str, list[str]] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes = b""
    is_presigned: bool = False


class SigV4Verifier:
    """AWS Signature Version 4 verification."""

    def __init__(self, credentials_store: dict[str, str]):
        """Initialize with a mapping of access_key -> secret_key."""
        self.credentials_store = credentials_store

    def verify(
        self, request: ParsedRequest, path: str
    ) -> tuple[bool, S3Credentials | None, str]:
        """Verify SigV4 signature.

        Returns:
            (is_valid, credentials, error_message)
        """
        # Check for Authorization header (standard SigV4)
        auth_header = request.headers.get("authorization", "")
        if auth_header.startswith("AWS4-HMAC-SHA256"):
            return self._verify_header_signature(request, path, auth_header)

        # Check for presigned URL (query params)
        if "X-Amz-Signature" in request.query_params:
            return self._verify_presigned_v4(request, path)

        # Check for legacy presigned V2
        if "Signature" in request.query_params:
            return self._verify_presigned_v2(request, path)

        return False, None, "No AWS signature found"

    def _verify_header_signature(
        self, request: ParsedRequest, path: str, auth_header: str
    ) -> tuple[bool, S3Credentials | None, str]:
        """Verify Authorization header signature."""
        try:
            parts = auth_header.replace("AWS4-HMAC-SHA256 ", "").split(",")
            auth_parts = {}
            for part in parts:
                key, value = part.strip().split("=", 1)
                auth_parts[key.strip()] = value.strip()

            credential = auth_parts["Credential"]
            signed_headers = auth_parts["SignedHeaders"]
            signature = auth_parts["Signature"]

            cred_parts = credential.split("/")
            access_key = cred_parts[0]
            date_stamp = cred_parts[1]
            region = cred_parts[2]
            service = cred_parts[3]

            secret_key = self.credentials_store.get(access_key)
            if not secret_key:
                return False, None, f"Unknown access key: {access_key}"

            credentials = S3Credentials(
                access_key=access_key,
                secret_key=secret_key,
                region=region,
                service=service,
            )

            amz_date = request.headers.get("x-amz-date", "")
            if not amz_date:
                return False, credentials, "Missing x-amz-date header"

            try:
                request_time = datetime.strptime(amz_date, "%Y%m%dT%H%M%SZ").replace(
                    tzinfo=UTC
                )
                now = datetime.now(UTC)
                if abs(now - request_time) > CLOCK_SKEW_TOLERANCE:
                    return False, credentials, "Request time too skewed"
            except ValueError:
                return False, credentials, "Invalid x-amz-date format"

            canonical_request = self._build_canonical_request(
                request, path, signed_headers.split(";")
            )

            string_to_sign = self._build_string_to_sign(
                amz_date, date_stamp, region, service, canonical_request
            )

            signing_key = self._get_signing_key(secret_key, date_stamp, region, service)
            calculated_sig = hmac.new(
                signing_key, string_to_sign.encode(), hashlib.sha256
            ).hexdigest()

            if hmac.compare_digest(calculated_sig, signature):
                return True, credentials, ""
            return False, credentials, "Signature mismatch"

        except (KeyError, ValueError, IndexError) as e:
            return False, None, f"Invalid Authorization header: {e}"

    def _verify_presigned_v4(
        self, request: ParsedRequest, path: str
    ) -> tuple[bool, S3Credentials | None, str]:
        """Verify presigned URL (V4)."""
        try:
            credential = request.query_params.get("X-Amz-Credential", [""])[0]
            amz_date = request.query_params.get("X-Amz-Date", [""])[0]
            expires = int(request.query_params.get("X-Amz-Expires", ["0"])[0])
            signed_headers = request.query_params.get("X-Amz-SignedHeaders", [""])[0]
            signature = request.query_params.get("X-Amz-Signature", [""])[0]

            cred_parts = credential.split("/")
            access_key = cred_parts[0]
            date_stamp = cred_parts[1]
            region = cred_parts[2]
            service = cred_parts[3]

            secret_key = self.credentials_store.get(access_key)
            if not secret_key:
                return False, None, f"Unknown access key: {access_key}"

            credentials = S3Credentials(
                access_key=access_key,
                secret_key=secret_key,
                region=region,
                service=service,
            )

            request_time = datetime.strptime(amz_date, "%Y%m%dT%H%M%SZ").replace(
                tzinfo=UTC
            )
            expiry_time = request_time + timedelta(seconds=expires)
            if datetime.now(UTC) > expiry_time:
                return False, credentials, "Presigned URL expired"

            query_for_signing = {
                k: v
                for k, v in request.query_params.items()
                if k != "X-Amz-Signature"
            }

            canonical_request = self._build_canonical_request_presigned(
                request, path, signed_headers.split(";"), query_for_signing
            )

            string_to_sign = self._build_string_to_sign(
                amz_date, date_stamp, region, service, canonical_request
            )

            signing_key = self._get_signing_key(secret_key, date_stamp, region, service)
            calculated_sig = hmac.new(
                signing_key, string_to_sign.encode(), hashlib.sha256
            ).hexdigest()

            if hmac.compare_digest(calculated_sig, signature):
                return True, credentials, ""
            return False, credentials, "Signature mismatch"

        except (KeyError, ValueError, IndexError) as e:
            return False, None, f"Invalid presigned URL: {e}"

    def _verify_presigned_v2(
        self, request: ParsedRequest, path: str
    ) -> tuple[bool, S3Credentials | None, str]:
        """Verify legacy presigned URL (V2)."""
        try:
            access_key = request.query_params.get("AWSAccessKeyId", [""])[0]
            signature = request.query_params.get("Signature", [""])[0]
            expires = request.query_params.get("Expires", [""])[0]

            secret_key = self.credentials_store.get(access_key)
            if not secret_key:
                return False, None, f"Unknown access key: {access_key}"

            credentials = S3Credentials(
                access_key=access_key,
                secret_key=secret_key,
                region="us-east-1",
            )

            expiry_time = datetime.fromtimestamp(int(expires), tz=UTC)
            if datetime.now(UTC) > expiry_time:
                return False, credentials, "Presigned URL expired"

            string_to_sign = f"{request.method}\n\n\n{expires}\n{path}"
            calculated_sig = base64.b64encode(
                hmac.new(
                    secret_key.encode(), string_to_sign.encode(), hashlib.sha1
                ).digest()
            ).decode()

            if hmac.compare_digest(calculated_sig, signature):
                return True, credentials, ""
            return False, credentials, "Signature mismatch"

        except (KeyError, ValueError) as e:
            return False, None, f"Invalid V2 presigned URL: {e}"

    def _build_canonical_request(
        self, request: ParsedRequest, path: str, signed_headers: list[str]
    ) -> str:
        """Build canonical request for signature verification."""
        method = request.method.upper()
        canonical_uri = path or "/"
        canonical_query = self._build_canonical_query_string(request.query_params)

        canonical_headers = ""
        for header in sorted(signed_headers):
            value = request.headers.get(header.lower(), "")
            canonical_headers += f"{header.lower()}:{value.strip()}\n"

        signed_headers_str = ";".join(sorted(signed_headers))

        payload_hash = request.headers.get(
            "x-amz-content-sha256", hashlib.sha256(request.body).hexdigest()
        )

        return "\n".join([
            method,
            canonical_uri,
            canonical_query,
            canonical_headers,
            signed_headers_str,
            payload_hash,
        ])

    def _build_canonical_request_presigned(
        self,
        request: ParsedRequest,
        path: str,
        signed_headers: list[str],
        query_params: dict[str, list[str]],
    ) -> str:
        """Build canonical request for presigned URL verification."""
        method = request.method.upper()
        canonical_uri = path or "/"
        canonical_query = self._build_canonical_query_string(query_params)

        canonical_headers = ""
        for header in sorted(signed_headers):
            value = request.headers.get(header.lower(), "")
            canonical_headers += f"{header.lower()}:{value.strip()}\n"

        signed_headers_str = ";".join(sorted(signed_headers))
        payload_hash = "UNSIGNED-PAYLOAD"

        return "\n".join([
            method,
            canonical_uri,
            canonical_query,
            canonical_headers,
            signed_headers_str,
            payload_hash,
        ])

    def _build_canonical_query_string(self, query_params: dict[str, list[str]]) -> str:
        """Build canonical query string with proper URL encoding for SigV4."""
        from urllib.parse import quote

        if not query_params:
            return ""

        sorted_params = []
        for key in sorted(query_params.keys()):
            for value in sorted(query_params[key]):
                # URL-encode key and value per AWS SigV4 spec
                # Use safe='' to encode everything except unreserved chars
                encoded_key = quote(key, safe="-_.~")
                encoded_value = quote(value, safe="-_.~")
                sorted_params.append((encoded_key, encoded_value))

        return "&".join(f"{k}={v}" for k, v in sorted_params)

    def _build_string_to_sign(
        self,
        amz_date: str,
        date_stamp: str,
        region: str,
        service: str,
        canonical_request: str,
    ) -> str:
        """Build string to sign."""
        credential_scope = f"{date_stamp}/{region}/{service}/aws4_request"
        canonical_request_hash = hashlib.sha256(canonical_request.encode()).hexdigest()

        return "\n".join([
            "AWS4-HMAC-SHA256",
            amz_date,
            credential_scope,
            canonical_request_hash,
        ])

    def _get_signing_key(
        self, secret_key: str, date_stamp: str, region: str, service: str
    ) -> bytes:
        """Derive the signing key (cached)."""
        return _derive_signing_key(secret_key, date_stamp, region, service)


class S3ClientPool:
    """Shared S3 client pool for connection reuse."""

    _instances: dict[str, "S3ClientPool"] = {}
    _class_lock = threading.Lock()  # Protects _instances access

    def __init__(self, settings: Settings, credentials: S3Credentials):
        self.settings = settings
        self.credentials = credentials
        self._session = aioboto3.Session(
            aws_access_key_id=credentials.access_key,
            aws_secret_access_key=credentials.secret_key,
            region_name=credentials.region,
        )
        self._config = Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 3, "mode": "adaptive"},
            max_pool_connections=100,  # Increased for better concurrency
            connect_timeout=10,
            read_timeout=60,
        )
        self._client = None
        self._client_lock = None

    @classmethod
    def get_pool(cls, settings: Settings, credentials: S3Credentials) -> "S3ClientPool":
        """Get or create a shared pool for these credentials."""
        key = f"{credentials.access_key}:{settings.s3_endpoint}"
        with cls._class_lock:
            if key not in cls._instances:
                cls._instances[key] = cls(settings, credentials)
            return cls._instances[key]

    async def _get_client(self):
        """Get or create the shared client."""
        import asyncio

        if self._client_lock is None:
            self._client_lock = asyncio.Lock()

        async with self._client_lock:
            if self._client is None:
                self._client = await self._session.client(
                    "s3",
                    endpoint_url=self.settings.s3_endpoint,
                    config=self._config,
                ).__aenter__()
            return self._client

    async def close(self):
        """Close the client."""
        if self._client is not None:
            await self._client.__aexit__(None, None, None)
            self._client = None


class S3Client:
    """Async S3 client wrapper using shared connection pool."""

    def __init__(self, settings: Settings, credentials: S3Credentials):
        """Initialize S3 client with credentials."""
        self._pool = S3ClientPool.get_pool(settings, credentials)

    async def _client(self):
        """Get the shared client."""
        return await self._pool._get_client()

    async def get_object(
        self,
        bucket: str,
        key: str,
        range_header: str | None = None,
    ) -> dict[str, Any]:
        """Get object from S3."""
        client = await self._client()
        kwargs: dict[str, Any] = {"Bucket": bucket, "Key": key}
        if range_header:
            kwargs["Range"] = range_header
        return await client.get_object(**kwargs)

    async def put_object(
        self,
        bucket: str,
        key: str,
        body: bytes,
        metadata: dict[str, str] | None = None,
        content_type: str | None = None,
    ) -> dict[str, Any]:
        """Put object to S3."""
        client = await self._client()
        kwargs: dict[str, Any] = {"Bucket": bucket, "Key": key, "Body": body}
        if metadata:
            kwargs["Metadata"] = metadata
        if content_type:
            kwargs["ContentType"] = content_type
        return await client.put_object(**kwargs)

    async def head_object(self, bucket: str, key: str) -> dict[str, Any]:
        """Get object metadata."""
        client = await self._client()
        return await client.head_object(Bucket=bucket, Key=key)

    async def delete_object(self, bucket: str, key: str) -> dict[str, Any]:
        """Delete object from S3."""
        client = await self._client()
        return await client.delete_object(Bucket=bucket, Key=key)

    async def create_multipart_upload(
        self,
        bucket: str,
        key: str,
        metadata: dict[str, str] | None = None,
        content_type: str | None = None,
    ) -> dict[str, Any]:
        """Create multipart upload."""
        client = await self._client()
        kwargs: dict[str, Any] = {"Bucket": bucket, "Key": key}
        if metadata:
            kwargs["Metadata"] = metadata
        if content_type:
            kwargs["ContentType"] = content_type
        return await client.create_multipart_upload(**kwargs)

    async def upload_part(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        part_number: int,
        body: bytes,
    ) -> dict[str, Any]:
        """Upload a part."""
        client = await self._client()
        return await client.upload_part(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            PartNumber=part_number,
            Body=body,
        )

    async def complete_multipart_upload(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        parts: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Complete multipart upload."""
        client = await self._client()
        return await client.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

    async def abort_multipart_upload(
        self, bucket: str, key: str, upload_id: str
    ) -> dict[str, Any]:
        """Abort multipart upload."""
        client = await self._client()
        return await client.abort_multipart_upload(
            Bucket=bucket, Key=key, UploadId=upload_id
        )

    async def list_objects_v2(
        self,
        bucket: str,
        prefix: str | None = None,
        continuation_token: str | None = None,
        max_keys: int = 1000,
    ) -> dict[str, Any]:
        """List objects in bucket."""
        client = await self._client()
        kwargs: dict[str, Any] = {"Bucket": bucket, "MaxKeys": max_keys}
        if prefix:
            kwargs["Prefix"] = prefix
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token
        return await client.list_objects_v2(**kwargs)

    async def create_bucket(self, bucket: str) -> dict[str, Any]:
        """Create a bucket."""
        client = await self._client()
        return await client.create_bucket(Bucket=bucket)

    async def delete_bucket(self, bucket: str) -> dict[str, Any]:
        """Delete a bucket."""
        client = await self._client()
        return await client.delete_bucket(Bucket=bucket)

    async def head_bucket(self, bucket: str) -> dict[str, Any]:
        """Check if bucket exists."""
        client = await self._client()
        return await client.head_bucket(Bucket=bucket)

    async def get_bucket_location(self, bucket: str) -> dict[str, Any]:
        """Get bucket location/region."""
        client = await self._client()
        return await client.get_bucket_location(Bucket=bucket)

    async def copy_object(
        self,
        bucket: str,
        key: str,
        copy_source: str,
        metadata: dict[str, str] | None = None,
        metadata_directive: str = "COPY",
        content_type: str | None = None,
    ) -> dict[str, Any]:
        """Copy object within S3.

        Args:
            bucket: Destination bucket
            key: Destination key
            copy_source: Source in format "bucket/key" or "/bucket/key"
            metadata: Optional metadata for destination object
            metadata_directive: COPY or REPLACE
            content_type: Optional content type for destination
        """
        client = await self._client()
        kwargs: dict[str, Any] = {
            "Bucket": bucket,
            "Key": key,
            "CopySource": copy_source,
            "MetadataDirective": metadata_directive,
        }
        if metadata and metadata_directive == "REPLACE":
            kwargs["Metadata"] = metadata
        if content_type:
            kwargs["ContentType"] = content_type
        return await client.copy_object(**kwargs)

    async def delete_objects(
        self,
        bucket: str,
        objects: list[dict[str, str]],
        quiet: bool = False,
    ) -> dict[str, Any]:
        """Delete multiple objects.

        Args:
            bucket: Bucket name
            objects: List of {"Key": "key", "VersionId": "vid"} dicts
            quiet: If True, only return errors (not successful deletions)
        """
        client = await self._client()
        return await client.delete_objects(
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
        """List in-progress multipart uploads.

        Args:
            bucket: Bucket name
            prefix: Filter uploads by key prefix
            key_marker: Key to start listing after
            upload_id_marker: Upload ID to start listing after
            max_uploads: Maximum uploads to return
        """
        client = await self._client()
        kwargs: dict[str, Any] = {"Bucket": bucket, "MaxUploads": max_uploads}
        if prefix:
            kwargs["Prefix"] = prefix
        if key_marker:
            kwargs["KeyMarker"] = key_marker
        if upload_id_marker:
            kwargs["UploadIdMarker"] = upload_id_marker
        return await client.list_multipart_uploads(**kwargs)

    async def list_parts(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        part_number_marker: int | None = None,
        max_parts: int = 1000,
    ) -> dict[str, Any]:
        """List parts of a multipart upload.

        Args:
            bucket: Bucket name
            key: Object key
            upload_id: Multipart upload ID
            part_number_marker: Part number to start listing after
            max_parts: Maximum parts to return
        """
        client = await self._client()
        kwargs: dict[str, Any] = {
            "Bucket": bucket,
            "Key": key,
            "UploadId": upload_id,
            "MaxParts": max_parts,
        }
        if part_number_marker:
            kwargs["PartNumberMarker"] = part_number_marker
        return await client.list_parts(**kwargs)
