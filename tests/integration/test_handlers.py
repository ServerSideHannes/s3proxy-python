"""Tests for S3 proxy handlers."""

import hashlib
import os
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from s3proxy.config import Settings
from s3proxy.main import create_app


@pytest.fixture
def settings():
    """Create test settings."""
    return Settings(
        host="http://localhost:9000",
        encrypt_key="test-encryption-key",
        region="us-east-1",
        no_tls=True,
        port=4433,
    )


@pytest.fixture
def mock_credentials():
    """Set up mock AWS credentials."""
    with patch.dict(
        os.environ,
        {
            "AWS_ACCESS_KEY_ID": "test-access-key",
            "AWS_SECRET_ACCESS_KEY": "test-secret-key",
        },
    ):
        yield


@pytest.fixture
def client(settings, mock_credentials):
    """Create test client with proper lifespan handling."""
    app = create_app(settings)
    with TestClient(app, raise_server_exceptions=False) as client:
        yield client


class TestHealthEndpoints:
    """Test health check endpoints."""

    def test_healthz(self, client):
        """Test /healthz returns ok."""
        response = client.get("/healthz")
        assert response.status_code == 200
        assert response.text == "ok"

    def test_readyz(self, client):
        """Test /readyz returns ok."""
        response = client.get("/readyz")
        assert response.status_code == 200
        assert response.text == "ok"


class TestAuthValidation:
    """Test authentication validation."""

    def test_missing_auth_header(self, client):
        """Test request without Authorization header fails."""
        response = client.get("/test-bucket/test-key")
        assert response.status_code in (401, 403)

    def test_invalid_auth_format(self, client):
        """Test request with invalid Authorization format fails."""
        response = client.get("/test-bucket/test-key", headers={"Authorization": "InvalidFormat"})
        assert response.status_code in (401, 403)

    def test_missing_signature(self, client):
        """Test AWS4 auth without Signature field fails."""
        response = client.get(
            "/test-bucket/test-key",
            headers={
                "Authorization": "AWS4-HMAC-SHA256 Credential=key/date/region/s3/aws4_request"
            },
        )
        assert response.status_code in (401, 403)


class TestSettings:
    """Test settings and configuration."""

    def test_default_settings(self):
        """Test default settings values."""
        with patch.dict(os.environ, {"S3PROXY_ENCRYPT_KEY": "test-key"}):
            settings = Settings()
            assert settings.region == "us-east-1"
            assert settings.no_tls is False
            assert settings.port == 4433


class TestRangeParsing:
    """Test byte range parsing."""

    def test_parse_simple_range(self, settings):
        """Test parsing a simple byte range."""
        from s3proxy.handlers import S3ProxyHandler
        from s3proxy.state import MultipartStateManager

        handler = S3ProxyHandler(settings, {}, MultipartStateManager())
        start, end = handler._parse_range("bytes=0-999", 10000)
        assert start == 0
        assert end == 999

    def test_parse_range_missing_end(self, settings):
        """Test parsing range with missing end."""
        from s3proxy.handlers import S3ProxyHandler
        from s3proxy.state import MultipartStateManager

        handler = S3ProxyHandler(settings, {}, MultipartStateManager())
        start, end = handler._parse_range("bytes=1000-", 10000)
        assert start == 1000
        assert end == 9999

    def test_parse_range_suffix(self, settings):
        """Test parsing suffix range."""
        from s3proxy.handlers import S3ProxyHandler
        from s3proxy.state import MultipartStateManager

        handler = S3ProxyHandler(settings, {}, MultipartStateManager())
        start, end = handler._parse_range("bytes=-500", 10000)
        assert start == 9500
        assert end == 9999

    def test_parse_range_invalid_raises_error(self, settings):
        """Test invalid range format raises error."""
        from s3proxy.errors import S3Error
        from s3proxy.handlers import S3ProxyHandler
        from s3proxy.state import MultipartStateManager

        handler = S3ProxyHandler(settings, {}, MultipartStateManager())
        with pytest.raises(S3Error):
            handler._parse_range("invalid-range", 10000)


class TestLargeUploadSignatureVerification:
    """Test late signature verification for large streaming uploads."""

    @pytest.mark.asyncio
    async def test_streaming_upload_with_correct_hash(self, settings, mock_s3):
        """Test streaming upload succeeds when computed hash matches expected."""
        from unittest.mock import MagicMock

        from s3proxy.handlers import S3ProxyHandler
        from s3proxy.state import MultipartStateManager

        handler = S3ProxyHandler(settings, {}, MultipartStateManager())
        handler._client = MagicMock(return_value=mock_s3)

        # Create mock request with streaming body
        test_data = b"test data for streaming upload " * 1000
        expected_sha256 = hashlib.sha256(test_data).hexdigest()

        mock_request = MagicMock()
        mock_request.headers = {}

        async def mock_stream():
            chunk_size = 1024
            for i in range(0, len(test_data), chunk_size):
                yield test_data[i : i + chunk_size]

        mock_request.stream = mock_stream

        # Create bucket first
        await mock_s3.create_bucket("test-bucket")

        # Call streaming upload with correct expected hash
        response = await handler._put_streaming(
            mock_request,
            mock_s3,
            "test-bucket",
            "test-key",
            "application/octet-stream",
            expected_sha256=expected_sha256,
        )

        assert response.status_code == 200
        # Object should exist
        obj_key = mock_s3._key("test-bucket", "test-key")
        assert obj_key in mock_s3.objects

    @pytest.mark.asyncio
    async def test_streaming_upload_with_incorrect_hash_fails(self, settings, mock_s3):
        """Test streaming upload fails and cleans up when hash doesn't match."""
        from unittest.mock import MagicMock

        from s3proxy.errors import S3Error
        from s3proxy.handlers import S3ProxyHandler
        from s3proxy.state import MultipartStateManager

        handler = S3ProxyHandler(settings, {}, MultipartStateManager())
        handler._client = MagicMock(return_value=mock_s3)

        # Create mock request with streaming body
        test_data = b"test data for streaming upload " * 1000

        mock_request = MagicMock()
        mock_request.headers = {}

        async def mock_stream():
            chunk_size = 1024
            for i in range(0, len(test_data), chunk_size):
                yield test_data[i : i + chunk_size]

        mock_request.stream = mock_stream

        # Create bucket first
        await mock_s3.create_bucket("test-bucket")

        # Call streaming upload with WRONG expected hash
        wrong_hash = "0" * 64  # Invalid hash

        with pytest.raises(S3Error) as exc_info:
            await handler._put_streaming(
                mock_request,
                mock_s3,
                "test-bucket",
                "test-key",
                "application/octet-stream",
                expected_sha256=wrong_hash,
            )

        # Should be signature error
        assert exc_info.value.code == "SignatureDoesNotMatch"

        # Object should be deleted (cleanup after verification failure)
        obj_key = mock_s3._key("test-bucket", "test-key")
        assert obj_key not in mock_s3.objects

    @pytest.mark.asyncio
    async def test_streaming_upload_without_hash_succeeds(self, settings, mock_s3):
        """Test streaming upload without expected hash (unsigned) succeeds."""
        from unittest.mock import MagicMock

        from s3proxy.handlers import S3ProxyHandler
        from s3proxy.state import MultipartStateManager

        handler = S3ProxyHandler(settings, {}, MultipartStateManager())
        handler._client = MagicMock(return_value=mock_s3)

        # Create mock request with streaming body
        test_data = b"test data for streaming upload " * 1000

        mock_request = MagicMock()
        mock_request.headers = {}

        async def mock_stream():
            chunk_size = 1024
            for i in range(0, len(test_data), chunk_size):
                yield test_data[i : i + chunk_size]

        mock_request.stream = mock_stream

        # Create bucket first
        await mock_s3.create_bucket("test-bucket")

        # Call streaming upload without expected hash (like UNSIGNED-PAYLOAD)
        response = await handler._put_streaming(
            mock_request,
            mock_s3,
            "test-bucket",
            "test-key",
            "application/octet-stream",
            expected_sha256=None,
        )

        assert response.status_code == 200
        # Object should exist
        obj_key = mock_s3._key("test-bucket", "test-key")
        assert obj_key in mock_s3.objects


class TestMultipartDownloadWithInternalParts:
    """Test downloading multipart objects with internal parts (streaming uploads)."""

    @pytest.mark.asyncio
    async def test_download_multipart_with_internal_parts(self, settings, mock_s3):
        """Test downloading an object that was uploaded with internal parts."""
        from unittest.mock import MagicMock

        from s3proxy import crypto
        from s3proxy.handlers import S3ProxyHandler
        from s3proxy.state import (
            InternalPartMetadata,
            MultipartMetadata,
            MultipartStateManager,
            PartMetadata,
            save_multipart_metadata,
        )

        handler = S3ProxyHandler(settings, {}, MultipartStateManager())
        handler._client = MagicMock(return_value=mock_s3)

        # Create bucket
        await mock_s3.create_bucket("test-bucket")

        # Simulate a large object split into internal parts
        # Part 1: 50MB plaintext split into 4 internal parts (16MB + 16MB + 16MB + 2MB)
        test_data = b"x" * (50 * 1024 * 1024)  # 50MB
        dek = crypto.generate_dek()
        wrapped_dek = crypto.wrap_key(dek, settings.kek)

        # Split into internal parts
        part_size = 16 * 1024 * 1024
        internal_parts = []
        ciphertext_parts = []
        total_ct_size = 0

        upload_id = "test-upload-id"
        offset = 0
        internal_part_num = 1
        while offset < len(test_data):
            end = min(offset + part_size, len(test_data))
            chunk = test_data[offset:end]

            # Encrypt chunk
            nonce = crypto.derive_part_nonce(upload_id, internal_part_num)
            ciphertext = crypto.encrypt(chunk, dek, nonce)
            ciphertext_parts.append(ciphertext)

            internal_parts.append(
                InternalPartMetadata(
                    internal_part_number=internal_part_num,
                    plaintext_size=len(chunk),
                    ciphertext_size=len(ciphertext),
                    etag=f"etag{internal_part_num}",
                )
            )
            total_ct_size += len(ciphertext)
            offset = end
            internal_part_num += 1

        # Create metadata with internal parts
        part_meta = PartMetadata(
            part_number=1,
            plaintext_size=len(test_data),
            ciphertext_size=total_ct_size,
            etag="synthetic-etag",
            md5=hashlib.md5(test_data).hexdigest(),
            internal_parts=internal_parts,
        )

        metadata = MultipartMetadata(
            version=1,
            part_count=1,
            total_plaintext_size=len(test_data),
            parts=[part_meta],
            wrapped_dek=wrapped_dek,
        )

        # Upload concatenated ciphertext as the S3 object
        concatenated_ciphertext = b"".join(ciphertext_parts)
        await mock_s3.put_object(
            "test-bucket",
            "test-key",
            concatenated_ciphertext,
        )

        # Save metadata
        await save_multipart_metadata(mock_s3, "test-bucket", "test-key", metadata)

        # Now try to download it
        mock_request = MagicMock()
        mock_request.url.path = "/test-bucket/test-key"
        mock_request.headers = {}

        mock_creds = MagicMock()
        response = await handler.handle_get_object(mock_request, mock_creds)

        # Read the response
        chunks = []
        async for chunk in response.body_iterator:
            chunks.append(chunk)

        downloaded_data = b"".join(chunks)

        # Verify we got the original data back
        assert downloaded_data == test_data
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_download_multipart_with_range_request(self, settings, mock_s3):
        """Test range download from object with internal parts."""
        from unittest.mock import MagicMock

        from s3proxy import crypto
        from s3proxy.handlers import S3ProxyHandler
        from s3proxy.state import (
            InternalPartMetadata,
            MultipartMetadata,
            MultipartStateManager,
            PartMetadata,
            save_multipart_metadata,
        )

        handler = S3ProxyHandler(settings, {}, MultipartStateManager())
        handler._client = MagicMock(return_value=mock_s3)

        # Create bucket
        await mock_s3.create_bucket("test-bucket")

        # Create test data with recognizable pattern
        test_data = b"".join([bytes([i % 256]) * 1024 for i in range(1024)])  # 1MB with pattern
        dek = crypto.generate_dek()
        wrapped_dek = crypto.wrap_key(dek, settings.kek)

        # Split into 2 internal parts
        part_size = len(test_data) // 2
        internal_parts = []
        ciphertext_parts = []
        total_ct_size = 0

        upload_id = "test-upload-id"
        offset = 0
        internal_part_num = 1
        while offset < len(test_data):
            end = min(offset + part_size, len(test_data))
            chunk = test_data[offset:end]

            nonce = crypto.derive_part_nonce(upload_id, internal_part_num)
            ciphertext = crypto.encrypt(chunk, dek, nonce)
            ciphertext_parts.append(ciphertext)

            internal_parts.append(
                InternalPartMetadata(
                    internal_part_number=internal_part_num,
                    plaintext_size=len(chunk),
                    ciphertext_size=len(ciphertext),
                    etag=f"etag{internal_part_num}",
                )
            )
            total_ct_size += len(ciphertext)
            offset = end
            internal_part_num += 1

        part_meta = PartMetadata(
            part_number=1,
            plaintext_size=len(test_data),
            ciphertext_size=total_ct_size,
            etag="synthetic-etag",
            md5=hashlib.md5(test_data).hexdigest(),
            internal_parts=internal_parts,
        )

        metadata = MultipartMetadata(
            version=1,
            part_count=1,
            total_plaintext_size=len(test_data),
            parts=[part_meta],
            wrapped_dek=wrapped_dek,
        )

        # Upload concatenated ciphertext
        concatenated_ciphertext = b"".join(ciphertext_parts)
        await mock_s3.put_object("test-bucket", "test-key", concatenated_ciphertext)
        await save_multipart_metadata(mock_s3, "test-bucket", "test-key", metadata)

        # Request a range that spans both internal parts
        mock_request = MagicMock()
        mock_request.url.path = "/test-bucket/test-key"
        mock_request.headers = {"range": "bytes=500000-600000"}  # 100KB range

        mock_creds = MagicMock()
        response = await handler.handle_get_object(mock_request, mock_creds)

        # Read response
        chunks = []
        async for chunk in response.body_iterator:
            chunks.append(chunk)

        downloaded_data = b"".join(chunks)

        # Verify we got the correct range
        expected_data = test_data[500000:600001]
        assert downloaded_data == expected_data
        assert response.status_code == 206  # Partial content
        assert "Content-Range" in response.headers
