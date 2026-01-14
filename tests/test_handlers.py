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
    with patch.dict(os.environ, {
        "AWS_ACCESS_KEY_ID": "test-access-key",
        "AWS_SECRET_ACCESS_KEY": "test-secret-key",
    }):
        yield


@pytest.fixture
def client(settings, mock_credentials):
    """Create test client."""
    app = create_app(settings)
    return TestClient(app)


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

    def test_missing_auth_returns_403(self, client):
        """Test request without auth returns 403."""
        response = client.get("/test-bucket/test-key")
        assert response.status_code == 403

    def test_invalid_signature_returns_403(self, client):
        """Test request with invalid signature returns 403."""
        auth = (
            "AWS4-HMAC-SHA256 "
            "Credential=INVALID/20230101/us-east-1/s3/aws4_request,"
            "SignedHeaders=host,Signature=invalid"
        )
        response = client.get(
            "/test-bucket/test-key",
            headers={
                "Authorization": auth,
                "x-amz-date": "20230101T000000Z",
            },
        )
        assert response.status_code == 403


class TestSettings:
    """Test settings configuration."""

    def test_kek_derivation(self, settings):
        """Test KEK is derived from encrypt_key."""
        expected = hashlib.sha256(b"test-encryption-key").digest()
        assert settings.kek == expected
        assert len(settings.kek) == 32

    def test_s3_endpoint_with_scheme(self):
        """Test S3 endpoint preserves scheme."""
        settings = Settings(
            host="http://localhost:9000",
            encrypt_key="test",
        )
        assert settings.s3_endpoint == "http://localhost:9000"

    def test_s3_endpoint_without_scheme(self):
        """Test S3 endpoint adds https scheme."""
        settings = Settings(
            host="s3.amazonaws.com",
            encrypt_key="test",
        )
        assert settings.s3_endpoint == "https://s3.amazonaws.com"

    def test_size_calculations(self, settings):
        """Test size calculations."""
        assert settings.max_single_encrypted_bytes == 16 * 1024 * 1024
        assert settings.auto_multipart_bytes == 16 * 1024 * 1024


class TestRangeParsing:
    """Test HTTP Range header parsing."""

    def test_parse_fixed_range(self, settings):
        """Test parsing fixed range."""
        from s3proxy.handlers import S3ProxyHandler
        from s3proxy.multipart import MultipartStateManager

        handler = S3ProxyHandler(settings, {}, MultipartStateManager())

        start, end = handler._parse_range("bytes=0-1023", 10000)
        assert start == 0
        assert end == 1023

    def test_parse_open_range(self, settings):
        """Test parsing open-ended range."""
        from s3proxy.handlers import S3ProxyHandler
        from s3proxy.multipart import MultipartStateManager

        handler = S3ProxyHandler(settings, {}, MultipartStateManager())

        start, end = handler._parse_range("bytes=1000-", 10000)
        assert start == 1000
        assert end == 9999

    def test_parse_suffix_range(self, settings):
        """Test parsing suffix range."""
        from s3proxy.handlers import S3ProxyHandler
        from s3proxy.multipart import MultipartStateManager

        handler = S3ProxyHandler(settings, {}, MultipartStateManager())

        start, end = handler._parse_range("bytes=-500", 10000)
        assert start == 9500
        assert end == 9999

    def test_range_clamped_to_size(self, settings):
        """Test range end is clamped to object size."""
        from s3proxy.handlers import S3ProxyHandler
        from s3proxy.multipart import MultipartStateManager

        handler = S3ProxyHandler(settings, {}, MultipartStateManager())

        start, end = handler._parse_range("bytes=0-99999", 1000)
        assert start == 0
        assert end == 999
