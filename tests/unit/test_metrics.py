"""Tests for Prometheus metrics."""

import os

import pytest

# Set env vars before importing s3proxy modules
os.environ.setdefault("S3PROXY_MEMORY_LIMIT_MB", "64")


class TestGetOperationName:
    """Test operation name derivation from request attributes."""

    def test_list_buckets(self):
        from s3proxy.metrics import get_operation_name

        assert get_operation_name("GET", "/", "") == "ListBuckets"

    def test_delete_objects(self):
        from s3proxy.metrics import get_operation_name

        assert get_operation_name("POST", "/bucket", "delete") == "DeleteObjects"

    def test_multipart_operations(self):
        from s3proxy.metrics import get_operation_name

        # List parts
        assert get_operation_name("GET", "/bucket/key", "uploadId=123") == "ListParts"
        # Upload part
        assert get_operation_name("PUT", "/bucket/key", "uploadId=123&partNumber=1") == "UploadPart"
        # Complete multipart
        assert (
            get_operation_name("POST", "/bucket/key", "uploadId=123") == "CompleteMultipartUpload"
        )
        # Abort multipart
        assert get_operation_name("DELETE", "/bucket/key", "uploadId=123") == "AbortMultipartUpload"

    def test_list_multipart_uploads(self):
        from s3proxy.metrics import get_operation_name

        assert get_operation_name("GET", "/bucket", "uploads") == "ListMultipartUploads"

    def test_create_multipart_upload(self):
        from s3proxy.metrics import get_operation_name

        assert get_operation_name("POST", "/bucket/key", "uploads") == "CreateMultipartUpload"

    def test_bucket_operations(self):
        from s3proxy.metrics import get_operation_name

        # Get bucket location
        assert get_operation_name("GET", "/bucket", "location") == "GetBucketLocation"
        # Create bucket
        assert get_operation_name("PUT", "/bucket", "") == "CreateBucket"
        # Delete bucket
        assert get_operation_name("DELETE", "/bucket", "") == "DeleteBucket"
        # Head bucket
        assert get_operation_name("HEAD", "/bucket", "") == "HeadBucket"
        # List objects
        assert get_operation_name("GET", "/bucket", "") == "ListObjects"

    def test_object_tagging(self):
        from s3proxy.metrics import get_operation_name

        assert get_operation_name("GET", "/bucket/key", "tagging") == "GetObjectTagging"
        assert get_operation_name("PUT", "/bucket/key", "tagging") == "PutObjectTagging"
        assert get_operation_name("DELETE", "/bucket/key", "tagging") == "DeleteObjectTagging"

    def test_standard_object_operations(self):
        from s3proxy.metrics import get_operation_name

        assert get_operation_name("GET", "/bucket/key", "") == "GetObject"
        assert get_operation_name("PUT", "/bucket/key", "") == "PutObject"
        assert get_operation_name("HEAD", "/bucket/key", "") == "HeadObject"
        assert get_operation_name("DELETE", "/bucket/key", "") == "DeleteObject"

    def test_unknown_operation(self):
        from s3proxy.metrics import get_operation_name

        assert get_operation_name("PATCH", "/bucket/key", "") == "Unknown"


class TestRequestMetrics:
    """Test request-related metrics."""

    def test_request_count_metric_exists(self):
        from s3proxy.metrics import REQUEST_COUNT

        # Verify metric is registered (prometheus_client stores base name without _total suffix)
        assert REQUEST_COUNT._name == "s3proxy_requests"
        assert "method" in REQUEST_COUNT._labelnames
        assert "operation" in REQUEST_COUNT._labelnames
        assert "status" in REQUEST_COUNT._labelnames

    def test_request_duration_metric_exists(self):
        from s3proxy.metrics import REQUEST_DURATION

        assert REQUEST_DURATION._name == "s3proxy_request_duration_seconds"
        assert "method" in REQUEST_DURATION._labelnames
        assert "operation" in REQUEST_DURATION._labelnames

    def test_requests_in_flight_metric_exists(self):
        from s3proxy.metrics import REQUESTS_IN_FLIGHT

        assert REQUESTS_IN_FLIGHT._name == "s3proxy_requests_in_flight"
        assert "method" in REQUESTS_IN_FLIGHT._labelnames


class TestMemoryMetrics:
    """Test memory-related metrics."""

    def test_memory_reserved_bytes_metric_exists(self):
        from s3proxy.metrics import MEMORY_RESERVED_BYTES

        assert MEMORY_RESERVED_BYTES._name == "s3proxy_memory_reserved_bytes"

    def test_memory_limit_bytes_metric_exists(self):
        from s3proxy.metrics import MEMORY_LIMIT_BYTES

        assert MEMORY_LIMIT_BYTES._name == "s3proxy_memory_limit_bytes"

    def test_memory_rejections_metric_exists(self):
        from s3proxy.metrics import MEMORY_REJECTIONS

        # prometheus_client stores base name without _total suffix
        assert MEMORY_REJECTIONS._name == "s3proxy_memory_rejections"


class TestEncryptionMetrics:
    """Test encryption-related metrics."""

    def test_encryption_operations_metric_exists(self):
        from s3proxy.metrics import ENCRYPTION_OPERATIONS

        # prometheus_client stores base name without _total suffix
        assert ENCRYPTION_OPERATIONS._name == "s3proxy_encryption_operations"
        assert "operation" in ENCRYPTION_OPERATIONS._labelnames

    def test_bytes_encrypted_metric_exists(self):
        from s3proxy.metrics import BYTES_ENCRYPTED

        # prometheus_client stores base name without _total suffix
        assert BYTES_ENCRYPTED._name == "s3proxy_bytes_encrypted"

    def test_bytes_decrypted_metric_exists(self):
        from s3proxy.metrics import BYTES_DECRYPTED

        # prometheus_client stores base name without _total suffix
        assert BYTES_DECRYPTED._name == "s3proxy_bytes_decrypted"


class TestCryptoMetricsIntegration:
    """Test that crypto operations update metrics."""

    @pytest.fixture(autouse=True)
    def reset_metrics(self):
        """Note: Prometheus metrics are cumulative and can't be easily reset.
        We test by checking that values increase."""
        yield

    def test_encrypt_updates_metrics(self):
        from s3proxy import crypto
        from s3proxy.metrics import BYTES_ENCRYPTED, ENCRYPTION_OPERATIONS

        # Get initial values
        initial_ops = ENCRYPTION_OPERATIONS.labels(operation="encrypt")._value.get()
        initial_bytes = BYTES_ENCRYPTED._value.get()

        # Perform encryption
        dek = crypto.generate_dek()
        plaintext = b"test data for metrics"
        crypto.encrypt(plaintext, dek)

        # Check metrics increased
        assert ENCRYPTION_OPERATIONS.labels(operation="encrypt")._value.get() > initial_ops
        assert BYTES_ENCRYPTED._value.get() >= initial_bytes + len(plaintext)

    def test_decrypt_updates_metrics(self):
        from s3proxy import crypto
        from s3proxy.metrics import BYTES_DECRYPTED, ENCRYPTION_OPERATIONS

        # Get initial values
        initial_ops = ENCRYPTION_OPERATIONS.labels(operation="decrypt")._value.get()
        initial_bytes = BYTES_DECRYPTED._value.get()

        # Perform encryption then decryption
        dek = crypto.generate_dek()
        plaintext = b"test data for metrics"
        ciphertext = crypto.encrypt(plaintext, dek)
        crypto.decrypt(ciphertext, dek)

        # Check metrics increased
        assert ENCRYPTION_OPERATIONS.labels(operation="decrypt")._value.get() > initial_ops
        assert BYTES_DECRYPTED._value.get() >= initial_bytes + len(plaintext)


class TestConcurrencyMetricsIntegration:
    """Test that concurrency operations update metrics."""

    @pytest.fixture(autouse=True)
    def reset_state(self):
        """Reset concurrency state before each test."""
        import s3proxy.concurrency as concurrency_module

        concurrency_module.reset_state()
        concurrency_module.set_memory_limit(64)
        yield
        concurrency_module.reset_state()

    @pytest.mark.asyncio
    async def test_memory_reservation_updates_metrics(self):
        import s3proxy.concurrency as concurrency_module
        from s3proxy.metrics import MEMORY_RESERVED_BYTES

        # Initial state
        assert MEMORY_RESERVED_BYTES._value.get() == 0

        # Reserve memory
        reserved = await concurrency_module.try_acquire_memory(1 * 1024 * 1024)
        assert MEMORY_RESERVED_BYTES._value.get() == reserved

        # Release memory
        await concurrency_module.release_memory(reserved)
        assert MEMORY_RESERVED_BYTES._value.get() == 0

    @pytest.mark.asyncio
    async def test_memory_limit_metric_set(self):
        import s3proxy.concurrency as concurrency_module
        from s3proxy.metrics import MEMORY_LIMIT_BYTES

        # Should be set to 64MB from fixture
        concurrency_module.set_memory_limit(64)
        assert MEMORY_LIMIT_BYTES._value.get() == 64 * 1024 * 1024

        # Change limit
        concurrency_module.set_memory_limit(128)
        assert MEMORY_LIMIT_BYTES._value.get() == 128 * 1024 * 1024

    @pytest.mark.asyncio
    async def test_memory_rejection_increments_counter(self):
        import s3proxy.concurrency as concurrency_module
        from s3proxy.errors import S3Error
        from s3proxy.metrics import MEMORY_REJECTIONS

        initial_rejections = MEMORY_REJECTIONS._value.get()

        # Fill up budget
        concurrency_module.set_active_memory(64 * 1024 * 1024)

        # Try to acquire more - should be rejected
        with pytest.raises(S3Error):
            await concurrency_module.try_acquire_memory(concurrency_module.MIN_RESERVATION)

        # Rejection counter should have increased
        assert MEMORY_REJECTIONS._value.get() == initial_rejections + 1


class TestMetricsEndpoint:
    """Test the /metrics endpoint."""

    @pytest.fixture
    def client(self):
        """Create test client for the app."""
        from fastapi.testclient import TestClient

        from s3proxy.app import create_app
        from s3proxy.config import Settings

        settings = Settings(
            host="http://localhost:9000",
            encrypt_key="test-encryption-key-32bytes!!!!",
            region="us-east-1",
            no_tls=True,
            port=4433,
        )
        app = create_app(settings)
        return TestClient(app)

    def test_metrics_endpoint_returns_prometheus_format(self, client):
        """Test that /metrics returns Prometheus text format."""
        response = client.get("/metrics")
        assert response.status_code == 200
        assert "text/plain" in response.headers["content-type"]

        # Check for expected metric names in output
        content = response.text
        assert "s3proxy_requests_total" in content
        assert "s3proxy_request_duration_seconds" in content
        assert "s3proxy_requests_in_flight" in content
        assert "s3proxy_memory_reserved_bytes" in content
        assert "s3proxy_memory_limit_bytes" in content
        assert "s3proxy_memory_rejections_total" in content
        assert "s3proxy_encryption_operations_total" in content
        assert "s3proxy_bytes_encrypted_total" in content
        assert "s3proxy_bytes_decrypted_total" in content

    def test_health_endpoints_still_work(self, client):
        """Ensure health endpoints are not affected."""
        assert client.get("/healthz").status_code == 200
        assert client.get("/readyz").status_code == 200
