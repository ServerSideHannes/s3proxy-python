"""Tests for the memory-based concurrency limiting mechanism."""

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import Request

# We need to set the env var BEFORE importing the modules
os.environ["S3PROXY_MEMORY_LIMIT_MB"] = "64"


class TestMemoryBasedConcurrencyLimit:
    """Test the request concurrency limiting mechanism with memory-based limits."""

    @pytest.fixture(autouse=True)
    def reset_globals(self):
        """Reset global state before each test."""
        import s3proxy.concurrency as concurrency_module

        # Reset the global state
        concurrency_module.reset_state()
        # Set a known memory limit for tests (64MB)
        concurrency_module.set_memory_limit(64)
        yield
        # Cleanup after test
        concurrency_module.reset_state()

    @pytest.fixture
    def mock_request(self):
        """Create a mock request."""

        def _make_request(
            method: str,
            path: str = "/test-bucket/test-key",
            content_length: int = 0,
        ):
            request = MagicMock(spec=Request)
            request.method = method
            request.url = MagicMock()
            request.url.path = path
            request.url.query = ""
            request.headers = {"content-length": str(content_length)}
            request.scope = {"raw_path": path.encode()}
            return request

        return _make_request

    @pytest.mark.asyncio
    async def test_put_request_acquires_memory(self, mock_request):
        """PUT requests should acquire memory based on content-length."""
        import s3proxy.concurrency as concurrency_module
        import s3proxy.request_handler as request_handler_module

        request = mock_request("PUT", content_length=1024)

        # Mock the implementation to just return immediately
        with patch.object(
            request_handler_module, "_handle_proxy_request_impl", new_callable=AsyncMock
        ) as mock_impl:
            mock_impl.return_value = None

            await request_handler_module.handle_proxy_request(request, MagicMock(), MagicMock())

            # After request completes, memory should be released back to 0
            assert concurrency_module.get_active_memory() == 0

    @pytest.mark.asyncio
    async def test_head_request_bypasses_limit(self, mock_request):
        """HEAD requests should bypass the concurrency limit."""
        import s3proxy.concurrency as concurrency_module
        import s3proxy.request_handler as request_handler_module

        # Use most of the memory budget
        concurrency_module.set_active_memory(60 * 1024 * 1024)  # 60MB used

        request = mock_request("HEAD")

        with patch.object(
            request_handler_module, "_handle_proxy_request_impl", new_callable=AsyncMock
        ) as mock_impl:
            mock_impl.return_value = None

            # HEAD should succeed even when near capacity
            await request_handler_module.handle_proxy_request(request, MagicMock(), MagicMock())

            # Memory should not have been modified (HEAD bypasses)
            assert concurrency_module.get_active_memory() == 60 * 1024 * 1024

    @pytest.mark.asyncio
    async def test_get_request_acquires_memory(self, mock_request):
        """GET requests should acquire memory (fixed buffer size)."""
        import s3proxy.concurrency as concurrency_module
        import s3proxy.request_handler as request_handler_module

        request = mock_request("GET")

        with patch.object(
            request_handler_module, "_handle_proxy_request_impl", new_callable=AsyncMock
        ) as mock_impl:
            mock_impl.return_value = None

            await request_handler_module.handle_proxy_request(request, MagicMock(), MagicMock())

            # After request completes, memory should be released
            assert concurrency_module.get_active_memory() == 0

    @pytest.mark.asyncio
    async def test_get_rejected_when_memory_exhausted(self, mock_request):
        """GET requests should be rejected with 503 when memory is exhausted."""
        import s3proxy.concurrency as concurrency_module
        import s3proxy.request_handler as request_handler_module
        from s3proxy.errors import S3Error

        # Use up the memory budget (leave less than 8MB for GET)
        concurrency_module.set_active_memory(60 * 1024 * 1024)  # 60MB used

        request = mock_request("GET")

        with pytest.raises(S3Error) as exc_info:
            await request_handler_module.handle_proxy_request(request, MagicMock(), MagicMock())

        assert exc_info.value.status_code == 503
        assert exc_info.value.code == "SlowDown"
        # Memory should remain unchanged (request was rejected)
        assert concurrency_module.get_active_memory() == 60 * 1024 * 1024

    @pytest.mark.asyncio
    async def test_put_rejected_when_memory_exhausted(self, mock_request):
        """PUT requests should be rejected with 503 when memory is exhausted."""
        import s3proxy.concurrency as concurrency_module
        import s3proxy.request_handler as request_handler_module
        from s3proxy.errors import S3Error

        # Use up the memory budget
        concurrency_module.set_active_memory(63 * 1024 * 1024)  # 63MB used

        request = mock_request("PUT", content_length=2 * 1024 * 1024)  # 2MB file

        with pytest.raises(S3Error) as exc_info:
            await request_handler_module.handle_proxy_request(request, MagicMock(), MagicMock())

        assert exc_info.value.status_code == 503
        assert exc_info.value.code == "SlowDown"

    @pytest.mark.asyncio
    async def test_post_rejected_when_memory_exhausted(self, mock_request):
        """POST requests should be rejected with 503 when memory is exhausted."""
        import s3proxy.concurrency as concurrency_module
        import s3proxy.request_handler as request_handler_module
        from s3proxy.errors import S3Error

        # Use up the memory budget (leave less than 64KB minimum)
        concurrency_module.set_active_memory(64 * 1024 * 1024 - 32 * 1024)

        request = mock_request("POST")

        with pytest.raises(S3Error) as exc_info:
            await request_handler_module.handle_proxy_request(request, MagicMock(), MagicMock())

        assert exc_info.value.status_code == 503
        assert exc_info.value.code == "SlowDown"

    @pytest.mark.asyncio
    async def test_memory_released_on_error(self, mock_request):
        """Memory should be released even if request handler raises."""
        import s3proxy.concurrency as concurrency_module
        import s3proxy.request_handler as request_handler_module

        request = mock_request("PUT", content_length=1024)

        async def failing_handler(*args, **kwargs):
            raise ValueError("Something went wrong")

        with patch.object(
            request_handler_module, "_handle_proxy_request_impl", side_effect=failing_handler
        ):
            with pytest.raises(ValueError):
                await request_handler_module.handle_proxy_request(request, MagicMock(), MagicMock())

            # Memory should still be released
            assert concurrency_module.get_active_memory() == 0

    @pytest.mark.asyncio
    async def test_delete_bypasses_limit(self, mock_request):
        """DELETE requests should bypass the concurrency limit."""
        import s3proxy.concurrency as concurrency_module
        import s3proxy.request_handler as request_handler_module

        # Use most of the memory budget
        concurrency_module.set_active_memory(60 * 1024 * 1024)

        request = mock_request("DELETE")

        with patch.object(
            request_handler_module, "_handle_proxy_request_impl", new_callable=AsyncMock
        ) as mock_impl:
            mock_impl.return_value = None

            # DELETE should succeed even when near capacity
            await request_handler_module.handle_proxy_request(request, MagicMock(), MagicMock())

            # Memory should not have been modified
            assert concurrency_module.get_active_memory() == 60 * 1024 * 1024


class TestConcurrencyLimitDisabled:
    """Test behavior when concurrency limit is disabled."""

    @pytest.fixture(autouse=True)
    def disable_limit(self):
        """Disable the concurrency limit."""
        import s3proxy.concurrency as concurrency_module

        concurrency_module.reset_state()
        concurrency_module.set_memory_limit(0)  # Disable limiting
        yield
        concurrency_module.reset_state()

    @pytest.fixture
    def mock_request(self):
        def _make_request(
            method: str,
            path: str = "/test-bucket/test-key",
            content_length: int = 0,
        ):
            request = MagicMock(spec=Request)
            request.method = method
            request.url = MagicMock()
            request.url.path = path
            request.url.query = ""
            request.headers = {"content-length": str(content_length)}
            request.scope = {"raw_path": path.encode()}
            return request

        return _make_request

    @pytest.mark.asyncio
    async def test_no_limit_when_disabled(self, mock_request):
        """When limit is 0, all requests should pass through."""
        import s3proxy.concurrency as concurrency_module
        import s3proxy.request_handler as request_handler_module

        request = mock_request("PUT", content_length=100 * 1024 * 1024)  # 100MB

        with patch.object(
            request_handler_module, "_handle_proxy_request_impl", new_callable=AsyncMock
        ) as mock_impl:
            mock_impl.return_value = None

            # Should succeed without any limiting
            await request_handler_module.handle_proxy_request(request, MagicMock(), MagicMock())

            # Memory should remain 0 (not used when disabled)
            assert concurrency_module.get_active_memory() == 0


class TestMemoryConcurrencyModule:
    """Test the concurrency module directly."""

    @pytest.fixture(autouse=True)
    def reset_state(self):
        """Reset state before each test."""
        import s3proxy.concurrency as concurrency_module

        concurrency_module.reset_state()
        # Set a known memory limit for tests (64MB)
        concurrency_module.set_memory_limit(64)
        yield
        concurrency_module.reset_state()

    @pytest.mark.asyncio
    async def test_try_acquire_memory_success(self):
        """Should acquire memory when under limit."""
        import s3proxy.concurrency as concurrency_module

        # Request 1MB
        reserved = await concurrency_module.try_acquire_memory(1 * 1024 * 1024)
        assert reserved == 1 * 1024 * 1024
        assert concurrency_module.get_active_memory() == 1 * 1024 * 1024

    @pytest.mark.asyncio
    async def test_try_acquire_memory_enforces_minimum(self):
        """Should enforce minimum reservation for small requests."""
        import s3proxy.concurrency as concurrency_module

        # Request 100 bytes, should get MIN_RESERVATION (64KB)
        reserved = await concurrency_module.try_acquire_memory(100)
        assert reserved == concurrency_module.MIN_RESERVATION
        assert concurrency_module.get_active_memory() == concurrency_module.MIN_RESERVATION

    @pytest.mark.asyncio
    async def test_try_acquire_memory_at_capacity(self):
        """Should raise S3Error when memory is exhausted."""
        import s3proxy.concurrency as concurrency_module
        from s3proxy.errors import S3Error

        # Fill to near capacity
        concurrency_module.set_active_memory(63 * 1024 * 1024)

        with pytest.raises(S3Error) as exc_info:
            await concurrency_module.try_acquire_memory(8 * 1024 * 1024)  # 8MB

        assert exc_info.value.status_code == 503
        assert exc_info.value.code == "SlowDown"

    @pytest.mark.asyncio
    async def test_release_memory(self):
        """Should decrement memory on release."""
        import s3proxy.concurrency as concurrency_module

        concurrency_module.set_active_memory(10 * 1024 * 1024)
        await concurrency_module.release_memory(5 * 1024 * 1024)
        assert concurrency_module.get_active_memory() == 5 * 1024 * 1024

    @pytest.mark.asyncio
    async def test_release_memory_never_negative(self):
        """Memory counter should never go negative."""
        import s3proxy.concurrency as concurrency_module

        concurrency_module.set_active_memory(0)
        await concurrency_module.release_memory(1 * 1024 * 1024)
        assert concurrency_module.get_active_memory() == 0

    def test_estimate_memory_footprint_put_small(self):
        """PUT with small file should use content_length * 2."""
        import s3proxy.concurrency as concurrency_module

        # 1MB file → 2MB footprint
        footprint = concurrency_module.estimate_memory_footprint("PUT", 1 * 1024 * 1024)
        assert footprint == 2 * 1024 * 1024

    def test_estimate_memory_footprint_put_large(self):
        """PUT with large file should use 2x buffer size (buffer + ciphertext)."""
        import s3proxy.concurrency as concurrency_module

        # 100MB file → 16MB footprint (8MB buffer + 8MB ciphertext simultaneously)
        footprint = concurrency_module.estimate_memory_footprint("PUT", 100 * 1024 * 1024)
        assert footprint == concurrency_module.MAX_BUFFER_SIZE * 2

    def test_estimate_memory_footprint_get(self):
        """GET should always use fixed buffer size."""
        import s3proxy.concurrency as concurrency_module

        footprint = concurrency_module.estimate_memory_footprint("GET", 0)
        assert footprint == concurrency_module.MAX_BUFFER_SIZE

    def test_estimate_memory_footprint_head(self):
        """HEAD should return 0 (bypass)."""
        import s3proxy.concurrency as concurrency_module

        footprint = concurrency_module.estimate_memory_footprint("HEAD", 0)
        assert footprint == 0

    def test_estimate_memory_footprint_delete(self):
        """DELETE should return 0 (bypass)."""
        import s3proxy.concurrency as concurrency_module

        footprint = concurrency_module.estimate_memory_footprint("DELETE", 0)
        assert footprint == 0

    def test_estimate_memory_footprint_post(self):
        """POST should use minimum reservation."""
        import s3proxy.concurrency as concurrency_module

        footprint = concurrency_module.estimate_memory_footprint("POST", 0)
        assert footprint == concurrency_module.MIN_RESERVATION
