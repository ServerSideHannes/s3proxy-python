"""Comprehensive tests for memory-based concurrency limiting.

These tests verify the memory-based concurrency limiting system that replaced
the count-based system. The key insight is that small files (e.g., ES metadata
at 733 bytes) should not be treated the same as large uploads (100MB+).

Memory estimation logic:
- PUT ≤8MB: content_length * 2 (body + ciphertext buffer)
- PUT >8MB: MAX_BUFFER_SIZE (8MB, streaming uses fixed buffer)
- GET: MAX_BUFFER_SIZE (8MB, streaming decryption)
- POST: MIN_RESERVATION (64KB, metadata only)
- HEAD/DELETE: 0 (no buffering, bypass limit)
"""

import asyncio
import os

import pytest

# Set the env var BEFORE importing the modules
os.environ["S3PROXY_MEMORY_LIMIT_MB"] = "64"


class TestMemoryFootprintEstimation:
    """Test the estimate_memory_footprint function."""

    @pytest.fixture(autouse=True)
    def reset_state(self):
        """Reset state before each test."""
        import s3proxy.concurrency as concurrency_module

        concurrency_module.reset_state()
        concurrency_module.set_memory_limit(64)
        yield
        concurrency_module.reset_state()

    def test_small_file_uses_content_length_x2(self):
        """PUT with 1KB file should reserve 2KB (content_length * 2)."""
        import s3proxy.concurrency as concurrency_module

        footprint = concurrency_module.estimate_memory_footprint("PUT", 1024)
        # 1KB * 2 = 2KB, but minimum is 64KB
        assert footprint == concurrency_module.MIN_RESERVATION

        # With 100KB file: 100KB * 2 = 200KB
        footprint = concurrency_module.estimate_memory_footprint("PUT", 100 * 1024)
        assert footprint == 200 * 1024

    def test_large_file_uses_fixed_buffer(self):
        """PUT with 100MB file should reserve 8MB (streaming fixed buffer)."""
        import s3proxy.concurrency as concurrency_module

        footprint = concurrency_module.estimate_memory_footprint("PUT", 100 * 1024 * 1024)
        assert footprint == concurrency_module.MAX_BUFFER_SIZE  # 8MB

    def test_minimum_reservation_enforced(self):
        """0-byte file should still reserve MIN_RESERVATION (64KB)."""
        import s3proxy.concurrency as concurrency_module

        footprint = concurrency_module.estimate_memory_footprint("PUT", 0)
        assert footprint == concurrency_module.MIN_RESERVATION

    def test_get_uses_fixed_buffer(self):
        """GET always reserves 8MB (streaming decryption buffer)."""
        import s3proxy.concurrency as concurrency_module

        footprint = concurrency_module.estimate_memory_footprint("GET", 0)
        assert footprint == concurrency_module.MAX_BUFFER_SIZE

    def test_head_delete_bypass(self):
        """HEAD and DELETE reserve 0 (no buffering, bypass limit)."""
        import s3proxy.concurrency as concurrency_module

        assert concurrency_module.estimate_memory_footprint("HEAD", 0) == 0
        assert concurrency_module.estimate_memory_footprint("DELETE", 0) == 0

    def test_post_uses_minimum(self):
        """POST (create multipart) uses MIN_RESERVATION (64KB)."""
        import s3proxy.concurrency as concurrency_module

        footprint = concurrency_module.estimate_memory_footprint("POST", 0)
        assert footprint == concurrency_module.MIN_RESERVATION


class TestMemoryBudgetManagement:
    """Test memory budget acquisition and release."""

    @pytest.fixture(autouse=True)
    def reset_state(self):
        """Reset state before each test."""
        import s3proxy.concurrency as concurrency_module

        concurrency_module.reset_state()
        concurrency_module.set_memory_limit(64)
        yield
        concurrency_module.reset_state()

    @pytest.mark.asyncio
    async def test_many_small_files_fit_in_budget(self):
        """64MB budget should fit thousands of small file requests."""
        import s3proxy.concurrency as concurrency_module

        # Each small file reserves MIN_RESERVATION (64KB)
        # 64MB / 64KB = 1024 small files should fit
        reservations = []
        for _ in range(1000):
            # Each reserves 64KB minimum
            reserved = await concurrency_module.try_acquire_memory(1024)  # 1KB file
            reservations.append(reserved)

        # Should have reserved 1000 * 64KB = 64000KB = ~62.5MB
        total_reserved = sum(reservations)
        assert total_reserved == 1000 * concurrency_module.MIN_RESERVATION

        # Clean up
        for r in reservations:
            await concurrency_module.release_memory(r)

        assert concurrency_module.get_active_memory() == 0

    @pytest.mark.asyncio
    async def test_budget_exhausted_rejects_request(self):
        """When at 64MB, next request should get 503 SlowDown."""
        import s3proxy.concurrency as concurrency_module
        from s3proxy.errors import S3Error

        # Fill up budget
        concurrency_module.set_active_memory(64 * 1024 * 1024)  # 64MB

        with pytest.raises(S3Error) as exc_info:
            await concurrency_module.try_acquire_memory(concurrency_module.MIN_RESERVATION)

        assert exc_info.value.status_code == 503
        assert exc_info.value.code == "SlowDown"

    @pytest.mark.asyncio
    async def test_memory_released_on_completion(self):
        """After request completes, memory should be freed."""
        import s3proxy.concurrency as concurrency_module

        reserved = await concurrency_module.try_acquire_memory(1 * 1024 * 1024)
        assert concurrency_module.get_active_memory() == 1 * 1024 * 1024

        await concurrency_module.release_memory(reserved)
        assert concurrency_module.get_active_memory() == 0

    @pytest.mark.asyncio
    async def test_single_request_cannot_exceed_budget(self):
        """A single 100MB request should be capped at the 64MB budget."""
        import s3proxy.concurrency as concurrency_module

        # Request 100MB, but should be capped at 64MB limit
        reserved = await concurrency_module.try_acquire_memory(100 * 1024 * 1024)

        # Should be capped at the total budget
        assert reserved == 64 * 1024 * 1024

        await concurrency_module.release_memory(reserved)
        assert concurrency_module.get_active_memory() == 0

    @pytest.mark.asyncio
    async def test_concurrent_requests_share_budget(self):
        """Multiple concurrent requests should share the 64MB pool."""
        import s3proxy.concurrency as concurrency_module

        # First request: 32MB
        reserved1 = await concurrency_module.try_acquire_memory(32 * 1024 * 1024)
        assert reserved1 == 32 * 1024 * 1024

        # Second request: 16MB
        reserved2 = await concurrency_module.try_acquire_memory(16 * 1024 * 1024)
        assert reserved2 == 16 * 1024 * 1024

        # Total: 48MB used
        assert concurrency_module.get_active_memory() == 48 * 1024 * 1024

        # Third request for 32MB should fail (48 + 32 = 80 > 64)
        from s3proxy.errors import S3Error

        with pytest.raises(S3Error) as exc_info:
            await concurrency_module.try_acquire_memory(32 * 1024 * 1024)

        assert exc_info.value.status_code == 503

        # But 16MB should succeed (48 + 16 = 64)
        reserved3 = await concurrency_module.try_acquire_memory(16 * 1024 * 1024)
        assert reserved3 == 16 * 1024 * 1024
        assert concurrency_module.get_active_memory() == 64 * 1024 * 1024

        # Clean up
        await concurrency_module.release_memory(reserved1)
        await concurrency_module.release_memory(reserved2)
        await concurrency_module.release_memory(reserved3)
        assert concurrency_module.get_active_memory() == 0

    @pytest.mark.asyncio
    async def test_disabled_when_limit_zero(self):
        """memory_limit_mb=0 should disable limiting entirely."""
        import s3proxy.concurrency as concurrency_module

        concurrency_module.set_memory_limit(0)

        # Should return 0 (no reservation tracked)
        reserved = await concurrency_module.try_acquire_memory(100 * 1024 * 1024)
        assert reserved == 0

        # Memory counter should remain 0
        assert concurrency_module.get_active_memory() == 0

        # Release should be a no-op
        await concurrency_module.release_memory(100 * 1024 * 1024)
        assert concurrency_module.get_active_memory() == 0


class TestRealWorldScenarios:
    """Test scenarios based on real-world usage patterns."""

    @pytest.fixture(autouse=True)
    def reset_state(self):
        """Reset state before each test."""
        import s3proxy.concurrency as concurrency_module

        concurrency_module.reset_state()
        concurrency_module.set_memory_limit(64)
        yield
        concurrency_module.reset_state()

    @pytest.mark.asyncio
    async def test_elasticsearch_shard_backup_scenario(self):
        """Simulate ES backup: many small metadata files + some data files.

        This is the original problem scenario: ES backup sends many 733-byte
        metadata files in parallel, which should not be rejected.
        """
        import s3proxy.concurrency as concurrency_module

        reservations = []

        # Simulate 50 parallel small metadata files (733 bytes each)
        for _ in range(50):
            footprint = concurrency_module.estimate_memory_footprint("PUT", 733)
            assert footprint == concurrency_module.MIN_RESERVATION  # 64KB each

            reserved = await concurrency_module.try_acquire_memory(footprint)
            reservations.append(reserved)

        # 50 * 64KB = 3.2MB used
        assert concurrency_module.get_active_memory() == 50 * concurrency_module.MIN_RESERVATION

        # Should still have plenty of room for more
        assert concurrency_module.get_active_memory() < 10 * 1024 * 1024  # < 10MB

        # Clean up
        for r in reservations:
            await concurrency_module.release_memory(r)

        assert concurrency_module.get_active_memory() == 0

    @pytest.mark.asyncio
    async def test_mixed_workload_scenario(self):
        """Simulate mixed workload: small files + streaming uploads."""
        import s3proxy.concurrency as concurrency_module
        from s3proxy.errors import S3Error

        reservations = []

        # 2 large streaming uploads (8MB each = 16MB)
        for _ in range(2):
            footprint = concurrency_module.estimate_memory_footprint("PUT", 100 * 1024 * 1024)
            assert footprint == 8 * 1024 * 1024  # Fixed streaming buffer
            reserved = await concurrency_module.try_acquire_memory(footprint)
            reservations.append(reserved)

        # 2 GET requests (8MB each = 16MB)
        for _ in range(2):
            footprint = concurrency_module.estimate_memory_footprint("GET", 0)
            assert footprint == 8 * 1024 * 1024
            reserved = await concurrency_module.try_acquire_memory(footprint)
            reservations.append(reserved)

        # Total: 32MB used, 32MB remaining
        assert concurrency_module.get_active_memory() == 32 * 1024 * 1024

        # Calculate how many small files fit in remaining 32MB budget
        # Each small file reserves MIN_RESERVATION (64KB = 65536 bytes)
        remaining_budget = 64 * 1024 * 1024 - 32 * 1024 * 1024  # 32MB
        files_that_fit = remaining_budget // concurrency_module.MIN_RESERVATION  # 512 files

        small_reservations = []
        for _ in range(files_that_fit):
            footprint = concurrency_module.estimate_memory_footprint("PUT", 1024)
            reserved = await concurrency_module.try_acquire_memory(footprint)
            small_reservations.append(reserved)

        # Now at 64MB (32MB large + 512 * 64KB = 32MB small)
        expected_total = 32 * 1024 * 1024 + files_that_fit * concurrency_module.MIN_RESERVATION
        assert concurrency_module.get_active_memory() == expected_total

        # Next request should fail
        with pytest.raises(S3Error):
            await concurrency_module.try_acquire_memory(concurrency_module.MIN_RESERVATION)

        # Clean up
        for r in reservations + small_reservations:
            await concurrency_module.release_memory(r)

        assert concurrency_module.get_active_memory() == 0

    def test_head_delete_bypass_via_zero_footprint(self):
        """HEAD and DELETE bypass limiting by returning 0 from estimate_memory_footprint.

        In main.py, when estimate_memory_footprint returns 0, the code doesn't call
        try_acquire_memory at all. HEAD/DELETE requests bypass the limiting mechanism
        entirely because they don't need memory buffers.
        """
        import s3proxy.concurrency as concurrency_module

        # HEAD should return 0 (bypass)
        head_footprint = concurrency_module.estimate_memory_footprint("HEAD", 0)
        assert head_footprint == 0

        # DELETE should return 0 (bypass)
        delete_footprint = concurrency_module.estimate_memory_footprint("DELETE", 0)
        assert delete_footprint == 0

        # These zero values signal to main.py not to call try_acquire_memory
        # This is how HEAD/DELETE bypass the memory limit even when exhausted

    @pytest.mark.asyncio
    async def test_release_on_exception(self):
        """Memory should be released even if request processing fails."""
        import s3proxy.concurrency as concurrency_module

        reserved = await concurrency_module.try_acquire_memory(10 * 1024 * 1024)
        assert concurrency_module.get_active_memory() == 10 * 1024 * 1024

        error_raised = False
        try:
            # Simulate processing that raises
            raise ValueError("Simulated error")
        except ValueError:
            error_raised = True
        finally:
            await concurrency_module.release_memory(reserved)

        assert error_raised, "Exception should have been raised"
        assert concurrency_module.get_active_memory() == 0


class TestThreadSafety:
    """Test concurrent access to memory tracking."""

    @pytest.fixture(autouse=True)
    def reset_state(self):
        """Reset state before each test."""
        import s3proxy.concurrency as concurrency_module

        concurrency_module.reset_state()
        concurrency_module.set_memory_limit(64)
        yield
        concurrency_module.reset_state()

    @pytest.mark.asyncio
    async def test_concurrent_acquire_release(self):
        """Multiple tasks acquiring/releasing concurrently should be safe."""
        import s3proxy.concurrency as concurrency_module

        async def worker(worker_id: int):
            for _ in range(10):
                reserved = await concurrency_module.try_acquire_memory(64 * 1024)
                await asyncio.sleep(0.001)  # Simulate work
                await concurrency_module.release_memory(reserved)

        # Run 10 concurrent workers
        await asyncio.gather(*[worker(i) for i in range(10)])

        # After all workers complete, memory should be 0
        assert concurrency_module.get_active_memory() == 0

    @pytest.mark.asyncio
    async def test_no_negative_memory(self):
        """Memory counter should never go negative even with buggy releases."""
        import s3proxy.concurrency as concurrency_module

        # Start at 0
        assert concurrency_module.get_active_memory() == 0

        # Release more than was ever acquired (simulating a bug)
        await concurrency_module.release_memory(100 * 1024 * 1024)

        # Should be 0, not negative
        assert concurrency_module.get_active_memory() == 0
