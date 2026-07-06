"""Comprehensive tests for memory-based concurrency limiting.

These tests verify the memory-based concurrency limiting system that replaced
the count-based system. The key insight is that small files (e.g., ES metadata
at 733 bytes) should not be treated the same as large uploads (100MB+).

Memory estimation logic:
- PUT: streaming_upload_peak(content_length) -- the framed upload path's true
  peak (accumulated ciphertext + encrypt transient + held frame + HTTP body
  copy), NOT the bare internal-part size. Reserving the part size under-counted
  ~3x and let the limiter admit too many concurrent uploads -> OOM.
- GET: MAX_BUFFER_SIZE (8MB baseline, handler acquires more for encrypted decrypts)
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

    def test_small_file_reserves_streaming_peak(self):
        """Tiny PUTs floor at MIN_RESERVATION; small ones reserve the framed peak."""
        import s3proxy.concurrency as concurrency_module
        from s3proxy import crypto

        # 1KB peak is tiny -> floored at MIN_RESERVATION (64KB)
        assert concurrency_module.estimate_memory_footprint("PUT", 1024) == (
            concurrency_module.MIN_RESERVATION
        )
        # 100KB part stays single-frame: peak = 2*part + 2*frame = 4*100KB
        footprint = concurrency_module.estimate_memory_footprint("PUT", 100 * 1024)
        assert footprint == crypto.streaming_upload_peak(100 * 1024) == 400 * 1024

    def test_large_file_reserves_framed_peak_not_part_size(self):
        """Large PUTs must reserve the framed path's true peak, NOT the bare
        internal-part size -- reserving the part size under-counted ~3x and let
        the limiter admit too many concurrent uploads (the OOM). The peak must
        strictly exceed the part size."""
        import s3proxy.concurrency as concurrency_module
        from s3proxy import crypto

        for mb in (50, 100, 512, 1024):
            cl = mb * 1024 * 1024
            footprint = concurrency_module.estimate_memory_footprint("PUT", cl)
            assert footprint == crypto.governor_memory_footprint(cl)
            if mb <= 512:
                assert footprint == crypto.streaming_upload_peak(cl)
            assert footprint >= crypto.memory_bounded_part_size(cl)

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
    async def test_single_request_larger_than_budget_clamps_to_routine_peak(self):
        """A request whose footprint exceeds the budget is clamped to the routine
        workload peak (512MB client part), not the whole budget — so concurrent
        normal-sized parts are not starved.
        """
        import s3proxy.concurrency as concurrency_module
        from s3proxy import crypto

        limit = concurrency_module.get_memory_limit()
        routine = crypto.streaming_upload_peak(crypto.STREAMING_GOVERNOR_CLIENT_PART_BYTES)
        reserved = await concurrency_module.try_acquire_memory(100 * 1024 * 1024)
        assert reserved == min(100 * 1024 * 1024, routine, limit)
        assert concurrency_module.get_active_memory() == reserved
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
        """Mixed workload at the deployed 64MB budget: the real 16MB ES snapshot
        uploads (framed) plus small metadata files, then full."""
        import s3proxy.concurrency as concurrency_module
        from s3proxy import crypto

        concurrency_module.set_memory_limit(64)  # deployed governor budget
        limit = 64 * 1024 * 1024
        reservations = []

        # 16MB ES part: honest reservation fits the 64MB budget with room for
        # concurrency (the under-count admitted ~8 -> OOM).
        es = concurrency_module.estimate_memory_footprint("PUT", 16 * 1024 * 1024)
        assert es == crypto.streaming_upload_peak(16 * 1024 * 1024)
        assert es <= limit // 2
        reservations.append(await concurrency_module.try_acquire_memory(es))

        used = es
        assert concurrency_module.get_active_memory() == used

        # Small metadata files share the remaining budget. The remaining 32MB fits
        # hundreds of MIN_RESERVATION files; acquire a handful to confirm they
        # coexist with the ES part (full saturation is covered elsewhere).
        remaining_budget = limit - used
        assert remaining_budget // concurrency_module.MIN_RESERVATION >= 8
        for _ in range(8):
            footprint = concurrency_module.estimate_memory_footprint("PUT", 1024)
            assert footprint == concurrency_module.MIN_RESERVATION
            reservations.append(await concurrency_module.try_acquire_memory(footprint))

        assert (
            concurrency_module.get_active_memory() == used + 8 * concurrency_module.MIN_RESERVATION
        )

        # Clean up
        for r in reservations:
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
