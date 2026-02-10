"""Unit tests for buffer size constants."""

from s3proxy import crypto


class TestBufferSizeConstants:
    """Test that buffer size constants are properly configured for memory safety."""

    def test_max_buffer_size_is_reasonable(self):
        """MAX_BUFFER_SIZE should be small enough for memory safety."""
        # 8MB is the cap per concurrent upload
        assert crypto.MAX_BUFFER_SIZE == 8 * 1024 * 1024
        assert crypto.MAX_BUFFER_SIZE < crypto.PART_SIZE

    def test_max_buffer_respects_min_part_size(self):
        """MAX_BUFFER_SIZE must be >= MIN_PART_SIZE to avoid EntityTooSmall."""
        # 8MB > 5MB, so we can always create valid S3 parts
        assert crypto.MAX_BUFFER_SIZE >= crypto.MIN_PART_SIZE

    def test_streaming_buffer_cap_calculation(self):
        """Verify memory math: 10 concurrent × 8MB = 80MB buffer space."""
        max_concurrent = 10  # Default throttle
        expected_max_buffer_memory = max_concurrent * crypto.MAX_BUFFER_SIZE

        # With 10 concurrent uploads at 8MB each = 80MB
        # This should fit comfortably in 512MB pod limit
        assert expected_max_buffer_memory == 80 * 1024 * 1024
        assert expected_max_buffer_memory < 512 * 1024 * 1024
