"""Tests for calculate_optimal_part_size - the general solution to EntityTooSmall."""

import pytest

from s3proxy import crypto


class TestOptimalPartSize:
    """Test calculate_optimal_part_size for various content sizes."""

    def test_small_content_no_split(self):
        """Content smaller than MAX_BUFFER_SIZE should not be split."""
        # 5MB content <= 8MB MAX_BUFFER_SIZE → use 5MB (no split)
        size = crypto.calculate_optimal_part_size(5 * 1024 * 1024)
        assert size == 5 * 1024 * 1024
        # Result: [5MB] ✓

    def test_elasticsearch_50mb_splits_for_memory(self):
        """Elasticsearch typical 50MB parts split for memory management."""
        # 50MB > 8MB MAX_BUFFER_SIZE → splits into ~7 parts to limit memory
        size = crypto.calculate_optimal_part_size(50 * 1024 * 1024)
        # Should be close to MAX_BUFFER_SIZE but ensuring >= MIN_PART_SIZE
        assert size >= crypto.MIN_PART_SIZE
        assert size <= 50 * 1024 * 1024
        # Result: ~7-8MB parts ✓

    def test_elasticsearch_60mb_splits_for_memory(self):
        """Elasticsearch typical 60MB parts split for memory management."""
        # 60MB > 8MB MAX_BUFFER_SIZE → splits into ~8 parts to limit memory
        size = crypto.calculate_optimal_part_size(60 * 1024 * 1024)
        # Should be close to MAX_BUFFER_SIZE but ensuring >= MIN_PART_SIZE
        assert size >= crypto.MIN_PART_SIZE
        assert size <= 60 * 1024 * 1024
        # Result: ~7-8MB parts ✓

    def test_100mb_good_remainder(self):
        """100MB with default size gives good remainder."""
        # 100MB / 64MB = 1 remainder 36MB
        # 36MB > 5MB → OK to use default size
        size = crypto.calculate_optimal_part_size(100 * 1024 * 1024)
        assert size == 64 * 1024 * 1024
        # Result: [64MB, 36MB] ✓

    def test_130mb_small_remainder_adjusted(self):
        """130MB would create 2MB remainder - should be adjusted."""
        # 130MB / 64MB = 2 remainder 2MB
        # 2MB < 5MB → BAD, need to adjust
        # Optimal: distribute evenly across 2 parts → 65MB each
        size = crypto.calculate_optimal_part_size(130 * 1024 * 1024)

        # Calculate what the split would look like
        num_parts = (130 * 1024 * 1024 + size - 1) // size
        remainder = 130 * 1024 * 1024 % size

        # Should create 2 parts, each >= 5MB
        assert num_parts == 2
        # Both parts should be >= 5MB
        assert size >= 5 * 1024 * 1024
        if remainder > 0:
            assert remainder >= 5 * 1024 * 1024
        # Result: [65MB, 65MB] ✓

    def test_200mb_no_adjustment_needed(self):
        """200MB with default size gives good splits."""
        # 200MB / 64MB = 3 remainder 8MB
        # 8MB > 5MB → OK to use default size
        size = crypto.calculate_optimal_part_size(200 * 1024 * 1024)
        assert size == 64 * 1024 * 1024
        # Result: [64MB, 64MB, 64MB, 8MB] ✓

    def test_production_shard_0_293mb(self):
        """Test with actual production size from shard 0: 293134606 bytes."""
        # This was one of the failing shards in production
        size = crypto.calculate_optimal_part_size(293134606)

        # Calculate split
        num_parts = (293134606 + size - 1) // size
        remainder = 293134606 % size

        # All parts except last must be >= 5MB
        assert size >= 5 * 1024 * 1024
        if remainder > 0:
            # If there's a remainder, it should be the last part
            # which is allowed to be any size
            pass
        # Ensure we don't create too many small parts
        assert num_parts <= 10  # Reasonable number of parts

    def test_production_shard_2_422mb(self):
        """Test with actual production size from shard 2: 422883533 bytes."""
        size = crypto.calculate_optimal_part_size(422883533)

        num_parts = (422883533 + size - 1) // size
        422883533 % size

        # All parts except last must be >= 5MB
        assert size >= 5 * 1024 * 1024
        # Last part can be any size
        assert num_parts <= 10

    def test_67mb_creates_3mb_remainder_adjusted(self):
        """67MB / 64MB = 1 remainder 3MB - should be adjusted."""
        # 67MB / 64MB = 1 remainder 3MB
        # 3MB < 5MB → BAD if not the final part
        size = crypto.calculate_optimal_part_size(67 * 1024 * 1024)

        num_parts = (67 * 1024 * 1024 + size - 1) // size
        remainder = 67 * 1024 * 1024 % size

        # Should adjust to avoid 3MB remainder
        # Optimal: 67MB as single part or split evenly
        if num_parts > 1 and remainder > 0:
            # If split, remainder should be >= 5MB
            assert remainder >= 5 * 1024 * 1024

    def test_500mb_large_content(self):
        """Large 500MB content should split into reasonable chunks."""
        size = crypto.calculate_optimal_part_size(500 * 1024 * 1024)

        num_parts = (500 * 1024 * 1024 + size - 1) // size
        500 * 1024 * 1024 % size

        # Should use default size or something reasonable
        assert size >= 5 * 1024 * 1024
        assert num_parts >= 7  # At least 7 parts for 500MB with 64MB chunks
        assert num_parts <= 100  # But not too many

        # Last part (if exists) can be any size, but non-final parts must be >= 5MB
        assert size >= 5 * 1024 * 1024

    def test_edge_case_exactly_64mb(self):
        """Exactly 64MB splits for memory management."""
        # 64MB > 8MB MAX_BUFFER_SIZE → splits into ~8 parts to limit memory
        size = crypto.calculate_optimal_part_size(64 * 1024 * 1024)
        # Should be close to MAX_BUFFER_SIZE but ensuring >= MIN_PART_SIZE
        assert size >= crypto.MIN_PART_SIZE
        assert size <= 64 * 1024 * 1024
        # Result: ~8MB parts ✓

    def test_edge_case_64mb_plus_1mb(self):
        """65MB should split reasonably."""
        size = crypto.calculate_optimal_part_size(65 * 1024 * 1024)

        num_parts = (65 * 1024 * 1024 + size - 1) // size
        65 * 1024 * 1024 % size

        # 65MB / 64MB = 1 remainder 1MB
        # 1MB < 5MB → should adjust
        if num_parts > 1:
            # If split, all parts should be reasonable
            assert size >= 5 * 1024 * 1024

    def test_minimum_size_5mb(self):
        """5MB content should use 5MB."""
        size = crypto.calculate_optimal_part_size(5 * 1024 * 1024)
        assert size == 5 * 1024 * 1024
        # Result: [5MB] ✓

    def test_very_small_1mb(self):
        """1MB content should use 1MB."""
        size = crypto.calculate_optimal_part_size(1 * 1024 * 1024)
        assert size == 1 * 1024 * 1024
        # Result: [1MB] ✓ (last part can be any size)

    @pytest.mark.parametrize(
        "content_size,description",
        [
            (10 * 1024 * 1024, "10MB"),
            (25 * 1024 * 1024, "25MB"),
            (50 * 1024 * 1024, "50MB (Elasticsearch typical)"),
            (60 * 1024 * 1024, "60MB (Elasticsearch typical)"),
            (64 * 1024 * 1024, "64MB (boundary)"),
            (65 * 1024 * 1024, "65MB"),
            (67 * 1024 * 1024, "67MB (3MB remainder)"),
            (100 * 1024 * 1024, "100MB"),
            (130 * 1024 * 1024, "130MB (2MB remainder)"),
            (200 * 1024 * 1024, "200MB"),
            (293134606, "293MB (production shard 0)"),
            (301342926, "301MB (production shard 4)"),
            (305309959, "305MB (production shard 1)"),
            (305654456, "305MB (production shard 3)"),
            (422883533, "422MB (production shard 2)"),
        ],
    )
    def test_no_entity_too_small_violations(self, content_size, description):
        """
        Verify that calculate_optimal_part_size never creates EntityTooSmall violations.

        For ANY content size, all parts except the last must be >= 5MB.
        """
        optimal_size = crypto.calculate_optimal_part_size(content_size)

        # Calculate how many parts we'll create
        num_parts = (content_size + optimal_size - 1) // optimal_size
        remainder = content_size % optimal_size

        # All full chunks must be >= 5MB
        assert optimal_size >= crypto.MIN_PART_SIZE, (
            f"{description}: optimal_size {optimal_size / 1024 / 1024:.1f}MB < 5MB"
        )

        # If remainder is 0, all parts are full-sized and OK
        # If remainder > 0, it's the last part and can be any size
        # But if we have multiple parts and remainder < 5MB, it means we're creating
        # a small non-final part somewhere, which is BAD

        if num_parts > 1 and remainder > 0:
            # The remainder is the last part, which is allowed to be < 5MB
            # But we need to ensure we didn't create small parts in the middle

            # Calculate actual sizes of all parts
            parts = []
            remaining = content_size
            while remaining > 0:
                chunk_size = min(optimal_size, remaining)
                parts.append(chunk_size)
                remaining -= chunk_size

            # All parts except the last must be >= 5MB
            for i, part_size in enumerate(parts[:-1]):  # All except last
                assert part_size >= crypto.MIN_PART_SIZE, (
                    f"{description}: Part {i + 1}/{len(parts)} "
                    f"is {part_size / 1024 / 1024:.1f}MB < 5MB. "
                    f"This would cause EntityTooSmall! "
                    f"Parts: {[p // 1024 // 1024 for p in parts]}MB"
                )

        # Success! This configuration won't trigger EntityTooSmall
