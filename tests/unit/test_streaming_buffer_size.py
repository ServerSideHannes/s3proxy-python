"""Test that streaming uploads use MAX_BUFFER_SIZE, not PART_SIZE.

This test verifies the fix for the OOM bug where streaming uploads
were buffering 64MB (PART_SIZE) instead of 8MB (MAX_BUFFER_SIZE).
"""

import ast
import inspect
import textwrap

from s3proxy import crypto
from s3proxy.handlers import objects


class TestStreamingBufferSize:
    """Verify streaming upload uses MAX_BUFFER_SIZE for memory safety."""

    def test_streaming_upload_uses_max_buffer_size_not_part_size(self):
        """
        The streaming upload code must use MAX_BUFFER_SIZE (8MB) as the buffer
        threshold, NOT PART_SIZE (64MB).

        With 10 concurrent uploads:
        - PART_SIZE (64MB): 10 × 64MB = 640MB → OOM in 512MB pod
        - MAX_BUFFER_SIZE (8MB): 10 × 8MB = 80MB → fits in 512MB pod

        This test inspects the source code to verify the fix is in place.
        """
        # Get the source code of the streaming upload function
        source = inspect.getsource(objects.ObjectHandlerMixin._put_streaming)
        source = textwrap.dedent(source)

        # Parse it into an AST
        tree = ast.parse(source)

        # Find all attribute accesses like crypto.SOMETHING
        buffer_checks = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Compare):
                # Looking for: len(buffer) >= crypto.MAX_BUFFER_SIZE
                for comparator in node.comparators:
                    if isinstance(comparator, ast.Attribute) and comparator.attr in (
                        "MAX_BUFFER_SIZE",
                        "PART_SIZE",
                    ):
                        buffer_checks.append(comparator.attr)

        # Verify MAX_BUFFER_SIZE is used, not PART_SIZE
        assert "MAX_BUFFER_SIZE" in buffer_checks, (
            f"Streaming upload must use crypto.MAX_BUFFER_SIZE for buffer threshold. "
            f"Found: {buffer_checks}. "
            f"Using PART_SIZE (64MB) causes OOM with 10 concurrent uploads in 512MB pods."
        )

        # Verify PART_SIZE is NOT used for buffer comparison
        # (PART_SIZE may appear elsewhere, but not in buffer size checks)
        assert "PART_SIZE" not in buffer_checks, (
            f"Streaming upload must NOT use crypto.PART_SIZE for buffer threshold. "
            f"Found buffer checks using: {buffer_checks}. "
            f"PART_SIZE (64MB) × 10 concurrent = 640MB > 512MB pod limit = OOM!"
        )

    def test_max_buffer_size_is_memory_safe(self):
        """Verify MAX_BUFFER_SIZE allows 10 concurrent uploads in 512MB."""
        max_concurrent = 10
        pod_memory_limit = 512 * 1024 * 1024  # 512MB
        python_overhead = 200 * 1024 * 1024  # ~200MB for Python + libs

        # Memory needed for buffers (plaintext + ciphertext)
        buffer_memory = max_concurrent * crypto.MAX_BUFFER_SIZE * 2

        total_memory = buffer_memory + python_overhead

        assert total_memory < pod_memory_limit, (
            f"MAX_BUFFER_SIZE ({crypto.MAX_BUFFER_SIZE // 1024 // 1024}MB) is too large! "
            f"10 concurrent × {crypto.MAX_BUFFER_SIZE // 1024 // 1024}MB × 2 buffers = "
            f"{buffer_memory // 1024 // 1024}MB + {python_overhead // 1024 // 1024}MB overhead = "
            f"{total_memory // 1024 // 1024}MB > {pod_memory_limit // 1024 // 1024}MB limit"
        )

    def test_part_size_would_cause_oom(self):
        """Verify PART_SIZE would cause OOM - proving the fix is necessary."""
        max_concurrent = 10
        pod_memory_limit = 512 * 1024 * 1024  # 512MB

        # If we used PART_SIZE (64MB) instead of MAX_BUFFER_SIZE (8MB)
        bad_buffer_memory = max_concurrent * crypto.PART_SIZE

        # This SHOULD exceed the limit (proving the bug)
        assert bad_buffer_memory > pod_memory_limit, (
            f"PART_SIZE should cause OOM: "
            f"10 × {crypto.PART_SIZE // 1024 // 1024}MB = "
            f"{bad_buffer_memory // 1024 // 1024}MB > {pod_memory_limit // 1024 // 1024}MB"
        )

    def test_download_streams_without_buffering(self):
        """
        Verify download code yields chunks immediately instead of buffering.

        The _get_multipart stream() function should yield each internal part
        as it's decrypted, not accumulate all parts in full_plaintext first.
        """
        source = inspect.getsource(objects.ObjectHandlerMixin._get_multipart)
        source = textwrap.dedent(source)

        # Check that we don't accumulate all data before yielding
        # Old code had: full_plaintext.extend(plaintext_chunk) then yield at end
        # New code should: yield plaintext_chunk[slice_start:slice_end] inside loop

        # The fix removes accumulation into full_plaintext and yields immediately
        assert "full_plaintext.extend" not in source, (
            "Download code should NOT accumulate all data in full_plaintext. "
            "This causes OOM with large files. Each chunk should be yielded immediately."
        )
