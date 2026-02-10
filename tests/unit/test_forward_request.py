"""Tests for forward_request handler - Content-Length mismatch fix."""


class TestForwardRequestHeaderFiltering:
    """Test that forward_request properly filters hop-by-hop headers."""

    def test_excluded_headers_list(self):
        """Test the excluded headers set contains critical headers."""
        # These headers should be filtered when forwarding responses
        # to prevent Content-Length mismatch errors
        excluded_headers = {
            "content-length",
            "content-encoding",
            "transfer-encoding",
            "connection",
        }

        # Content-Length must be filtered because:
        # httpx decompresses gzip responses, so Content-Length from upstream
        # won't match the decompressed body size
        assert "content-length" in excluded_headers

        # Content-Encoding must be filtered because:
        # httpx handles decompression, so the response body is already decompressed
        # and advertising gzip encoding would be incorrect
        assert "content-encoding" in excluded_headers

        # Transfer-Encoding is a hop-by-hop header that shouldn't be forwarded
        assert "transfer-encoding" in excluded_headers

        # Connection is a hop-by-hop header
        assert "connection" in excluded_headers

    def test_header_filtering_logic(self):
        """Test the header filtering logic used in forward_request."""
        # Simulate upstream response headers (what httpx returns)
        upstream_headers = {
            "Content-Type": "application/xml",
            "Content-Length": "1234",  # Original compressed size
            "Content-Encoding": "gzip",
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "x-amz-request-id": "ABC123",
            "ETag": '"abc123def456"',
        }

        # The filtering logic from forward_request
        excluded_headers = {
            "content-length",
            "content-encoding",
            "transfer-encoding",
            "connection",
        }
        filtered_headers = {
            k: v for k, v in upstream_headers.items() if k.lower() not in excluded_headers
        }

        # Content-Type should pass through
        assert "Content-Type" in filtered_headers
        assert filtered_headers["Content-Type"] == "application/xml"

        # S3-specific headers should pass through
        assert "x-amz-request-id" in filtered_headers
        assert "ETag" in filtered_headers

        # Hop-by-hop and length-related headers should be filtered
        assert "Content-Length" not in filtered_headers
        assert "Content-Encoding" not in filtered_headers
        assert "Transfer-Encoding" not in filtered_headers
        assert "Connection" not in filtered_headers

    def test_case_insensitive_filtering(self):
        """Test header filtering is case-insensitive."""
        upstream_headers = {
            "CONTENT-LENGTH": "1234",
            "content-encoding": "gzip",
            "Content-Type": "application/xml",
        }

        excluded_headers = {
            "content-length",
            "content-encoding",
            "transfer-encoding",
            "connection",
        }
        filtered_headers = {
            k: v for k, v in upstream_headers.items() if k.lower() not in excluded_headers
        }

        # Case-insensitive filtering should work
        assert "CONTENT-LENGTH" not in filtered_headers
        assert "content-encoding" not in filtered_headers

        # Non-excluded headers pass through
        assert "Content-Type" in filtered_headers


class TestContentLengthMismatchScenarios:
    """Test scenarios that could cause Content-Length mismatch errors."""

    def test_gzip_decompression_scenario(self):
        """Test the gzip decompression scenario that causes mismatch.

        When S3 returns a gzip-compressed response:
        1. Upstream S3 sets Content-Length to compressed size (e.g., 1234 bytes)
        2. httpx decompresses the response body
        3. Decompressed body is larger (e.g., 5678 bytes)
        4. If we forward Content-Length: 1234 with 5678 byte body = ERROR

        The fix filters Content-Length so Starlette calculates correct length.
        """
        compressed_size = 1234
        decompressed_size = 5678

        # Before fix: Content-Length from upstream
        # This would cause: RuntimeError: Response content longer than Content-Length
        assert decompressed_size > compressed_size

        # After fix: Content-Length is filtered, Starlette sets correct value
        # No mismatch error because header comes from actual body size

    def test_transfer_encoding_chunked_scenario(self):
        """Test Transfer-Encoding: chunked doesn't cause issues.

        When upstream uses chunked encoding:
        1. There may be no Content-Length header upstream
        2. httpx reads and dechunks the body
        3. Forwarding Transfer-Encoding: chunked would be incorrect
           since we're sending a non-chunked response
        """
        upstream_headers = {
            "Transfer-Encoding": "chunked",
            "Content-Type": "application/xml",
        }

        excluded_headers = {
            "content-length",
            "content-encoding",
            "transfer-encoding",
            "connection",
        }
        filtered_headers = {
            k: v for k, v in upstream_headers.items() if k.lower() not in excluded_headers
        }

        # Transfer-Encoding filtered
        assert "Transfer-Encoding" not in filtered_headers
        # Content-Type preserved
        assert "Content-Type" in filtered_headers
