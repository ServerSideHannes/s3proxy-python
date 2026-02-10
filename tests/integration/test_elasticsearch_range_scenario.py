"""Test for the actual Elasticsearch backup scenario from logs.

This test reproduces the InvalidRange error that occurs when fetching Elasticsearch backups
where the metadata indicates larger ciphertext sizes than the actual S3 object.
"""

from unittest.mock import AsyncMock, Mock, patch

import pytest

from s3proxy import crypto
from s3proxy.errors import S3Error
from s3proxy.handlers.objects import ObjectHandlerMixin
from s3proxy.state import (
    InternalPartMetadata,
    MultipartMetadata,
    PartMetadata,
)


@pytest.fixture
def handler(settings, manager):
    """Create ObjectHandlerMixin instance for testing."""
    return ObjectHandlerMixin(settings, {}, manager)


class TestElasticsearchRangeScenario:
    """Test the actual scenario from Elasticsearch backup logs."""

    @pytest.mark.asyncio
    async def test_elasticsearch_backup_range_error(self, handler, settings):
        """
        Test scenario from logs: range bytes=53687203-70464446 fails.

        This represents a ~16.77MB chunk starting at offset ~53.68MB,
        but the actual object is smaller than 70.46MB.
        """
        mock_client = AsyncMock()
        # Make mock_client an async context manager
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)

        # Set object size to be smaller than what's needed for 4 parts
        # 4th part would need to start at 3 * 16777244 = 50331732
        # and end at 4 * 16777244 - 1 = 67108975
        # But we'll set size to only hold 3.2 parts, so 4th part fetch will fail validation
        actual_object_size = int(3.2 * 16777244)  # ~53.7MB

        mock_client.head_object = AsyncMock(
            return_value={"ContentLength": actual_object_size, "LastModified": None}
        )

        # Mock get_object to return empty body
        # Validation should catch the error before reading
        mock_body = AsyncMock()
        mock_body.read = AsyncMock(return_value=b"")
        mock_body.__aenter__ = AsyncMock(return_value=mock_body)
        mock_body.__aexit__ = AsyncMock(return_value=None)
        mock_client.get_object = AsyncMock(return_value={"Body": mock_body})

        # Create metadata that would cause the problematic range
        # If we have 3 internal parts of ~16.77MB each, and we're trying to fetch the 4th:
        # Part 1: 0 - 16777243 (0-16MB)
        # Part 2: 16777244 - 33554487 (16-32MB)
        # Part 3: 33554488 - 50331731 (32-48MB)
        # Part 4: 50331732 - 67108975 (48-64MB) <- This would be 53687203-70464446 range

        # But metadata incorrectly claims there are 4 parts when object only has ~3
        internal_parts = []
        for i in range(1, 5):  # 4 parts
            internal_parts.append(
                InternalPartMetadata(
                    internal_part_number=i,
                    plaintext_size=16 * 1024 * 1024,  # 16MB
                    ciphertext_size=16777244,  # 16MB + encryption overhead
                    etag=f"etag-{i}",
                )
            )

        # Generate DEK first so we can use it in metadata
        dek = crypto.generate_dek()

        part_meta = PartMetadata(
            part_number=1,
            plaintext_size=64 * 1024 * 1024,  # Claims 64MB plaintext
            ciphertext_size=67108976,  # Total ciphertext for 4 parts
            etag="combined-etag",
            md5="combined-md5",
            internal_parts=internal_parts,
        )

        meta = MultipartMetadata(
            version=1,
            part_count=1,
            total_plaintext_size=64 * 1024 * 1024,
            parts=[part_meta],
            wrapped_dek=crypto.wrap_key(dek, settings.kek),
        )

        with patch.object(handler, "_client", return_value=mock_client):
            mock_request = Mock()
            mock_request.url = Mock()
            mock_request.url.path = "/elasticsearch-backups/indices/test-index/0/__test-file"
            mock_request.headers = {}

            with patch(
                "s3proxy.handlers.objects.get.load_multipart_metadata",
                return_value=meta,
            ):
                creds = Mock()
                creds.access_key_id = "test-key"
                creds.secret_access_key = "test-secret"

                # Get response
                response = await handler.handle_get_object(mock_request, creds)

                # Error should be raised when consuming stream (trying to fetch 4th part)
                with pytest.raises(S3Error) as exc_info:
                    async for _ in response.body_iterator:
                        pass

                # Verify error message is helpful
                error_str = str(exc_info.value).lower()
                assert "metadata" in error_str
                assert "corruption" in error_str or "mismatch" in error_str, (
                    f"Expected 'corruption' or 'mismatch' in error: {exc_info.value}"
                )

                # Should mention the problematic part
                assert "internal part" in error_str, (
                    f"Expected 'internal part' in error: {exc_info.value}"
                )

    @pytest.mark.asyncio
    async def test_partial_object_with_3_of_4_parts(self, handler, settings):
        """
        Test when metadata claims 4 internal parts but only 3 were uploaded.

        This simulates an incomplete upload where the metadata was saved
        but the last internal part never made it to S3.
        """
        mock_client = AsyncMock()
        # Make mock_client an async context manager
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)

        # Generate DEK first
        dek = crypto.generate_dek()

        # Object has exactly 3 parts worth of data
        size_per_part = 16777244  # ~16MB + overhead
        actual_size = 3 * size_per_part  # Only 3 parts present

        mock_client.head_object = AsyncMock(
            return_value={"ContentLength": actual_size, "LastModified": None}
        )

        # Mock get_object to return valid ciphertext for parts 1-3, nothing for part 4
        def get_object_side_effect(bucket, key, range_header=None):
            if range_header:
                range_str = range_header.replace("bytes=", "")
                start, end = map(int, range_str.split("-"))

                # For parts 1-3 (within actual_size), return valid ciphertext
                if end < actual_size:
                    plaintext = b"x" * (16 * 1024 * 1024)
                    nonce = crypto.derive_part_nonce("test-upload", start // size_per_part + 1)
                    ciphertext = crypto.encrypt(plaintext, dek, nonce)

                    mock_body = AsyncMock()
                    mock_body.read = AsyncMock(return_value=ciphertext)
                    mock_body.__aenter__ = AsyncMock(return_value=mock_body)
                    mock_body.__aexit__ = AsyncMock(return_value=None)
                    return {"Body": mock_body}

            # Default: empty body (shouldn't be reached for part 4 due to validation)
            mock_body = AsyncMock()
            mock_body.read = AsyncMock(return_value=b"")
            mock_body.__aenter__ = AsyncMock(return_value=mock_body)
            mock_body.__aexit__ = AsyncMock(return_value=None)
            return {"Body": mock_body}

        mock_client.get_object = AsyncMock(side_effect=get_object_side_effect)

        # But metadata claims 4 parts
        internal_parts = [
            InternalPartMetadata(
                internal_part_number=i,
                plaintext_size=16 * 1024 * 1024,
                ciphertext_size=size_per_part,
                etag=f"etag-{i}",
            )
            for i in range(1, 5)  # 4 parts claimed
        ]

        part_meta = PartMetadata(
            part_number=1,
            plaintext_size=64 * 1024 * 1024,
            ciphertext_size=4 * size_per_part,
            etag="etag",
            md5="md5",
            internal_parts=internal_parts,
        )

        meta = MultipartMetadata(
            version=1,
            part_count=1,
            total_plaintext_size=64 * 1024 * 1024,
            parts=[part_meta],
            wrapped_dek=crypto.wrap_key(dek, settings.kek),
        )

        with patch.object(handler, "_client", return_value=mock_client):
            mock_request = Mock()
            mock_request.url = Mock()
            mock_request.url.path = "/test-bucket/incomplete-object"
            mock_request.headers = {}

            with patch(
                "s3proxy.handlers.objects.get.load_multipart_metadata",
                return_value=meta,
            ):
                creds = Mock()
                creds.access_key_id = "test-key"
                creds.secret_access_key = "test-secret"

                response = await handler.handle_get_object(mock_request, creds)

                # Should detect the mismatch when trying to fetch 4th part
                with pytest.raises(S3Error) as exc_info:
                    async for _ in response.body_iterator:
                        pass

                # Check that it mentions the specific problem
                error_str = str(exc_info.value).lower()
                assert "metadata" in error_str
                # Should specifically mention internal part 4
                assert "internal part 4" in error_str or "part 4" in str(exc_info.value), (
                    f"Expected 'part 4' in error: {exc_info.value}"
                )
                # Should mention the actual size limitation
                assert str(actual_size) in str(exc_info.value), (
                    f"Expected actual size {actual_size} in error: {exc_info.value}"
                )

    @pytest.mark.asyncio
    async def test_successful_3_part_fetch(self, handler, settings):
        """Test that fetching 3 complete parts works correctly."""
        mock_client = AsyncMock()
        # Make mock_client an async context manager
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)

        # Create proper test data
        dek = crypto.generate_dek()
        size_per_part = 1000  # Smaller for testing

        # Create 3 internal parts with actual encrypted data
        internal_parts_data = []
        total_ciphertext_size = 0

        for i in range(1, 4):
            plaintext = b"x" * size_per_part
            nonce = crypto.derive_part_nonce("test-upload", i)
            ciphertext = crypto.encrypt(plaintext, dek, nonce)

            internal_parts_data.append(
                {
                    "meta": InternalPartMetadata(
                        internal_part_number=i,
                        plaintext_size=len(plaintext),
                        ciphertext_size=len(ciphertext),
                        etag=f"etag-{i}",
                    ),
                    "ciphertext": ciphertext,
                }
            )
            total_ciphertext_size += len(ciphertext)

        mock_client.head_object = AsyncMock(
            return_value={"ContentLength": total_ciphertext_size, "LastModified": None}
        )

        # Mock get_object to return the correct ciphertext for each range
        def get_object_side_effect(bucket, key, range_header=None):
            if range_header:
                # Parse range to determine which part to return
                range_str = range_header.replace("bytes=", "")
                start, end = map(int, range_str.split("-"))

                # Find which internal part this range corresponds to
                current_offset = 0
                for part_data in internal_parts_data:
                    part_size = part_data["meta"].ciphertext_size
                    if start >= current_offset and start < current_offset + part_size:
                        # This is the right part
                        mock_body = AsyncMock()
                        mock_body.read = AsyncMock(return_value=part_data["ciphertext"])
                        mock_body.__aenter__ = AsyncMock(return_value=mock_body)
                        mock_body.__aexit__ = AsyncMock(return_value=None)
                        return {"Body": mock_body}
                    current_offset += part_size

            # Default mock
            mock_body = AsyncMock()
            mock_body.read = AsyncMock(return_value=b"")
            mock_body.__aenter__ = AsyncMock(return_value=mock_body)
            mock_body.__aexit__ = AsyncMock(return_value=None)
            return {"Body": mock_body}

        mock_client.get_object = AsyncMock(side_effect=get_object_side_effect)

        part_meta = PartMetadata(
            part_number=1,
            plaintext_size=3 * size_per_part,
            ciphertext_size=total_ciphertext_size,
            etag="etag",
            md5="md5",
            internal_parts=[p["meta"] for p in internal_parts_data],
        )

        meta = MultipartMetadata(
            version=1,
            part_count=1,
            total_plaintext_size=3 * size_per_part,
            parts=[part_meta],
            wrapped_dek=crypto.wrap_key(dek, settings.kek),
        )

        with patch.object(handler, "_client", return_value=mock_client):
            mock_request = Mock()
            mock_request.url = Mock()
            mock_request.url.path = "/test-bucket/good-object"
            mock_request.headers = {}

            with patch(
                "s3proxy.handlers.objects.get.load_multipart_metadata",
                return_value=meta,
            ):
                creds = Mock()
                creds.access_key_id = "test-key"
                creds.secret_access_key = "test-secret"

                # Should succeed without errors
                response = await handler.handle_get_object(mock_request, creds)

                # Consume the stream - should not raise
                chunks = []
                async for chunk in response.body_iterator:
                    chunks.append(chunk)

                # Verify we got data
                assert len(chunks) > 0
                full_response = b"".join(chunks)
                assert len(full_response) == 3 * size_per_part
