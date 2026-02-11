"""Tests for multipart download range validation with internal parts."""

from unittest.mock import AsyncMock, Mock, patch

import pytest
from botocore.exceptions import ClientError

from s3proxy import crypto
from s3proxy.errors import S3Error
from s3proxy.handlers.objects import ObjectHandlerMixin
from s3proxy.state import InternalPartMetadata, MultipartMetadata, PartMetadata


@pytest.fixture
def mock_s3_client():
    """Create mock S3 client."""
    client = AsyncMock()
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=None)
    client.head_object = AsyncMock(return_value={"ContentLength": 50000000})
    return client


@pytest.fixture
def handler(settings, manager):
    """Create ObjectHandlerMixin instance for testing."""
    return ObjectHandlerMixin(settings, {}, manager)


class TestMultipartRangeValidation:
    """Test range validation for multipart downloads with internal parts."""

    @pytest.mark.asyncio
    async def test_invalid_range_detected_before_fetch(self, handler, settings, mock_s3_client):
        """Test that invalid ranges are detected before making S3 requests."""
        # Create metadata with internal parts that exceed actual object size
        internal_parts = [
            InternalPartMetadata(
                internal_part_number=1,
                plaintext_size=16 * 1024 * 1024,  # 16MB
                ciphertext_size=16 * 1024 * 1024 + 28,  # 16MB + overhead
                etag="etag1",
            ),
            InternalPartMetadata(
                internal_part_number=2,
                plaintext_size=16 * 1024 * 1024,  # 16MB
                ciphertext_size=16 * 1024 * 1024 + 28,  # 16MB + overhead
                etag="etag2",
            ),
            InternalPartMetadata(
                internal_part_number=3,
                plaintext_size=16 * 1024 * 1024,  # 16MB
                ciphertext_size=16 * 1024 * 1024 + 28,  # 16MB + overhead
                etag="etag3",
            ),
        ]

        part_meta = PartMetadata(
            part_number=1,
            plaintext_size=48 * 1024 * 1024,  # 48MB total plaintext
            ciphertext_size=48 * 1024 * 1024 + 84,  # Total ciphertext
            etag="part-etag",
            md5="md5-hash",
            internal_parts=internal_parts,
        )

        meta = MultipartMetadata(
            version=1,
            part_count=1,
            total_plaintext_size=48 * 1024 * 1024,
            parts=[part_meta],
            wrapped_dek=crypto.wrap_key(crypto.generate_dek(), settings.kek),
        )

        # Mock head_object to return a size smaller than what metadata expects
        mock_s3_client.head_object = AsyncMock(
            return_value={"ContentLength": 20 * 1024 * 1024}  # Only 20MB actual size
        )

        # Mock get_object to return empty body for any requests that might occur
        mock_body = AsyncMock()
        mock_body.read = AsyncMock(return_value=b"")
        mock_body.__aenter__ = AsyncMock(return_value=mock_body)
        mock_body.__aexit__ = AsyncMock(return_value=None)
        mock_s3_client.get_object = AsyncMock(return_value={"Body": mock_body})

        # Mock the client method
        with patch.object(handler, "_client", return_value=mock_s3_client):
            # Create a mock request
            mock_request = Mock()
            mock_request.url = Mock()
            mock_request.url.path = "/test-bucket/test-key"
            mock_request.headers = {}

            # Mock load_multipart_metadata to return our test metadata
            with patch(
                "s3proxy.handlers.objects.get.load_multipart_metadata",
                return_value=meta,
            ):
                # Mock credentials
                creds = Mock()
                creds.access_key_id = "test-key"
                creds.secret_access_key = "test-secret"

                # Attempt to get the object - returns streaming response
                response = await handler.handle_get_object(mock_request, creds)

                # Error should be raised when consuming the stream
                with pytest.raises(S3Error) as exc_info:
                    # Consume the streaming response body
                    async for _ in response.body_iterator:
                        pass

                # Verify error message contains helpful information
                assert "metadata" in str(exc_info.value).lower()
                assert (
                    "corruption" in str(exc_info.value).lower()
                    or "mismatch" in str(exc_info.value).lower()
                )

                # Verify that get_object was never called with invalid range
                # (it might be called for the first part that fits)
                for call in mock_s3_client.get_object.call_args_list:
                    args = call[0]
                    if len(args) >= 3:
                        range_str = args[2]
                        if "bytes=" in range_str:
                            # Extract end byte from range
                            range_part = range_str.replace("bytes=", "")
                            start, end = map(int, range_part.split("-"))
                            # Verify we didn't request beyond actual size
                            assert end < 20 * 1024 * 1024, (
                                f"Requested range {range_str} exceeds object size"
                            )

    @pytest.mark.asyncio
    async def test_handles_s3_invalid_range_error(self, handler, settings):
        """Test that S3 InvalidRange errors are caught and wrapped properly."""
        # Create a mock S3 client that raises InvalidRange
        mock_client = AsyncMock()
        # Make mock_client an async context manager
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)

        # Create the botocore ClientError for InvalidRange
        error_response = {
            "Error": {
                "Code": "InvalidRange",
                "Message": "The requested range is not satisfiable",
            }
        }
        invalid_range_error = ClientError(error_response, "GetObject")

        mock_client.head_object = AsyncMock(
            return_value={"ContentLength": 100, "LastModified": None}
        )
        mock_client.get_object = AsyncMock(side_effect=invalid_range_error)

        # Create test metadata with internal parts
        internal_parts = [
            InternalPartMetadata(
                internal_part_number=1,
                plaintext_size=1000,
                ciphertext_size=1028,
                etag="etag1",
            ),
        ]

        part_meta = PartMetadata(
            part_number=1,
            plaintext_size=1000,
            ciphertext_size=1028,
            etag="etag",
            md5="md5",
            internal_parts=internal_parts,
        )

        meta = MultipartMetadata(
            version=1,
            part_count=1,
            total_plaintext_size=1000,
            parts=[part_meta],
            wrapped_dek=crypto.wrap_key(crypto.generate_dek(), settings.kek),
        )

        with patch.object(handler, "_client", return_value=mock_client):
            mock_request = Mock()
            mock_request.url = Mock()
            mock_request.url.path = "/test-bucket/test-key"
            mock_request.headers = {}

            with patch(
                "s3proxy.handlers.objects.get.load_multipart_metadata",
                return_value=meta,
            ):
                creds = Mock()
                creds.access_key_id = "test-key"
                creds.secret_access_key = "test-secret"

                # Get response and consume stream - should catch InvalidRange and raise S3Error
                response = await handler.handle_get_object(mock_request, creds)

                with pytest.raises(S3Error) as exc_info:
                    async for _ in response.body_iterator:
                        pass

                # Verify error message is helpful
                assert (
                    "metadata corruption" in str(exc_info.value).lower()
                    or "cannot read" in str(exc_info.value).lower()
                )

    @pytest.mark.asyncio
    async def test_valid_range_succeeds(self, handler, settings):
        """Test that valid ranges work correctly."""
        mock_client = AsyncMock()
        # Make mock_client an async context manager
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)

        # Create test ciphertext and plaintext
        plaintext = b"x" * 1000
        dek = crypto.generate_dek()
        nonce = crypto.derive_part_nonce("upload-123", 1)
        ciphertext = crypto.encrypt(plaintext, dek, nonce)

        # Mock S3 responses
        mock_client.head_object = AsyncMock(
            return_value={"ContentLength": len(ciphertext), "LastModified": None}
        )

        # Mock get_object to return ciphertext
        mock_body = AsyncMock()
        mock_body.read = AsyncMock(return_value=ciphertext)
        mock_body.__aenter__ = AsyncMock(return_value=mock_body)
        mock_body.__aexit__ = AsyncMock(return_value=None)
        mock_client.get_object = AsyncMock(
            return_value={"Body": mock_body, "ContentType": "application/octet-stream"}
        )

        # Create metadata with internal parts that fit within object size
        internal_parts = [
            InternalPartMetadata(
                internal_part_number=1,
                plaintext_size=len(plaintext),
                ciphertext_size=len(ciphertext),
                etag="etag1",
            ),
        ]

        part_meta = PartMetadata(
            part_number=1,
            plaintext_size=len(plaintext),
            ciphertext_size=len(ciphertext),
            etag="etag",
            md5="md5",
            internal_parts=internal_parts,
        )

        meta = MultipartMetadata(
            version=1,
            part_count=1,
            total_plaintext_size=len(plaintext),
            parts=[part_meta],
            wrapped_dek=crypto.wrap_key(dek, settings.kek),
        )

        with patch.object(handler, "_client", return_value=mock_client):
            mock_request = Mock()
            mock_request.url = Mock()
            mock_request.url.path = "/test-bucket/test-key"
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

                # Verify response is valid
                assert response is not None
                assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_multiple_internal_parts_validation(self, handler, settings):
        """Test validation with multiple internal parts."""
        mock_client = AsyncMock()
        # Make mock_client an async context manager
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)

        # Object has 3 internal parts, but actual size only fits 2
        actual_size = 2 * (16 * 1024 * 1024 + 28)  # Size for 2 parts only

        mock_client.head_object = AsyncMock(
            return_value={"ContentLength": actual_size, "LastModified": None}
        )

        # Mock get_object to return empty body
        mock_body = AsyncMock()
        mock_body.read = AsyncMock(return_value=b"")
        mock_body.__aenter__ = AsyncMock(return_value=mock_body)
        mock_body.__aexit__ = AsyncMock(return_value=None)
        mock_client.get_object = AsyncMock(return_value={"Body": mock_body})

        # Create metadata claiming 3 internal parts
        internal_parts = [
            InternalPartMetadata(
                internal_part_number=i,
                plaintext_size=16 * 1024 * 1024,
                ciphertext_size=16 * 1024 * 1024 + 28,
                etag=f"etag{i}",
            )
            for i in range(1, 4)  # 3 parts
        ]

        part_meta = PartMetadata(
            part_number=1,
            plaintext_size=48 * 1024 * 1024,
            ciphertext_size=48 * 1024 * 1024 + 84,
            etag="etag",
            md5="md5",
            internal_parts=internal_parts,
        )

        meta = MultipartMetadata(
            version=1,
            part_count=1,
            total_plaintext_size=48 * 1024 * 1024,
            parts=[part_meta],
            wrapped_dek=crypto.wrap_key(crypto.generate_dek(), settings.kek),
        )

        with patch.object(handler, "_client", return_value=mock_client):
            mock_request = Mock()
            mock_request.url = Mock()
            mock_request.url.path = "/test-bucket/test-key"
            mock_request.headers = {}

            with patch(
                "s3proxy.handlers.objects.get.load_multipart_metadata",
                return_value=meta,
            ):
                creds = Mock()
                creds.access_key_id = "test-key"
                creds.secret_access_key = "test-secret"

                # Get response - should detect that 3rd part exceeds object size when streaming
                response = await handler.handle_get_object(mock_request, creds)

                with pytest.raises(S3Error) as exc_info:
                    async for _ in response.body_iterator:
                        pass

                # Verify error mentions internal part number
                assert (
                    "internal part" in str(exc_info.value).lower()
                    or "part 1" in str(exc_info.value).lower()
                )
