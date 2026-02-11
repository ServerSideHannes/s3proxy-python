"""Tests for EntityTooSmall error handling."""

from unittest.mock import AsyncMock, Mock, patch

import pytest
from botocore.exceptions import ClientError

from s3proxy import crypto
from s3proxy.errors import S3Error
from s3proxy.state import InternalPartMetadata, PartMetadata


class TestEntityTooSmallHandling:
    """Test EntityTooSmall error scenarios."""

    @pytest.mark.asyncio
    async def test_complete_with_missing_part_rejected(self, handler, settings):
        """Test that CompleteMultipartUpload fails when client requests non-existent parts."""
        mock_client = AsyncMock()
        # Make mock_client an async context manager
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        bucket = "test-bucket"
        key = "test-key"
        upload_id = "test-upload-123"

        # Create upload state with parts 1, 2, 3, 5 (missing 4)
        dek = crypto.generate_dek()
        await handler.multipart_manager.create_upload(bucket, key, upload_id, dek)

        # Add parts 1, 2, 3, 5 to state
        for part_num in [1, 2, 3, 5]:
            internal_part = InternalPartMetadata(
                internal_part_number=part_num,
                plaintext_size=1000,
                ciphertext_size=1028,
                etag=f"etag-{part_num}",
            )
            part_meta = PartMetadata(
                part_number=part_num,
                plaintext_size=1000,
                ciphertext_size=1028,
                etag=f"etag-{part_num}",
                md5=f"md5-{part_num}",
                internal_parts=[internal_part],
            )
            await handler.multipart_manager.add_part(bucket, key, upload_id, part_meta)

        # Mock request body with Elasticsearch requesting parts 1-5 (including missing 4)
        complete_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Part><PartNumber>1</PartNumber><ETag>"etag-1"</ETag></Part>
            <Part><PartNumber>2</PartNumber><ETag>"etag-2"</ETag></Part>
            <Part><PartNumber>3</PartNumber><ETag>"etag-3"</ETag></Part>
            <Part><PartNumber>4</PartNumber><ETag>"etag-4"</ETag></Part>
            <Part><PartNumber>5</PartNumber><ETag>"etag-5"</ETag></Part>
        </CompleteMultipartUpload>"""

        mock_request = Mock()
        mock_request.url = Mock()
        mock_request.url.path = f"/{bucket}/{key}"
        mock_request.url.query = f"uploadId={upload_id}"
        mock_request.body = AsyncMock(return_value=complete_xml.encode())
        mock_request.headers = {}  # Use real dict for headers

        creds = Mock()
        creds.access_key_id = "test-key"
        creds.secret_access_key = "test-secret"

        with patch.object(handler, "_client", return_value=mock_client):
            # Should fail because part 4 is missing from state
            with pytest.raises(S3Error) as exc_info:
                await handler.handle_complete_multipart_upload(mock_request, creds)

            # Verify error message mentions the missing part
            error_msg = str(exc_info.value).lower()
            assert "[4]" in error_msg or "part 4" in error_msg or "never uploaded" in error_msg

    @pytest.mark.asyncio
    async def test_entity_too_small_with_small_parts(self, handler, settings):
        """Test EntityTooSmall error when multiple parts are < 5MB."""
        mock_client = AsyncMock()
        # Make mock_client an async context manager
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        bucket = "test-bucket"
        key = "test-key"
        upload_id = "test-upload-456"

        # Create upload with 5 parts of 1KB each (total 5KB)
        dek = crypto.generate_dek()
        await handler.multipart_manager.create_upload(bucket, key, upload_id, dek)

        # Add 5 parts of 1KB each
        for part_num in range(1, 6):
            internal_part = InternalPartMetadata(
                internal_part_number=part_num,
                plaintext_size=1000,
                ciphertext_size=1028,
                etag=f"etag-{part_num}",
            )
            part_meta = PartMetadata(
                part_number=part_num,
                plaintext_size=1000,
                ciphertext_size=1028,
                etag=f"etag-{part_num}",
                md5=f"md5-{part_num}",
                internal_parts=[internal_part],
            )
            await handler.multipart_manager.add_part(bucket, key, upload_id, part_meta)

        # Mock S3 to return EntityTooSmall
        error_response = {
            "Error": {
                "Code": "EntityTooSmall",
                "Message": "Your proposed upload is smaller than the minimum allowed object size.",
            }
        }
        mock_client.complete_multipart_upload = AsyncMock(
            side_effect=ClientError(error_response, "CompleteMultipartUpload")
        )
        mock_client.abort_multipart_upload = AsyncMock()

        # Mock request body
        complete_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Part><PartNumber>1</PartNumber><ETag>"etag-1"</ETag></Part>
            <Part><PartNumber>2</PartNumber><ETag>"etag-2"</ETag></Part>
            <Part><PartNumber>3</PartNumber><ETag>"etag-3"</ETag></Part>
            <Part><PartNumber>4</PartNumber><ETag>"etag-4"</ETag></Part>
            <Part><PartNumber>5</PartNumber><ETag>"etag-5"</ETag></Part>
        </CompleteMultipartUpload>"""

        mock_request = Mock()
        mock_request.url = Mock()
        mock_request.url.path = f"/{bucket}/{key}"
        mock_request.url.query = f"uploadId={upload_id}"
        mock_request.body = AsyncMock(return_value=complete_xml.encode())
        mock_request.headers = {}  # Use real dict for headers

        creds = Mock()
        creds.access_key_id = "test-key"
        creds.secret_access_key = "test-secret"

        with patch.object(handler, "_client", return_value=mock_client):
            # Should return helpful error about small parts
            with pytest.raises(S3Error) as exc_info:
                await handler.handle_complete_multipart_upload(mock_request, creds)

            # Verify error mentions 5MB minimum
            assert "5mb" in str(exc_info.value).lower() or "5 mb" in str(exc_info.value).lower()
            # Verify abort was called
            mock_client.abort_multipart_upload.assert_called_once()
