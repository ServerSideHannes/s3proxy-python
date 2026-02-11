"""Tests for internal part ordering in CompleteMultipartUpload."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from s3proxy.handlers import S3ProxyHandler
from s3proxy.s3client import S3Credentials
from s3proxy.state import InternalPartMetadata, PartMetadata


class TestPartOrdering:
    """Test that internal parts are sorted correctly before CompleteMultipartUpload."""

    @pytest.mark.asyncio
    async def test_out_of_order_client_parts_sorted_internally(self, manager, settings):
        """
        Test that when client uploads parts out of order, internal parts are sorted.

        NEW BEHAVIOR (EntityTooSmall fix):
        - Client part 3 uploaded first → uses internal part 3 (direct mapping)
        - Client part 1 uploaded second → uses internal part 1 (direct mapping)
        - Client part 2 uploaded third → uses internal part 2 (direct mapping)
        - CompleteMultipartUpload sorts and sends: [1, 2, 3]
        - MinIO receives parts in correct order
        """
        handler = S3ProxyHandler(settings, {}, manager)

        bucket = "test-bucket"
        key = "test-key"
        upload_id = "test-upload-out-of-order"

        # Create upload
        from s3proxy import crypto

        dek = crypto.generate_dek()
        await manager.create_upload(bucket, key, upload_id, dek)

        # Simulate parts uploaded out of order with direct client→internal mapping
        # (NEW: no splitting = use client part number as internal part number)

        # Client part 3 uploaded first, uses internal part 3
        part3 = PartMetadata(
            part_number=3,
            plaintext_size=1024,
            ciphertext_size=1052,
            etag="etag-3",
            md5="md5-3",
            internal_parts=[
                InternalPartMetadata(
                    internal_part_number=3,  # Direct mapping!
                    plaintext_size=1024,
                    ciphertext_size=1052,
                    etag="internal-etag-3",
                ),
            ],
        )
        await manager.add_part(bucket, key, upload_id, part3)

        # Client part 1 uploaded second, uses internal part 1
        part1 = PartMetadata(
            part_number=1,
            plaintext_size=1024,
            ciphertext_size=1052,
            etag="etag-1",
            md5="md5-1",
            internal_parts=[
                InternalPartMetadata(
                    internal_part_number=1,  # Direct mapping!
                    plaintext_size=1024,
                    ciphertext_size=1052,
                    etag="internal-etag-1",
                ),
            ],
        )
        await manager.add_part(bucket, key, upload_id, part1)

        # Client part 2 uploaded third, uses internal part 2
        part2 = PartMetadata(
            part_number=2,
            plaintext_size=1024,
            ciphertext_size=1052,
            etag="etag-2",
            md5="md5-2",
            internal_parts=[
                InternalPartMetadata(
                    internal_part_number=2,  # Direct mapping!
                    plaintext_size=1024,
                    ciphertext_size=1052,
                    etag="internal-etag-2",
                ),
            ],
        )
        await manager.add_part(bucket, key, upload_id, part2)

        # Create mock request for CompleteMultipartUpload
        # Client sends parts in order [1, 2, 3] (sorted by client part number)
        mock_request = MagicMock()
        mock_request.url.path = f"/{bucket}/{key}"
        mock_request.url.query = f"uploadId={upload_id}"
        mock_request.body = AsyncMock(
            return_value=b"""<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Part><PartNumber>1</PartNumber><ETag>"etag-1"</ETag></Part>
    <Part><PartNumber>2</PartNumber><ETag>"etag-2"</ETag></Part>
    <Part><PartNumber>3</PartNumber><ETag>"etag-3"</ETag></Part>
</CompleteMultipartUpload>"""
        )

        # Mock S3 client
        mock_client = AsyncMock()
        # Make mock_client an async context manager
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.complete_multipart_upload = AsyncMock()
        mock_client.head_object = AsyncMock(return_value={"ContentLength": 3156})

        creds = S3Credentials(
            access_key="test-key",
            secret_key="test-secret",
            region="us-east-1",
            service="s3",
        )

        with (
            patch.object(handler, "_client", return_value=mock_client),
            patch("s3proxy.handlers.multipart.lifecycle.save_multipart_metadata", AsyncMock()),
            patch("s3proxy.handlers.multipart.lifecycle.delete_upload_state", AsyncMock()),
        ):
            await handler.handle_complete_multipart_upload(mock_request, creds)

        # Verify complete_multipart_upload was called with SORTED parts
        mock_client.complete_multipart_upload.assert_called_once()
        call_args = mock_client.complete_multipart_upload.call_args
        s3_parts = call_args[0][3]  # Fourth positional arg

        # Extract part numbers
        part_numbers = [p["PartNumber"] for p in s3_parts]

        # NEW BEHAVIOR: With direct mapping, parts are [1, 2, 3]
        # This is CORRECT - MinIO receives parts in natural order
        assert part_numbers == [1, 2, 3], (
            f"Internal parts must be sorted in ascending order. "
            f"Got {part_numbers}, expected [1, 2, 3]. "
            f"This would cause MinIO InvalidPartOrder error!"
        )

    @pytest.mark.asyncio
    async def test_sequential_parts_remain_sorted(self, manager, settings):
        """Test that normally uploaded sequential parts remain sorted."""
        handler = S3ProxyHandler(settings, {}, manager)

        bucket = "test-bucket"
        key = "test-key-sequential"
        upload_id = "test-upload-sequential"

        # Create upload
        from s3proxy import crypto

        dek = crypto.generate_dek()
        await manager.create_upload(bucket, key, upload_id, dek)

        # Upload parts in order with sequential internal parts
        for i in range(1, 4):
            part = PartMetadata(
                part_number=i,
                plaintext_size=1024,
                ciphertext_size=1052,
                etag=f"etag-{i}",
                md5=f"md5-{i}",
                internal_parts=[
                    InternalPartMetadata(
                        internal_part_number=i,
                        plaintext_size=1024,
                        ciphertext_size=1052,
                        etag=f"internal-etag-{i}",
                    ),
                ],
            )
            await manager.add_part(bucket, key, upload_id, part)

        # Mock request
        mock_request = MagicMock()
        mock_request.url.path = f"/{bucket}/{key}"
        mock_request.url.query = f"uploadId={upload_id}"
        mock_request.body = AsyncMock(
            return_value=b"""<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Part><PartNumber>1</PartNumber><ETag>"etag-1"</ETag></Part>
    <Part><PartNumber>2</PartNumber><ETag>"etag-2"</ETag></Part>
    <Part><PartNumber>3</PartNumber><ETag>"etag-3"</ETag></Part>
</CompleteMultipartUpload>"""
        )

        # Mock S3 client
        mock_client = AsyncMock()
        # Make mock_client an async context manager
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.complete_multipart_upload = AsyncMock()
        mock_client.head_object = AsyncMock(return_value={"ContentLength": 3156})

        creds = S3Credentials(
            access_key="test-key",
            secret_key="test-secret",
            region="us-east-1",
            service="s3",
        )

        with (
            patch.object(handler, "_client", return_value=mock_client),
            patch("s3proxy.handlers.multipart.lifecycle.save_multipart_metadata", AsyncMock()),
            patch("s3proxy.handlers.multipart.lifecycle.delete_upload_state", AsyncMock()),
        ):
            await handler.handle_complete_multipart_upload(mock_request, creds)

        # Verify parts are in order [1, 2, 3]
        call_args = mock_client.complete_multipart_upload.call_args
        s3_parts = call_args[0][3]
        part_numbers = [p["PartNumber"] for p in s3_parts]

        assert part_numbers == [1, 2, 3]
