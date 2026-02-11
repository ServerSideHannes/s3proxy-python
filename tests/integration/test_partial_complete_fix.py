"""Test that partial multipart completion doesn't create metadata mismatches."""

from unittest.mock import AsyncMock, Mock, patch

import pytest

from s3proxy import crypto
from s3proxy.state import InternalPartMetadata, PartMetadata


class TestPartialMultipartCompletion:
    """Test that completing with fewer parts than uploaded works correctly."""

    @pytest.mark.asyncio
    async def test_complete_with_subset_of_parts(self, handler, settings):
        """Test completing upload with only some of the uploaded parts.

        This is the fix for the root cause: if a client uploads 4 parts but only
        completes with 3 parts, the metadata should only reference the 3 completed parts.
        """
        mock_client = AsyncMock()
        # Make mock_client an async context manager
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        dek = crypto.generate_dek()

        # Simulate upload state with 4 parts uploaded
        await handler.multipart_manager.create_upload("bucket", "key", "upload-123", dek)

        # Create 4 parts in state (all uploaded)
        for i in range(1, 5):
            internal_parts = [
                InternalPartMetadata(
                    internal_part_number=i,
                    plaintext_size=1000,
                    ciphertext_size=1028,
                    etag=f"internal-etag-{i}",
                )
            ]
            part = PartMetadata(
                part_number=i,
                plaintext_size=1000,
                ciphertext_size=1028,
                etag=f"part-etag-{i}",
                md5=f"md5-{i}",
                internal_parts=internal_parts,
            )
            await handler.multipart_manager.add_part("bucket", "key", "upload-123", part)

        # Mock S3 client responses
        mock_client.complete_multipart_upload = AsyncMock()
        mock_client.head_object = AsyncMock(
            return_value={"ContentLength": 3 * 1028}  # Only 3 parts completed
        )

        saved_metadata = None

        async def capture_save(client, bucket, key, meta):
            nonlocal saved_metadata
            saved_metadata = meta

        # Create CompleteMultipartUpload request body with only parts 1, 2, 3
        # (client decided not to include part 4)
        complete_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Part>
                <PartNumber>1</PartNumber>
                <ETag>"part-etag-1"</ETag>
            </Part>
            <Part>
                <PartNumber>2</PartNumber>
                <ETag>"part-etag-2"</ETag>
            </Part>
            <Part>
                <PartNumber>3</PartNumber>
                <ETag>"part-etag-3"</ETag>
            </Part>
        </CompleteMultipartUpload>"""

        mock_request = Mock()
        mock_request.url = Mock()
        mock_request.url.path = "/bucket/key"
        mock_request.url.query = "uploadId=upload-123"
        mock_request.body = AsyncMock(return_value=complete_xml.encode())
        mock_request.headers = {}  # Use real dict for headers

        creds = Mock()
        creds.access_key_id = "test-key"
        creds.secret_access_key = "test-secret"

        with (
            patch.object(handler, "_client", return_value=mock_client),
            patch(
                "s3proxy.handlers.multipart.lifecycle.save_multipart_metadata",
                side_effect=capture_save,
            ),
            patch("s3proxy.handlers.multipart.lifecycle.delete_upload_state", AsyncMock()),
        ):
            # Complete the upload
            response = await handler.handle_complete_multipart_upload(mock_request, creds)

            assert response.status_code == 200

        # Verify that metadata only contains the 3 completed parts, not all 4
        assert saved_metadata is not None, "Metadata should have been saved"
        assert saved_metadata.part_count == 3, f"Expected 3 parts, got {saved_metadata.part_count}"
        assert len(saved_metadata.parts) == 3, (
            f"Expected 3 parts in list, got {len(saved_metadata.parts)}"
        )
        assert saved_metadata.total_plaintext_size == 3000, (
            f"Expected 3000 bytes, got {saved_metadata.total_plaintext_size}"
        )

        # Verify part numbers are 1, 2, 3 (not including 4)
        part_numbers = {p.part_number for p in saved_metadata.parts}
        assert part_numbers == {1, 2, 3}, f"Expected parts {{1,2,3}}, got {part_numbers}"

        # Verify S3 complete was called with correct internal parts
        complete_call = mock_client.complete_multipart_upload.call_args
        s3_parts = complete_call[0][3]  # 4th positional arg
        assert len(s3_parts) == 3, f"Expected 3 S3 parts, got {len(s3_parts)}"

    @pytest.mark.asyncio
    async def test_complete_logs_size_mismatch(self, handler, settings):
        """Test that size mismatches are logged but don't fail the upload."""
        mock_client = AsyncMock()
        # Make mock_client an async context manager
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        dek = crypto.generate_dek()

        await handler.multipart_manager.create_upload("bucket", "key", "upload-123", dek)

        # Add 2 parts
        for i in range(1, 3):
            part = PartMetadata(
                part_number=i,
                plaintext_size=1000,
                ciphertext_size=1028,
                etag=f"part-etag-{i}",
                md5=f"md5-{i}",
                internal_parts=[
                    InternalPartMetadata(
                        internal_part_number=i,
                        plaintext_size=1000,
                        ciphertext_size=1028,
                        etag=f"etag-{i}",
                    )
                ],
            )
            await handler.multipart_manager.add_part("bucket", "key", "upload-123", part)

        mock_client.complete_multipart_upload = AsyncMock()
        # Return size that doesn't match our metadata (simulate S3 corruption or issue)
        mock_client.head_object = AsyncMock(
            return_value={"ContentLength": 9999}  # Wrong size
        )

        complete_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Part><PartNumber>1</PartNumber><ETag>"part-etag-1"</ETag></Part>
            <Part><PartNumber>2</PartNumber><ETag>"part-etag-2"</ETag></Part>
        </CompleteMultipartUpload>"""

        mock_request = Mock()
        mock_request.url = Mock()
        mock_request.url.path = "/bucket/key"
        mock_request.url.query = "uploadId=upload-123"
        mock_request.body = AsyncMock(return_value=complete_xml.encode())
        mock_request.headers = {}  # Use real dict for headers

        creds = Mock()
        creds.access_key_id = "test-key"
        creds.secret_access_key = "test-secret"

        with (
            patch.object(handler, "_client", return_value=mock_client),
            patch("s3proxy.handlers.multipart.lifecycle.save_multipart_metadata", AsyncMock()),
            patch("s3proxy.handlers.multipart.lifecycle.delete_upload_state", AsyncMock()),
        ):
            # Should complete successfully even with size mismatch
            # (mismatch is logged but doesn't fail the upload)
            response = await handler.handle_complete_multipart_upload(mock_request, creds)

            assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_complete_with_no_parts_fails(self, handler, settings):
        """Test that completing with zero parts raises an error."""
        from s3proxy.errors import S3Error

        mock_client = AsyncMock()
        # Make mock_client an async context manager
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        dek = crypto.generate_dek()

        await handler.multipart_manager.create_upload("bucket", "key", "upload-123", dek)

        # Add some parts to state
        part = PartMetadata(
            part_number=1,
            plaintext_size=1000,
            ciphertext_size=1028,
            etag="part-etag-1",
            md5="md5-1",
            internal_parts=[],
        )
        await handler.multipart_manager.add_part("bucket", "key", "upload-123", part)

        # But complete with empty part list
        complete_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        </CompleteMultipartUpload>"""

        mock_request = Mock()
        mock_request.url = Mock()
        mock_request.url.path = "/bucket/key"
        mock_request.url.query = "uploadId=upload-123"
        mock_request.body = AsyncMock(return_value=complete_xml.encode())
        mock_request.headers = {}  # Use real dict for headers

        creds = Mock()
        creds.access_key_id = "test-key"
        creds.secret_access_key = "test-secret"

        with (
            patch.object(handler, "_client", return_value=mock_client),
            pytest.raises(S3Error),
        ):
            await handler.handle_complete_multipart_upload(mock_request, creds)
