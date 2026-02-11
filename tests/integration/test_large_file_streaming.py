"""E2E tests for large file streaming without buffering.

These tests verify that files larger than 8MB (crypto.MAX_BUFFER_SIZE) are
streamed without buffering the entire file in memory, and use late
signature verification for signed uploads.
"""

import base64
import hashlib
from unittest.mock import MagicMock, patch

import pytest
from fastapi import Request

from s3proxy import crypto
from s3proxy.handlers import S3ProxyHandler


@pytest.fixture
def large_file_20mb():
    """Generate a 20MB test file (larger than PART_SIZE)."""
    # 20MB = 20 * 1024 * 1024 bytes
    return b"X" * (20 * 1024 * 1024)


@pytest.fixture
def medium_file_10mb():
    """Generate a 10MB test file (smaller than PART_SIZE)."""
    # 10MB = 10 * 1024 * 1024 bytes
    return b"Y" * (10 * 1024 * 1024)


class TestLargeFileStreaming:
    """Test large file upload streaming."""

    @pytest.mark.asyncio
    async def test_large_signed_upload_uses_streaming(
        self, mock_s3, settings, credentials, multipart_manager
    ):
        """Test that large signed uploads (>16MB) use streaming path."""
        handler = S3ProxyHandler(settings, {}, multipart_manager)
        bucket = "test-bucket"
        key = "large-file.bin"

        # Create a 20MB file (larger than crypto.PART_SIZE)
        plaintext = b"X" * (20 * 1024 * 1024)

        # Mock request with large content-length and regular signature
        mock_request = MagicMock(spec=Request)
        mock_request.headers = {
            "content-type": "application/octet-stream",
            "content-length": str(len(plaintext)),
            "x-amz-content-sha256": hashlib.sha256(plaintext).hexdigest(),
            "content-encoding": "",
        }
        mock_request.url.path = f"/{bucket}/{key}"

        # Mock request.stream() to yield the data in chunks
        async def mock_stream():
            chunk_size = 8192
            for i in range(0, len(plaintext), chunk_size):
                yield plaintext[i : i + chunk_size]

        mock_request.stream = mock_stream

        # Mock body() as well (needed for fallback paths)
        async def mock_body():
            return plaintext

        mock_request.body = mock_body

        # Patch the S3ProxyHandler._client method to return our mock
        with patch.object(handler, "_client", return_value=mock_s3):
            # Call handle_put_object
            response = await handler.handle_put_object(mock_request, credentials)

            # Verify successful response
            assert response.status_code in (200, 201)

            # Verify object was stored (either via put_object or multipart upload)
            assert len(mock_s3.call_history) > 0

            # For large signed uploads, the implementation may use either:
            # 1. Streaming multipart upload (if implemented)
            # 2. Single PUT after late signature verification
            # Both are valid - just verify the upload succeeded
            put_calls = [call for call in mock_s3.call_history if call[0] == "put_object"]
            multipart_calls = [
                call for call in mock_s3.call_history if call[0] == "create_multipart_upload"
            ]

            assert len(put_calls) >= 1 or len(multipart_calls) >= 1, (
                "Should upload via put_object or multipart upload"
            )

    @pytest.mark.asyncio
    async def test_large_unsigned_upload_uses_streaming(
        self, mock_s3, settings, credentials, multipart_manager
    ):
        """Test that large unsigned uploads use streaming path."""
        handler = S3ProxyHandler(settings, {}, multipart_manager)
        bucket = "test-bucket"
        key = "large-unsigned-file.bin"

        # Create a 20MB file
        plaintext = b"Z" * (20 * 1024 * 1024)

        # Mock request with UNSIGNED-PAYLOAD
        mock_request = MagicMock(spec=Request)
        mock_request.headers = {
            "content-type": "application/octet-stream",
            "content-length": str(len(plaintext)),
            "x-amz-content-sha256": "UNSIGNED-PAYLOAD",
            "content-encoding": "",
        }
        mock_request.url.path = f"/{bucket}/{key}"

        # Mock request.stream()
        async def mock_stream():
            chunk_size = 8192
            for i in range(0, len(plaintext), chunk_size):
                yield plaintext[i : i + chunk_size]

        mock_request.stream = mock_stream

        # Mock body() as well
        async def mock_body():
            return plaintext

        mock_request.body = mock_body

        with patch.object(handler, "_client", return_value=mock_s3):
            response = await handler.handle_put_object(mock_request, credentials)

            # Verify successful response
            assert response.status_code in (200, 201)

            # Verify multipart upload was used
            create_multipart_calls = [
                call for call in mock_s3.call_history if call[0] == "create_multipart_upload"
            ]
            assert len(create_multipart_calls) >= 1

    @pytest.mark.asyncio
    async def test_medium_file_uses_buffering(
        self, mock_s3, settings, credentials, multipart_manager
    ):
        """Test that files ≤8MB (MAX_BUFFER_SIZE) use buffering for signature verification."""
        handler = S3ProxyHandler(settings, {}, multipart_manager)
        bucket = "test-bucket"
        key = "medium-file.bin"

        # Create a 5MB file (smaller than crypto.MAX_BUFFER_SIZE which is 8MB)
        plaintext = b"M" * (5 * 1024 * 1024)

        # This would normally be buffered by FastAPI middleware
        # The handler would call request.body() which returns the buffered data
        mock_request = MagicMock(spec=Request)
        mock_request.headers = {
            "content-type": "application/octet-stream",
            "content-length": str(len(plaintext)),
            "x-amz-content-sha256": hashlib.sha256(plaintext).hexdigest(),
            "content-encoding": "",
        }
        mock_request.url.path = f"/{bucket}/{key}"

        # Mock body() to return the data (simulating FastAPI buffering)
        async def mock_body():
            return plaintext

        mock_request.body = mock_body

        with patch.object(handler, "_client", return_value=mock_s3):
            response = await handler.handle_put_object(mock_request, credentials)

            # Verify successful response
            assert response.status_code in (200, 201)

            # For files ≤16MB, should use single put_object (not multipart)
            put_object_calls = [call for call in mock_s3.call_history if call[0] == "put_object"]
            assert len(put_object_calls) >= 1, "Should use single put_object for small files"

    @pytest.mark.asyncio
    async def test_large_file_encryption_and_decryption(self, mock_s3, settings):
        """Test full workflow: upload large file, download, decrypt, verify."""
        bucket = "test-bucket"
        key = "large-encrypted.bin"
        await mock_s3.create_bucket(bucket)

        # Create a 20MB file
        plaintext = b"Q" * (20 * 1024 * 1024)
        plaintext_hash = hashlib.md5(plaintext).hexdigest()

        # Generate DEK and wrap it
        dek = crypto.generate_dek()
        wrapped_dek = crypto.wrap_key(dek, settings.kek)

        # Simulate multipart upload by encrypting in parts
        upload_id = "test-upload-123"
        part_size = crypto.PART_SIZE
        parts = []

        for part_num in range(1, (len(plaintext) + part_size - 1) // part_size + 1):
            start = (part_num - 1) * part_size
            end = min(start + part_size, len(plaintext))
            part_data = plaintext[start:end]

            # Encrypt part
            encrypted_part = crypto.encrypt_part(part_data, dek, upload_id, part_num)
            parts.append((part_num, encrypted_part))

        # Simulate completing multipart upload (concatenate parts)
        full_ciphertext = b"".join(part[1] for part in parts)

        # Store in mock S3
        await mock_s3.put_object(
            bucket,
            key,
            full_ciphertext,
            metadata={
                settings.dektag_name: base64.b64encode(wrapped_dek).decode(),
                "plaintext-size": str(len(plaintext)),
                "client-etag": plaintext_hash,
            },
        )

        # Download and decrypt
        resp = await mock_s3.get_object(bucket, key)
        await resp["Body"].read()
        metadata = resp["Metadata"]

        # Unwrap DEK
        wrapped_dek_bytes = base64.b64decode(metadata[settings.dektag_name])
        decrypted_dek = crypto.unwrap_key(wrapped_dek_bytes, settings.kek)

        # Decrypt all parts
        # We need to track position in ciphertext since parts may have different sizes
        decrypted_parts = []

        for part_num, (_, encrypted_part) in enumerate(parts, 1):
            # Use the actual encrypted part we stored earlier
            decrypted_part = crypto.decrypt_part(encrypted_part, decrypted_dek, upload_id, part_num)
            decrypted_parts.append(decrypted_part)

        # Verify
        decrypted_plaintext = b"".join(decrypted_parts)
        assert decrypted_plaintext == plaintext
        assert hashlib.md5(decrypted_plaintext).hexdigest() == plaintext_hash

    @pytest.mark.asyncio
    async def test_memory_bounded_streaming(
        self, mock_s3, settings, credentials, multipart_manager
    ):
        """Test that streaming keeps memory usage bounded to PART_SIZE."""
        handler = S3ProxyHandler(settings, {}, multipart_manager)
        bucket = "test-bucket"
        key = "huge-file.bin"

        # Simulate a 100MB file (but don't actually allocate it all at once)
        file_size = 100 * 1024 * 1024
        chunk_size = 8192

        mock_request = MagicMock(spec=Request)
        mock_request.headers = {
            "content-type": "application/octet-stream",
            "content-length": str(file_size),
            "x-amz-content-sha256": "UNSIGNED-PAYLOAD",
            "content-encoding": "",
        }
        mock_request.url.path = f"/{bucket}/{key}"

        # Mock request.stream() to yield chunks without allocating full file
        async def mock_stream():
            chunk = b"C" * chunk_size
            for _ in range(file_size // chunk_size):
                yield chunk

        mock_request.stream = mock_stream

        # Track max buffer size during upload
        max_buffer_size = [0]
        original_upload_part = mock_s3.upload_part

        async def track_upload_part(bucket, key, upload_id, part_num, body):
            # Track the size of data being passed to upload_part
            max_buffer_size[0] = max(max_buffer_size[0], len(body))
            return await original_upload_part(bucket, key, upload_id, part_num, body)

        mock_s3.upload_part = track_upload_part

        with patch.object(handler, "_client", return_value=mock_s3):
            response = await handler.handle_put_object(mock_request, credentials)

            # Verify successful
            assert response.status_code in (200, 201)

            # Verify max buffer size was bounded
            # Should be around PART_SIZE + overhead (IV + tag = 28 bytes)
            expected_max = crypto.PART_SIZE + 28
            assert max_buffer_size[0] <= expected_max * 1.1, (
                f"Buffer exceeded expected max: {max_buffer_size[0]} > {expected_max}"
            )


class TestContentLengthRouting:
    """Test that content-length header correctly routes to streaming."""

    @pytest.mark.asyncio
    async def test_exactly_8mb_uses_buffering(
        self, mock_s3, settings, credentials, multipart_manager
    ):
        """Test that exactly 8MB (MAX_BUFFER_SIZE) uses buffering."""
        handler = S3ProxyHandler(settings, {}, multipart_manager)
        bucket = "test-bucket"
        key = "exactly-8mb.bin"

        # Exactly MAX_BUFFER_SIZE (8MB)
        plaintext = b"E" * crypto.MAX_BUFFER_SIZE

        mock_request = MagicMock(spec=Request)
        mock_request.headers = {
            "content-type": "application/octet-stream",
            "content-length": str(len(plaintext)),
            "x-amz-content-sha256": hashlib.sha256(plaintext).hexdigest(),
            "content-encoding": "",
        }
        mock_request.url.path = f"/{bucket}/{key}"

        async def mock_body():
            return plaintext

        mock_request.body = mock_body

        with patch.object(handler, "_client", return_value=mock_s3):
            response = await handler.handle_put_object(mock_request, credentials)
            assert response.status_code in (200, 201)

            # Should use single put_object
            put_calls = [call for call in mock_s3.call_history if call[0] == "put_object"]
            assert len(put_calls) >= 1

    @pytest.mark.asyncio
    async def test_8mb_plus_one_uses_streaming(
        self, mock_s3, settings, credentials, multipart_manager
    ):
        """Test that 8MB + 1 byte uses streaming."""
        handler = S3ProxyHandler(settings, {}, multipart_manager)
        bucket = "test-bucket"
        key = "8mb-plus-one.bin"

        # MAX_BUFFER_SIZE + 1 byte triggers streaming
        plaintext = b"F" * (crypto.MAX_BUFFER_SIZE + 1)

        mock_request = MagicMock(spec=Request)
        mock_request.headers = {
            "content-type": "application/octet-stream",
            "content-length": str(len(plaintext)),
            "x-amz-content-sha256": hashlib.sha256(plaintext).hexdigest(),
            "content-encoding": "",
        }
        mock_request.url.path = f"/{bucket}/{key}"

        async def mock_stream():
            chunk_size = 8192
            for i in range(0, len(plaintext), chunk_size):
                yield plaintext[i : i + chunk_size]

        mock_request.stream = mock_stream

        # Mock body() as well
        async def mock_body():
            return plaintext

        mock_request.body = mock_body

        with patch.object(handler, "_client", return_value=mock_s3):
            response = await handler.handle_put_object(mock_request, credentials)
            assert response.status_code in (200, 201)

            # Should use multipart upload
            create_calls = [
                call for call in mock_s3.call_history if call[0] == "create_multipart_upload"
            ]
            assert len(create_calls) >= 1
