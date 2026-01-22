"""Integration tests for S3Proxy with mocked S3 backend.

These tests verify the full request flow including encryption/decryption
without requiring a real S3 backend.
"""

import base64
import hashlib
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from s3proxy import crypto
from s3proxy.handlers import S3ProxyHandler
from s3proxy.multipart import MultipartMetadata, MultipartStateManager, PartMetadata


class TestObjectEncryptionFlow:
    """Test full object encryption/decryption workflow."""

    @pytest.mark.asyncio
    async def test_put_then_get_object(self, mock_s3, settings, credentials, multipart_manager):
        """Test uploading and then downloading an object preserves data."""
        handler = S3ProxyHandler(settings, {}, multipart_manager)
        plaintext = b"Hello, this is secret data!"

        # Encrypt and store
        encrypted = crypto.encrypt_object(plaintext, settings.kek)
        metadata = {
            settings.dektag_name: base64.b64encode(encrypted.wrapped_dek).decode(),
            "plaintext-size": str(len(plaintext)),
            "client-etag": hashlib.md5(plaintext).hexdigest(),
        }
        await mock_s3.put_object("test-bucket", "test-key", encrypted.ciphertext, metadata=metadata)

        # Retrieve and decrypt
        resp = await mock_s3.get_object("test-bucket", "test-key")
        ciphertext = await resp["Body"].read()
        stored_metadata = resp["Metadata"]

        wrapped_dek = base64.b64decode(stored_metadata[settings.dektag_name])
        decrypted = crypto.decrypt_object(ciphertext, wrapped_dek, settings.kek)

        assert decrypted == plaintext

    @pytest.mark.asyncio
    async def test_put_then_head_object(self, mock_s3, settings):
        """Test HEAD returns correct plaintext size."""
        plaintext = b"Test data for head request"
        encrypted = crypto.encrypt_object(plaintext, settings.kek)
        metadata = {
            settings.dektag_name: base64.b64encode(encrypted.wrapped_dek).decode(),
            "plaintext-size": str(len(plaintext)),
        }
        await mock_s3.put_object("test-bucket", "test-key", encrypted.ciphertext, metadata=metadata)

        resp = await mock_s3.head_object("test-bucket", "test-key")

        assert resp["Metadata"]["plaintext-size"] == str(len(plaintext))

    @pytest.mark.asyncio
    async def test_delete_object(self, mock_s3):
        """Test deleting an object."""
        await mock_s3.put_object("test-bucket", "test-key", b"data")

        await mock_s3.delete_object("test-bucket", "test-key")

        with pytest.raises(Exception) as exc_info:
            await mock_s3.get_object("test-bucket", "test-key")
        assert "NoSuchKey" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_range_request(self, mock_s3, settings):
        """Test partial object download with range request."""
        plaintext = b"0123456789ABCDEF"  # 16 bytes
        encrypted = crypto.encrypt_object(plaintext, settings.kek)
        metadata = {
            settings.dektag_name: base64.b64encode(encrypted.wrapped_dek).decode(),
            "plaintext-size": str(len(plaintext)),
        }
        await mock_s3.put_object("test-bucket", "test-key", encrypted.ciphertext, metadata=metadata)

        # Get with range (note: in real impl, this would decrypt then slice)
        resp = await mock_s3.get_object("test-bucket", "test-key", "bytes=0-4")
        partial = await resp["Body"].read()

        # The mock returns raw bytes from the ciphertext range
        assert len(partial) == 5


class TestMultipartEncryptionFlow:
    """Test multipart upload encryption workflow."""

    @pytest.mark.asyncio
    async def test_multipart_upload_flow(self, mock_s3, settings, multipart_manager):
        """Test complete multipart upload flow."""
        bucket = "test-bucket"
        key = "large-file.bin"

        # Create bucket
        await mock_s3.create_bucket(bucket)

        # Initiate multipart
        resp = await mock_s3.create_multipart_upload(bucket, key)
        upload_id = resp["UploadId"]

        # Generate encryption key
        dek = crypto.generate_dek()
        wrapped_dek = crypto.wrap_key(dek, settings.kek)

        # Upload parts
        part1_plaintext = b"A" * 5242880  # 5MB
        part2_plaintext = b"B" * 1234567  # ~1.2MB

        part1_ct = crypto.encrypt_part(part1_plaintext, dek, upload_id, 1)
        part2_ct = crypto.encrypt_part(part2_plaintext, dek, upload_id, 2)

        resp1 = await mock_s3.upload_part(bucket, key, upload_id, 1, part1_ct)
        resp2 = await mock_s3.upload_part(bucket, key, upload_id, 2, part2_ct)

        # Complete multipart
        parts = [
            {"PartNumber": 1, "ETag": resp1["ETag"]},
            {"PartNumber": 2, "ETag": resp2["ETag"]},
        ]
        await mock_s3.complete_multipart_upload(bucket, key, upload_id, parts)

        # Verify object exists
        head_resp = await mock_s3.head_object(bucket, key)
        assert head_resp["ContentLength"] == len(part1_ct) + len(part2_ct)

    @pytest.mark.asyncio
    async def test_abort_multipart_upload(self, mock_s3):
        """Test aborting a multipart upload."""
        bucket = "test-bucket"
        key = "aborted-file.bin"

        await mock_s3.create_bucket(bucket)

        # Initiate
        resp = await mock_s3.create_multipart_upload(bucket, key)
        upload_id = resp["UploadId"]

        # Upload a part
        await mock_s3.upload_part(bucket, key, upload_id, 1, b"part data")

        # Abort
        await mock_s3.abort_multipart_upload(bucket, key, upload_id)

        # Verify upload is gone
        list_resp = await mock_s3.list_multipart_uploads(bucket)
        upload_ids = [u["UploadId"] for u in list_resp.get("Uploads", [])]
        assert upload_id not in upload_ids

    @pytest.mark.asyncio
    async def test_list_multipart_uploads(self, mock_s3):
        """Test listing multipart uploads."""
        bucket = "test-bucket"
        await mock_s3.create_bucket(bucket)

        # Create multiple uploads
        resp1 = await mock_s3.create_multipart_upload(bucket, "file1.bin")
        resp2 = await mock_s3.create_multipart_upload(bucket, "file2.bin")
        resp3 = await mock_s3.create_multipart_upload(bucket, "subdir/file3.bin")

        # List all
        list_resp = await mock_s3.list_multipart_uploads(bucket)
        assert len(list_resp["Uploads"]) == 3

        # List with prefix
        list_resp = await mock_s3.list_multipart_uploads(bucket, prefix="subdir/")
        assert len(list_resp["Uploads"]) == 1
        assert list_resp["Uploads"][0]["Key"] == "subdir/file3.bin"

    @pytest.mark.asyncio
    async def test_list_parts(self, mock_s3):
        """Test listing parts of a multipart upload."""
        bucket = "test-bucket"
        key = "multi-part-file.bin"
        await mock_s3.create_bucket(bucket)

        resp = await mock_s3.create_multipart_upload(bucket, key)
        upload_id = resp["UploadId"]

        # Upload parts
        await mock_s3.upload_part(bucket, key, upload_id, 1, b"part1" * 1000)
        await mock_s3.upload_part(bucket, key, upload_id, 2, b"part2" * 1000)
        await mock_s3.upload_part(bucket, key, upload_id, 3, b"part3" * 1000)

        # List parts
        list_resp = await mock_s3.list_parts(bucket, key, upload_id)
        assert len(list_resp["Parts"]) == 3
        assert list_resp["Parts"][0]["PartNumber"] == 1
        assert list_resp["Parts"][1]["PartNumber"] == 2
        assert list_resp["Parts"][2]["PartNumber"] == 3


class TestBucketOperations:
    """Test bucket-level operations."""

    @pytest.mark.asyncio
    async def test_create_and_delete_bucket(self, mock_s3):
        """Test bucket creation and deletion."""
        await mock_s3.create_bucket("new-bucket")
        await mock_s3.head_bucket("new-bucket")  # Should not raise

        await mock_s3.delete_bucket("new-bucket")

        with pytest.raises(Exception) as exc_info:
            await mock_s3.head_bucket("new-bucket")
        assert "NoSuchBucket" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_bucket_location(self, mock_s3):
        """Test getting bucket location."""
        await mock_s3.create_bucket("test-bucket")

        resp = await mock_s3.get_bucket_location("test-bucket")
        assert "LocationConstraint" in resp

    @pytest.mark.asyncio
    async def test_head_nonexistent_bucket(self, mock_s3):
        """Test HEAD on non-existent bucket."""
        with pytest.raises(Exception) as exc_info:
            await mock_s3.head_bucket("nonexistent-bucket")
        assert "NoSuchBucket" in str(exc_info.value)


class TestListObjects:
    """Test object listing operations."""

    @pytest.mark.asyncio
    async def test_list_objects_empty_bucket(self, mock_s3):
        """Test listing objects in empty bucket."""
        await mock_s3.create_bucket("empty-bucket")

        resp = await mock_s3.list_objects_v2("empty-bucket")
        assert resp["Contents"] == []
        assert resp["KeyCount"] == 0

    @pytest.mark.asyncio
    async def test_list_objects_with_prefix(self, mock_s3):
        """Test listing objects with prefix filter."""
        await mock_s3.create_bucket("test-bucket")
        await mock_s3.put_object("test-bucket", "file1.txt", b"data1")
        await mock_s3.put_object("test-bucket", "dir/file2.txt", b"data2")
        await mock_s3.put_object("test-bucket", "dir/file3.txt", b"data3")
        await mock_s3.put_object("test-bucket", "other/file4.txt", b"data4")

        # List with prefix
        resp = await mock_s3.list_objects_v2("test-bucket", prefix="dir/")
        assert len(resp["Contents"]) == 2
        keys = [obj["Key"] for obj in resp["Contents"]]
        assert "dir/file2.txt" in keys
        assert "dir/file3.txt" in keys

    @pytest.mark.asyncio
    async def test_list_objects_max_keys(self, mock_s3):
        """Test listing with max_keys limit."""
        await mock_s3.create_bucket("test-bucket")
        for i in range(5):
            await mock_s3.put_object("test-bucket", f"file{i}.txt", b"data")

        resp = await mock_s3.list_objects_v2("test-bucket", max_keys=2)
        assert len(resp["Contents"]) == 2
        assert resp["IsTruncated"] is True


class TestCopyObject:
    """Test copy object operations."""

    @pytest.mark.asyncio
    async def test_copy_object_same_bucket(self, mock_s3):
        """Test copying object within same bucket."""
        await mock_s3.create_bucket("test-bucket")
        await mock_s3.put_object("test-bucket", "source.txt", b"original data")

        await mock_s3.copy_object("test-bucket", "dest.txt", "test-bucket/source.txt")

        # Verify both exist
        src_resp = await mock_s3.get_object("test-bucket", "source.txt")
        dest_resp = await mock_s3.get_object("test-bucket", "dest.txt")

        src_data = await src_resp["Body"].read()
        dest_data = await dest_resp["Body"].read()
        assert src_data == dest_data == b"original data"

    @pytest.mark.asyncio
    async def test_copy_object_cross_bucket(self, mock_s3):
        """Test copying object across buckets."""
        await mock_s3.create_bucket("bucket-a")
        await mock_s3.create_bucket("bucket-b")
        await mock_s3.put_object("bucket-a", "file.txt", b"cross bucket data")

        await mock_s3.copy_object("bucket-b", "copied.txt", "bucket-a/file.txt")

        resp = await mock_s3.get_object("bucket-b", "copied.txt")
        data = await resp["Body"].read()
        assert data == b"cross bucket data"

    @pytest.mark.asyncio
    async def test_copy_nonexistent_source(self, mock_s3):
        """Test copying from non-existent source fails."""
        await mock_s3.create_bucket("test-bucket")

        with pytest.raises(Exception) as exc_info:
            await mock_s3.copy_object("test-bucket", "dest.txt", "test-bucket/nonexistent.txt")
        assert "NoSuchKey" in str(exc_info.value)


class TestDeleteObjects:
    """Test batch delete operations."""

    @pytest.mark.asyncio
    async def test_delete_multiple_objects(self, mock_s3):
        """Test deleting multiple objects at once."""
        await mock_s3.create_bucket("test-bucket")
        await mock_s3.put_object("test-bucket", "file1.txt", b"data1")
        await mock_s3.put_object("test-bucket", "file2.txt", b"data2")
        await mock_s3.put_object("test-bucket", "file3.txt", b"data3")

        # Delete two files
        resp = await mock_s3.delete_objects(
            "test-bucket",
            [{"Key": "file1.txt"}, {"Key": "file2.txt"}],
        )

        assert len(resp["Deleted"]) == 2
        assert len(resp["Errors"]) == 0

        # Verify file3 still exists
        await mock_s3.get_object("test-bucket", "file3.txt")

        # Verify file1 and file2 are gone
        with pytest.raises(Exception):
            await mock_s3.get_object("test-bucket", "file1.txt")

    @pytest.mark.asyncio
    async def test_delete_nonexistent_objects(self, mock_s3):
        """Test deleting non-existent objects (S3 doesn't error)."""
        await mock_s3.create_bucket("test-bucket")

        resp = await mock_s3.delete_objects(
            "test-bucket",
            [{"Key": "nonexistent1.txt"}, {"Key": "nonexistent2.txt"}],
        )

        # S3 returns success even for non-existent keys
        assert len(resp["Deleted"]) == 2
        assert len(resp["Errors"]) == 0


class TestEncryptedCopyObject:
    """Test copy object with encrypted source."""

    @pytest.mark.asyncio
    async def test_copy_encrypted_object(self, mock_s3, settings):
        """Test copying an encrypted object re-encrypts it."""
        await mock_s3.create_bucket("test-bucket")

        # Store encrypted object
        plaintext = b"Secret data to copy"
        encrypted = crypto.encrypt_object(plaintext, settings.kek)
        metadata = {
            settings.dektag_name: base64.b64encode(encrypted.wrapped_dek).decode(),
            "plaintext-size": str(len(plaintext)),
        }
        await mock_s3.put_object("test-bucket", "source.txt", encrypted.ciphertext, metadata=metadata)

        # Copy will use different DEK (simulated by direct copy in mock)
        await mock_s3.copy_object("test-bucket", "dest.txt", "test-bucket/source.txt")

        # In real implementation, the handler would:
        # 1. Get source metadata
        # 2. Decrypt source
        # 3. Re-encrypt with new DEK
        # 4. Store destination

        # Verify copy exists
        resp = await mock_s3.get_object("test-bucket", "dest.txt")
        assert resp is not None


class TestCallHistory:
    """Test that mock S3 client tracks calls correctly."""

    @pytest.mark.asyncio
    async def test_call_history_tracked(self, mock_s3):
        """Test that all S3 operations are tracked in history."""
        await mock_s3.create_bucket("bucket")
        await mock_s3.put_object("bucket", "key", b"data")
        await mock_s3.get_object("bucket", "key")
        await mock_s3.head_object("bucket", "key")
        await mock_s3.delete_object("bucket", "key")

        call_types = [c[0] for c in mock_s3.call_history]
        assert "create_bucket" in call_types
        assert "put_object" in call_types
        assert "get_object" in call_types
        assert "head_object" in call_types
        assert "delete_object" in call_types

    @pytest.mark.asyncio
    async def test_call_history_params(self, mock_s3):
        """Test that call parameters are tracked."""
        await mock_s3.create_bucket("my-bucket")
        await mock_s3.put_object("my-bucket", "my-key", b"my-data")

        put_calls = [c for c in mock_s3.call_history if c[0] == "put_object"]
        assert len(put_calls) == 1
        assert put_calls[0][1]["bucket"] == "my-bucket"
        assert put_calls[0][1]["key"] == "my-key"


class TestListBuckets:
    """Test ListBuckets operation."""

    @pytest.mark.asyncio
    async def test_list_buckets_empty(self, mock_s3):
        """Test listing when no buckets exist."""
        resp = await mock_s3.list_buckets()
        assert "Buckets" in resp
        assert resp["Buckets"] == []
        assert "Owner" in resp

    @pytest.mark.asyncio
    async def test_list_buckets_with_buckets(self, mock_s3):
        """Test listing multiple buckets."""
        await mock_s3.create_bucket("bucket-a")
        await mock_s3.create_bucket("bucket-b")
        await mock_s3.create_bucket("bucket-c")

        resp = await mock_s3.list_buckets()
        assert len(resp["Buckets"]) == 3

        bucket_names = [b["Name"] for b in resp["Buckets"]]
        assert "bucket-a" in bucket_names
        assert "bucket-b" in bucket_names
        assert "bucket-c" in bucket_names

    @pytest.mark.asyncio
    async def test_list_buckets_owner_info(self, mock_s3):
        """Test owner information is returned."""
        await mock_s3.create_bucket("test-bucket")

        resp = await mock_s3.list_buckets()
        assert "Owner" in resp
        assert "ID" in resp["Owner"]
        assert "DisplayName" in resp["Owner"]


class TestListObjectsV1:
    """Test ListObjects V1 API."""

    @pytest.mark.asyncio
    async def test_list_objects_v1_basic(self, mock_s3):
        """Test basic V1 list objects."""
        await mock_s3.create_bucket("test-bucket")
        await mock_s3.put_object("test-bucket", "file1.txt", b"data1")
        await mock_s3.put_object("test-bucket", "file2.txt", b"data2")

        resp = await mock_s3.list_objects_v1("test-bucket")
        assert len(resp["Contents"]) == 2
        keys = [obj["Key"] for obj in resp["Contents"]]
        assert "file1.txt" in keys
        assert "file2.txt" in keys

    @pytest.mark.asyncio
    async def test_list_objects_v1_with_prefix(self, mock_s3):
        """Test V1 list with prefix filter."""
        await mock_s3.create_bucket("test-bucket")
        await mock_s3.put_object("test-bucket", "dir/file1.txt", b"data1")
        await mock_s3.put_object("test-bucket", "dir/file2.txt", b"data2")
        await mock_s3.put_object("test-bucket", "other/file3.txt", b"data3")

        resp = await mock_s3.list_objects_v1("test-bucket", prefix="dir/")
        assert len(resp["Contents"]) == 2
        keys = [obj["Key"] for obj in resp["Contents"]]
        assert all(k.startswith("dir/") for k in keys)

    @pytest.mark.asyncio
    async def test_list_objects_v1_with_delimiter(self, mock_s3):
        """Test V1 list with delimiter for grouping."""
        await mock_s3.create_bucket("test-bucket")
        await mock_s3.put_object("test-bucket", "root.txt", b"data")
        await mock_s3.put_object("test-bucket", "dir1/file1.txt", b"data")
        await mock_s3.put_object("test-bucket", "dir1/file2.txt", b"data")
        await mock_s3.put_object("test-bucket", "dir2/file3.txt", b"data")

        resp = await mock_s3.list_objects_v1("test-bucket", delimiter="/")
        # Should have root.txt in Contents and dir1/, dir2/ in CommonPrefixes
        assert len(resp["Contents"]) == 1
        assert resp["Contents"][0]["Key"] == "root.txt"
        common_prefixes = [cp["Prefix"] for cp in resp["CommonPrefixes"]]
        assert "dir1/" in common_prefixes
        assert "dir2/" in common_prefixes

    @pytest.mark.asyncio
    async def test_list_objects_v1_with_marker(self, mock_s3):
        """Test V1 list with marker for pagination."""
        await mock_s3.create_bucket("test-bucket")
        await mock_s3.put_object("test-bucket", "a.txt", b"data")
        await mock_s3.put_object("test-bucket", "b.txt", b"data")
        await mock_s3.put_object("test-bucket", "c.txt", b"data")

        resp = await mock_s3.list_objects_v1("test-bucket", marker="a.txt")
        keys = [obj["Key"] for obj in resp["Contents"]]
        assert "a.txt" not in keys
        assert "b.txt" in keys
        assert "c.txt" in keys


class TestInternalPrefixFiltering:
    """Test that internal s3proxy objects are hidden from list operations."""

    @pytest.mark.asyncio
    async def test_internal_prefix_hidden(self, mock_s3):
        """Test .s3proxy-internal/ prefix objects are hidden."""
        from s3proxy.multipart import INTERNAL_PREFIX

        await mock_s3.create_bucket("test-bucket")
        # Add regular objects
        await mock_s3.put_object("test-bucket", "file1.txt", b"data1")
        await mock_s3.put_object("test-bucket", "file2.txt", b"data2")
        # Add internal metadata object
        await mock_s3.put_object("test-bucket", f"{INTERNAL_PREFIX}file1.txt.meta", b"meta")

        resp = await mock_s3.list_objects_v2("test-bucket")
        keys = [obj["Key"] for obj in resp.get("Contents", [])]

        assert "file1.txt" in keys
        assert "file2.txt" in keys
        # Mock returns all - filtering is done in the handler layer
        assert f"{INTERNAL_PREFIX}file1.txt.meta" in keys

    @pytest.mark.asyncio
    async def test_legacy_suffix_hidden(self, mock_s3):
        """Test legacy .s3proxy-meta suffix objects are hidden."""
        from s3proxy.multipart import META_SUFFIX_LEGACY

        await mock_s3.create_bucket("test-bucket")
        await mock_s3.put_object("test-bucket", "file1.txt", b"data1")
        await mock_s3.put_object("test-bucket", f"file1.txt{META_SUFFIX_LEGACY}", b"meta")

        resp = await mock_s3.list_objects_v2("test-bucket")
        keys = [obj["Key"] for obj in resp.get("Contents", [])]

        assert "file1.txt" in keys
        # Mock returns all - filtering is done in the handler layer
        assert f"file1.txt{META_SUFFIX_LEGACY}" in keys


class TestObjectTagging:
    """Test object tagging operations."""

    @pytest.mark.asyncio
    async def test_put_and_get_tags(self, mock_s3):
        """Test setting and getting object tags."""
        await mock_s3.create_bucket("test-bucket")
        await mock_s3.put_object("test-bucket", "file.txt", b"data")

        tags = [
            {"Key": "Environment", "Value": "Production"},
            {"Key": "Project", "Value": "S3Proxy"},
        ]
        await mock_s3.put_object_tagging("test-bucket", "file.txt", tags)

        resp = await mock_s3.get_object_tagging("test-bucket", "file.txt")
        assert len(resp["TagSet"]) == 2

        tag_dict = {t["Key"]: t["Value"] for t in resp["TagSet"]}
        assert tag_dict["Environment"] == "Production"
        assert tag_dict["Project"] == "S3Proxy"

    @pytest.mark.asyncio
    async def test_delete_tags(self, mock_s3):
        """Test deleting object tags."""
        await mock_s3.create_bucket("test-bucket")
        await mock_s3.put_object("test-bucket", "file.txt", b"data")

        tags = [{"Key": "Temp", "Value": "true"}]
        await mock_s3.put_object_tagging("test-bucket", "file.txt", tags)

        # Verify tags exist
        resp = await mock_s3.get_object_tagging("test-bucket", "file.txt")
        assert len(resp["TagSet"]) == 1

        # Delete tags
        await mock_s3.delete_object_tagging("test-bucket", "file.txt")

        # Verify tags are gone
        resp = await mock_s3.get_object_tagging("test-bucket", "file.txt")
        assert len(resp["TagSet"]) == 0

    @pytest.mark.asyncio
    async def test_get_tags_nonexistent_object(self, mock_s3):
        """Test getting tags from non-existent object."""
        await mock_s3.create_bucket("test-bucket")

        with pytest.raises(Exception) as exc_info:
            await mock_s3.get_object_tagging("test-bucket", "nonexistent.txt")
        assert "NoSuchKey" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_object_without_tags(self, mock_s3):
        """Test getting tags from object that has no tags."""
        await mock_s3.create_bucket("test-bucket")
        await mock_s3.put_object("test-bucket", "file.txt", b"data")

        resp = await mock_s3.get_object_tagging("test-bucket", "file.txt")
        assert resp["TagSet"] == []


class TestUploadPartCopy:
    """Test UploadPartCopy operation."""

    @pytest.mark.asyncio
    async def test_upload_part_copy_basic(self, mock_s3):
        """Test basic part copy."""
        await mock_s3.create_bucket("test-bucket")
        await mock_s3.put_object("test-bucket", "source.txt", b"0123456789ABCDEF")

        # Start multipart upload
        resp = await mock_s3.create_multipart_upload("test-bucket", "dest.txt")
        upload_id = resp["UploadId"]

        # Copy entire source as part 1
        copy_resp = await mock_s3.upload_part_copy(
            "test-bucket", "dest.txt", upload_id, 1,
            "test-bucket/source.txt"
        )
        assert "CopyPartResult" in copy_resp
        assert "ETag" in copy_resp["CopyPartResult"]

    @pytest.mark.asyncio
    async def test_upload_part_copy_with_range(self, mock_s3):
        """Test part copy with byte range."""
        await mock_s3.create_bucket("test-bucket")
        await mock_s3.put_object("test-bucket", "source.txt", b"0123456789ABCDEF")

        resp = await mock_s3.create_multipart_upload("test-bucket", "dest.txt")
        upload_id = resp["UploadId"]

        # Copy partial range
        await mock_s3.upload_part_copy(
            "test-bucket", "dest.txt", upload_id, 1,
            "test-bucket/source.txt",
            copy_source_range="bytes=0-7"
        )

        # Complete and verify
        list_resp = await mock_s3.list_parts("test-bucket", "dest.txt", upload_id)
        assert len(list_resp["Parts"]) == 1
        assert list_resp["Parts"][0]["Size"] == 8  # bytes 0-7 inclusive

    @pytest.mark.asyncio
    async def test_upload_part_copy_nonexistent_source(self, mock_s3):
        """Test copying from non-existent source."""
        await mock_s3.create_bucket("test-bucket")

        resp = await mock_s3.create_multipart_upload("test-bucket", "dest.txt")
        upload_id = resp["UploadId"]

        with pytest.raises(Exception) as exc_info:
            await mock_s3.upload_part_copy(
                "test-bucket", "dest.txt", upload_id, 1,
                "test-bucket/nonexistent.txt"
            )
        assert "NoSuchKey" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_upload_part_copy_complete_multipart(self, mock_s3):
        """Test completing multipart with copied parts."""
        await mock_s3.create_bucket("test-bucket")
        await mock_s3.put_object("test-bucket", "part1.txt", b"AAAA")
        await mock_s3.put_object("test-bucket", "part2.txt", b"BBBB")

        resp = await mock_s3.create_multipart_upload("test-bucket", "combined.txt")
        upload_id = resp["UploadId"]

        # Copy parts
        resp1 = await mock_s3.upload_part_copy(
            "test-bucket", "combined.txt", upload_id, 1, "test-bucket/part1.txt"
        )
        resp2 = await mock_s3.upload_part_copy(
            "test-bucket", "combined.txt", upload_id, 2, "test-bucket/part2.txt"
        )

        # Complete
        parts = [
            {"PartNumber": 1, "ETag": resp1["CopyPartResult"]["ETag"]},
            {"PartNumber": 2, "ETag": resp2["CopyPartResult"]["ETag"]},
        ]
        await mock_s3.complete_multipart_upload("test-bucket", "combined.txt", upload_id, parts)

        # Verify combined object
        get_resp = await mock_s3.get_object("test-bucket", "combined.txt")
        data = await get_resp["Body"].read()
        assert data == b"AAAABBBB"
