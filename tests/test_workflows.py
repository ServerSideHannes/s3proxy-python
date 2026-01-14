"""End-to-end workflow tests simulating real backup/restore scenarios.

These tests simulate real-world usage patterns from database backup tools
like pgBackRest, WAL-G, Scylla Manager, etc.
"""

import base64
import hashlib
from datetime import UTC, datetime

import pytest

from s3proxy import crypto
from s3proxy.multipart import (
    MultipartMetadata,
    MultipartStateManager,
    PartMetadata,
    encode_multipart_metadata,
)


class TestPgBackRestWorkflow:
    """Simulate pgBackRest backup/restore workflow.

    pgBackRest workflow:
    1. Create bucket (if not exists)
    2. Upload manifest files (small)
    3. Upload WAL segments (medium, ~16MB)
    4. Upload backup files (large, multipart)
    5. List objects for restore
    6. Download files for restore
    7. Cleanup old backups (batch delete)
    """

    @pytest.mark.asyncio
    async def test_full_backup_workflow(self, mock_s3, settings):
        """Test complete backup workflow."""
        bucket = "pgbackrest-repo"

        # 1. Create bucket
        await mock_s3.create_bucket(bucket)

        # 2. Upload manifest (small file)
        manifest_content = b'{"backup_label": "20240115-120000F", "start_time": "2024-01-15 12:00:00"}'
        manifest_encrypted = crypto.encrypt_object(manifest_content, settings.kek)
        await mock_s3.put_object(
            bucket,
            "backup/20240115-120000F/backup.manifest",
            manifest_encrypted.ciphertext,
            metadata={
                settings.dektag_name: base64.b64encode(manifest_encrypted.wrapped_dek).decode(),
                "plaintext-size": str(len(manifest_content)),
            },
        )

        # 3. Upload WAL segment (medium file ~16MB simulated with smaller data)
        wal_content = b"WAL segment data " * 1000
        wal_encrypted = crypto.encrypt_object(wal_content, settings.kek)
        await mock_s3.put_object(
            bucket,
            "archive/000000010000000000000001",
            wal_encrypted.ciphertext,
            metadata={
                settings.dektag_name: base64.b64encode(wal_encrypted.wrapped_dek).decode(),
                "plaintext-size": str(len(wal_content)),
            },
        )

        # 4. List objects to verify
        list_resp = await mock_s3.list_objects_v2(bucket)
        keys = [obj["Key"] for obj in list_resp["Contents"]]
        assert "backup/20240115-120000F/backup.manifest" in keys
        assert "archive/000000010000000000000001" in keys

        # 5. Download and decrypt for restore
        get_resp = await mock_s3.get_object(bucket, "backup/20240115-120000F/backup.manifest")
        ciphertext = await get_resp["Body"].read()
        wrapped_dek = base64.b64decode(get_resp["Metadata"][settings.dektag_name])
        decrypted = crypto.decrypt_object(ciphertext, wrapped_dek, settings.kek)
        assert decrypted == manifest_content

    @pytest.mark.asyncio
    async def test_retention_cleanup(self, mock_s3, settings):
        """Test batch delete for retention policy cleanup."""
        bucket = "pgbackrest-repo"
        await mock_s3.create_bucket(bucket)

        # Create old backup files
        for i in range(5):
            content = f"old backup {i}".encode()
            encrypted = crypto.encrypt_object(content, settings.kek)
            await mock_s3.put_object(
                bucket,
                f"backup/old-backup-{i}/data.bin",
                encrypted.ciphertext,
                metadata={settings.dektag_name: base64.b64encode(encrypted.wrapped_dek).decode()},
            )

        # Batch delete old backups
        objects_to_delete = [{"Key": f"backup/old-backup-{i}/data.bin"} for i in range(3)]
        resp = await mock_s3.delete_objects(bucket, objects_to_delete)

        assert len(resp["Deleted"]) == 3
        assert len(resp["Errors"]) == 0

        # Verify remaining
        list_resp = await mock_s3.list_objects_v2(bucket)
        remaining_keys = [obj["Key"] for obj in list_resp["Contents"]]
        assert len(remaining_keys) == 2


class TestWALGWorkflow:
    """Simulate WAL-G backup/restore workflow.

    WAL-G workflow:
    1. Upload base backup segments
    2. Upload WAL files
    3. Copy objects for delta backups (uses CopyObject)
    4. List backups
    5. Restore specific backup
    """

    @pytest.mark.asyncio
    async def test_delta_backup_with_copy(self, mock_s3, settings):
        """Test delta backup using copy object."""
        bucket = "walg-repo"
        await mock_s3.create_bucket(bucket)

        # Upload base backup
        base_content = b"base backup data " * 100
        base_encrypted = crypto.encrypt_object(base_content, settings.kek)
        await mock_s3.put_object(
            bucket,
            "basebackups/base_000000010000000000000001/data.tar",
            base_encrypted.ciphertext,
            metadata={
                settings.dektag_name: base64.b64encode(base_encrypted.wrapped_dek).decode(),
                "plaintext-size": str(len(base_content)),
            },
        )

        # Copy for delta (unchanged files reference base)
        await mock_s3.copy_object(
            bucket,
            "basebackups/base_000000010000000000000002/data.tar",
            f"{bucket}/basebackups/base_000000010000000000000001/data.tar",
        )

        # Verify both exist
        list_resp = await mock_s3.list_objects_v2(bucket, prefix="basebackups/")
        keys = [obj["Key"] for obj in list_resp["Contents"]]
        assert len(keys) == 2

    @pytest.mark.asyncio
    async def test_wal_archiving(self, mock_s3, settings):
        """Test WAL file archiving."""
        bucket = "walg-repo"
        await mock_s3.create_bucket(bucket)

        # Archive multiple WAL files
        for segment_num in range(5):
            wal_content = f"WAL segment {segment_num:08d}".encode() * 100
            wal_encrypted = crypto.encrypt_object(wal_content, settings.kek)
            await mock_s3.put_object(
                bucket,
                f"wal_005/00000001000000000000000{segment_num}",
                wal_encrypted.ciphertext,
                metadata={
                    settings.dektag_name: base64.b64encode(wal_encrypted.wrapped_dek).decode(),
                    "plaintext-size": str(len(wal_content)),
                },
            )

        # List WAL files
        list_resp = await mock_s3.list_objects_v2(bucket, prefix="wal_005/")
        assert len(list_resp["Contents"]) == 5


class TestScyllaManagerWorkflow:
    """Simulate Scylla Manager (sctool) backup/restore workflow.

    Scylla Manager workflow:
    1. Create multipart upload for large SSTables
    2. Upload parts
    3. List multipart uploads (for resume after failure)
    4. List parts (for resume after failure)
    5. Complete upload
    6. Restore: download with range requests
    """

    @pytest.mark.asyncio
    async def test_sstable_multipart_upload(self, mock_s3, settings):
        """Test multipart upload for large SSTables."""
        bucket = "scylla-backup"
        key = "keyspace/table/mc-1-big-Data.db"
        await mock_s3.create_bucket(bucket)

        # Initiate multipart upload
        init_resp = await mock_s3.create_multipart_upload(bucket, key)
        upload_id = init_resp["UploadId"]

        # Generate encryption key for this upload
        dek = crypto.generate_dek()
        wrapped_dek = crypto.wrap_key(dek, settings.kek)

        # Upload parts (simulated SSTable chunks)
        part_etags = []
        for part_num in range(1, 4):
            part_content = f"SSTable part {part_num}".encode() * 10000
            part_encrypted = crypto.encrypt_part(part_content, dek, upload_id, part_num)
            resp = await mock_s3.upload_part(bucket, key, upload_id, part_num, part_encrypted)
            part_etags.append({"PartNumber": part_num, "ETag": resp["ETag"]})

        # Complete multipart upload
        await mock_s3.complete_multipart_upload(bucket, key, upload_id, part_etags)

        # Verify object exists
        head_resp = await mock_s3.head_object(bucket, key)
        assert head_resp["ContentLength"] > 0

    @pytest.mark.asyncio
    async def test_resume_failed_upload(self, mock_s3):
        """Test resuming a failed multipart upload."""
        bucket = "scylla-backup"
        key = "keyspace/table/mc-2-big-Data.db"
        await mock_s3.create_bucket(bucket)

        # Start upload
        init_resp = await mock_s3.create_multipart_upload(bucket, key)
        upload_id = init_resp["UploadId"]

        # Upload first part
        await mock_s3.upload_part(bucket, key, upload_id, 1, b"part1" * 1000)

        # Simulate failure - list uploads to find incomplete
        list_uploads_resp = await mock_s3.list_multipart_uploads(bucket)
        assert len(list_uploads_resp["Uploads"]) == 1
        found_upload_id = list_uploads_resp["Uploads"][0]["UploadId"]
        assert found_upload_id == upload_id

        # List parts to see what's already uploaded
        list_parts_resp = await mock_s3.list_parts(bucket, key, upload_id)
        assert len(list_parts_resp["Parts"]) == 1
        assert list_parts_resp["Parts"][0]["PartNumber"] == 1

        # Resume by uploading remaining parts
        await mock_s3.upload_part(bucket, key, upload_id, 2, b"part2" * 1000)
        await mock_s3.upload_part(bucket, key, upload_id, 3, b"part3" * 1000)

        # Complete
        parts = [
            {"PartNumber": 1, "ETag": '"part1-etag"'},
            {"PartNumber": 2, "ETag": '"part2-etag"'},
            {"PartNumber": 3, "ETag": '"part3-etag"'},
        ]
        await mock_s3.complete_multipart_upload(bucket, key, upload_id, parts)

        # Verify no more uploads pending
        list_uploads_resp = await mock_s3.list_multipart_uploads(bucket)
        assert len(list_uploads_resp.get("Uploads", [])) == 0


class TestClickHouseBackupWorkflow:
    """Simulate ClickHouse backup workflow.

    ClickHouse backup workflow:
    1. List existing backups
    2. Upload backup metadata
    3. Upload data parts
    4. Download for restore
    """

    @pytest.mark.asyncio
    async def test_backup_metadata(self, mock_s3, settings):
        """Test backing up ClickHouse metadata."""
        bucket = "clickhouse-backup"
        await mock_s3.create_bucket(bucket)

        # Upload backup metadata
        metadata = {
            "version": "1.0",
            "tables": ["default.events", "default.users"],
            "size": 1024000,
        }
        metadata_bytes = str(metadata).encode()
        encrypted = crypto.encrypt_object(metadata_bytes, settings.kek)

        await mock_s3.put_object(
            bucket,
            "backups/2024-01-15/metadata.json",
            encrypted.ciphertext,
            metadata={
                settings.dektag_name: base64.b64encode(encrypted.wrapped_dek).decode(),
                "plaintext-size": str(len(metadata_bytes)),
            },
            content_type="application/json",
        )

        # List backups
        list_resp = await mock_s3.list_objects_v2(bucket, prefix="backups/")
        assert len(list_resp["Contents"]) == 1


class TestElasticsearchSnapshotWorkflow:
    """Simulate Elasticsearch snapshot workflow.

    ES snapshot workflow:
    1. Create repository bucket
    2. Upload snapshot metadata
    3. Upload index data (potentially large)
    4. Download for restore
    """

    @pytest.mark.asyncio
    async def test_snapshot_to_s3(self, mock_s3, settings):
        """Test creating an ES snapshot to S3."""
        bucket = "es-snapshots"
        await mock_s3.create_bucket(bucket)

        # Get bucket location (ES checks this)
        location_resp = await mock_s3.get_bucket_location(bucket)
        assert "LocationConstraint" in location_resp

        # Upload snapshot metadata
        snapshot_metadata = b'{"snapshot_id": "snapshot_1", "indices": ["logs-2024.01"]}'
        encrypted = crypto.encrypt_object(snapshot_metadata, settings.kek)

        await mock_s3.put_object(
            bucket,
            "indices/logs-2024.01/meta-abc123.dat",
            encrypted.ciphertext,
            metadata={
                settings.dektag_name: base64.b64encode(encrypted.wrapped_dek).decode(),
            },
        )

        # Upload index data
        index_data = b"lucene index data " * 1000
        index_encrypted = crypto.encrypt_object(index_data, settings.kek)

        await mock_s3.put_object(
            bucket,
            "indices/logs-2024.01/__0/snap-abc123.dat",
            index_encrypted.ciphertext,
            metadata={
                settings.dektag_name: base64.b64encode(index_encrypted.wrapped_dek).decode(),
            },
        )

        # List snapshot contents
        list_resp = await mock_s3.list_objects_v2(bucket, prefix="indices/")
        assert len(list_resp["Contents"]) == 2


class TestBarmanCloudWorkflow:
    """Simulate Barman Cloud (CloudNativePG) backup workflow."""

    @pytest.mark.asyncio
    async def test_base_backup(self, mock_s3, settings):
        """Test Barman Cloud base backup."""
        bucket = "barman-backup"
        await mock_s3.create_bucket(bucket)

        # Check bucket exists (barman does this)
        await mock_s3.head_bucket(bucket)

        # Upload backup label
        backup_label = b"START WAL LOCATION: 0/1000000 (file 000000010000000000000001)"
        encrypted = crypto.encrypt_object(backup_label, settings.kek)

        await mock_s3.put_object(
            bucket,
            "base/20240115T120000/backup_label",
            encrypted.ciphertext,
            metadata={
                settings.dektag_name: base64.b64encode(encrypted.wrapped_dek).decode(),
            },
        )

        # Upload data directory tarball
        data_tar = b"postgres data directory contents " * 1000
        data_encrypted = crypto.encrypt_object(data_tar, settings.kek)

        await mock_s3.put_object(
            bucket,
            "base/20240115T120000/data.tar.gz",
            data_encrypted.ciphertext,
            metadata={
                settings.dektag_name: base64.b64encode(data_encrypted.wrapped_dek).decode(),
                "plaintext-size": str(len(data_tar)),
            },
        )

        # List base backups
        list_resp = await mock_s3.list_objects_v2(bucket, prefix="base/")
        keys = [obj["Key"] for obj in list_resp["Contents"]]
        assert len(keys) == 2


class TestEncryptionKeyRotation:
    """Test scenarios involving key rotation."""

    @pytest.mark.asyncio
    async def test_read_with_original_key(self, mock_s3, settings):
        """Test reading data encrypted with original key still works."""
        bucket = "key-rotation-test"
        await mock_s3.create_bucket(bucket)

        # Encrypt with current key
        plaintext = b"data encrypted before key rotation"
        encrypted = crypto.encrypt_object(plaintext, settings.kek)

        await mock_s3.put_object(
            bucket,
            "old-data.bin",
            encrypted.ciphertext,
            metadata={
                settings.dektag_name: base64.b64encode(encrypted.wrapped_dek).decode(),
            },
        )

        # Read back with same key
        get_resp = await mock_s3.get_object(bucket, "old-data.bin")
        ciphertext = await get_resp["Body"].read()
        wrapped_dek = base64.b64decode(get_resp["Metadata"][settings.dektag_name])

        decrypted = crypto.decrypt_object(ciphertext, wrapped_dek, settings.kek)
        assert decrypted == plaintext


class TestLargeFileScenarios:
    """Test scenarios with large files."""

    @pytest.mark.asyncio
    async def test_multipart_encryption_consistency(self, mock_s3, settings):
        """Test multipart upload produces consistent encryption."""
        bucket = "large-files"
        key = "big-backup.tar.gz"
        await mock_s3.create_bucket(bucket)

        # Initiate upload
        init_resp = await mock_s3.create_multipart_upload(bucket, key)
        upload_id = init_resp["UploadId"]

        # Use same DEK for all parts
        dek = crypto.generate_dek()

        # Encrypt multiple parts
        parts_plaintext = [
            b"Part 1 data " * 10000,
            b"Part 2 data " * 10000,
            b"Part 3 data " * 5000,
        ]

        part_etags = []
        for i, part_data in enumerate(parts_plaintext, 1):
            encrypted_part = crypto.encrypt_part(part_data, dek, upload_id, i)
            resp = await mock_s3.upload_part(bucket, key, upload_id, i, encrypted_part)
            part_etags.append({"PartNumber": i, "ETag": resp["ETag"]})

            # Verify we can decrypt each part individually
            decrypted = crypto.decrypt_part(encrypted_part, dek, upload_id, i)
            assert decrypted == part_data

        # Complete upload
        await mock_s3.complete_multipart_upload(bucket, key, upload_id, part_etags)
