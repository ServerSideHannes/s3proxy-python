"""Tests for S3 XML response builders."""

import xml.etree.ElementTree as ET

import pytest

from s3proxy import xml_responses


class TestInitiateMultipart:
    """Test InitiateMultipartUploadResult XML."""

    def test_basic_response(self):
        """Test basic initiate multipart response."""
        xml = xml_responses.initiate_multipart("my-bucket", "my-key", "upload-123")

        root = ET.fromstring(xml)
        assert root.find("{http://s3.amazonaws.com/doc/2006-03-01/}Bucket").text == "my-bucket"
        assert root.find("{http://s3.amazonaws.com/doc/2006-03-01/}Key").text == "my-key"
        assert root.find("{http://s3.amazonaws.com/doc/2006-03-01/}UploadId").text == "upload-123"

    def test_special_characters_in_key(self):
        """Test key with special characters."""
        xml = xml_responses.initiate_multipart("bucket", "path/to/file with spaces.txt", "id")
        root = ET.fromstring(xml)
        assert "path/to/file with spaces.txt" in root.find("{http://s3.amazonaws.com/doc/2006-03-01/}Key").text


class TestCompleteMultipart:
    """Test CompleteMultipartUploadResult XML."""

    def test_basic_response(self):
        """Test basic complete multipart response."""
        xml = xml_responses.complete_multipart(
            "https://bucket.s3.amazonaws.com/key",
            "my-bucket",
            "my-key",
            "abc123def456",
        )

        root = ET.fromstring(xml)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        assert root.find(f"{ns}Location").text == "https://bucket.s3.amazonaws.com/key"
        assert root.find(f"{ns}Bucket").text == "my-bucket"
        assert root.find(f"{ns}Key").text == "my-key"
        assert '"abc123def456"' in root.find(f"{ns}ETag").text


class TestListObjects:
    """Test ListBucketResult XML."""

    def test_empty_bucket(self):
        """Test listing empty bucket."""
        xml = xml_responses.list_objects(
            bucket="my-bucket",
            prefix="",
            max_keys=1000,
            is_truncated=False,
            next_token=None,
            objects=[],
        )

        root = ET.fromstring(xml)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        assert root.find(f"{ns}Name").text == "my-bucket"
        assert root.find(f"{ns}KeyCount").text == "0"
        assert root.find(f"{ns}IsTruncated").text == "false"

    def test_with_objects(self):
        """Test listing with objects."""
        objects = [
            {"key": "file1.txt", "last_modified": "2024-01-15T10:00:00Z", "etag": "abc", "size": 100},
            {"key": "file2.txt", "last_modified": "2024-01-15T11:00:00Z", "etag": "def", "size": 200},
        ]
        xml = xml_responses.list_objects(
            bucket="my-bucket",
            prefix="",
            max_keys=1000,
            is_truncated=False,
            next_token=None,
            objects=objects,
        )

        root = ET.fromstring(xml)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        contents = root.findall(f"{ns}Contents")
        assert len(contents) == 2
        assert root.find(f"{ns}KeyCount").text == "2"

    def test_truncated_with_token(self):
        """Test truncated response with continuation token."""
        xml = xml_responses.list_objects(
            bucket="my-bucket",
            prefix="prefix/",
            max_keys=100,
            is_truncated=True,
            next_token="next-token-abc",
            objects=[{"key": "file.txt", "last_modified": "2024-01-15T10:00:00Z", "etag": "abc", "size": 100}],
        )

        root = ET.fromstring(xml)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        assert root.find(f"{ns}IsTruncated").text == "true"
        assert root.find(f"{ns}NextContinuationToken").text == "next-token-abc"


class TestLocationConstraint:
    """Test GetBucketLocation XML."""

    def test_us_east_1(self):
        """Test us-east-1 returns empty LocationConstraint."""
        xml = xml_responses.location_constraint("us-east-1")
        root = ET.fromstring(xml)
        # us-east-1 returns empty element
        assert root.text is None or root.text.strip() == ""

    def test_none_location(self):
        """Test None location returns empty LocationConstraint."""
        xml = xml_responses.location_constraint(None)
        root = ET.fromstring(xml)
        assert root.text is None or root.text.strip() == ""

    def test_other_region(self):
        """Test other regions return region name."""
        xml = xml_responses.location_constraint("eu-west-1")
        root = ET.fromstring(xml)
        assert root.text == "eu-west-1"

    def test_ap_region(self):
        """Test Asia Pacific region."""
        xml = xml_responses.location_constraint("ap-northeast-1")
        root = ET.fromstring(xml)
        assert root.text == "ap-northeast-1"


class TestCopyObjectResult:
    """Test CopyObjectResult XML."""

    def test_basic_response(self):
        """Test basic copy object response."""
        xml = xml_responses.copy_object_result("abc123", "2024-01-15T10:30:00.000Z")

        root = ET.fromstring(xml)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        assert '"abc123"' in root.find(f"{ns}ETag").text
        assert root.find(f"{ns}LastModified").text == "2024-01-15T10:30:00.000Z"


class TestDeleteObjectsResult:
    """Test DeleteResult XML."""

    def test_all_deleted(self):
        """Test all objects deleted successfully."""
        deleted = [
            {"Key": "file1.txt", "VersionId": ""},
            {"Key": "file2.txt", "VersionId": ""},
        ]
        xml = xml_responses.delete_objects_result(deleted, [], quiet=False)

        root = ET.fromstring(xml)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        deleted_elements = root.findall(f"{ns}Deleted")
        assert len(deleted_elements) == 2

    def test_with_errors(self):
        """Test response with errors."""
        deleted = [{"Key": "file1.txt", "VersionId": ""}]
        errors = [{"Key": "file2.txt", "Code": "AccessDenied", "Message": "Access Denied"}]
        xml = xml_responses.delete_objects_result(deleted, errors, quiet=False)

        root = ET.fromstring(xml)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        assert len(root.findall(f"{ns}Deleted")) == 1
        assert len(root.findall(f"{ns}Error")) == 1

    def test_quiet_mode(self):
        """Test quiet mode only returns errors."""
        deleted = [{"Key": "file1.txt", "VersionId": ""}]
        errors = [{"Key": "file2.txt", "Code": "AccessDenied", "Message": "Access Denied"}]
        xml = xml_responses.delete_objects_result(deleted, errors, quiet=True)

        root = ET.fromstring(xml)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        # Quiet mode should not include deleted items
        assert len(root.findall(f"{ns}Deleted")) == 0
        assert len(root.findall(f"{ns}Error")) == 1

    def test_special_characters_escaped(self):
        """Test special characters are escaped."""
        deleted = [{"Key": "file<>&.txt", "VersionId": ""}]
        xml = xml_responses.delete_objects_result(deleted, [], quiet=False)

        # Should parse without error (special chars escaped)
        root = ET.fromstring(xml)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        key = root.find(f"{ns}Deleted/{ns}Key").text
        assert key == "file<>&.txt"


class TestListMultipartUploads:
    """Test ListMultipartUploadsResult XML."""

    def test_empty_list(self):
        """Test empty uploads list."""
        xml = xml_responses.list_multipart_uploads(
            bucket="my-bucket",
            uploads=[],
            key_marker=None,
            upload_id_marker=None,
            next_key_marker=None,
            next_upload_id_marker=None,
            max_uploads=1000,
            is_truncated=False,
        )

        root = ET.fromstring(xml)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        assert root.find(f"{ns}Bucket").text == "my-bucket"
        assert root.find(f"{ns}IsTruncated").text == "false"
        assert len(root.findall(f"{ns}Upload")) == 0

    def test_with_uploads(self):
        """Test with active uploads."""
        uploads = [
            {"Key": "big-file.tar", "UploadId": "abc123", "Initiated": "2024-01-15T10:00:00Z", "StorageClass": "STANDARD"},
            {"Key": "another.zip", "UploadId": "def456", "Initiated": "2024-01-15T11:00:00Z", "StorageClass": "STANDARD"},
        ]
        xml = xml_responses.list_multipart_uploads(
            bucket="my-bucket",
            uploads=uploads,
            key_marker=None,
            upload_id_marker=None,
            next_key_marker=None,
            next_upload_id_marker=None,
            max_uploads=1000,
            is_truncated=False,
        )

        root = ET.fromstring(xml)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        upload_elements = root.findall(f"{ns}Upload")
        assert len(upload_elements) == 2

    def test_truncated_with_markers(self):
        """Test truncated response with next markers."""
        xml = xml_responses.list_multipart_uploads(
            bucket="my-bucket",
            uploads=[{"Key": "file.tar", "UploadId": "abc", "Initiated": "2024-01-15T10:00:00Z"}],
            key_marker="start-key",
            upload_id_marker="start-upload",
            next_key_marker="next-key",
            next_upload_id_marker="next-upload",
            max_uploads=1,
            is_truncated=True,
        )

        root = ET.fromstring(xml)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        assert root.find(f"{ns}IsTruncated").text == "true"
        assert "next-key" in xml
        assert "next-upload" in xml

    def test_with_prefix(self):
        """Test with prefix filter."""
        xml = xml_responses.list_multipart_uploads(
            bucket="my-bucket",
            uploads=[],
            key_marker=None,
            upload_id_marker=None,
            next_key_marker=None,
            next_upload_id_marker=None,
            max_uploads=1000,
            is_truncated=False,
            prefix="backups/",
        )

        root = ET.fromstring(xml)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        assert root.find(f"{ns}Prefix").text == "backups/"


class TestListParts:
    """Test ListPartsResult XML."""

    def test_empty_parts(self):
        """Test empty parts list."""
        xml = xml_responses.list_parts(
            bucket="my-bucket",
            key="my-key",
            upload_id="upload-123",
            parts=[],
            part_number_marker=None,
            next_part_number_marker=None,
            max_parts=1000,
            is_truncated=False,
        )

        root = ET.fromstring(xml)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        assert root.find(f"{ns}Bucket").text == "my-bucket"
        assert root.find(f"{ns}Key").text == "my-key"
        assert root.find(f"{ns}UploadId").text == "upload-123"
        assert len(root.findall(f"{ns}Part")) == 0

    def test_with_parts(self):
        """Test with uploaded parts."""
        parts = [
            {"PartNumber": 1, "LastModified": "2024-01-15T10:00:00Z", "ETag": "abc", "Size": 5242880},
            {"PartNumber": 2, "LastModified": "2024-01-15T10:01:00Z", "ETag": "def", "Size": 5242880},
            {"PartNumber": 3, "LastModified": "2024-01-15T10:02:00Z", "ETag": "ghi", "Size": 1234567},
        ]
        xml = xml_responses.list_parts(
            bucket="my-bucket",
            key="my-key",
            upload_id="upload-123",
            parts=parts,
            part_number_marker=None,
            next_part_number_marker=None,
            max_parts=1000,
            is_truncated=False,
        )

        root = ET.fromstring(xml)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        part_elements = root.findall(f"{ns}Part")
        assert len(part_elements) == 3

        # Check first part
        first_part = part_elements[0]
        assert first_part.find(f"{ns}PartNumber").text == "1"
        assert first_part.find(f"{ns}Size").text == "5242880"

    def test_truncated(self):
        """Test truncated parts list."""
        parts = [{"PartNumber": 1, "LastModified": "2024-01-15T10:00:00Z", "ETag": "abc", "Size": 5242880}]
        xml = xml_responses.list_parts(
            bucket="my-bucket",
            key="my-key",
            upload_id="upload-123",
            parts=parts,
            part_number_marker=0,
            next_part_number_marker=1,
            max_parts=1,
            is_truncated=True,
        )

        root = ET.fromstring(xml)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        assert root.find(f"{ns}IsTruncated").text == "true"
        assert root.find(f"{ns}MaxParts").text == "1"

    def test_key_with_special_chars(self):
        """Test key with special characters is escaped."""
        xml = xml_responses.list_parts(
            bucket="bucket",
            key="path/to/file<>&.txt",
            upload_id="id",
            parts=[],
            part_number_marker=None,
            next_part_number_marker=None,
            max_parts=1000,
            is_truncated=False,
        )

        # Should parse without error
        root = ET.fromstring(xml)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        assert root.find(f"{ns}Key").text == "path/to/file<>&.txt"


class TestListBuckets:
    """Test ListAllMyBucketsResult XML."""

    def test_empty_buckets(self):
        """Test listing no buckets."""
        xml = xml_responses.list_buckets(
            owner={"ID": "owner-123", "DisplayName": "test-user"},
            buckets=[],
        )

        root = ET.fromstring(xml)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        assert root.find(f"{ns}Owner/{ns}ID").text == "owner-123"
        assert root.find(f"{ns}Owner/{ns}DisplayName").text == "test-user"
        assert len(root.findall(f"{ns}Buckets/{ns}Bucket")) == 0

    def test_with_buckets(self):
        """Test listing multiple buckets."""
        buckets = [
            {"Name": "bucket-a", "CreationDate": "2024-01-15T10:00:00Z"},
            {"Name": "bucket-b", "CreationDate": "2024-01-16T10:00:00Z"},
        ]
        xml = xml_responses.list_buckets(
            owner={"ID": "owner-123", "DisplayName": "test-user"},
            buckets=buckets,
        )

        root = ET.fromstring(xml)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        bucket_elements = root.findall(f"{ns}Buckets/{ns}Bucket")
        assert len(bucket_elements) == 2

        names = [b.find(f"{ns}Name").text for b in bucket_elements]
        assert "bucket-a" in names
        assert "bucket-b" in names

    def test_bucket_with_datetime(self):
        """Test bucket with datetime object for CreationDate."""
        from datetime import datetime, UTC
        buckets = [{"Name": "test", "CreationDate": datetime(2024, 1, 15, 10, 0, 0, tzinfo=UTC)}]
        xml = xml_responses.list_buckets(
            owner={"ID": "id", "DisplayName": "name"},
            buckets=buckets,
        )

        # Should parse without error
        root = ET.fromstring(xml)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        assert root.find(f"{ns}Buckets/{ns}Bucket/{ns}Name").text == "test"


class TestListObjectsV1:
    """Test ListBucketResult XML for V1 API."""

    def test_empty_bucket(self):
        """Test empty bucket response."""
        xml = xml_responses.list_objects_v1(
            bucket="my-bucket",
            prefix="",
            marker=None,
            delimiter=None,
            max_keys=1000,
            is_truncated=False,
            next_marker=None,
            objects=[],
        )

        root = ET.fromstring(xml)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        assert root.find(f"{ns}Name").text == "my-bucket"
        assert root.find(f"{ns}IsTruncated").text == "false"
        assert len(root.findall(f"{ns}Contents")) == 0

    def test_with_objects(self):
        """Test V1 list with objects."""
        objects = [
            {"key": "file1.txt", "last_modified": "2024-01-15T10:00:00Z", "etag": "abc", "size": 100},
            {"key": "file2.txt", "last_modified": "2024-01-15T11:00:00Z", "etag": "def", "size": 200},
        ]
        xml = xml_responses.list_objects_v1(
            bucket="my-bucket",
            prefix="",
            marker=None,
            delimiter=None,
            max_keys=1000,
            is_truncated=False,
            next_marker=None,
            objects=objects,
        )

        root = ET.fromstring(xml)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        contents = root.findall(f"{ns}Contents")
        assert len(contents) == 2

    def test_with_marker(self):
        """Test V1 list with marker."""
        xml = xml_responses.list_objects_v1(
            bucket="my-bucket",
            prefix="",
            marker="start-key",
            delimiter=None,
            max_keys=100,
            is_truncated=True,
            next_marker="next-key",
            objects=[{"key": "file.txt", "last_modified": "2024-01-15T10:00:00Z", "etag": "abc", "size": 100}],
        )

        root = ET.fromstring(xml)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        assert root.find(f"{ns}Marker").text == "start-key"
        assert root.find(f"{ns}NextMarker").text == "next-key"
        assert root.find(f"{ns}IsTruncated").text == "true"

    def test_with_delimiter_and_prefixes(self):
        """Test V1 list with delimiter and common prefixes."""
        xml = xml_responses.list_objects_v1(
            bucket="my-bucket",
            prefix="",
            marker=None,
            delimiter="/",
            max_keys=1000,
            is_truncated=False,
            next_marker=None,
            objects=[{"key": "root.txt", "last_modified": "2024-01-15T10:00:00Z", "etag": "abc", "size": 100}],
            common_prefixes=["dir1/", "dir2/"],
        )

        root = ET.fromstring(xml)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        assert root.find(f"{ns}Delimiter").text == "/"
        prefixes = root.findall(f"{ns}CommonPrefixes/{ns}Prefix")
        assert len(prefixes) == 2
        prefix_values = [p.text for p in prefixes]
        assert "dir1/" in prefix_values
        assert "dir2/" in prefix_values


class TestGetTagging:
    """Test GetObjectTaggingResult XML."""

    def test_empty_tags(self):
        """Test empty tag set."""
        xml = xml_responses.get_tagging(tags=[])

        root = ET.fromstring(xml)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        assert len(root.findall(f"{ns}TagSet/{ns}Tag")) == 0

    def test_with_tags(self):
        """Test with multiple tags."""
        tags = [
            {"Key": "Environment", "Value": "Production"},
            {"Key": "Project", "Value": "S3Proxy"},
        ]
        xml = xml_responses.get_tagging(tags=tags)

        root = ET.fromstring(xml)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        tag_elements = root.findall(f"{ns}TagSet/{ns}Tag")
        assert len(tag_elements) == 2

        # Check tag values
        tag_dict = {}
        for tag in tag_elements:
            key = tag.find(f"{ns}Key").text
            value = tag.find(f"{ns}Value").text
            tag_dict[key] = value

        assert tag_dict["Environment"] == "Production"
        assert tag_dict["Project"] == "S3Proxy"

    def test_special_characters_escaped(self):
        """Test special characters in tags are escaped."""
        tags = [{"Key": "key<>&", "Value": "value<>&"}]
        xml = xml_responses.get_tagging(tags=tags)

        # Should parse without error
        root = ET.fromstring(xml)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        tag = root.find(f"{ns}TagSet/{ns}Tag")
        assert tag.find(f"{ns}Key").text == "key<>&"
        assert tag.find(f"{ns}Value").text == "value<>&"


class TestUploadPartCopyResult:
    """Test CopyPartResult XML."""

    def test_basic_response(self):
        """Test basic copy part result."""
        xml = xml_responses.upload_part_copy_result("abc123", "2024-01-15T10:30:00.000Z")

        root = ET.fromstring(xml)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        assert '"abc123"' in root.find(f"{ns}ETag").text
        assert root.find(f"{ns}LastModified").text == "2024-01-15T10:30:00.000Z"
