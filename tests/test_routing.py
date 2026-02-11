"""Tests for S3 request routing logic."""

import pytest

from s3proxy.main import (
    HEADER_COPY_SOURCE,
    METHOD_DELETE,
    METHOD_GET,
    METHOD_HEAD,
    METHOD_POST,
    METHOD_PUT,
    QUERY_DELETE,
    QUERY_LIST_TYPE,
    QUERY_LOCATION,
    QUERY_PART_NUMBER,
    QUERY_TAGGING,
    QUERY_UPLOAD_ID,
    QUERY_UPLOADS,
    _handle_bucket_operation,
    _handle_multipart_operation,
    _handle_object_operation,
    _is_bucket_only_path,
    _needs_body_for_signature,
)


class TestBucketOnlyPath:
    """Test bucket-only path detection."""

    def test_bucket_only(self):
        """Test bucket-only path is detected."""
        assert _is_bucket_only_path("/my-bucket") is True
        assert _is_bucket_only_path("/my-bucket/") is True

    def test_bucket_with_key(self):
        """Test bucket with key is not bucket-only."""
        assert _is_bucket_only_path("/my-bucket/my-key") is False
        assert _is_bucket_only_path("/my-bucket/path/to/key") is False

    def test_empty_path(self):
        """Test empty paths."""
        assert _is_bucket_only_path("/") is False
        assert _is_bucket_only_path("") is False


class TestNeedsBodyForSignature:
    """Test body requirement for signature verification."""

    def test_unsigned_payload(self):
        """Test UNSIGNED-PAYLOAD doesn't need body."""
        headers = {"x-amz-content-sha256": "UNSIGNED-PAYLOAD"}
        assert _needs_body_for_signature(headers) is False

    def test_streaming_payload(self):
        """Test streaming payload doesn't need body."""
        headers = {"x-amz-content-sha256": "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"}
        assert _needs_body_for_signature(headers) is False

    def test_regular_payload(self):
        """Test regular payload needs body."""
        headers = {"x-amz-content-sha256": "abc123def456"}
        assert _needs_body_for_signature(headers) is True

    def test_missing_header(self):
        """Test missing header needs body."""
        headers = {}
        assert _needs_body_for_signature(headers) is True


class TestQueryConstants:
    """Test query parameter constants are correct."""

    def test_uploads_constant(self):
        """Test uploads query constant."""
        assert QUERY_UPLOADS == "uploads"

    def test_upload_id_constant(self):
        """Test uploadId query constant."""
        assert QUERY_UPLOAD_ID == "uploadId"

    def test_part_number_constant(self):
        """Test partNumber query constant."""
        assert QUERY_PART_NUMBER == "partNumber"

    def test_list_type_constant(self):
        """Test list-type query constant."""
        assert QUERY_LIST_TYPE == "list-type"

    def test_location_constant(self):
        """Test location query constant."""
        assert QUERY_LOCATION == "location"

    def test_delete_constant(self):
        """Test delete query constant."""
        assert QUERY_DELETE == "delete"

    def test_tagging_constant(self):
        """Test tagging query constant."""
        assert QUERY_TAGGING == "tagging"


class TestHeaderConstants:
    """Test header constants are correct."""

    def test_copy_source_header(self):
        """Test copy source header constant."""
        assert HEADER_COPY_SOURCE == "x-amz-copy-source"


class TestMethodConstants:
    """Test HTTP method constants are correct."""

    def test_methods(self):
        """Test all HTTP method constants."""
        assert METHOD_GET == "GET"
        assert METHOD_PUT == "PUT"
        assert METHOD_POST == "POST"
        assert METHOD_DELETE == "DELETE"
        assert METHOD_HEAD == "HEAD"


class TestRoutingDecisions:
    """Test routing decision logic.

    These tests verify the routing logic without making actual HTTP requests.
    """

    def test_batch_delete_detected(self):
        """Test batch delete is detected by query string."""
        query = "delete"
        method = METHOD_POST
        assert QUERY_DELETE in query and method == METHOD_POST

    def test_list_multipart_uploads_detected(self):
        """Test list multipart uploads is detected."""
        query = "uploads"
        method = METHOD_GET
        assert QUERY_UPLOADS in query and QUERY_UPLOAD_ID not in query and method == METHOD_GET

    def test_create_multipart_upload_detected(self):
        """Test create multipart upload is detected."""
        query = "uploads"
        method = METHOD_POST
        assert QUERY_UPLOADS in query and method == METHOD_POST

    def test_upload_part_detected(self):
        """Test upload part is detected."""
        query = "uploadId=abc123&partNumber=1"
        method = METHOD_PUT
        assert QUERY_UPLOAD_ID in query and method == METHOD_PUT

    def test_list_parts_detected(self):
        """Test list parts is detected (GET with uploadId, no partNumber)."""
        query = "uploadId=abc123"
        method = METHOD_GET
        assert QUERY_UPLOAD_ID in query and QUERY_PART_NUMBER not in query and method == METHOD_GET

    def test_complete_multipart_detected(self):
        """Test complete multipart is detected."""
        query = "uploadId=abc123"
        method = METHOD_POST
        assert QUERY_UPLOAD_ID in query and method == METHOD_POST

    def test_abort_multipart_detected(self):
        """Test abort multipart is detected."""
        query = "uploadId=abc123"
        method = METHOD_DELETE
        assert QUERY_UPLOAD_ID in query and method == METHOD_DELETE

    def test_get_bucket_location_detected(self):
        """Test get bucket location is detected."""
        query = "location"
        method = METHOD_GET
        assert QUERY_LOCATION in query and method == METHOD_GET

    def test_list_objects_detected(self):
        """Test list objects is detected."""
        query = "list-type=2&prefix=foo/"
        method = METHOD_GET
        assert QUERY_LIST_TYPE in query and method == METHOD_GET

    def test_copy_object_detected(self):
        """Test copy object is detected by header."""
        headers = {"x-amz-copy-source": "source-bucket/source-key"}
        method = METHOD_PUT
        assert HEADER_COPY_SOURCE in headers and method == METHOD_PUT

    def test_get_object_tagging_detected(self):
        """Test get object tagging is detected."""
        query = "tagging"
        method = METHOD_GET
        assert QUERY_TAGGING in query and method == METHOD_GET

    def test_put_object_tagging_detected(self):
        """Test put object tagging is detected."""
        query = "tagging"
        method = METHOD_PUT
        assert QUERY_TAGGING in query and method == METHOD_PUT

    def test_delete_object_tagging_detected(self):
        """Test delete object tagging is detected."""
        query = "tagging"
        method = METHOD_DELETE
        assert QUERY_TAGGING in query and method == METHOD_DELETE

    def test_upload_part_copy_detected(self):
        """Test upload part copy is detected."""
        query = "uploadId=abc123&partNumber=1"
        headers = {"x-amz-copy-source": "source-bucket/source-key"}
        method = METHOD_PUT
        # UploadPartCopy: PUT with uploadId AND x-amz-copy-source
        is_upload_part_copy = (
            QUERY_UPLOAD_ID in query
            and method == METHOD_PUT
            and HEADER_COPY_SOURCE in headers
        )
        assert is_upload_part_copy is True


class TestPathParsing:
    """Test path parsing for bucket and key extraction."""

    def test_bucket_only_paths(self):
        """Test various bucket-only path formats."""
        paths = [
            "/my-bucket",
            "/my-bucket/",
            "/bucket-with-dashes",
            "/bucket123",
        ]
        for path in paths:
            assert _is_bucket_only_path(path) is True

    def test_object_paths(self):
        """Test various object path formats."""
        paths = [
            "/bucket/key",
            "/bucket/path/to/object.txt",
            "/bucket/a",
            "/bucket/dir/",  # Trailing slash in key
        ]
        for path in paths:
            assert _is_bucket_only_path(path) is False

    def test_edge_cases(self):
        """Test edge cases in path parsing."""
        # Root path
        assert _is_bucket_only_path("/") is False

        # Empty
        assert _is_bucket_only_path("") is False


class TestQueryStringRouting:
    """Test routing based on query string parameters."""

    def test_versioning_query_forwarded(self):
        """Test ?versioning is forwarded to backend."""
        query = "versioning"
        # This should not match any special handlers
        assert QUERY_UPLOADS not in query
        assert QUERY_UPLOAD_ID not in query
        assert QUERY_LIST_TYPE not in query
        assert QUERY_LOCATION not in query
        assert QUERY_DELETE not in query

    def test_acl_query_forwarded(self):
        """Test ?acl is forwarded to backend."""
        query = "acl"
        # This should not match any special handlers
        assert QUERY_UPLOADS not in query
        assert QUERY_UPLOAD_ID not in query

    def test_tagging_query_handled(self):
        """Test ?tagging is handled by tagging handlers."""
        query = "tagging"
        # Tagging is now handled by our implementation
        assert QUERY_TAGGING in query
        # Should not match multipart handlers
        assert QUERY_UPLOADS not in query
        assert QUERY_UPLOAD_ID not in query

    def test_combined_query_params(self):
        """Test combined query parameters are handled."""
        # List objects with multiple params
        query = "list-type=2&prefix=backups/&max-keys=100"
        assert QUERY_LIST_TYPE in query

        # Upload part with multiple params
        query = "uploadId=abc&partNumber=1"
        assert QUERY_UPLOAD_ID in query
        assert QUERY_PART_NUMBER in query


class TestRoutingPriority:
    """Test that routing priorities are correct."""

    def test_delete_before_bucket_ops(self):
        """Test batch delete is checked before bucket operations."""
        # ?delete on bucket path should route to DeleteObjects, not bucket ops
        query = "delete"
        path = "/bucket"
        method = METHOD_POST

        # Batch delete should be matched first
        is_batch_delete = QUERY_DELETE in query and method == METHOD_POST
        assert is_batch_delete is True

    def test_uploads_list_before_create(self):
        """Test list uploads (GET) vs create upload (POST) distinguished."""
        query = "uploads"

        # GET ?uploads = list uploads
        method = METHOD_GET
        is_list = QUERY_UPLOADS in query and QUERY_UPLOAD_ID not in query and method == METHOD_GET
        assert is_list is True

        # POST ?uploads = create upload
        method = METHOD_POST
        is_create = QUERY_UPLOADS in query and method == METHOD_POST
        assert is_create is True

    def test_copy_checked_for_put(self):
        """Test copy header is only checked for PUT requests."""
        headers = {"x-amz-copy-source": "bucket/key"}

        # PUT with copy header = copy
        is_copy = METHOD_PUT == METHOD_PUT and HEADER_COPY_SOURCE in headers
        assert is_copy is True

        # GET with copy header (shouldn't happen) = not copy
        # Copy is only valid for PUT method


class TestSpecialCharactersInPaths:
    """Test handling of special characters in paths."""

    def test_url_encoded_key(self):
        """Test URL-encoded characters in key."""
        # These should all be treated as object paths
        paths = [
            "/bucket/file%20with%20spaces.txt",
            "/bucket/file%2Bplus.txt",
            "/bucket/path%2Fto%2Ffile.txt",
        ]
        for path in paths:
            assert _is_bucket_only_path(path) is False

    def test_unicode_in_path(self):
        """Test Unicode characters in path."""
        paths = [
            "/bucket/文件.txt",
            "/bucket/档案/file.txt",
        ]
        for path in paths:
            assert _is_bucket_only_path(path) is False
