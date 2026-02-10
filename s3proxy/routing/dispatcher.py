"""S3 request routing and dispatching."""

from __future__ import annotations

from urllib.parse import parse_qs

from fastapi import Request
from fastapi.responses import PlainTextResponse

from ..handlers import S3ProxyHandler
from ..s3client import S3Credentials

# Query parameter constants
QUERY_UPLOADS = "uploads"
QUERY_UPLOAD_ID = "uploadId"
QUERY_PART_NUMBER = "partNumber"
QUERY_LIST_TYPE = "list-type"
QUERY_LOCATION = "location"
QUERY_DELETE = "delete"
QUERY_TAGGING = "tagging"

# Header constants
HEADER_COPY_SOURCE = "x-amz-copy-source"

# HTTP method constants
METHOD_GET = "GET"
METHOD_PUT = "PUT"
METHOD_POST = "POST"
METHOD_DELETE = "DELETE"
METHOD_HEAD = "HEAD"


class RequestDispatcher:
    """Routes S3 requests to appropriate handler methods.

    Encapsulates all routing logic for S3 API operations, converting
    the HTTP request into the appropriate handler method call.
    """

    def __init__(self, handler: S3ProxyHandler) -> None:
        """Initialize dispatcher with handler.

        Args:
            handler: The S3ProxyHandler that implements all operations.
        """
        self.handler = handler

    async def dispatch(self, request: Request, creds: S3Credentials) -> PlainTextResponse:
        """Route request to appropriate handler method.

        Args:
            request: The incoming FastAPI request.
            creds: Verified S3 credentials for the request.

        Returns:
            Response from the appropriate handler method.
        """
        method = request.method
        query = str(request.url.query)
        path = request.url.path
        headers = {k.lower(): v for k, v in request.headers.items()}

        # Root path - list buckets
        if path.strip("/") == "":
            return await self.handler.handle_list_buckets(request, creds)

        # Batch delete
        if QUERY_DELETE in query and method == METHOD_POST:
            return await self.handler.handle_delete_objects(request, creds)

        # List multipart uploads
        if QUERY_UPLOADS in query and QUERY_UPLOAD_ID not in query and method == METHOD_GET:
            return await self.handler.handle_list_multipart_uploads(request, creds)

        # Create multipart upload
        if QUERY_UPLOADS in query and method == METHOD_POST:
            return await self.handler.handle_create_multipart_upload(request, creds)

        # Multipart operations (with uploadId)
        if QUERY_UPLOAD_ID in query:
            return await self._dispatch_multipart(request, creds, method, query, headers)

        # Bucket-level operations
        if self._is_bucket_only_path(path):
            result = await self._dispatch_bucket(request, creds, method, query)
            if result is not None:
                return result

        # Bucket listing (fallthrough from bucket operations)
        if self._is_bucket_only_path(path) and method == METHOD_GET:
            query_params = parse_qs(query, keep_blank_values=True)
            list_type = query_params.get("list-type", ["1"])[0]
            if list_type == "2":
                return await self.handler.handle_list_objects(request, creds)
            return await self.handler.handle_list_objects_v1(request, creds)

        # Copy object
        if method == METHOD_PUT and HEADER_COPY_SOURCE in headers:
            return await self.handler.handle_copy_object(request, creds)

        # Standard object operations
        return await self._dispatch_object(request, creds, method, query)

    async def _dispatch_multipart(
        self,
        request: Request,
        creds: S3Credentials,
        method: str,
        query: str,
        headers: dict[str, str],
    ) -> PlainTextResponse:
        """Handle multipart upload operations."""
        if method == METHOD_GET and QUERY_PART_NUMBER not in query:
            return await self.handler.handle_list_parts(request, creds)
        if method == METHOD_PUT:
            if HEADER_COPY_SOURCE in headers:
                return await self.handler.handle_upload_part_copy(request, creds)
            return await self.handler.handle_upload_part(request, creds)
        if method == METHOD_POST:
            return await self.handler.handle_complete_multipart_upload(request, creds)
        if method == METHOD_DELETE:
            return await self.handler.handle_abort_multipart_upload(request, creds)
        return await self.handler.forward_request(request, creds)

    async def _dispatch_bucket(
        self,
        request: Request,
        creds: S3Credentials,
        method: str,
        query: str,
    ) -> PlainTextResponse | None:
        """Handle bucket-level operations.

        Returns None to fall through to object/listing handling.
        """
        if QUERY_LOCATION in query and method == METHOD_GET:
            return await self.handler.handle_get_bucket_location(request, creds)

        skip_queries = (QUERY_LIST_TYPE, QUERY_DELETE, QUERY_UPLOADS, QUERY_LOCATION)
        if query and not any(q in query for q in skip_queries):
            return await self.handler.forward_request(request, creds)

        if not query:
            if method == METHOD_PUT:
                return await self.handler.handle_create_bucket(request, creds)
            if method == METHOD_DELETE:
                return await self.handler.handle_delete_bucket(request, creds)
            if method == METHOD_HEAD:
                return await self.handler.handle_head_bucket(request, creds)

        return None

    async def _dispatch_object(
        self,
        request: Request,
        creds: S3Credentials,
        method: str,
        query: str,
    ) -> PlainTextResponse:
        """Handle standard object operations."""
        if QUERY_TAGGING in query:
            if method == METHOD_GET:
                return await self.handler.handle_get_object_tagging(request, creds)
            if method == METHOD_PUT:
                return await self.handler.handle_put_object_tagging(request, creds)
            if method == METHOD_DELETE:
                return await self.handler.handle_delete_object_tagging(request, creds)

        if method == METHOD_GET:
            return await self.handler.handle_get_object(request, creds)
        if method == METHOD_PUT:
            return await self.handler.handle_put_object(request, creds)
        if method == METHOD_HEAD:
            return await self.handler.handle_head_object(request, creds)
        if method == METHOD_DELETE:
            return await self.handler.handle_delete_object(request, creds)
        return await self.handler.forward_request(request, creds)

    @staticmethod
    def _is_bucket_only_path(path: str) -> bool:
        """Check if path is bucket-only (no object key)."""
        stripped = path.strip("/")
        return "/" not in stripped and bool(stripped)
