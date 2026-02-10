"""S3-compatible error handling."""

from __future__ import annotations

from typing import NoReturn

from botocore.exceptions import ClientError
from fastapi import HTTPException

# S3 Error Code mappings
# https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
S3_ERROR_CODES = {
    # 4xx Client Errors
    400: "BadRequest",
    403: "AccessDenied",
    404: "NoSuchKey",
    405: "MethodNotAllowed",
    409: "BucketNotEmpty",
    412: "PreconditionFailed",
    413: "EntityTooLarge",
    416: "InvalidRange",
    # 5xx Server Errors
    500: "InternalError",
    501: "NotImplemented",
    503: "ServiceUnavailable",
}


class S3Error(HTTPException):
    """S3-compatible error with proper error codes.

    Usage:
        raise S3Error.access_denied("Signature mismatch")
        raise S3Error.no_such_key("Object not found")
        raise S3Error.no_such_bucket("Bucket not found")
    """

    def __init__(
        self,
        status_code: int,
        code: str,
        message: str,
        resource: str | None = None,
    ):
        super().__init__(status_code=status_code, detail=message)
        self.code = code
        self.message = message
        self.resource = resource

    # 400 Bad Request variants
    @classmethod
    def bad_request(cls, message: str = "Bad Request") -> S3Error:
        return cls(400, "BadRequest", message)

    @classmethod
    def invalid_bucket_name(cls, bucket: str) -> S3Error:
        return cls(400, "InvalidBucketName", f"The specified bucket is not valid: {bucket}", bucket)

    @classmethod
    def invalid_argument(cls, message: str) -> S3Error:
        return cls(400, "InvalidArgument", message)

    @classmethod
    def invalid_range(cls, message: str = "The requested range is not satisfiable") -> S3Error:
        return cls(416, "InvalidRange", message)

    @classmethod
    def invalid_part(cls, message: str = "Invalid part") -> S3Error:
        return cls(400, "InvalidPart", message)

    @classmethod
    def invalid_part_order(cls, message: str = "Part list is not in ascending order") -> S3Error:
        return cls(400, "InvalidPartOrder", message)

    @classmethod
    def entity_too_small(cls, message: str = "Part too small") -> S3Error:
        return cls(400, "EntityTooSmall", message)

    @classmethod
    def entity_too_large(cls, max_size_mb: int) -> S3Error:
        return cls(413, "EntityTooLarge", f"Maximum upload size is {max_size_mb}MB")

    @classmethod
    def malformed_xml(cls, message: str = "The XML you provided was not well-formed") -> S3Error:
        return cls(400, "MalformedXML", message)

    @classmethod
    def invalid_request(cls, message: str) -> S3Error:
        return cls(400, "InvalidRequest", message)

    # 403 Forbidden variants
    @classmethod
    def access_denied(cls, message: str = "Access Denied") -> S3Error:
        return cls(403, "AccessDenied", message)

    @classmethod
    def signature_does_not_match(cls, message: str = "Signature mismatch") -> S3Error:
        return cls(403, "SignatureDoesNotMatch", message)

    # 404 Not Found variants
    @classmethod
    def no_such_key(cls, key: str | None = None) -> S3Error:
        msg = (
            f"The specified key does not exist: {key}"
            if key
            else "The specified key does not exist"
        )
        return cls(404, "NoSuchKey", msg, key)

    @classmethod
    def no_such_bucket(cls, bucket: str | None = None) -> S3Error:
        msg = (
            f"The specified bucket does not exist: {bucket}"
            if bucket
            else "The specified bucket does not exist"
        )
        return cls(404, "NoSuchBucket", msg, bucket)

    @classmethod
    def no_such_upload(cls, upload_id: str | None = None) -> S3Error:
        msg = (
            f"The specified upload does not exist: {upload_id}"
            if upload_id
            else "The specified upload does not exist"
        )
        return cls(404, "NoSuchUpload", msg, upload_id)

    # 409 Conflict variants
    @classmethod
    def bucket_not_empty(cls, bucket: str | None = None) -> S3Error:
        msg = "The bucket you tried to delete is not empty"
        return cls(409, "BucketNotEmpty", msg, bucket)

    @classmethod
    def bucket_already_exists(cls, bucket: str | None = None) -> S3Error:
        msg = "The requested bucket name is not available"
        return cls(409, "BucketAlreadyExists", msg, bucket)

    @classmethod
    def bucket_already_owned_by_you(cls, bucket: str | None = None) -> S3Error:
        msg = "Your previous request to create the named bucket succeeded and you already own it"
        return cls(409, "BucketAlreadyOwnedByYou", msg, bucket)

    # 412 Precondition Failed
    @classmethod
    def precondition_failed(cls, message: str = "Precondition Failed") -> S3Error:
        return cls(412, "PreconditionFailed", message)

    # 500 Internal Error
    @classmethod
    def internal_error(cls, message: str = "Internal Server Error") -> S3Error:
        return cls(500, "InternalError", message)

    # 501 Not Implemented
    @classmethod
    def not_implemented(cls, message: str = "Not Implemented") -> S3Error:
        return cls(501, "NotImplemented", message)

    # 503 Service Unavailable / Slow Down
    @classmethod
    def slow_down(cls, message: str = "Please reduce your request rate.") -> S3Error:
        return cls(503, "SlowDown", message)


def get_s3_error_code(status_code: int, detail: str | None = None) -> str:
    """Get S3 error code from HTTP status code and message.

    This is a fallback for HTTPExceptions that aren't S3Error instances.
    """
    # Check for specific error messages that map to specific codes
    if detail:
        detail_lower = detail.lower()
        if status_code == 400:
            if "bucket" in detail_lower and ("invalid" in detail_lower or "name" in detail_lower):
                return "InvalidBucketName"
            if "xml" in detail_lower:
                return "MalformedXML"
            if "range" in detail_lower:
                return "InvalidRange"
        elif status_code == 403:
            if "signature" in detail_lower:
                return "SignatureDoesNotMatch"
            return "AccessDenied"
        elif status_code == 404:
            if "bucket" in detail_lower:
                return "NoSuchBucket"
            if "upload" in detail_lower:
                return "NoSuchUpload"
            return "NoSuchKey"
        elif status_code == 409:
            if "empty" in detail_lower:
                return "BucketNotEmpty"
            return "BucketAlreadyExists"
        elif status_code == 416:
            return "InvalidRange"

    # Fall back to generic mapping
    return S3_ERROR_CODES.get(status_code, "InternalError")


def raise_for_client_error(
    e: ClientError,
    bucket: str | None = None,
    key: str | None = None,
) -> NoReturn:
    """Convert botocore ClientError to S3Error. Always raises."""
    code = e.response.get("Error", {}).get("Code", "")
    msg = e.response.get("Error", {}).get("Message", str(e))

    if code == "NoSuchUpload":
        raise S3Error.no_such_upload(msg) from e
    if code in ("NoSuchKey", "404"):
        raise S3Error.no_such_key(key) from e
    if code in ("NoSuchBucket", "NotFound"):
        raise S3Error.no_such_bucket(bucket) from e
    if code == "BucketNotEmpty":
        raise S3Error.bucket_not_empty(bucket) from e
    if code == "BucketAlreadyExists":
        raise S3Error.bucket_already_exists(bucket) from e
    if code == "BucketAlreadyOwnedByYou":
        raise S3Error.bucket_already_owned_by_you(bucket) from e
    raise S3Error.internal_error(msg) from e


def raise_for_exception(e: Exception) -> NoReturn:
    """Convert generic exception to S3Error. Always raises."""
    exc_name = type(e).__name__
    error_str = str(e)

    # Handle NoSuchUpload that may come as a non-ClientError
    if exc_name == "NoSuchUpload" or "NoSuchUpload" in error_str:
        raise S3Error.no_such_upload(error_str) from e
    raise S3Error.internal_error(error_str) from e
