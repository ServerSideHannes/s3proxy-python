"""Prometheus metrics for S3Proxy."""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

# Request metrics
REQUEST_COUNT = Counter(
    "s3proxy_requests_total",
    "Total number of requests",
    ["method", "operation", "status"],
)

REQUEST_DURATION = Histogram(
    "s3proxy_request_duration_seconds",
    "Request duration in seconds",
    ["method", "operation"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

REQUESTS_IN_FLIGHT = Gauge(
    "s3proxy_requests_in_flight",
    "Number of requests currently being processed",
    ["method"],
)

# Memory/Concurrency metrics
MEMORY_RESERVED_BYTES = Gauge(
    "s3proxy_memory_reserved_bytes",
    "Currently reserved memory in bytes",
)

MEMORY_LIMIT_BYTES = Gauge(
    "s3proxy_memory_limit_bytes",
    "Configured memory limit in bytes",
)

MEMORY_REJECTIONS = Counter(
    "s3proxy_memory_rejections_total",
    "Total number of requests rejected due to memory limits",
)

# Encryption metrics
ENCRYPTION_OPERATIONS = Counter(
    "s3proxy_encryption_operations_total",
    "Total number of encryption/decryption operations",
    ["operation"],
)

BYTES_ENCRYPTED = Counter(
    "s3proxy_bytes_encrypted_total",
    "Total bytes encrypted",
)

BYTES_DECRYPTED = Counter(
    "s3proxy_bytes_decrypted_total",
    "Total bytes decrypted",
)


def get_operation_name(method: str, path: str, query: str) -> str:
    """Derive S3 operation name from request attributes.

    Args:
        method: HTTP method (GET, PUT, POST, DELETE, HEAD).
        path: Request path.
        query: Query string.

    Returns:
        S3 operation name for metrics labeling.
    """
    is_bucket_only = "/" not in path.strip("/") and bool(path.strip("/"))
    is_root = path.strip("/") == ""

    # Root path
    if is_root:
        return "ListBuckets"

    # Batch delete
    if "delete" in query and method == "POST":
        return "DeleteObjects"

    # Multipart operations
    if "uploadId" in query:
        if method == "GET" and "partNumber" not in query:
            return "ListParts"
        if method == "PUT":
            if "x-amz-copy-source" in query:
                return "UploadPartCopy"
            return "UploadPart"
        if method == "POST":
            return "CompleteMultipartUpload"
        if method == "DELETE":
            return "AbortMultipartUpload"

    # List/Create multipart uploads
    if "uploads" in query:
        if method == "GET":
            return "ListMultipartUploads"
        if method == "POST":
            return "CreateMultipartUpload"

    # Bucket operations
    if is_bucket_only:
        if "location" in query and method == "GET":
            return "GetBucketLocation"
        if method == "PUT":
            return "CreateBucket"
        if method == "DELETE":
            return "DeleteBucket"
        if method == "HEAD":
            return "HeadBucket"
        if method == "GET":
            return "ListObjects"

    # Object tagging
    if "tagging" in query:
        if method == "GET":
            return "GetObjectTagging"
        if method == "PUT":
            return "PutObjectTagging"
        if method == "DELETE":
            return "DeleteObjectTagging"

    # Standard object operations
    if method == "GET":
        return "GetObject"
    if method == "PUT":
        return "PutObject"
    if method == "HEAD":
        return "HeadObject"
    if method == "DELETE":
        return "DeleteObject"

    return "Unknown"
