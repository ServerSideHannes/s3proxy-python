"""S3 proxy request handlers."""

from .buckets import BucketHandlerMixin
from .multipart_ops import MultipartHandlerMixin
from .objects import ObjectHandlerMixin


class S3ProxyHandler(ObjectHandlerMixin, MultipartHandlerMixin, BucketHandlerMixin):
    """Combined handler for all S3 proxy operations.

    Inherits from:
    - ObjectHandlerMixin: GET, PUT, HEAD, DELETE objects
    - MultipartHandlerMixin: multipart upload API
    - BucketHandlerMixin: bucket operations, list objects
    - BaseHandler: shared utilities (via mixins)
    """

    pass


__all__ = ["S3ProxyHandler"]
