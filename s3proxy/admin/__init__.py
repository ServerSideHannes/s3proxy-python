"""Admin dashboard for S3Proxy."""

from .collectors import record_request
from .router import create_admin_router

__all__ = ["create_admin_router", "record_request"]
