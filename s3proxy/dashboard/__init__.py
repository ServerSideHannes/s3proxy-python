"""Dashboard for S3Proxy."""

from .collectors import record_request
from .router import create_dashboard_router

__all__ = ["create_dashboard_router", "record_request"]
