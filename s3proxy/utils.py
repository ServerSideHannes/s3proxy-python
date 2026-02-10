"""Shared utilities for S3Proxy."""

from datetime import datetime
from email.utils import parsedate_to_datetime
from urllib.parse import parse_qs

# HTTP date format per RFC 7231
HTTP_DATE_FORMAT = "%a, %d %b %Y %H:%M:%S GMT"

# ISO 8601 format for S3 API responses
ISO8601_FORMAT = "%Y-%m-%dT%H:%M:%S.000Z"


def parse_http_date(date_str: str | None) -> datetime | None:
    """Parse HTTP date string to datetime."""
    if not date_str:
        return None
    try:
        return parsedate_to_datetime(date_str)
    except ValueError, TypeError:
        return None


def etag_matches(etag: str, header_value: str) -> bool:
    """Check if etag matches any value in If-Match/If-None-Match header.

    Handles wildcard (*) and comma-separated lists of ETags.
    """
    if header_value.strip() == "*":
        return True
    for value in header_value.split(","):
        value = value.strip().strip('"')
        if value == etag or value == f'"{etag}"':
            return True
    return False


def get_query_param(query: str | dict[str, list[str]], key: str, default: str = "") -> str:
    """Get a single query parameter value with safe default.

    Handles both raw query strings and pre-parsed dicts from parse_qs().
    """
    if isinstance(query, str):
        query = parse_qs(query, keep_blank_values=True)
    values = query.get(key, [default])
    return values[0] if values else default


def get_query_param_int(query: str | dict[str, list[str]], key: str, default: int) -> int:
    """Get a query parameter as integer with safe default."""
    value = get_query_param(query, key, "")
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def format_http_date(dt: datetime | str | None) -> str | None:
    """Format datetime as HTTP date string (RFC 7231)."""
    if dt is None:
        return None
    if isinstance(dt, str):
        return dt
    if hasattr(dt, "strftime"):
        return dt.strftime(HTTP_DATE_FORMAT)
    return str(dt)


def format_iso8601(dt: datetime | None) -> str:
    """Format datetime as ISO 8601 for S3 API responses."""
    if dt is None:
        return datetime.utcnow().strftime(ISO8601_FORMAT)
    return dt.strftime(ISO8601_FORMAT)
