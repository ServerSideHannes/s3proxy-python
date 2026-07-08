"""Per-request context for structured logging across async call stacks."""

from __future__ import annotations

from contextvars import ContextVar
from typing import Any
from urllib.parse import parse_qs

_request_ctx: ContextVar[dict[str, Any]] = ContextVar("s3proxy_request_ctx", default={})


def bind_request(
    *,
    method: str,
    path: str,
    query: str = "",
    content_length: int | None = None,
) -> None:
    """Attach request fields visible to nested handlers (memory governor, errors)."""
    ctx: dict[str, Any] = {"method": method, "path": path}
    if content_length is not None:
        ctx["content_length"] = content_length
    if query:
        params = parse_qs(query, keep_blank_values=True)
        upload_ids = params.get("uploadId") or params.get("uploadid")
        part_nums = params.get("partNumber") or params.get("partnumber")
        if upload_ids:
            ctx["upload_id"] = upload_ids[0]
        if part_nums:
            try:
                ctx["part_number"] = int(part_nums[0])
            except ValueError:
                ctx["part_number"] = part_nums[0]
    _request_ctx.set(ctx)


def clear_request() -> None:
    _request_ctx.set({})


def get_request_context() -> dict[str, Any]:
    return dict(_request_ctx.get())
