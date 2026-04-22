"""Admin dashboard router."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, JSONResponse

from .auth import create_auth_dependency
from .collectors import collect_all
from .templates import render_dashboard

if TYPE_CHECKING:
    from ..config import Settings


def create_admin_router(
    settings: Settings,
    credentials_store: dict[str, str],
    version: str = "1.0.0",
) -> APIRouter:
    """Build the admin dashboard router with Basic Auth."""
    verify = create_auth_dependency(settings, credentials_store)
    router = APIRouter(dependencies=[Depends(verify)])

    @router.get("/", response_class=HTMLResponse)
    async def dashboard() -> HTMLResponse:
        return HTMLResponse(render_dashboard(admin_path=settings.admin_path))

    @router.get("/api/status")
    async def status(request: Request) -> JSONResponse:
        data = collect_all(
            request.app.state.settings,
            request.app.state.start_time,
            version=version,
        )
        return JSONResponse(data)

    return router
