"""Admin dashboard router."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, JSONResponse

from .auth import create_auth_dependency
from .collectors import collect_all
from .templates import DASHBOARD_HTML

if TYPE_CHECKING:
    from ..config import Settings


def create_admin_router(settings: Settings, credentials_store: dict[str, str]) -> APIRouter:
    """Create the admin dashboard router with auth."""
    verify_admin = create_auth_dependency(settings, credentials_store)
    router = APIRouter(dependencies=[Depends(verify_admin)])

    @router.get("/", response_class=HTMLResponse)
    async def dashboard():
        return HTMLResponse(DASHBOARD_HTML)

    @router.get("/api/status")
    async def status(request: Request):
        data = await collect_all(
            request.app.state.settings,
            request.app.state.handler,
            request.app.state.start_time,
        )
        return JSONResponse(data)

    return router
