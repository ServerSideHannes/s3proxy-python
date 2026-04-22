"""Admin dashboard router."""

from __future__ import annotations

from typing import TYPE_CHECKING

from botocore.exceptions import ClientError
from fastapi import APIRouter, Depends, Form, HTTPException, Request, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from .auth import (
    AdminCredentials,
    clear_session_cookie,
    issue_session,
    make_verify_api,
    make_verify_html,
    set_session_cookie,
)
from .collectors import collect_all, head_object_detail, list_bucket_objects
from .templates import render_dashboard, render_login

if TYPE_CHECKING:
    from ..config import Settings


def create_admin_router(
    settings: Settings,
    credentials_store: dict[str, str],
    version: str = "1.0.0",
) -> APIRouter:
    """Build the admin dashboard router with session cookie + Basic Auth."""
    admin = AdminCredentials(settings, credentials_store)
    prefix = settings.admin_path.rstrip("/")
    login_url = f"{prefix}/login"
    verify_html = make_verify_html(admin, login_url)
    verify_api = make_verify_api(admin)
    cookie_secure = not settings.no_tls

    router = APIRouter()

    # ---- Login / logout (unauthenticated) ------------------------------------

    @router.get("/login", response_class=HTMLResponse)
    async def login_page(request: Request) -> HTMLResponse:
        error = request.query_params.get("error")
        return HTMLResponse(render_login(admin_path=settings.admin_path, error=error))

    @router.post("/login")
    async def login_submit(
        request: Request,
        username: str = Form(...),
        password: str = Form(...),
    ) -> RedirectResponse:
        if not admin.valid(username, password):
            dest = f"{settings.admin_path.rstrip('/')}/login?error=1"
            return RedirectResponse(dest, status_code=status.HTTP_303_SEE_OTHER)
        token = issue_session(username, admin.session_secret)
        response = RedirectResponse(
            settings.admin_path.rstrip("/") + "/",
            status_code=status.HTTP_303_SEE_OTHER,
        )
        set_session_cookie(response, token, secure=cookie_secure)
        return response

    @router.get("/logout")
    async def logout() -> RedirectResponse:
        response = RedirectResponse(
            settings.admin_path.rstrip("/") + "/login",
            status_code=status.HTTP_303_SEE_OTHER,
        )
        clear_session_cookie(response)
        return response

    # ---- Authenticated routes ------------------------------------------------

    @router.get("/", response_class=HTMLResponse, dependencies=[Depends(verify_html)])
    async def dashboard() -> HTMLResponse:
        return HTMLResponse(render_dashboard(admin_path=settings.admin_path))

    @router.get("/api/status", dependencies=[Depends(verify_api)])
    async def status_api(request: Request) -> JSONResponse:
        data = collect_all(
            request.app.state.settings,
            request.app.state.start_time,
            version=version,
        )
        return JSONResponse(data)

    @router.get("/api/buckets/{bucket}", dependencies=[Depends(verify_api)])
    async def list_bucket(bucket: str, prefix: str = "", limit: int = 500) -> JSONResponse:
        try:
            data = await list_bucket_objects(
                settings, credentials_store, bucket, prefix=prefix, max_keys=min(limit, 1000)
            )
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "Error")
            raise HTTPException(status_code=404, detail=f"{code}: {bucket}") from exc
        return JSONResponse(data)

    @router.get("/api/objects/{bucket}/{key:path}", dependencies=[Depends(verify_api)])
    async def head_object_api(bucket: str, key: str) -> JSONResponse:
        try:
            data = await head_object_detail(settings, credentials_store, bucket, key)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "Error")
            raise HTTPException(status_code=404, detail=f"{code}: {bucket}/{key}") from exc
        return JSONResponse(data)

    return router
