"""Dashboard router."""

from __future__ import annotations

import asyncio
import json as _json
import time
from typing import TYPE_CHECKING

from botocore.exceptions import ClientError
from fastapi import APIRouter, Depends, Form, HTTPException, Request, status
from fastapi.responses import JSONResponse, RedirectResponse, StreamingResponse

from .auth import (
    SESSION_COOKIE,
    DashboardCredentials,
    clear_session_cookie,
    make_verify_api,
    set_session_cookie,
)
from .collectors import (
    collect_all,
    collect_series,
    collect_throughput,
    head_object_detail,
    list_bucket_objects,
    list_logs,
)
from .oidc import OIDCError
from .stats_store import DEFAULT_RANGE, RANGE_SPECS

if TYPE_CHECKING:
    from ..config import Settings


def create_dashboard_router(
    settings: Settings,
    credentials_store: dict[str, str],
    version: str = "1.0.0",
) -> APIRouter:
    """Build the dashboard API router with session cookie + Basic Auth.

    The dashboard UI is a Svelte static build served by its own deployment; the
    proxy exposes only the JSON/SSE API plus form auth under ``{prefix}/api``.
    That deployment's nginx serves the static UI and reverse-proxies
    ``{prefix}/api`` here, so the login page is served statically and posts back
    to ``{prefix}/api/login``.
    """
    dashboard = DashboardCredentials(settings, credentials_store)
    prefix = settings.dashboard_path.rstrip("/")
    verify_api = make_verify_api(dashboard)
    cookie_secure = not settings.no_tls
    password_enabled = settings.dashboard_password_enabled
    oidc_enabled = settings.dashboard_oidc_enabled

    if not (password_enabled or oidc_enabled):
        raise RuntimeError(
            "Dashboard has no login method enabled — set dashboard_password_enabled "
            "and/or dashboard_oidc_enabled"
        )

    def _callback_url(request: Request) -> str:
        if settings.dashboard_oidc_redirect_url:
            return settings.dashboard_oidc_redirect_url
        scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
        host = request.headers.get("host", request.url.netloc)
        return f"{scheme}://{host}{prefix}/api/oidc/callback"

    router = APIRouter()

    # ---- Auth (unauthenticated) ----------------------------------------------

    @router.get("/api/authmodes")
    async def auth_modes() -> JSONResponse:
        """Which login methods the login page should render."""
        return JSONResponse(
            {
                "password": password_enabled,
                "oidc": oidc_enabled,
                "oidc_label": settings.dashboard_oidc_button_label,
            }
        )

    @router.post("/api/login")
    async def login_submit(
        request: Request, username: str = Form(...), password: str = Form(...)
    ) -> RedirectResponse:
        if not (password_enabled and dashboard.valid(username, password)):
            dest = f"{prefix}/login?error=1"
            return RedirectResponse(dest, status_code=status.HTTP_303_SEE_OTHER)
        token = await request.app.state.session_store.create(username)
        response = RedirectResponse(f"{prefix}/", status_code=status.HTTP_303_SEE_OTHER)
        set_session_cookie(response, token, secure=cookie_secure)
        return response

    if oidc_enabled:

        @router.get("/api/oidc/login")
        async def oidc_login(request: Request) -> RedirectResponse:
            client = request.app.state.oidc_client
            url, login_state, state = await client.authorization_url(_callback_url(request))
            await request.app.state.oidc_state_store.put(state, login_state)
            return RedirectResponse(url, status_code=status.HTTP_303_SEE_OTHER)

        @router.get("/api/oidc/callback")
        async def oidc_callback(
            request: Request, code: str = "", state: str = "", error: str = ""
        ) -> RedirectResponse:
            def _fail() -> RedirectResponse:
                return RedirectResponse(
                    f"{prefix}/login?error=sso", status_code=status.HTTP_303_SEE_OTHER
                )

            if error or not (code and state):
                return _fail()
            login_state = await request.app.state.oidc_state_store.take(state)
            if login_state is None:
                return _fail()
            try:
                username = await request.app.state.oidc_client.complete_login(code, login_state)
            except OIDCError:
                return _fail()
            token = await request.app.state.session_store.create(username)
            response = RedirectResponse(f"{prefix}/", status_code=status.HTTP_303_SEE_OTHER)
            set_session_cookie(response, token, secure=cookie_secure)
            return response

    @router.get("/api/logout")
    async def logout(request: Request) -> RedirectResponse:
        token = request.cookies.get(SESSION_COOKIE)
        if token:
            await request.app.state.session_store.delete(token)
        response = RedirectResponse(f"{prefix}/login", status_code=status.HTTP_303_SEE_OTHER)
        clear_session_cookie(response)
        return response

    def _range(request: Request) -> str:
        r = request.query_params.get("range", DEFAULT_RANGE)
        return r if r in RANGE_SPECS else DEFAULT_RANGE

    @router.get("/api/status", dependencies=[Depends(verify_api)])
    async def status_api(request: Request) -> JSONResponse:
        data = await collect_all(
            request.app.state.stats_store,
            request.app.state.settings,
            request.app.state.start_time,
            version=version,
            range_key=_range(request),
        )
        return JSONResponse(data)

    @router.get("/api/series", dependencies=[Depends(verify_api)])
    async def series_api(
        request: Request, metric: str = "requests", range: str = DEFAULT_RANGE
    ) -> JSONResponse:
        allowed = ("requests", "crypto", "errors", "bytes_put", "bytes_get")
        metric = metric if metric in allowed else "requests"
        range_key = range if range in RANGE_SPECS else DEFAULT_RANGE
        data = await collect_series(request.app.state.stats_store, metric, range_key)
        return JSONResponse(data)

    @router.get("/api/throughput", dependencies=[Depends(verify_api)])
    async def throughput_api(request: Request, range: str = DEFAULT_RANGE) -> JSONResponse:
        range_key = range if range in RANGE_SPECS else DEFAULT_RANGE
        data = await collect_throughput(request.app.state.stats_store, range_key)
        return JSONResponse(data)

    @router.get("/api/stream", dependencies=[Depends(verify_api)])
    async def status_stream(request: Request) -> StreamingResponse:
        """Push status updates via SSE — only emits when the payload changes."""

        range_key = _range(request)

        async def event_gen():
            last_payload: str | None = None
            last_heartbeat = time.monotonic()
            try:
                while True:
                    if await request.is_disconnected():
                        return
                    data = await collect_all(
                        request.app.state.stats_store,
                        request.app.state.settings,
                        request.app.state.start_time,
                        version=version,
                        range_key=range_key,
                    )
                    payload = _json.dumps(data)
                    now = time.monotonic()
                    if payload != last_payload:
                        last_payload = payload
                        last_heartbeat = now
                        yield f"event: status\ndata: {payload}\n\n"
                    elif now - last_heartbeat > 15:
                        last_heartbeat = now
                        yield ": hb\n\n"
                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                return

        return StreamingResponse(
            event_gen(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
                "Connection": "keep-alive",
            },
        )

    @router.get("/api/logs", dependencies=[Depends(verify_api)])
    async def logs_api(
        request: Request,
        q: str = "",
        operation: str = "",
        status: str = "",
        limit: int = 50,
        offset: int = 0,
    ) -> JSONResponse:
        data = await list_logs(
            request.app.state.stats_store,
            limit=min(max(limit, 1), 500),
            offset=max(offset, 0),
            query=q,
            operation=operation,
            status=status,
        )
        return JSONResponse(data)

    @router.get("/api/buckets/{bucket}", dependencies=[Depends(verify_api)])
    async def list_bucket(
        bucket: str,
        prefix: str = "",
        delimiter: str = "/",
        limit: int = 1000,
        offset: int = 0,
        page_size: int = 20,
    ) -> JSONResponse:
        try:
            data = await list_bucket_objects(
                settings,
                credentials_store,
                bucket,
                prefix=prefix,
                delimiter=delimiter or None,
                max_keys=min(limit, 1000),
                offset=max(offset, 0),
                page_size=min(max(page_size, 1), 100),
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
