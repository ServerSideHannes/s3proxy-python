"""Auth for dashboard — session cookies + Basic Auth fallback."""

from __future__ import annotations

import secrets
from typing import TYPE_CHECKING

from fastapi import Depends, HTTPException, Request, status
from fastapi.responses import RedirectResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials

if TYPE_CHECKING:
    from ..config import Settings

SESSION_COOKIE = "s3proxy_session"
SESSION_TTL_SECONDS = 24 * 3600
_BASIC_REALM = "S3Proxy Dashboard"


class RedisSessionStore:
    """Redis-backed session store (HA / multi-replica mode)."""

    _PREFIX = "s3proxy:session:"

    def __init__(self, redis) -> None:
        self._redis = redis

    async def create(self, username: str, ttl: int = SESSION_TTL_SECONDS) -> str:
        token = secrets.token_hex(32)
        await self._redis.set(f"{self._PREFIX}{token}", username.encode(), ex=ttl)
        return token

    async def get(self, token: str) -> str | None:
        val = await self._redis.get(f"{self._PREFIX}{token}")
        return val.decode() if val is not None else None

    async def delete(self, token: str) -> None:
        await self._redis.delete(f"{self._PREFIX}{token}")


class DashboardCredentials:
    """Resolved dashboard credentials."""

    def __init__(self, settings: Settings, credentials_store: dict[str, str]):
        if not (settings.dashboard_username and settings.dashboard_password):
            raise RuntimeError(
                "Dashboard requires S3PROXY_DASHBOARD_USERNAME and S3PROXY_DASHBOARD_PASSWORD"
            )
        self.username = settings.dashboard_username
        self.password = settings.dashboard_password

    def valid(self, username: str, password: str) -> bool:
        return secrets.compare_digest(username.encode(), self.username.encode()) and (
            secrets.compare_digest(password.encode(), self.password.encode())
        )


# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------


_basic_security = HTTPBasic(realm=_BASIC_REALM, auto_error=False)
_basic_dep = Depends(_basic_security)


def _check_basic(creds: HTTPBasicCredentials | None, dashboard: DashboardCredentials) -> str | None:
    if creds is None:
        return None
    return dashboard.username if dashboard.valid(creds.username, creds.password) else None


async def _check_cookie(request: Request, store) -> str | None:
    token = request.cookies.get(SESSION_COOKIE)
    if not token:
        return None
    return await store.get(token)


def make_verify_api(dashboard: DashboardCredentials):
    """Auth dep for JSON API routes — returns 401 if not logged in.

    Only challenges with ``WWW-Authenticate: Basic`` when the caller is actually
    doing Basic Auth (sent an ``Authorization`` header) — i.e. CLI/programmatic
    clients like the e2e encryption check. Browser fetch/EventSource requests get
    a plain 401 so the SPA can redirect to its own login page instead of the
    browser popping up the native Basic Auth dialog.
    """

    async def verify(
        request: Request,
        creds: HTTPBasicCredentials | None = _basic_dep,
    ) -> str:
        store = request.app.state.session_store
        user = await _check_cookie(request, store) or _check_basic(creds, dashboard)
        if user:
            return user
        headers = {}
        if request.headers.get("authorization"):
            headers["WWW-Authenticate"] = f'Basic realm="{_BASIC_REALM}"'
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers=headers,
        )

    return verify


def set_session_cookie(response: RedirectResponse, token: str, secure: bool) -> None:
    response.set_cookie(
        SESSION_COOKIE,
        token,
        max_age=SESSION_TTL_SECONDS,
        httponly=True,
        samesite="strict",
        secure=secure,
        path="/",
    )


def clear_session_cookie(response: RedirectResponse) -> None:
    response.delete_cookie(SESSION_COOKIE, path="/")


# Backwards-compat helper for existing tests.
def create_auth_dependency(settings: Settings, credentials_store: dict[str, str]):
    dashboard = DashboardCredentials(settings, credentials_store)
    return make_verify_api(dashboard)
