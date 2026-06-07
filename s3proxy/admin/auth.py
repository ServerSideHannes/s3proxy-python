"""Auth for admin dashboard — session cookies + Basic Auth fallback."""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import secrets
import time
from typing import TYPE_CHECKING

from fastapi import Depends, HTTPException, Request, status
from fastapi.responses import RedirectResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials

if TYPE_CHECKING:
    from ..config import Settings

SESSION_COOKIE = "s3proxy_session"
SESSION_TTL_SECONDS = 24 * 3600
_BASIC_REALM = "S3Proxy Admin"


class AdminCredentials:
    """Resolved admin credentials with session-signing key."""

    def __init__(self, settings: Settings, credentials_store: dict[str, str]):
        if not (settings.admin_username and settings.admin_password):
            raise RuntimeError(
                "Admin dashboard requires S3PROXY_ADMIN_USERNAME and S3PROXY_ADMIN_PASSWORD"
            )
        self.username = settings.admin_username
        self.password = settings.admin_password
        # Stable session-signing secret (survives pod restarts, shared across replicas).
        self.session_secret = settings.admin_session_secret

    def valid(self, username: str, password: str) -> bool:
        return secrets.compare_digest(username.encode(), self.username.encode()) and (
            secrets.compare_digest(password.encode(), self.password.encode())
        )


# ---------------------------------------------------------------------------
# Session cookie — base64(json({u, exp})) "." hex(hmac-sha256)
# ---------------------------------------------------------------------------


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(s: str) -> bytes:
    padding = "=" * ((4 - len(s) % 4) % 4)
    return base64.urlsafe_b64decode(s + padding)


def issue_session(username: str, secret: bytes, ttl: int = SESSION_TTL_SECONDS) -> str:
    payload = {"u": username, "e": int(time.time()) + ttl}
    body = _b64url_encode(json.dumps(payload, separators=(",", ":")).encode())
    mac = hmac.new(secret, body.encode(), hashlib.sha256).hexdigest()
    return f"{body}.{mac}"


def verify_session(token: str, secret: bytes) -> str | None:
    if not token or "." not in token:
        return None
    body, _, mac = token.rpartition(".")
    try:
        expected = hmac.new(secret, body.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(mac, expected):
            return None
        payload = json.loads(_b64url_decode(body))
    except ValueError, binascii.Error, json.JSONDecodeError:
        return None
    if int(payload.get("e", 0)) < int(time.time()):
        return None
    return str(payload.get("u", ""))


# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------


_basic_security = HTTPBasic(realm=_BASIC_REALM, auto_error=False)
_basic_dep = Depends(_basic_security)


def _check_basic(creds: HTTPBasicCredentials | None, admin: AdminCredentials) -> str | None:
    if creds is None:
        return None
    return admin.username if admin.valid(creds.username, creds.password) else None


def _check_cookie(request: Request, admin: AdminCredentials) -> str | None:
    token = request.cookies.get(SESSION_COOKIE)
    if not token:
        return None
    return verify_session(token, admin.session_secret)


def make_verify_html(admin: AdminCredentials, login_url: str):
    """Auth dep for HTML routes — redirects to login_url if not logged in."""

    async def verify(
        request: Request,
        creds: HTTPBasicCredentials | None = _basic_dep,
    ) -> str:
        user = _check_cookie(request, admin) or _check_basic(creds, admin)
        if user:
            return user
        raise HTTPException(
            status_code=status.HTTP_303_SEE_OTHER,
            headers={"Location": login_url},
        )

    return verify


def make_verify_api(admin: AdminCredentials):
    """Auth dep for JSON API routes — returns 401 if not logged in."""

    async def verify(
        request: Request,
        creds: HTTPBasicCredentials | None = _basic_dep,
    ) -> str:
        user = _check_cookie(request, admin) or _check_basic(creds, admin)
        if user:
            return user
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": f'Basic realm="{_BASIC_REALM}"'},
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
    admin = AdminCredentials(settings, credentials_store)
    return make_verify_api(admin)
