"""HTTP Basic Auth for admin dashboard."""

from __future__ import annotations

import secrets
from typing import TYPE_CHECKING

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

if TYPE_CHECKING:
    from ..config import Settings

security = HTTPBasic(realm="S3Proxy Admin")
_security_dep = Depends(security)


def create_auth_dependency(settings: Settings, credentials_store: dict[str, str]):
    """Build a Basic Auth dependency using configured or AWS-derived credentials."""
    if settings.admin_username and settings.admin_password:
        valid_username = settings.admin_username
        valid_password = settings.admin_password
    elif credentials_store:
        valid_username = next(iter(credentials_store.keys()))
        valid_password = credentials_store[valid_username]
    else:
        raise RuntimeError("No credentials configured for admin auth")

    async def verify(credentials: HTTPBasicCredentials = _security_dep):
        ok_user = secrets.compare_digest(
            credentials.username.encode(), valid_username.encode()
        )
        ok_pass = secrets.compare_digest(
            credentials.password.encode(), valid_password.encode()
        )
        if not (ok_user and ok_pass):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid credentials",
                headers={"WWW-Authenticate": 'Basic realm="S3Proxy Admin"'},
            )
        return credentials

    return verify
