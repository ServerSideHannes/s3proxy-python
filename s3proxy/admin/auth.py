"""Basic auth for admin dashboard."""

import secrets

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

security = HTTPBasic(realm="S3Proxy Admin")

_security_dep = Depends(security)


def create_auth_dependency(settings, credentials_store: dict[str, str]):
    """Create a Basic Auth dependency for the admin router."""
    if settings.admin_username and settings.admin_password:
        valid_username = settings.admin_username
        valid_password = settings.admin_password
    else:
        if not credentials_store:
            raise RuntimeError("No credentials configured for admin auth")
        valid_username = next(iter(credentials_store.keys()))
        valid_password = credentials_store[valid_username]

    async def verify(credentials: HTTPBasicCredentials = _security_dep):
        username_ok = secrets.compare_digest(credentials.username.encode(), valid_username.encode())
        password_ok = secrets.compare_digest(credentials.password.encode(), valid_password.encode())
        if not (username_ok and password_ok):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid credentials",
                headers={"WWW-Authenticate": 'Basic realm="S3Proxy Admin"'},
            )
        return credentials

    return verify
