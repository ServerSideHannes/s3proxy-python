"""Tests for dashboard OIDC SSO and the password-toggle login modes."""

from __future__ import annotations

import base64
import json
import time

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from s3proxy.config import Settings
from s3proxy.dashboard.oidc import (
    OIDCClient,
    OIDCError,
    OIDCLoginState,
    OIDCStateStore,
    decode_jwt_claims,
    make_pkce,
)

ISSUER = "https://idp.example.com"
DISCOVERY = {
    "issuer": ISSUER,
    "authorization_endpoint": f"{ISSUER}/authorize",
    "token_endpoint": f"{ISSUER}/token",
}


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def make_id_token(claims: dict) -> str:
    header = _b64url(json.dumps({"alg": "RS256", "typ": "JWT"}).encode())
    payload = _b64url(json.dumps(claims).encode())
    return f"{header}.{payload}.signature"


def oidc_settings(**overrides):
    base = {
        "host": "http://localhost:9000",
        "dashboard_ui": True,
        "dashboard_username": "admin",
        "dashboard_password": "admin",
        "dashboard_password_enabled": True,
        "dashboard_oidc_enabled": True,
        "dashboard_oidc_issuer": ISSUER,
        "dashboard_oidc_client_id": "client-123",
        "dashboard_oidc_client_secret": "secret-xyz",
    }
    base.update(overrides)
    return Settings(**base)


def make_client(settings: Settings, nonce_box: dict) -> OIDCClient:
    """An OIDCClient whose HTTP layer is a deterministic MockTransport."""
    client = OIDCClient(settings)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("openid-configuration"):
            return httpx.Response(200, json=DISCOVERY)
        if str(request.url) == DISCOVERY["token_endpoint"]:
            claims = {
                "iss": ISSUER,
                "aud": settings.dashboard_oidc_client_id,
                "exp": time.time() + 3600,
                "nonce": nonce_box.get("nonce", ""),
                "email": nonce_box.get("email", "alice@ocean.io"),
                "sub": "user-1",
            }
            body = {"id_token": make_id_token(claims), "access_token": "a"}
            return httpx.Response(200, json=body)
        return httpx.Response(404)

    client._http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return client


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def test_make_pkce_challenge_is_s256_of_verifier() -> None:
    import hashlib

    verifier, challenge = make_pkce()
    assert challenge == _b64url(hashlib.sha256(verifier.encode()).digest())


def test_decode_jwt_claims_roundtrip() -> None:
    token = make_id_token({"sub": "x", "email": "a@b.com"})
    assert decode_jwt_claims(token)["email"] == "a@b.com"


def test_decode_jwt_claims_rejects_garbage() -> None:
    with pytest.raises(OIDCError):
        decode_jwt_claims("not-a-jwt")


# ---------------------------------------------------------------------------
# OIDCClient core
# ---------------------------------------------------------------------------


async def test_authorization_url_includes_pkce_and_state() -> None:
    client = make_client(oidc_settings(), {})
    url, state, state_key = await client.authorization_url(f"{ISSUER}/cb")
    assert url.startswith(f"{ISSUER}/authorize?")
    assert "code_challenge_method=S256" in url
    assert f"state={state_key}" in url
    assert state.nonce and state.code_verifier


async def test_complete_login_returns_username() -> None:
    box = {"nonce": "n-1", "email": "alice@ocean.io"}
    client = make_client(oidc_settings(), box)
    login_state = OIDCLoginState(nonce="n-1", code_verifier="v", redirect_uri=f"{ISSUER}/cb")
    assert await client.complete_login("code", login_state) == "alice@ocean.io"


async def test_complete_login_rejects_nonce_mismatch() -> None:
    client = make_client(oidc_settings(), {"nonce": "served-nonce"})
    login_state = OIDCLoginState(nonce="expected", code_verifier="v", redirect_uri=f"{ISSUER}/cb")
    with pytest.raises(OIDCError):
        await client.complete_login("code", login_state)


async def test_complete_login_enforces_domain_allowlist() -> None:
    box = {"nonce": "n", "email": "mallory@evil.com"}
    client = make_client(oidc_settings(dashboard_oidc_allowed_domains="ocean.io"), box)
    login_state = OIDCLoginState(nonce="n", code_verifier="v", redirect_uri=f"{ISSUER}/cb")
    with pytest.raises(OIDCError):
        await client.complete_login("code", login_state)


async def test_complete_login_allows_listed_domain() -> None:
    box = {"nonce": "n", "email": "bob@ocean.io"}
    client = make_client(oidc_settings(dashboard_oidc_allowed_domains="ocean.io, other.com"), box)
    login_state = OIDCLoginState(nonce="n", code_verifier="v", redirect_uri=f"{ISSUER}/cb")
    assert await client.complete_login("code", login_state) == "bob@ocean.io"


def test_client_requires_issuer_and_credentials() -> None:
    with pytest.raises(RuntimeError):
        OIDCClient(Settings(host="http://x", dashboard_oidc_enabled=True))


# ---------------------------------------------------------------------------
# State store
# ---------------------------------------------------------------------------


async def test_state_store_is_single_use(mock_redis) -> None:
    store = OIDCStateStore(mock_redis)
    await store.put("s1", OIDCLoginState("n", "v", "uri"))
    first = await store.take("s1")
    assert first is not None and first.nonce == "n"
    assert await store.take("s1") is None  # consumed


# ---------------------------------------------------------------------------
# Router / login modes
# ---------------------------------------------------------------------------


def _make_app(settings: Settings, nonce_box: dict | None = None) -> FastAPI:
    import time as _time

    from s3proxy.dashboard.auth import RedisSessionStore
    from s3proxy.dashboard.router import create_dashboard_router
    from s3proxy.dashboard.stats_store import MemoryStatsStore
    from s3proxy.state.redis import get_redis

    app = FastAPI()
    app.include_router(
        create_dashboard_router(settings, credentials_store={}, version="1.2.3"),
        prefix=settings.dashboard_path,
    )
    app.state.settings = settings
    app.state.start_time = _time.monotonic()
    app.state.stats_store = MemoryStatsStore(settings)
    app.state.session_store = RedisSessionStore(get_redis())
    if settings.dashboard_oidc_enabled:
        app.state.oidc_client = make_client(settings, {} if nonce_box is None else nonce_box)
        app.state.oidc_state_store = OIDCStateStore(get_redis())
    return app


def test_authmodes_reports_enabled_methods() -> None:
    settings = oidc_settings(dashboard_oidc_button_label="JumpCloud")
    client = TestClient(_make_app(settings))
    payload = client.get("/dashboard/api/authmodes").json()
    assert payload == {"password": True, "oidc": True, "oidc_label": "JumpCloud"}


def test_password_login_rejected_when_disabled() -> None:
    settings = oidc_settings(dashboard_password_enabled=False)
    client = TestClient(_make_app(settings), follow_redirects=False)
    r = client.post("/dashboard/api/login", data={"username": "admin", "password": "admin"})
    assert r.status_code == 303
    assert "error=1" in r.headers["location"]


def test_no_login_method_enabled_raises() -> None:
    settings = Settings(
        host="http://localhost:9000",
        dashboard_ui=True,
        dashboard_password_enabled=False,
        dashboard_oidc_enabled=False,
    )
    with pytest.raises(RuntimeError):
        _make_app(settings)


def test_oidc_login_redirects_to_idp_and_persists_state() -> None:
    client = TestClient(_make_app(oidc_settings()), follow_redirects=False)
    r = client.get("/dashboard/api/oidc/login")
    assert r.status_code == 303
    assert r.headers["location"].startswith(f"{ISSUER}/authorize?")


def test_oidc_callback_bad_state_redirects_to_error() -> None:
    client = TestClient(_make_app(oidc_settings()), follow_redirects=False)
    r = client.get("/dashboard/api/oidc/callback?code=c&state=unknown")
    assert r.status_code == 303
    assert "error=sso" in r.headers["location"]


async def test_oidc_full_flow_sets_session_cookie(mock_redis) -> None:
    box: dict = {}
    settings = oidc_settings(dashboard_password_enabled=False)
    app = _make_app(settings, box)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://testserver", follow_redirects=False
    ) as http:
        login = await http.get("/dashboard/api/oidc/login")
        state = login.headers["location"].split("state=")[1].split("&")[0]

        raw = await mock_redis.get(f"s3proxy:oidc:state:{state}")
        box["nonce"] = json.loads(raw)["nonce"]
        box["email"] = "carol@ocean.io"

        cb = await http.get(f"/dashboard/api/oidc/callback?code=abc&state={state}")
    assert cb.status_code == 303
    assert cb.headers["location"].endswith("/dashboard/")
    assert "s3proxy_session=" in cb.headers.get("set-cookie", "")
