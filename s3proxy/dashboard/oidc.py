"""OIDC single sign-on for the dashboard.

Generic OpenID Connect authorization-code flow with PKCE. The issuer's
discovery document (``.well-known/openid-configuration``) supplies the
authorization, token and userinfo endpoints, so any compliant provider works
(JumpCloud, Okta, Google, Entra ID, ...).

The ID token is read directly from the token-endpoint response, which is a
back-channel HTTPS call to the issuer. Per OIDC Core §3.1.3.7, signature
validation MAY be skipped for tokens obtained this way over TLS, so we verify
the standard claims (iss / aud / exp / nonce) rather than fetching JWKS.
"""

from __future__ import annotations

import base64
import hashlib
import json
import secrets
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode

import httpx

if TYPE_CHECKING:
    from ..config import Settings

OIDC_STATE_TTL_SECONDS = 600  # login round-trip window


class OIDCError(Exception):
    """Raised when an OIDC login cannot be completed."""


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def make_pkce() -> tuple[str, str]:
    """Return (code_verifier, code_challenge) for PKCE S256."""
    verifier = _b64url(secrets.token_bytes(32))
    challenge = _b64url(hashlib.sha256(verifier.encode("ascii")).digest())
    return verifier, challenge


def decode_jwt_claims(token: str) -> dict[str, Any]:
    """Decode a JWT payload without verifying the signature."""
    try:
        payload_b64 = token.split(".")[1]
        padding = "=" * (-len(payload_b64) % 4)
        return json.loads(base64.urlsafe_b64decode(payload_b64 + padding))
    except (IndexError, ValueError) as exc:
        raise OIDCError("Malformed ID token") from exc


@dataclass
class OIDCLoginState:
    """Transient per-login data, stored server-side keyed by ``state``."""

    nonce: str
    code_verifier: str
    redirect_uri: str

    def to_json(self) -> str:
        return json.dumps(
            {
                "nonce": self.nonce,
                "code_verifier": self.code_verifier,
                "redirect_uri": self.redirect_uri,
            }
        )

    @classmethod
    def from_json(cls, raw: str) -> OIDCLoginState:
        d = json.loads(raw)
        return cls(d["nonce"], d["code_verifier"], d["redirect_uri"])


class OIDCStateStore:
    """Redis-backed store for in-flight OIDC login state (short TTL)."""

    _PREFIX = "s3proxy:oidc:state:"

    def __init__(self, redis) -> None:
        self._redis = redis

    async def put(self, state: str, data: OIDCLoginState) -> None:
        await self._redis.set(
            f"{self._PREFIX}{state}", data.to_json().encode(), ex=OIDC_STATE_TTL_SECONDS
        )

    async def take(self, state: str) -> OIDCLoginState | None:
        """Fetch and delete the state (single-use, prevents replay)."""
        key = f"{self._PREFIX}{state}"
        raw = await self._redis.get(key)
        if raw is None:
            return None
        await self._redis.delete(key)
        return OIDCLoginState.from_json(raw.decode())


class OIDCClient:
    """OIDC relying-party client driven by issuer discovery."""

    def __init__(self, settings: Settings) -> None:
        if not (
            settings.dashboard_oidc_issuer
            and settings.dashboard_oidc_client_id
            and settings.dashboard_oidc_client_secret
        ):
            raise RuntimeError(
                "OIDC requires S3PROXY_DASHBOARD_OIDC_ISSUER, _CLIENT_ID and _CLIENT_SECRET"
            )
        self._settings = settings
        self._issuer = settings.dashboard_oidc_issuer.rstrip("/")
        self._client_id = settings.dashboard_oidc_client_id
        self._client_secret = settings.dashboard_oidc_client_secret
        self._scopes = settings.dashboard_oidc_scopes
        self._username_claim = settings.dashboard_oidc_username_claim
        self._allowed_domains = settings.dashboard_oidc_allowed_domain_set
        self._http = httpx.AsyncClient(timeout=10.0)
        self._discovery: dict[str, Any] | None = None

    async def aclose(self) -> None:
        await self._http.aclose()

    async def _discover(self) -> dict[str, Any]:
        if self._discovery is None:
            url = f"{self._issuer}/.well-known/openid-configuration"
            resp = await self._http.get(url)
            resp.raise_for_status()
            self._discovery = resp.json()
        return self._discovery

    async def authorization_url(self, redirect_uri: str) -> tuple[str, OIDCLoginState, str]:
        """Build the IdP authorization URL plus the state to persist."""
        disc = await self._discover()
        state = secrets.token_urlsafe(32)
        nonce = secrets.token_urlsafe(32)
        verifier, challenge = make_pkce()
        params = {
            "response_type": "code",
            "client_id": self._client_id,
            "redirect_uri": redirect_uri,
            "scope": self._scopes,
            "state": state,
            "nonce": nonce,
            "code_challenge": challenge,
            "code_challenge_method": "S256",
        }
        url = f"{disc['authorization_endpoint']}?{urlencode(params)}"
        return url, OIDCLoginState(nonce, verifier, redirect_uri), state

    async def exchange_code(self, code: str, login_state: OIDCLoginState) -> dict[str, Any]:
        disc = await self._discover()
        resp = await self._http.post(
            disc["token_endpoint"],
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": login_state.redirect_uri,
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                "code_verifier": login_state.code_verifier,
            },
            headers={"Accept": "application/json"},
        )
        if resp.status_code != 200:
            raise OIDCError(f"Token exchange failed ({resp.status_code})")
        return resp.json()

    def _verify_id_token(self, claims: dict[str, Any], nonce: str) -> None:
        iss = claims.get("iss", "").rstrip("/")
        if iss != self._issuer:
            raise OIDCError("ID token issuer mismatch")
        aud = claims.get("aud")
        aud_ok = self._client_id == aud or (isinstance(aud, list) and self._client_id in aud)
        if not aud_ok:
            raise OIDCError("ID token audience mismatch")
        if claims.get("nonce") != nonce:
            raise OIDCError("ID token nonce mismatch")
        exp = claims.get("exp")
        if not isinstance(exp, (int, float)) or exp < time.time():
            raise OIDCError("ID token expired")

    async def complete_login(self, code: str, login_state: OIDCLoginState) -> str:
        """Exchange the code, validate the ID token, return the session username."""
        tokens = await self.exchange_code(code, login_state)
        id_token = tokens.get("id_token")
        if not id_token:
            raise OIDCError("No ID token in token response")
        claims = decode_jwt_claims(id_token)
        self._verify_id_token(claims, login_state.nonce)

        username = claims.get(self._username_claim) or claims.get("email") or claims.get("sub")
        if not username:
            raise OIDCError("No usable identity claim in ID token")
        username = str(username)

        email = claims.get("email")
        if self._allowed_domains:
            domain = email.rsplit("@", 1)[-1].lower() if email and "@" in email else ""
            if domain not in self._allowed_domains:
                raise OIDCError("Email domain is not allowed")

        return username
