"""Configuration management for S3Proxy."""

import hashlib

from pydantic import BaseModel, Field, PrivateAttr
from pydantic_settings import BaseSettings, SettingsConfigDict

from .keyring import KeyRing, derive_kek


class CredentialEntry(BaseModel):
    """An AWS credential with its own encryption key, via S3PROXY_CREDENTIALS.

    The proxy verifies the client's SigV4 signature with `secret_key`, then
    encrypts/decrypts that credential's objects with the KEK derived from `kek`.
    """

    access_key: str
    secret_key: str
    kek: str = Field(..., description="Per-credential KEK secret - SHA256 hashed into a KEK")


class Settings(BaseSettings):
    """S3Proxy configuration settings."""

    model_config = SettingsConfigDict(env_prefix="S3PROXY_", env_file=".env")

    # S3 endpoint configuration
    host: str = Field(default="s3.amazonaws.com", description="S3 endpoint hostname or URL")
    region: str = Field(default="us-east-1", description="AWS region")

    # Encryption settings
    dektag_name: str = Field(default="isec", description="Metadata tag name for encrypted DEK")
    kidtag_name: str = Field(
        default="isec-kid", description="Metadata tag name for the key id that wrapped the DEK"
    )

    # Per-access-key encryption. Each entry is an AWS credential with its own
    # KEK. The access key that wrote an object is stored as its kid; objects are
    # decrypted with the KEK of that access key. Replaces the single
    # AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY credential.
    credentials: list[CredentialEntry] = Field(
        default_factory=list,
        description="AWS credentials with per-credential KEKs (JSON list)",
    )

    # Server settings
    ip: str = Field(default="0.0.0.0", description="Bind address")
    port: int = Field(default=4433, description="Listen port")
    no_tls: bool = Field(default=False, description="Disable TLS")
    cert_path: str = Field(default="/etc/s3proxy/certs", description="TLS certificate directory")

    # Memory settings
    # This is the ONLY setting needed for OOM protection.
    # Reject oversized requests at the front proxy to stop them before they reach Python.
    memory_limit_mb: int = Field(
        default=64,
        description="Memory budget for concurrent requests in MB. 0=unlimited. "
        "Small files use content_length*2, large files use 8MB (streaming). "
        "Excess requests wait up to 30s (backpressure), then get 503.",
    )

    # Redis settings (for distributed state in HA deployments)
    redis_url: str = Field(
        default="", description="Redis URL for HA mode (empty = in-memory single-instance)"
    )
    redis_password: str = Field(
        default="", description="Redis password (optional, can also be in URL)"
    )
    redis_upload_ttl_hours: int = Field(
        default=24, description="TTL for upload state in Redis (hours)"
    )

    # Admin stats store (Redis-backed cluster-wide dashboard state). Only used
    # when Redis is configured; single-instance mode keeps per-pod in-memory.
    request_log_cap: int = Field(
        default=10000, description="Max request-log entries kept in the Redis capped list"
    )
    request_log_ttl_hours: int = Field(
        default=24, description="TTL for the request log in Redis (hours)"
    )
    stats_ttl_hours: int = Field(
        default=24, description="TTL for shared counters/breakdowns in Redis (hours)"
    )
    stats_series_ttl_hours: int = Field(
        default=168, description="TTL for the per-minute time-series in Redis (hours, 7d default)"
    )

    # Logging
    log_level: str = Field(default="INFO", description="Log level (DEBUG, INFO, WARNING, ERROR)")

    # Admin dashboard
    admin_ui: bool = Field(default=False, description="Enable the admin dashboard at admin_path")
    admin_path: str = Field(default="/admin", description="URL path prefix for the admin UI")
    admin_username: str = Field(default="", description="Admin dashboard username")
    admin_password: str = Field(default="", description="Admin dashboard password")
    admin_secret: str = Field(
        default="",
        description="Stable secret for signing admin session cookies (required when admin_ui)",
    )

    # Cached KeyRing + credentials store (computed once in model_post_init).
    _keyring: KeyRing = PrivateAttr()
    _credentials_store: dict[str, str] = PrivateAttr()

    def model_post_init(self, __context: object) -> None:
        self._credentials_store = {}
        keys: dict[str, bytes] = {}
        for entry in self.credentials:
            if entry.access_key in self._credentials_store:
                raise ValueError(f"Duplicate access key: {entry.access_key!r}")
            self._credentials_store[entry.access_key] = entry.secret_key
            keys[entry.access_key] = derive_kek(entry.kek)
        self._keyring = KeyRing(keys=keys)

        if self.admin_ui and not self.admin_secret:
            raise ValueError("S3PROXY_ADMIN_SECRET is required when the admin dashboard is enabled")

    @property
    def admin_session_secret(self) -> bytes:
        """Stable 32-byte secret for signing admin session cookies."""
        return hashlib.sha256(b"s3proxy-admin-session|" + self.admin_secret.encode()).digest()

    @property
    def keyring(self) -> KeyRing:
        """Get the KeyRing resolving per-access-key encryption keys."""
        return self._keyring

    @property
    def credentials_store(self) -> dict[str, str]:
        """Get the access_key -> secret_key map for signature verification."""
        return self._credentials_store

    @property
    def s3_endpoint(self) -> str:
        """Get the full S3 endpoint URL."""
        if self.host.startswith("http://") or self.host.startswith("https://"):
            return self.host
        return f"https://{self.host}"

    @property
    def redis_upload_ttl_seconds(self) -> int:
        """Get Redis upload TTL in seconds."""
        return self.redis_upload_ttl_hours * 3600

    @property
    def request_log_ttl_seconds(self) -> int:
        """Get request-log TTL in seconds."""
        return self.request_log_ttl_hours * 3600

    @property
    def stats_ttl_seconds(self) -> int:
        """Get shared-counters TTL in seconds."""
        return self.stats_ttl_hours * 3600

    @property
    def stats_series_ttl_seconds(self) -> int:
        """Get per-minute time-series TTL in seconds."""
        return self.stats_series_ttl_hours * 3600
