"""Configuration management for S3Proxy."""

import hashlib

from pydantic import Field, PrivateAttr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """S3Proxy configuration settings."""

    model_config = SettingsConfigDict(env_prefix="S3PROXY_", env_file=".env")

    # S3 endpoint configuration
    host: str = Field(default="s3.amazonaws.com", description="S3 endpoint hostname or URL")
    region: str = Field(default="us-east-1", description="AWS region")

    # Encryption settings
    encrypt_key: str = Field(..., description="Key Encryption Key (KEK) - will be SHA256 hashed")
    dektag_name: str = Field(default="isec", description="Metadata tag name for encrypted DEK")

    # Server settings
    ip: str = Field(default="0.0.0.0", description="Bind address")
    port: int = Field(default=4433, description="Listen port")
    no_tls: bool = Field(default=False, description="Disable TLS")
    cert_path: str = Field(default="/etc/s3proxy/certs", description="TLS certificate directory")

    # Memory settings
    # This is the ONLY setting needed for OOM protection.
    # Use nginx proxy-body-size at ingress to reject oversized requests before they reach Python.
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

    # Logging
    log_level: str = Field(default="INFO", description="Log level (DEBUG, INFO, WARNING, ERROR)")

    # Admin dashboard settings
    admin_ui: bool = Field(default=False, description="Enable admin dashboard")
    admin_path: str = Field(default="/admin", description="URL path prefix for admin dashboard")
    admin_username: str = Field(
        default="", description="Admin dashboard username (default: AWS access key)"
    )
    admin_password: str = Field(
        default="", description="Admin dashboard password (default: AWS secret key)"
    )

    # Cached KEK derived from encrypt_key (computed once in model_post_init)
    _kek: bytes = PrivateAttr()

    def model_post_init(self, __context: object) -> None:
        self._kek = hashlib.sha256(self.encrypt_key.encode()).digest()

    @property
    def kek(self) -> bytes:
        """Get the 32-byte Key Encryption Key (SHA256 of encrypt_key)."""
        return self._kek

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
