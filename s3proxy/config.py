"""Configuration management for S3Proxy."""

import hashlib
from functools import lru_cache

from pydantic import Field, field_validator
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

    # Performance settings
    # Memory usage: file_size + ~64MB per concurrent upload
    # For 1GB pod with 10MB files: ~13 concurrent safe, default 10 for margin
    # Files >16MB automatically use multipart encryption
    throttling_requests_max: int = Field(default=10, description="Max concurrent requests (0=unlimited)")
    max_upload_size_mb: int = Field(default=45, description="Max single-request upload size (MB)")

    # Redis settings (for distributed state in HA deployments)
    redis_url: str = Field(default="", description="Redis URL for HA mode (empty = in-memory single-instance)")
    redis_password: str = Field(default="", description="Redis password (optional, can also be in URL)")
    redis_upload_ttl_hours: int = Field(default=24, description="TTL for upload state in Redis (hours)")

    # Logging
    log_level: str = Field(default="INFO", description="Log level (DEBUG, INFO, WARNING, ERROR)")

    @field_validator("encrypt_key")
    @classmethod
    def hash_encrypt_key(cls, v: str) -> str:
        """Store the raw key - we'll hash it when needed."""
        return v

    @property
    def kek(self) -> bytes:
        """Get the 32-byte Key Encryption Key (SHA256 of encrypt_key)."""
        return hashlib.sha256(self.encrypt_key.encode()).digest()

    @property
    def max_upload_size_bytes(self) -> int:
        """Max upload size in bytes."""
        return self.max_upload_size_mb * 1024 * 1024

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


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
