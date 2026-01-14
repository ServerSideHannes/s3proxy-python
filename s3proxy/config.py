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
    throttling_requests_max: int = Field(default=0, description="Max concurrent requests")
    max_single_encrypted_mb: int = Field(default=16, description="Max single-part object size (MB)")
    auto_multipart_mb: int = Field(default=16, description="Auto-multipart threshold (MB)")
    max_concurrent_uploads: int = Field(default=10, description="Max concurrent uploads")
    max_concurrent_downloads: int = Field(default=10, description="Max concurrent downloads")

    # Feature flags
    allow_multipart: bool = Field(default=False, description="Allow unencrypted multipart")

    # Redis settings (for distributed state)
    redis_url: str = Field(default="redis://localhost:6379/0", description="Redis connection URL")
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
    def max_single_encrypted_bytes(self) -> int:
        """Max single encrypted object size in bytes."""
        return self.max_single_encrypted_mb * 1024 * 1024

    @property
    def auto_multipart_bytes(self) -> int:
        """Auto-multipart threshold in bytes."""
        return self.auto_multipart_mb * 1024 * 1024

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
