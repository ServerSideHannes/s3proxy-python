"""CLI entry point for S3Proxy server."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import structlog
import uvicorn
from structlog.stdlib import BoundLogger

from .app import create_app
from .config import Settings

pod_name = os.environ.get("HOSTNAME", "unknown")
logger: BoundLogger = structlog.get_logger(__name__).bind(pod=pod_name)


def main():
    """CLI entry point for running S3Proxy server."""
    try:
        import uvloop

        uvloop.install()
        logger.info("Using uvloop for improved performance")
    except ImportError:
        pass

    parser = argparse.ArgumentParser(description="S3Proxy - Transparent S3 encryption")
    parser.add_argument("--ip", default="0.0.0.0", help="Bind address")
    parser.add_argument("--port", type=int, default=4433, help="Listen port")
    parser.add_argument("--no-tls", action="store_true", help="Disable TLS")
    parser.add_argument("--cert-path", default="/etc/s3proxy/certs", help="Cert directory")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    parser.add_argument("--log-level", default="INFO", help="Log level")
    args = parser.parse_args()

    # Set environment variables from CLI args
    os.environ.setdefault("S3PROXY_IP", args.ip)
    os.environ.setdefault("S3PROXY_PORT", str(args.port))
    os.environ.setdefault("S3PROXY_NO_TLS", str(args.no_tls).lower())
    os.environ.setdefault("S3PROXY_CERT_PATH", args.cert_path)
    os.environ.setdefault("S3PROXY_REGION", args.region)
    os.environ.setdefault("S3PROXY_LOG_LEVEL", args.log_level)

    if not os.environ.get("S3PROXY_ENCRYPT_KEY"):
        sys.exit("Error: S3PROXY_ENCRYPT_KEY environment variable required")

    settings = Settings()
    application = create_app(settings)

    config = {
        "app": application,
        "host": settings.ip,
        "port": settings.port,
        "log_level": settings.log_level.lower(),
    }

    if settings.memory_limit_mb > 0:
        print(
            f"Memory bounded: memory_limit_mb={settings.memory_limit_mb} (excess requests get 503)",
            file=sys.stderr,
        )

    if not settings.no_tls:
        cert_path = Path(settings.cert_path)
        cert_file = cert_path / "s3proxy.crt"
        key_file = cert_path / "s3proxy.key"
        if cert_file.exists() and key_file.exists():
            config["ssl_certfile"] = str(cert_file)
            config["ssl_keyfile"] = str(key_file)
        else:
            print(f"Warning: No certs at {cert_path}, running without TLS", file=sys.stderr)

    uvicorn.run(**config)


# Re-export app for backward compatibility with existing deployments
# that use "s3proxy.main:app" as the ASGI application path
from .app import app  # noqa: E402, F401

if __name__ == "__main__":
    main()
