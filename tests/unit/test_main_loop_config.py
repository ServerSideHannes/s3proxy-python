"""uvicorn must not auto-install uvloop: the loop has to be pinned to asyncio.

2026-07-18: a build that skipped uvloop.install() still died with libuv's
uv__io_poll abort — uvicorn's default loop="auto" installs uvloop by itself
whenever the package is importable. The uvicorn config must pass
loop="asyncio" unless S3PROXY_UVLOOP=1 opts back in.
"""

import json
import sys

import uvicorn

from s3proxy import main as main_module

_CREDS = json.dumps([{"access_key": "AK", "secret_key": "SK", "kek": "kek-secret"}])


def _captured_uvicorn_config(monkeypatch, extra_env=None):
    captured = {}
    monkeypatch.setattr(uvicorn, "run", lambda **config: captured.update(config))
    monkeypatch.setenv("S3PROXY_CREDENTIALS", _CREDS)
    for key in ("IP", "PORT", "NO_TLS", "CERT_PATH", "REGION", "LOG_LEVEL"):
        monkeypatch.delenv(f"S3PROXY_{key}", raising=False)
    for k, v in (extra_env or {}).items():
        monkeypatch.setenv(k, v)
    monkeypatch.setattr(sys, "argv", ["s3proxy", "--no-tls"])
    main_module.main()
    return captured


def test_loop_defaults_to_asyncio(monkeypatch):
    config = _captured_uvicorn_config(monkeypatch)
    assert config["loop"] == "asyncio"


def test_loop_auto_only_when_uvloop_opted_in(monkeypatch):
    import uvloop

    monkeypatch.setattr(uvloop, "install", lambda: None)
    config = _captured_uvicorn_config(monkeypatch, {"S3PROXY_UVLOOP": "1"})
    assert config["loop"] == "auto"
