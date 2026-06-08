"""The S3 catch-all must not record admin-dashboard requests as proxied traffic.

A bare ``/admin`` (no trailing slash) misses the mounted admin router and falls
through to the S3 catch-all, where it would be logged as a phantom "admin"
bucket. _is_admin_path filters those out.
"""

from __future__ import annotations

from types import SimpleNamespace

from s3proxy.config import Settings
from s3proxy.request_handler import _is_admin_path


def _req(settings: Settings):
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(settings=settings)))


def _settings(**kw) -> Settings:
    base = {
        "host": "http://localhost:9000",
        "admin_ui": True,
        "admin_username": "admin",
        "admin_password": "admin",
        "admin_secret": "s",
        "credentials": [{"access_key": "AK", "secret_key": "s", "kek": "k"}],
    }
    base.update(kw)
    return Settings(**base)


def test_bare_admin_prefix_is_filtered() -> None:
    r = _req(_settings())
    assert _is_admin_path(r, "/admin") is True
    assert _is_admin_path(r, "/admin/") is True
    assert _is_admin_path(r, "/admin/api/status") is True


def test_real_buckets_are_not_filtered() -> None:
    r = _req(_settings())
    assert _is_admin_path(r, "/scylla-backups/obj.bin") is False
    assert _is_admin_path(r, "/my-bucket") is False
    # A bucket literally named "admin-data" must not be swallowed by prefix match.
    assert _is_admin_path(r, "/admin-data/key") is False


def test_custom_admin_path() -> None:
    r = _req(_settings(admin_path="/ops/dash"))
    assert _is_admin_path(r, "/ops/dash") is True
    assert _is_admin_path(r, "/ops/dash/login") is True
    assert _is_admin_path(r, "/admin") is False  # not the configured prefix


def test_not_filtered_when_admin_ui_disabled() -> None:
    # admin_secret not required when admin_ui is off
    r = _req(_settings(admin_ui=False, admin_secret=""))
    assert _is_admin_path(r, "/admin") is False
