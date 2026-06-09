"""The S3 catch-all must not record dashboard requests as proxied traffic.

A bare ``/dashboard`` (no trailing slash) misses the mounted dashboard router and
falls through to the S3 catch-all, where it would be logged as a phantom
"dashboard" bucket. _is_dashboard_path filters those out.
"""

from __future__ import annotations

from types import SimpleNamespace

from s3proxy.config import Settings
from s3proxy.request_handler import _is_dashboard_path


def _req(settings: Settings):
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(settings=settings)))


def _settings(**kw) -> Settings:
    base = {
        "host": "http://localhost:9000",
        "dashboard_ui": True,
        "dashboard_username": "admin",
        "dashboard_password": "admin",
        "dashboard_secret": "s",
        "credentials": [{"access_key": "AK", "secret_key": "s", "kek": "k"}],
    }
    base.update(kw)
    return Settings(**base)


def test_bare_dashboard_prefix_is_filtered() -> None:
    r = _req(_settings())
    assert _is_dashboard_path(r, "/dashboard") is True
    assert _is_dashboard_path(r, "/dashboard/") is True
    assert _is_dashboard_path(r, "/dashboard/api/status") is True


def test_real_buckets_are_not_filtered() -> None:
    r = _req(_settings())
    assert _is_dashboard_path(r, "/scylla-backups/obj.bin") is False
    assert _is_dashboard_path(r, "/my-bucket") is False
    # A bucket literally named "dashboard-data" must not be swallowed by prefix match.
    assert _is_dashboard_path(r, "/dashboard-data/key") is False


def test_custom_dashboard_path() -> None:
    r = _req(_settings(dashboard_path="/ops/dash"))
    assert _is_dashboard_path(r, "/ops/dash") is True
    assert _is_dashboard_path(r, "/ops/dash/login") is True
    assert _is_dashboard_path(r, "/dashboard") is False  # not the configured prefix


def test_not_filtered_when_dashboard_ui_disabled() -> None:
    # dashboard_secret not required when dashboard_ui is off
    r = _req(_settings(dashboard_ui=False, dashboard_secret=""))
    assert _is_dashboard_path(r, "/dashboard") is False
