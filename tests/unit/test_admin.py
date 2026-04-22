"""Tests for the admin dashboard."""

from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from s3proxy import metrics
from s3proxy.admin import collectors, record_request
from s3proxy.admin.auth import create_auth_dependency
from s3proxy.admin.router import create_admin_router
from s3proxy.admin.templates import render_dashboard
from s3proxy.config import Settings


def _reset_collector_state() -> None:
    collectors._request_log._entries.clear()
    collectors._rate_tracker._snapshots.clear()


@pytest.fixture(autouse=True)
def _clean_state():
    _reset_collector_state()
    yield
    _reset_collector_state()


@pytest.fixture
def admin_settings():
    return Settings(
        host="http://localhost:9000",
        encrypt_key="test-kek-32bytes!!!!!!!!!!!!!!!!",
        admin_ui=True,
        admin_username="admin",
        admin_password="secret",
    )


def test_record_request_splits_bucket_and_key() -> None:
    record_request("GET", "/my-bucket/path/to/file.txt", "GetObject", 200, 0.042, 1024, "10.0.0.1")
    entries = collectors._request_log.all()
    assert len(entries) == 1
    e = entries[0]
    assert e.bucket == "my-bucket"
    assert e.key == "path/to/file.txt"
    assert e.status == 200
    assert e.duration_ms == pytest.approx(42.0)
    assert e.client_ip == "10.0.0.1"


def test_collect_all_builds_expected_sections(admin_settings) -> None:
    record_request("PUT", "/customer-data/invoice.pdf", "PutObject", 200, 0.05, 2048, "10.0.0.1")
    record_request("GET", "/archives/log.gz", "GetObject", 500, 0.1, 0, "10.0.0.2")

    start = time.monotonic() - 120  # 2 minutes
    data = collectors.collect_all(admin_settings, start_time=start, version="9.9.9")

    assert data["header"]["title"] == "S3 Encryption Proxy"
    assert data["header"]["status"] == "Running"
    assert "m" in data["header"]["uptime"]

    assert set(data["cards"].keys()) == {"requests", "data_encrypted", "errors", "active_buckets"}
    assert data["cards"]["active_buckets"]["value"] == "2"

    ops = [row["operation"] for row in data["activity"]]
    assert ops == ["GET", "PUT"]  # newest first
    assert data["activity"][0]["status"] == "Error"
    assert data["activity"][1]["status"] == "Success"
    assert data["activity"][1]["bucket"] == "customer-data"
    assert data["activity"][1]["size"] == "2.0 KB"

    bucket_names = {b["name"] for b in data["buckets"]}
    assert bucket_names == {"customer-data", "archives"}

    assert data["keys"][0]["status"] == "Active"
    assert data["footer"]["version"] == "9.9.9"


def test_render_dashboard_injects_api_url() -> None:
    html = render_dashboard(admin_path="/ops")
    assert '"/ops/api/status"' in html
    assert "__API_URL__" not in html


def _make_app(settings: Settings):
    from fastapi import FastAPI

    app = FastAPI()
    router = create_admin_router(settings, credentials_store={}, version="1.2.3")
    app.include_router(router, prefix=settings.admin_path)
    app.state.settings = settings
    app.state.start_time = time.monotonic()
    return app


def test_dashboard_requires_auth(admin_settings) -> None:
    client = TestClient(_make_app(admin_settings))
    r = client.get("/admin/")
    assert r.status_code == 401
    assert r.headers.get("WWW-Authenticate", "").lower().startswith("basic")


def test_dashboard_html_served_with_auth(admin_settings) -> None:
    client = TestClient(_make_app(admin_settings))
    r = client.get("/admin/", auth=("admin", "secret"))
    assert r.status_code == 200
    assert "S3 Encryption Proxy" in r.text
    assert "Recent Activity" in r.text


def test_status_api_returns_expected_shape(admin_settings) -> None:
    client = TestClient(_make_app(admin_settings))
    r = client.get("/admin/api/status", auth=("admin", "secret"))
    assert r.status_code == 200
    payload = r.json()
    assert payload["header"]["status"] == "Running"
    assert payload["footer"]["version"] == "1.2.3"
    for key in ("requests", "data_encrypted", "errors", "active_buckets"):
        assert key in payload["cards"]


def test_auth_falls_back_to_aws_credentials() -> None:
    settings = Settings(
        host="http://localhost:9000",
        encrypt_key="test-kek",
        admin_ui=True,
    )
    dep = create_auth_dependency(settings, {"AKIAEXAMPLE": "secret-key"})
    assert callable(dep)


def test_auth_raises_when_no_credentials() -> None:
    settings = Settings(
        host="http://localhost:9000",
        encrypt_key="test-kek",
        admin_ui=True,
    )
    with pytest.raises(RuntimeError):
        create_auth_dependency(settings, {})


def test_collector_does_not_crash_on_empty_metrics(admin_settings) -> None:
    """collect_all must work even before any request has been recorded."""
    # Ensure we don't blow up on cold start
    data = collectors.collect_all(admin_settings, start_time=time.monotonic(), version="x")
    expected = f"{int(collectors._read_labeled_counter_sum(metrics.REQUEST_COUNT)):,}"
    assert data["cards"]["requests"]["value"] == expected
    assert data["activity"] == []
    assert data["buckets"] == []
