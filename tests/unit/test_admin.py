"""Tests for admin dashboard."""

import base64
import hashlib
import os
import time
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from s3proxy.admin.collectors import (
    RateTracker,
    RequestLog,
    _format_bytes,
    _format_uptime,
    collect_health,
    collect_pod_identity,
    collect_throughput,
    record_request,
)
from s3proxy.config import Settings

# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def admin_settings():
    return Settings(
        host="http://localhost:9000",
        encrypt_key="test-key-for-admin",
        admin_ui=True,
        admin_path="/admin",
    )


@pytest.fixture
def admin_disabled_settings():
    return Settings(
        host="http://localhost:9000",
        encrypt_key="test-key-for-admin",
        admin_ui=False,
    )


@pytest.fixture
def admin_credentials():
    return ("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")


@pytest.fixture
def admin_app(admin_settings, admin_credentials):
    with patch.dict(
        os.environ,
        {
            "AWS_ACCESS_KEY_ID": admin_credentials[0],
            "AWS_SECRET_ACCESS_KEY": admin_credentials[1],
        },
    ):
        from s3proxy.app import create_app

        return create_app(admin_settings)


@pytest.fixture
def admin_disabled_app(admin_disabled_settings, admin_credentials):
    with patch.dict(
        os.environ,
        {
            "AWS_ACCESS_KEY_ID": admin_credentials[0],
            "AWS_SECRET_ACCESS_KEY": admin_credentials[1],
        },
    ):
        from s3proxy.app import create_app

        return create_app(admin_disabled_settings)


@pytest.fixture
def client(admin_app):
    with TestClient(admin_app) as c:
        yield c


@pytest.fixture
def disabled_client(admin_disabled_app):
    with TestClient(admin_disabled_app) as c:
        yield c


def _basic_auth_header(username: str, password: str) -> dict:
    token = base64.b64encode(f"{username}:{password}".encode()).decode()
    return {"Authorization": f"Basic {token}"}


# ============================================================================
# Auth Tests
# ============================================================================


class TestAdminAuth:
    def test_no_credentials_returns_401(self, client):
        response = client.get("/admin/")
        assert response.status_code == 401
        assert "WWW-Authenticate" in response.headers

    def test_wrong_credentials_returns_401(self, client):
        headers = _basic_auth_header("wrong", "wrong")
        response = client.get("/admin/", headers=headers)
        assert response.status_code == 401

    def test_valid_credentials_returns_200(self, client, admin_credentials):
        headers = _basic_auth_header(admin_credentials[0], admin_credentials[1])
        response = client.get("/admin/", headers=headers)
        assert response.status_code == 200

    def test_custom_admin_credentials(self, admin_credentials):
        with patch.dict(
            os.environ,
            {
                "AWS_ACCESS_KEY_ID": admin_credentials[0],
                "AWS_SECRET_ACCESS_KEY": admin_credentials[1],
            },
        ):
            from s3proxy.app import create_app

            settings = Settings(
                host="http://localhost:9000",
                encrypt_key="test-key",
                admin_ui=True,
                admin_username="myadmin",
                admin_password="mysecret",
            )
            app = create_app(settings)
            with TestClient(app) as c:
                # AWS creds should NOT work
                headers = _basic_auth_header(admin_credentials[0], admin_credentials[1])
                assert c.get("/admin/", headers=headers).status_code == 401

                # Custom creds should work
                headers = _basic_auth_header("myadmin", "mysecret")
                assert c.get("/admin/", headers=headers).status_code == 200


# ============================================================================
# Dashboard HTML Tests
# ============================================================================


class TestDashboardHTML:
    def test_returns_html(self, client, admin_credentials):
        headers = _basic_auth_header(admin_credentials[0], admin_credentials[1])
        response = client.get("/admin/", headers=headers)
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

    def test_contains_expected_sections(self, client, admin_credentials):
        headers = _basic_auth_header(admin_credentials[0], admin_credentials[1])
        html = client.get("/admin/", headers=headers).text
        assert "S3Proxy Admin" in html
        assert "Health" in html
        assert "Throughput" in html
        assert "Live Feed" in html

    def test_no_sensitive_data_in_html(self, client, admin_credentials, admin_settings):
        headers = _basic_auth_header(admin_credentials[0], admin_credentials[1])
        html = client.get("/admin/", headers=headers).text
        # Raw key should never appear
        assert admin_settings.encrypt_key not in html
        # KEK bytes should never appear
        kek_hex = admin_settings.kek.hex()
        assert kek_hex not in html
        # AWS secret key should never appear
        assert admin_credentials[1] not in html


# ============================================================================
# API Status Endpoint Tests
# ============================================================================


class TestApiStatus:
    def test_returns_json(self, client, admin_credentials):
        headers = _basic_auth_header(admin_credentials[0], admin_credentials[1])
        response = client.get("/admin/api/status", headers=headers)
        assert response.status_code == 200
        assert "application/json" in response.headers["content-type"]

    def test_contains_expected_keys(self, client, admin_credentials):
        headers = _basic_auth_header(admin_credentials[0], admin_credentials[1])
        data = client.get("/admin/api/status", headers=headers).json()
        assert "pod" in data
        assert "health" in data
        assert "throughput" in data
        assert "request_log" in data
        assert "formatted" in data
        assert "all_pods" in data

    def test_pod_has_identity_fields(self, client, admin_credentials, admin_settings):
        headers = _basic_auth_header(admin_credentials[0], admin_credentials[1])
        data = client.get("/admin/api/status", headers=headers).json()
        pod = data["pod"]
        expected_fp = hashlib.sha256(admin_settings.kek).hexdigest()[:16]
        assert pod["kek_fingerprint"] == expected_fp
        assert "pod_name" in pod
        assert "uptime_seconds" in pod
        assert "storage_backend" in pod

    def test_no_sensitive_data_in_json(self, client, admin_credentials, admin_settings):
        headers = _basic_auth_header(admin_credentials[0], admin_credentials[1])
        response_text = client.get("/admin/api/status", headers=headers).text
        # Raw encryption key
        assert admin_settings.encrypt_key not in response_text
        # Full KEK hex
        assert admin_settings.kek.hex() not in response_text
        # AWS secret key
        assert admin_credentials[1] not in response_text

    def test_requires_auth(self, client):
        response = client.get("/admin/api/status")
        assert response.status_code == 401

    def test_x_served_by_header(self, client, admin_credentials):
        headers = _basic_auth_header(admin_credentials[0], admin_credentials[1])
        response = client.get("/admin/api/status", headers=headers)
        assert "x-served-by" in response.headers


# ============================================================================
# Route Priority Tests
# ============================================================================


class TestRoutePriority:
    def test_admin_not_caught_by_s3_catchall(self, client, admin_credentials):
        """Admin routes should return HTML/JSON, not S3 XML."""
        headers = _basic_auth_header(admin_credentials[0], admin_credentials[1])
        response = client.get("/admin/", headers=headers)
        assert response.status_code == 200
        assert "application/xml" not in response.headers.get("content-type", "")

    def test_admin_disabled_falls_through(self, disabled_client):
        """When admin is disabled, /admin should be caught by S3 catch-all."""
        response = disabled_client.get("/admin/")
        # Will be caught by the S3 proxy catch-all (may error, but should be XML)
        assert response.status_code != 200 or "application/xml" in response.headers.get(
            "content-type", ""
        )


# ============================================================================
# Collector Tests
# ============================================================================


class TestCollectors:
    def test_pod_identity(self, admin_settings):
        start = time.monotonic()
        result = collect_pod_identity(admin_settings, start)
        assert "pod_name" in result
        assert "uptime_seconds" in result
        assert "storage_backend" in result
        expected_fp = hashlib.sha256(admin_settings.kek).hexdigest()[:16]
        assert result["kek_fingerprint"] == expected_fp
        assert len(result["kek_fingerprint"]) == 16
        # Must not contain the actual key
        assert admin_settings.kek.hex() not in str(result)

    def test_health_keys(self):
        result = collect_health()
        assert "memory_reserved_bytes" in result
        assert "memory_limit_bytes" in result
        assert "memory_usage_pct" in result
        assert "requests_in_flight" in result
        assert "errors_4xx" in result
        assert "errors_5xx" in result
        assert "errors_503" in result

    def test_throughput_keys(self):
        result = collect_throughput()
        rates = result["rates"]
        assert "requests_per_min" in rates
        assert "encrypt_per_min" in rates
        assert "decrypt_per_min" in rates
        assert "bytes_encrypted_per_min" in rates
        assert "bytes_decrypted_per_min" in rates
        assert "errors_4xx_per_min" in rates
        assert "errors_5xx_per_min" in rates
        assert "errors_503_per_min" in rates
        history = result["history"]
        assert "requests_per_min" in history
        assert "bytes_encrypted_per_min" in history
        assert "bytes_decrypted_per_min" in history

    def test_format_bytes(self):
        assert _format_bytes(0) == "0 B"
        assert _format_bytes(1023) == "1023 B"
        assert _format_bytes(1024) == "1.0 KB"
        assert _format_bytes(1048576) == "1.0 MB"
        assert _format_bytes(1073741824) == "1.0 GB"

    def test_format_uptime(self):
        assert _format_uptime(30) == "0m"
        assert _format_uptime(60) == "1m"
        assert _format_uptime(3661) == "1h 1m"
        assert _format_uptime(90061) == "1d 1h 1m"


# ============================================================================
# Rate Tracker Tests
# ============================================================================


class TestRateTracker:
    def test_empty_tracker_returns_zero(self):
        tracker = RateTracker()
        assert tracker.rate_per_minute("requests") == 0.0

    def test_single_snapshot_returns_zero(self):
        tracker = RateTracker()
        tracker.record({"requests": 100})
        assert tracker.rate_per_minute("requests") == 0.0

    def test_rate_computation(self):
        tracker = RateTracker(window_seconds=300)
        # Simulate two snapshots 60 seconds apart
        tracker._snapshots.clear()
        tracker._snapshots.append((1000.0, {"requests": 100}))
        tracker._snapshots.append((1060.0, {"requests": 200}))
        # 100 requests in 60 seconds = 100/min
        assert tracker.rate_per_minute("requests") == 100.0

    def test_rate_unknown_key_returns_zero(self):
        tracker = RateTracker()
        tracker._snapshots.clear()
        tracker._snapshots.append((1000.0, {"requests": 100}))
        tracker._snapshots.append((1060.0, {"requests": 200}))
        assert tracker.rate_per_minute("nonexistent") == 0.0

    def test_pruning(self):
        tracker = RateTracker(window_seconds=10)
        now = time.monotonic()
        # Add old snapshots well before the window
        for i in range(50):
            tracker._snapshots.append((now - 100 + i, {"x": float(i)}))
        tracker.record({"x": 100.0})
        # Old entries beyond window + 10s buffer should be pruned
        assert len(tracker._snapshots) < 50

    def test_history_empty(self):
        tracker = RateTracker()
        assert tracker.history("requests") == []

    def test_history_single_snapshot(self):
        tracker = RateTracker()
        tracker.record({"requests": 100})
        assert tracker.history("requests") == []

    def test_history_computation(self):
        tracker = RateTracker()
        tracker._snapshots.clear()
        # 3 snapshots 60s apart: 100→200→400
        tracker._snapshots.append((1000.0, {"requests": 100}))
        tracker._snapshots.append((1060.0, {"requests": 200}))
        tracker._snapshots.append((1120.0, {"requests": 400}))
        hist = tracker.history("requests")
        assert len(hist) == 2
        assert hist[0] == 100.0  # (200-100)/60*60
        assert hist[1] == 200.0  # (400-200)/60*60

    def test_history_downsampling(self):
        tracker = RateTracker()
        tracker._snapshots.clear()
        for i in range(101):
            tracker._snapshots.append((1000.0 + i * 3, {"x": float(i * 10)}))
        hist = tracker.history("x", max_points=20)
        assert len(hist) == 20


# ============================================================================
# Request Log Tests
# ============================================================================


class TestRequestLog:
    def test_empty_log(self):
        log = RequestLog(maxlen=10)
        assert log.recent() == []

    def test_record_and_recent(self):
        log = RequestLog(maxlen=10)
        log.record("GET", "/bucket/key", "GetObject", 200, 0.05, 1024)
        entries = log.recent(10)
        assert len(entries) == 1
        assert entries[0]["method"] == "GET"
        assert entries[0]["operation"] == "GetObject"
        assert entries[0]["status"] == 200
        assert entries[0]["crypto"] == "decrypt"
        assert entries[0]["duration_ms"] == 50.0
        assert entries[0]["size"] == 1024

    def test_encrypt_crypto_tag(self):
        log = RequestLog(maxlen=10)
        log.record("PUT", "/bucket/key", "PutObject", 200, 0.1, 2048)
        assert log.recent()[0]["crypto"] == "encrypt"

    def test_no_crypto_for_list(self):
        log = RequestLog(maxlen=10)
        log.record("GET", "/bucket/", "ListObjects", 200, 0.02, 0)
        assert log.recent()[0]["crypto"] == ""

    def test_maxlen_eviction(self):
        log = RequestLog(maxlen=5)
        for i in range(10):
            log.record("GET", f"/b/k{i}", "GetObject", 200, 0.01, 0)
        entries = log.recent(10)
        assert len(entries) == 5
        # Newest first
        assert entries[0]["path"] == "/b/k9"

    def test_newest_first(self):
        log = RequestLog(maxlen=10)
        log.record("GET", "/first", "GetObject", 200, 0.01, 0)
        log.record("PUT", "/second", "PutObject", 200, 0.01, 0)
        entries = log.recent()
        assert entries[0]["path"] == "/second"
        assert entries[1]["path"] == "/first"

    def test_record_request_function(self):
        from s3proxy.admin.collectors import _request_log

        initial = len(_request_log.recent(200))
        record_request("HEAD", "/bucket/key", "HeadObject", 200, 0.003, 0)
        assert len(_request_log.recent(200)) == initial + 1
