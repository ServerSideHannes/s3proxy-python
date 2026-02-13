"""Tests for admin dashboard."""

import base64
import hashlib
import os
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from s3proxy.admin.collectors import (
    _format_bytes,
    _format_uptime,
    collect_key_status,
    collect_request_stats,
    collect_system_health,
)
from s3proxy.config import Settings
from s3proxy.state import MultipartStateManager

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
        assert "Key Status" in html
        assert "Active Uploads" in html
        assert "System Health" in html
        assert "Request Stats" in html

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
        assert "key_status" in data
        assert "upload_status" in data
        assert "system_health" in data
        assert "request_stats" in data
        assert "formatted" in data

    def test_key_status_has_fingerprint(self, client, admin_credentials, admin_settings):
        headers = _basic_auth_header(admin_credentials[0], admin_credentials[1])
        data = client.get("/admin/api/status", headers=headers).json()
        ks = data["key_status"]
        expected_fp = hashlib.sha256(admin_settings.kek).hexdigest()[:16]
        assert ks["kek_fingerprint"] == expected_fp
        assert ks["algorithm"] == "AES-256-GCM + AES-KWP"

    def test_no_sensitive_data_in_json(self, client, admin_credentials, admin_settings):
        headers = _basic_auth_header(admin_credentials[0], admin_credentials[1])
        response_text = client.get("/admin/api/status", headers=headers).text
        # Raw encryption key
        assert admin_settings.encrypt_key not in response_text
        # Full KEK hex
        assert admin_settings.kek.hex() not in response_text
        # AWS secret key
        assert admin_credentials[1] not in response_text
        # DEK bytes (base64 encoded) should not be present
        # (no uploads active, but check the pattern)

    def test_requires_auth(self, client):
        response = client.get("/admin/api/status")
        assert response.status_code == 401


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
    def test_key_status_fingerprint(self, admin_settings):
        result = collect_key_status(admin_settings)
        expected = hashlib.sha256(admin_settings.kek).hexdigest()[:16]
        assert result["kek_fingerprint"] == expected
        assert len(result["kek_fingerprint"]) == 16
        # Must not contain the actual key
        assert admin_settings.kek.hex() not in str(result)

    def test_system_health_keys(self):
        import time

        result = collect_system_health(time.monotonic())
        assert "memory_reserved_bytes" in result
        assert "memory_limit_bytes" in result
        assert "memory_usage_pct" in result
        assert "requests_in_flight" in result
        assert "memory_rejections" in result
        assert "uptime_seconds" in result
        assert "storage_backend" in result

    def test_request_stats_keys(self):
        result = collect_request_stats()
        assert "total_requests" in result
        assert "encrypt_ops" in result
        assert "decrypt_ops" in result
        assert "bytes_encrypted" in result
        assert "bytes_decrypted" in result

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
# State Store list_keys Tests
# ============================================================================


class TestStateStoreListKeys:
    @pytest.mark.asyncio
    async def test_memory_store_list_keys(self):
        from s3proxy.state.storage import MemoryStateStore

        store = MemoryStateStore()
        assert await store.list_keys() == []
        await store.set("key1", b"data1", 3600)
        await store.set("key2", b"data2", 3600)
        keys = await store.list_keys()
        assert sorted(keys) == ["key1", "key2"]

    @pytest.mark.asyncio
    async def test_manager_list_active_uploads_empty(self):
        manager = MultipartStateManager()
        uploads = await manager.list_active_uploads()
        assert uploads == []

    @pytest.mark.asyncio
    async def test_manager_list_active_uploads_with_data(self):
        from s3proxy.crypto import generate_dek

        manager = MultipartStateManager()
        dek = generate_dek()
        await manager.create_upload("mybucket", "mykey.txt", "upload-123", dek)
        uploads = await manager.list_active_uploads()
        assert len(uploads) == 1
        assert uploads[0]["bucket"] == "mybucket"
        assert uploads[0]["key"] == "mykey.txt"
        assert uploads[0]["parts_count"] == 0
        # DEK must never be in the response
        upload_str = str(uploads[0])
        assert base64.b64encode(dek).decode() not in upload_str
        assert dek.hex() not in upload_str
