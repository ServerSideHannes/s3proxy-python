"""Tests for per-access-key key resolution (KeyRing) and config wiring."""

import pytest
from pydantic import ValidationError

from s3proxy.config import Settings
from s3proxy.keyring import KeyRing, derive_kek


def _ring():
    return KeyRing(keys={"AKIA-A": derive_kek("a-kek"), "AKIA-B": derive_kek("b-kek")})


class TestKeyRingResolution:
    def test_key_for_known_access_key(self):
        ring = _ring()
        kid, kek = ring.key_for("AKIA-A")
        assert kid == "AKIA-A"
        assert kek == derive_kek("a-kek")

    def test_key_for_unknown_access_key_raises(self):
        ring = _ring()
        with pytest.raises(KeyError):
            ring.key_for("AKIA-UNKNOWN")

    def test_key_by_id_roundtrip(self):
        ring = _ring()
        # kid stored on an object == the access key that wrote it
        assert ring.key_by_id("AKIA-B") == derive_kek("b-kek")

    def test_key_by_id_empty_raises(self):
        ring = _ring()
        with pytest.raises(KeyError):
            ring.key_by_id("")

    def test_key_by_id_unknown_raises(self):
        ring = _ring()
        with pytest.raises(KeyError):
            ring.key_by_id("AKIA-GHOST")


class TestSettingsKeyRing:
    def test_no_credentials(self):
        s = Settings()
        assert s.credentials_store == {}

    def test_credentials_build_store_and_keyring(self):
        s = Settings(
            credentials=[
                {"access_key": "AKIA-A", "secret_key": "a-sec", "kek": "a-kek"},
                {"access_key": "AKIA-B", "secret_key": "b-sec", "kek": "b-kek"},
            ],
        )
        assert s.credentials_store == {"AKIA-A": "a-sec", "AKIA-B": "b-sec"}
        assert s.keyring.key_for("AKIA-A")[0] == "AKIA-A"
        assert s.keyring.key_for("AKIA-A")[1] == derive_kek("a-kek")
        # decrypt resolves by the stored kid (= access key)
        assert s.keyring.key_by_id("AKIA-B") == derive_kek("b-kek")

    def test_credentials_from_env_json(self, monkeypatch):
        monkeypatch.setenv(
            "S3PROXY_CREDENTIALS",
            '[{"access_key":"AKIA-A","secret_key":"s","kek":"e"}]',
        )
        s = Settings()
        assert s.credentials_store == {"AKIA-A": "s"}
        assert s.keyring.key_for("AKIA-A")[1] == derive_kek("e")

    def test_duplicate_access_key_raises(self):
        with pytest.raises(ValidationError):
            Settings(
                credentials=[
                    {"access_key": "dup", "secret_key": "1", "kek": "a"},
                    {"access_key": "dup", "secret_key": "2", "kek": "b"},
                ],
            )

    def test_unknown_access_key_rejected_at_encrypt(self):
        s = Settings(credentials=[{"access_key": "AKIA-A", "secret_key": "s", "kek": "e"}])
        with pytest.raises(KeyError):
            s.keyring.key_for("AKIA-NOT-CONFIGURED")


class TestAdminSecret:
    def test_admin_ui_requires_secret(self):
        with pytest.raises(ValidationError):
            Settings(admin_ui=True, admin_username="a", admin_password="b")

    def test_admin_session_secret_is_stable(self):
        a = Settings(
            admin_ui=True, admin_username="a", admin_password="b", admin_secret="sek"
        ).admin_session_secret
        b = Settings(
            admin_ui=True, admin_username="a", admin_password="b", admin_secret="sek"
        ).admin_session_secret
        assert a == b
        assert len(a) == 32
