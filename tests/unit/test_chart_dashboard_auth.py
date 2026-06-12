"""Helm chart tests for dashboard login methods (password + OIDC SSO).

Renders the chart with `helm template` and asserts on the proxy ConfigMap, the
chart Secret, and the proxy Deployment env. Skipped when `helm` is unavailable.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

CHART_DIR = Path(__file__).resolve().parents[2] / "chart"

CREDS = (
    "secrets.credentials[0].accessKey=a",
    "secrets.credentials[0].secretKey=b",
    "secrets.credentials[0].kek=c",
)

pytestmark = pytest.mark.skipif(shutil.which("helm") is None, reason="helm binary not available")


def render(*set_args: str) -> list[dict]:
    cmd = ["helm", "template", "t", str(CHART_DIR)]
    for s in CREDS + set_args:
        cmd += ["--set", s]
    out = subprocess.run(cmd, capture_output=True, text=True)
    if out.returncode != 0:
        raise AssertionError(f"helm template failed:\n{out.stderr}")
    return [d for d in yaml.safe_load_all(out.stdout) if d]


def by_kind_name(docs: list[dict], kind: str, name: str) -> dict | None:
    for d in docs:
        if d.get("kind") == kind and d["metadata"]["name"] == name:
            return d
    return None


def config(docs: list[dict]) -> dict:
    return by_kind_name(docs, "ConfigMap", "s3proxy-python-config")["data"]


def secret(docs: list[dict]) -> dict:
    return by_kind_name(docs, "Secret", "s3proxy-python-secrets")["stringData"]


def proxy_env(docs: list[dict]) -> dict[str, dict]:
    dep = by_kind_name(docs, "Deployment", "s3proxy-python")
    container = dep["spec"]["template"]["spec"]["containers"][0]
    return {e["name"]: e for e in container.get("env", [])}


OIDC = (
    "dashboard.enabled=true",
    "dashboard.auth.oidc.enabled=true",
    "dashboard.auth.oidc.issuer=https://oauth.id.jumpcloud.com/",
    "dashboard.auth.oidc.clientId=cid",
    "dashboard.auth.oidc.clientSecret=topsecret",
)


def test_password_enabled_by_default():
    docs = render("dashboard.enabled=true")
    assert config(docs)["S3PROXY_DASHBOARD_PASSWORD_ENABLED"] == "true"
    assert "S3PROXY_DASHBOARD_OIDC_ENABLED" not in config(docs)
    assert secret(docs)["S3PROXY_DASHBOARD_USERNAME"] == "admin"


def test_oidc_config_in_configmap():
    docs = render(*OIDC, "dashboard.auth.oidc.allowedDomains=example.com")
    data = config(docs)
    assert data["S3PROXY_DASHBOARD_OIDC_ENABLED"] == "true"
    assert data["S3PROXY_DASHBOARD_OIDC_ISSUER"] == "https://oauth.id.jumpcloud.com/"
    assert data["S3PROXY_DASHBOARD_OIDC_CLIENT_ID"] == "cid"
    assert data["S3PROXY_DASHBOARD_OIDC_ALLOWED_DOMAINS"] == "example.com"


def test_oidc_client_secret_goes_to_chart_secret():
    docs = render(*OIDC)
    assert secret(docs)["S3PROXY_DASHBOARD_OIDC_CLIENT_SECRET"] == "topsecret"


def test_sso_only_omits_password_from_secret():
    docs = render(*OIDC, "dashboard.auth.password.enabled=false")
    assert config(docs)["S3PROXY_DASHBOARD_PASSWORD_ENABLED"] == "false"
    data = secret(docs)
    assert "S3PROXY_DASHBOARD_USERNAME" not in data
    assert "S3PROXY_DASHBOARD_PASSWORD" not in data
    assert data["S3PROXY_DASHBOARD_OIDC_CLIENT_SECRET"] == "topsecret"


def test_oidc_existing_secret_pulled_via_env():
    docs = render(
        *OIDC,
        "dashboard.auth.oidc.existingSecret.name=my-oidc",
        "dashboard.auth.oidc.existingSecret.clientSecretKey=CS",
    )
    env = proxy_env(docs)
    ref = env["S3PROXY_DASHBOARD_OIDC_CLIENT_SECRET"]["valueFrom"]["secretKeyRef"]
    assert ref["name"] == "my-oidc"
    assert ref["key"] == "CS"
    # ...and it is not duplicated into the chart secret.
    assert "S3PROXY_DASHBOARD_OIDC_CLIENT_SECRET" not in secret(docs)


def test_no_pod_tls_by_default():
    docs = render("dashboard.enabled=true")
    assert config(docs)["S3PROXY_NO_TLS"] == "true"
    dep = by_kind_name(docs, "Deployment", "s3proxy-python")
    assert not dep["spec"]["template"]["spec"].get("volumes")
    cm = by_kind_name(docs, "ConfigMap", "s3proxy-python-dashboard-nginx")["data"]["default.conf"]
    assert "proxy_pass http://" in cm


def test_pod_tls_from_existing_secret():
    docs = render("dashboard.enabled=true", "server.tls.existingSecret=s3proxy-tls")
    data = config(docs)
    assert data["S3PROXY_NO_TLS"] == "false"
    assert data["S3PROXY_CERT_PATH"] == "/etc/s3proxy/certs"

    dep = by_kind_name(docs, "Deployment", "s3proxy-python")
    spec = dep["spec"]["template"]["spec"]
    vol = next(v for v in spec["volumes"] if v["name"] == "tls")
    assert vol["secret"]["secretName"] == "s3proxy-tls"
    paths = {i["key"]: i["path"] for i in vol["secret"]["items"]}
    assert paths == {"tls.crt": "s3proxy.crt", "tls.key": "s3proxy.key"}

    container = spec["containers"][0]
    assert any(m["mountPath"] == "/etc/s3proxy/certs" for m in container["volumeMounts"])
    for probe in ("startupProbe", "livenessProbe", "readinessProbe"):
        assert container[probe]["httpGet"]["scheme"] == "HTTPS"


def test_pod_tls_switches_dashboard_nginx_to_https():
    docs = render("dashboard.enabled=true", "server.tls.existingSecret=s3proxy-tls")
    cm = by_kind_name(docs, "ConfigMap", "s3proxy-python-dashboard-nginx")["data"]["default.conf"]
    assert "proxy_pass https://" in cm
    assert "proxy_ssl_verify off;" in cm


def test_app_existing_secret_forces_explicit_oidc_env():
    docs = render(
        *OIDC,
        "secrets.existingSecrets.enabled=true",
        "secrets.existingSecrets.name=appsec",
    )
    env = proxy_env(docs)
    ref = env["S3PROXY_DASHBOARD_OIDC_CLIENT_SECRET"]["valueFrom"]["secretKeyRef"]
    assert ref["name"] == "s3proxy-python-secrets"
    assert ref["key"] == "S3PROXY_DASHBOARD_OIDC_CLIENT_SECRET"
