"""Helm chart tests for the bundled single Redis.

Renders the chart with `helm template` and asserts on the output. The chart has
no external dependencies (redis-ha was removed), so these run on a clean checkout
with no `helm dependency build`. Skipped when the `helm` binary is unavailable.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

CHART_DIR = Path(__file__).resolve().parents[2] / "chart"

# Minimal valid credentials so the chart renders.
CREDS = (
    "secrets.credentials[0].accessKey=a",
    "secrets.credentials[0].secretKey=b",
    "secrets.credentials[0].kek=c",
)

pytestmark = pytest.mark.skipif(shutil.which("helm") is None, reason="helm binary not available")


def _run(set_args: tuple[str, ...]) -> subprocess.CompletedProcess:
    cmd = ["helm", "template", "t", str(CHART_DIR)]
    for s in CREDS + set_args:
        cmd += ["--set", s]
    return subprocess.run(cmd, capture_output=True, text=True)


def render(*set_args: str) -> list[dict]:
    out = _run(set_args)
    if out.returncode != 0:
        raise AssertionError(f"helm template failed:\n{out.stderr}")
    return [d for d in yaml.safe_load_all(out.stdout) if d]


def render_expect_error(*set_args: str) -> str:
    out = _run(set_args)
    assert out.returncode != 0, "expected helm template to fail but it succeeded"
    return out.stderr


def by_kind_name(docs: list[dict], kind: str, name: str) -> dict | None:
    for d in docs:
        if d.get("kind") == kind and d["metadata"]["name"] == name:
            return d
    return None


def configmap_data(docs: list[dict]) -> dict:
    cm = by_kind_name(docs, "ConfigMap", "s3proxy-python-config")
    return cm["data"]


# --------------------------------------------------------------------------
# Bundled Redis is on by default
# --------------------------------------------------------------------------


def test_no_redis_ha_dependency():
    """redis-ha is fully removed — Chart.yaml must declare no dependencies."""
    meta = yaml.safe_load((CHART_DIR / "Chart.yaml").read_text())
    assert "dependencies" not in meta or not meta["dependencies"]
    assert not (CHART_DIR / "Chart.lock").exists()


def test_redis_rendered_by_default():
    docs = render()
    assert by_kind_name(docs, "Deployment", "s3proxy-python-redis")
    assert by_kind_name(docs, "Service", "s3proxy-python-redis")


def test_proxy_points_at_bundled_redis():
    docs = render()
    assert configmap_data(docs)["S3PROXY_REDIS_URL"] == "redis://s3proxy-python-redis:6379/0"


def test_redis_uses_emptydir_not_pvc():
    docs = render()
    dep = by_kind_name(docs, "Deployment", "s3proxy-python-redis")
    vols = dep["spec"]["template"]["spec"]["volumes"]
    assert any("emptyDir" in v for v in vols)
    assert not any(d.get("kind") == "PersistentVolumeClaim" for d in docs)


def test_redis_image_pinned_from_values():
    docs = render("redis.image.tag=7.4-alpine")
    dep = by_kind_name(docs, "Deployment", "s3proxy-python-redis")
    img = dep["spec"]["template"]["spec"]["containers"][0]["image"]
    assert img == "redis:7.4-alpine"


def test_service_selectors_do_not_overlap():
    """Main Service must select only server pods, Redis Service only redis pods."""
    docs = render()
    main = by_kind_name(docs, "Service", "s3proxy-python")
    redis = by_kind_name(docs, "Service", "s3proxy-python-redis")
    assert main["spec"]["selector"]["app.kubernetes.io/component"] == "server"
    assert redis["spec"]["selector"]["app.kubernetes.io/component"] == "redis"

    server_pod = by_kind_name(docs, "Deployment", "s3proxy-python")
    redis_pod = by_kind_name(docs, "Deployment", "s3proxy-python-redis")
    assert (
        server_pod["spec"]["template"]["metadata"]["labels"]["app.kubernetes.io/component"]
        == "server"
    )
    assert (
        redis_pod["spec"]["template"]["metadata"]["labels"]["app.kubernetes.io/component"]
        == "redis"
    )


# --------------------------------------------------------------------------
# Auth
# --------------------------------------------------------------------------


def test_no_auth_by_default():
    docs = render()
    dep = by_kind_name(docs, "Deployment", "s3proxy-python-redis")
    args = dep["spec"]["template"]["spec"]["containers"][0].get("args", [])
    assert "--requirepass" not in args
    assert not by_kind_name(docs, "Secret", "s3proxy-python-redis")


def test_auth_wires_secret_and_requirepass_and_proxy_env():
    docs = render("redis.auth.enabled=true", "redis.auth.password=secret123")
    secret = by_kind_name(docs, "Secret", "s3proxy-python-redis")
    assert secret["stringData"]["redis-password"] == "secret123"

    redis = by_kind_name(docs, "Deployment", "s3proxy-python-redis")
    assert "--requirepass" in redis["spec"]["template"]["spec"]["containers"][0]["args"]

    proxy = by_kind_name(docs, "Deployment", "s3proxy-python")
    env = proxy["spec"]["template"]["spec"]["containers"][0].get("env", [])
    assert any(e["name"] == "S3PROXY_REDIS_PASSWORD" for e in env)


def test_auth_without_password_fails():
    err = render_expect_error("redis.auth.enabled=true")
    assert "redis.auth.password is empty" in err


# --------------------------------------------------------------------------
# External Redis (redis.enabled=false)
# --------------------------------------------------------------------------


def test_external_redis_no_bundled_pod():
    docs = render("redis.enabled=false", "externalRedis.url=redis://ext:6379/0")
    assert not by_kind_name(docs, "Deployment", "s3proxy-python-redis")
    assert not by_kind_name(docs, "Service", "s3proxy-python-redis")
    assert configmap_data(docs)["S3PROXY_REDIS_URL"] == "redis://ext:6379/0"
