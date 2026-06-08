"""Helm chart tests for the bundled front proxy and the optional ingress.

These render the chart with `helm template` and assert on the output, so they
cover the template logic (guards, selectors, backend wiring) without a cluster.
Skipped automatically when the `helm` binary is unavailable.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

CHART_DIR = Path(__file__).resolve().parents[2] / "chart"

pytestmark = pytest.mark.skipif(shutil.which("helm") is None, reason="helm binary not available")


@pytest.fixture(scope="module")
def chart_dir(tmp_path_factory) -> Path:
    """A copy of the chart with the redis-ha dependency stripped from Chart.yaml.

    `helm template` checks declared dependencies before evaluating their
    `condition`, so it fails on a clean checkout where charts/ is empty — even
    with redis-ha.enabled=false. These tests don't exercise redis-ha, so we
    render a dependency-free copy and stay fully hermetic (no network, no
    `helm dependency build`)."""
    dest = tmp_path_factory.mktemp("chart") / "chart"
    shutil.copytree(CHART_DIR, dest)
    shutil.rmtree(dest / "charts", ignore_errors=True)
    chart_yaml = dest / "Chart.yaml"
    meta = yaml.safe_load(chart_yaml.read_text())
    meta.pop("dependencies", None)
    chart_yaml.write_text(yaml.safe_dump(meta, sort_keys=False))
    return dest


def _render(chart_dir: Path, set_args: tuple[str, ...]) -> subprocess.CompletedProcess:
    cmd = [
        "helm",
        "template",
        "t",
        str(chart_dir),
        "--set",
        "secrets.encryptKey=x",
        "--set",
        "secrets.awsAccessKeyId=a",
        "--set",
        "secrets.awsSecretAccessKey=b",
    ]
    for s in set_args:
        cmd += ["--set", s]
    return subprocess.run(cmd, capture_output=True, text=True)


def render(chart_dir: Path, *set_args: str) -> list[dict]:
    """Render the chart and return parsed manifests."""
    out = _render(chart_dir, set_args)
    if out.returncode != 0:
        raise AssertionError(f"helm template failed:\n{out.stderr}")
    return [d for d in yaml.safe_load_all(out.stdout) if d]


def render_expect_error(chart_dir: Path, *set_args: str) -> str:
    """Render expecting failure; return stderr for assertion."""
    out = _render(chart_dir, set_args)
    assert out.returncode != 0, "expected helm template to fail but it succeeded"
    return out.stderr


def by_kind_name(docs: list[dict], kind: str, name: str) -> dict | None:
    for d in docs:
        if d.get("kind") == kind and d["metadata"]["name"] == name:
            return d
    return None


# --------------------------------------------------------------------------
# Default: front proxy and ingress are off
# --------------------------------------------------------------------------


def test_disabled_by_default(chart_dir):
    docs = render(chart_dir)
    names = {(d.get("kind"), d["metadata"]["name"]) for d in docs}
    assert ("Deployment", "s3proxy-python-frontproxy") not in names
    assert ("Service", "s3proxy-python-frontproxy") not in names
    assert ("Service", "s3proxy-python-headless") not in names
    assert not any(d.get("kind") == "Ingress" for d in docs)


def test_no_nginx_or_gateway_anywhere(chart_dir):
    """The whole point of the change: no nginx/gateway artifacts, on or off."""
    docs = render(chart_dir, "frontproxy.enabled=true")
    blob = yaml.safe_dump_all(docs).lower()
    assert "nginx" not in blob
    assert "gateway" not in blob
    assert "externalname" not in blob


# --------------------------------------------------------------------------
# Front proxy
# --------------------------------------------------------------------------


def test_frontproxy_resources_rendered(chart_dir):
    docs = render(chart_dir, "frontproxy.enabled=true")
    for kind, name in [
        ("Deployment", "s3proxy-python-frontproxy"),
        ("Service", "s3proxy-python-frontproxy"),
        ("ConfigMap", "s3proxy-python-frontproxy"),
        ("PodDisruptionBudget", "s3proxy-python-frontproxy"),
        ("Service", "s3proxy-python-headless"),
    ]:
        assert by_kind_name(docs, kind, name), f"missing {kind}/{name}"


def test_headless_service_is_headless(chart_dir):
    docs = render(chart_dir, "frontproxy.enabled=true")
    hs = by_kind_name(docs, "Service", "s3proxy-python-headless")
    assert hs["spec"]["clusterIP"] == "None"
    assert hs["spec"]["selector"]["app.kubernetes.io/component"] == "server"


def test_service_selectors_do_not_overlap(chart_dir):
    """The main/headless Services must select only s3proxy pods, the frontproxy
    Service only HAProxy pods — otherwise the front proxy round-robins to itself."""
    docs = render(chart_dir, "frontproxy.enabled=true")
    main = by_kind_name(docs, "Service", "s3proxy-python")
    fp = by_kind_name(docs, "Service", "s3proxy-python-frontproxy")
    assert main["spec"]["selector"]["app.kubernetes.io/component"] == "server"
    assert fp["spec"]["selector"]["app.kubernetes.io/component"] == "frontproxy"

    server_pod = by_kind_name(docs, "Deployment", "s3proxy-python")
    fp_pod = by_kind_name(docs, "Deployment", "s3proxy-python-frontproxy")
    server_labels = server_pod["spec"]["template"]["metadata"]["labels"]
    fp_labels = fp_pod["spec"]["template"]["metadata"]["labels"]
    assert server_labels["app.kubernetes.io/component"] == "server"
    assert fp_labels["app.kubernetes.io/component"] == "frontproxy"


def test_haproxy_config_balances_and_scales_with_replicas(chart_dir):
    docs = render(chart_dir, "frontproxy.enabled=true", "replicaCount=5")
    cm = by_kind_name(docs, "ConfigMap", "s3proxy-python-frontproxy")
    cfg = cm["data"]["haproxy.cfg"]
    assert "balance leastconn" in cfg
    # per-request balancing across all pods via the headless Service
    assert "server-template pod 1-5 s3proxy-python-headless:4433" in cfg
    assert "parse-resolv-conf" in cfg


def test_frontproxy_replicas_and_pdb(chart_dir):
    docs = render(chart_dir, "frontproxy.enabled=true")
    dep = by_kind_name(docs, "Deployment", "s3proxy-python-frontproxy")
    assert dep["spec"]["replicas"] == 2
    pdb = by_kind_name(docs, "PodDisruptionBudget", "s3proxy-python-frontproxy")
    assert pdb["spec"]["minAvailable"] == 1


# --------------------------------------------------------------------------
# Ingress (external access) — routes to the front proxy
# --------------------------------------------------------------------------


def _ingress_args() -> list[str]:
    return [
        "frontproxy.enabled=true",
        "ingress.enabled=true",
        "ingress.className=nginx",
        "ingress.hosts[0].host=s3.example.com",
        "ingress.hosts[0].paths[0].path=/",
        "ingress.hosts[0].paths[0].pathType=Prefix",
    ]


def test_ingress_routes_to_frontproxy(chart_dir):
    docs = render(chart_dir, *_ingress_args())
    ing = by_kind_name(docs, "Ingress", "s3proxy-python")
    assert ing is not None
    assert ing["spec"]["ingressClassName"] == "nginx"
    backend = ing["spec"]["rules"][0]["http"]["paths"][0]["backend"]["service"]
    assert backend["name"] == "s3proxy-python-frontproxy"
    assert backend["port"]["number"] == 80


def test_ingress_requires_frontproxy(chart_dir):
    err = render_expect_error(
        chart_dir,
        "ingress.enabled=true",
        "ingress.hosts[0].host=s3.example.com",
        "ingress.hosts[0].paths[0].path=/",
        "ingress.hosts[0].paths[0].pathType=Prefix",
    )
    assert "frontproxy.enabled=true" in err


def test_ingress_requires_hosts(chart_dir):
    err = render_expect_error(chart_dir, "frontproxy.enabled=true", "ingress.enabled=true")
    assert "ingress.hosts is empty" in err


def test_ingress_tls_rendered(chart_dir):
    docs = render(
        chart_dir,
        *_ingress_args(),
        "ingress.tls[0].secretName=s3-tls",
        "ingress.tls[0].hosts[0]=s3.example.com",
    )
    ing = by_kind_name(docs, "Ingress", "s3proxy-python")
    assert ing["spec"]["tls"][0]["secretName"] == "s3-tls"
    assert "s3.example.com" in ing["spec"]["tls"][0]["hosts"]
