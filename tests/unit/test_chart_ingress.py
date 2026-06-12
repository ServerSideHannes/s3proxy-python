"""Helm chart tests for the standard Ingress and the Traefik IngressRoute variant."""

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

S3_INGRESS = (
    "frontproxy.enabled=true",
    "ingress.enabled=true",
    "ingress.hosts[0].host=s3.example.com",
    "ingress.hosts[0].paths[0].path=/",
    "ingress.hosts[0].paths[0].pathType=Prefix",
)
DASH_INGRESS = (
    "dashboard.enabled=true",
    "dashboard.ingress.enabled=true",
    "dashboard.ingress.host=dash.example.com",
)


def render(*set_args: str) -> list[dict]:
    cmd = ["helm", "template", "t", str(CHART_DIR)]
    for s in CREDS + set_args:
        cmd += ["--set", s]
    out = subprocess.run(cmd, capture_output=True, text=True)
    if out.returncode != 0:
        raise AssertionError(f"helm template failed:\n{out.stderr}")
    return [d for d in yaml.safe_load_all(out.stdout) if d]


def of_kind(docs: list[dict], kind: str) -> list[dict]:
    return [d for d in docs if d.get("kind") == kind]


def test_default_renders_standard_ingress():
    docs = render(*S3_INGRESS, *DASH_INGRESS)
    ingresses = of_kind(docs, "Ingress")
    assert len(ingresses) == 2
    assert all(i["apiVersion"] == "networking.k8s.io/v1" for i in ingresses)
    assert not of_kind(docs, "IngressRoute")


def test_s3_ingressroute():
    docs = render(
        *S3_INGRESS,
        "ingress.kind=IngressRoute",
        "ingress.entryPoints[0]=websecure",
        "ingress.middlewares[0].name=mw",
        "ingress.tls[0].secretName=s3-tls",
    )
    route = of_kind(docs, "IngressRoute")[0]
    assert route["apiVersion"] == "traefik.io/v1alpha1"
    assert route["spec"]["entryPoints"] == ["websecure"]
    r = route["spec"]["routes"][0]
    assert r["match"] == "Host(`s3.example.com`) && PathPrefix(`/`)"
    assert r["middlewares"] == [{"name": "mw"}]
    assert r["services"][0]["name"] == "s3proxy-python-frontproxy"
    assert route["spec"]["tls"] == {"secretName": "s3-tls"}


def test_dashboard_ingressroute():
    docs = render(
        *DASH_INGRESS,
        "dashboard.ingress.kind=IngressRoute",
        "dashboard.ingress.tls[0].secretName=dash-tls",
    )
    route = of_kind(docs, "IngressRoute")[0]
    r = route["spec"]["routes"][0]
    assert r["match"] == "Host(`dash.example.com`) && PathPrefix(`/dashboard`)"
    assert r["services"][0]["name"] == "s3proxy-python-dashboard"
    assert route["spec"]["tls"] == {"secretName": "dash-tls"}
