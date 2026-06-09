"""Tests for the dashboard."""

from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from s3proxy import metrics
from s3proxy.config import Settings
from s3proxy.dashboard import collectors, record_request
from s3proxy.dashboard.auth import DashboardCredentials, create_auth_dependency
from s3proxy.dashboard.router import create_dashboard_router
from s3proxy.dashboard.stats_store import (
    MemoryStatsStore,
    RedisStatsStore,
    RequestSample,
    bucket_series,
    set_store,
)


@pytest.fixture
def dashboard_settings():
    return Settings(
        host="http://localhost:9000",
        dashboard_ui=True,
        dashboard_username="creds",
        dashboard_password="secret",
        credentials=[{"access_key": "AKIA-TEST", "secret_key": "s", "kek": "k"}],
    )


@pytest.fixture
def mem_store(dashboard_settings):
    """A fresh per-pod in-memory store, registered as the global record target."""
    store = MemoryStatsStore(dashboard_settings)
    set_store(store)
    yield store
    set_store(None)


@pytest.fixture
def redis_store(dashboard_settings, mock_redis):
    """A Redis-backed store on fakeredis, registered as the global target."""
    store = RedisStatsStore(mock_redis, dashboard_settings)
    set_store(store)
    yield store
    set_store(None)


# ---------------------------------------------------------------------------
# record_request + collect_all (in-memory / per-pod path)
# ---------------------------------------------------------------------------


async def test_record_request_splits_bucket_and_key(mem_store) -> None:
    await record_request(
        "GET", "/my-bucket/path/to/file.txt", "GetObject", 200, 0.042, 1024, "10.0.0.1"
    )
    entries = mem_store._log.all()
    assert len(entries) == 1
    e = entries[0]
    assert e.bucket == "my-bucket"
    assert e.key == "path/to/file.txt"
    assert e.status == 200
    assert e.duration_ms == pytest.approx(42.0)
    assert e.client_ip == "10.0.0.1"


async def test_collect_all_builds_expected_sections(dashboard_settings, mem_store) -> None:
    await record_request(
        "PUT", "/customer-data/invoice.pdf", "PutObject", 200, 0.05, 2048, "10.0.0.1"
    )
    await record_request("GET", "/archives/log.gz", "GetObject", 500, 0.1, 0, "10.0.0.2")

    start = time.monotonic() - 120  # 2 minutes
    data = await collectors.collect_all(
        mem_store, dashboard_settings, start_time=start, version="9.9.9"
    )

    assert data["header"]["title"] == "S3 Encryption Proxy"
    assert data["header"]["status"] == "Running"
    assert data["header"]["cluster_wide"] is False
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


async def test_activity_timestamp_is_absolute(dashboard_settings, mem_store) -> None:
    await record_request("GET", "/b/k", "GetObject", 200, 0.01, 1, "10.0.0.1")
    data = await collectors.collect_all(mem_store, dashboard_settings, start_time=time.monotonic())
    row = data["activity"][0]
    # Absolute "YYYY-MM-DD HH:MM:SS" primary display; relative kept as a tooltip.
    assert row["time"][:2] == "20" and row["time"][4] == "-" and ":" in row["time"]
    assert row["time_relative"].endswith("ago")


async def test_collector_does_not_crash_on_empty_metrics(dashboard_settings, mem_store) -> None:
    """collect_all must work even before any request has been recorded."""
    data = await collectors.collect_all(mem_store, dashboard_settings, start_time=time.monotonic())
    expected = f"{int(collectors._read_labeled_counter_sum(metrics.REQUEST_COUNT)):,}"
    assert data["cards"]["requests"]["value"] == expected
    assert data["activity"] == []
    assert data["buckets"] == []


# ---------------------------------------------------------------------------
# Redis-backed store (cluster-wide path) on fakeredis
# ---------------------------------------------------------------------------


async def test_redis_store_records_and_paginates(redis_store) -> None:
    for i in range(120):
        await redis_store.record(
            RequestSample(
                timestamp=time.time(),
                method="GET",
                operation="GetObject",
                bucket="b",
                key=f"k{i}",
                status=200,
                duration_ms=1.0,
                size=10,
                client_ip="10.0.0.1",
            )
        )
    page1 = await redis_store.page(0, 50, "", "", "")
    assert page1["count"] == 50
    assert page1["total"] == 120
    assert page1["has_more"] is True
    # newest first
    assert page1["entries"][0]["key"] == "k119"

    page3 = await redis_store.page(100, 50, "", "", "")
    assert page3["count"] == 20
    assert page3["has_more"] is False


async def test_redis_store_caps_the_log(dashboard_settings, mock_redis) -> None:
    dashboard_settings.request_log_cap = 5
    store = RedisStatsStore(mock_redis, dashboard_settings)
    for i in range(20):
        await store.record(
            RequestSample(time.time(), "GET", "GetObject", "b", f"k{i}", 200, 1.0, 1, "ip")
        )
    total = await mock_redis.llen("s3proxy:stats:reqlog")
    assert total == 5
    ttl = await mock_redis.ttl("s3proxy:stats:reqlog")
    assert 0 < ttl <= dashboard_settings.request_log_ttl_seconds


async def test_redis_store_filter_pagination(redis_store) -> None:
    await redis_store.record(
        RequestSample(time.time(), "PUT", "PutObject", "b", "a", 200, 1, 1, "ip")
    )
    await redis_store.record(
        RequestSample(time.time(), "GET", "GetObject", "b", "x", 500, 1, 1, "ip")
    )
    await redis_store.record(
        RequestSample(time.time(), "GET", "GetObject", "b", "y", 200, 1, 1, "ip")
    )
    errors = await redis_store.page(0, 50, "", "", "error")
    assert errors["total"] == 1
    assert errors["entries"][0]["key"] == "x"
    puts = await redis_store.page(0, 50, "", "PUT", "")
    assert puts["total"] == 1
    assert puts["entries"][0]["method"] == "PUT"


def _sample(method="GET", status=200, size=0, dur_ms=10.0):
    return RequestSample(
        timestamp=time.time(),
        method=method,
        operation=method,
        bucket="b",
        key="k",
        status=status,
        duration_ms=dur_ms,
        size=size,
        client_ip="ip",
    )


async def test_redis_store_cluster_wide_aggregate(dashboard_settings, mock_redis) -> None:
    """Counters are written per-request by record() so they sum across pods.

    Two store instances on the same Redis simulate two replicas — the aggregate
    must reflect both, regardless of which pod serves the dashboard.
    """
    pod_a = RedisStatsStore(mock_redis, dashboard_settings)
    pod_b = RedisStatsStore(mock_redis, dashboard_settings)

    # pod A handles 7 GET + 3 PUT (one PUT errors 500), pod B handles 5 GET (one 404)
    for _ in range(7):
        await pod_a.record(_sample("GET", 200, size=100))
    for i in range(3):
        await pod_a.record(_sample("PUT", 500 if i == 0 else 200, size=1000))
    for i in range(5):
        await pod_b.record(_sample("GET", 404 if i == 0 else 200, size=50))

    agg = await pod_a.aggregate()  # any pod sees the cluster-wide totals
    assert agg is not None
    assert agg.requests == 15  # 10 (A) + 5 (B)
    assert agg.errors == 2  # one 500 + one 404
    assert agg.methods["GET"] == 12
    assert agg.methods["PUT"] == 3
    assert agg.errors_by_class["5xx"] == 1
    assert agg.errors_by_class["4xx"] == 1
    # PUT bytes counted as encrypted, GET bytes as decrypted
    assert agg.bytes_encrypted == 3000  # 3 PUT * 1000
    assert agg.bytes_decrypted == 7 * 100 + 5 * 50


async def test_redis_store_latency_is_cluster_wide(dashboard_settings, mock_redis) -> None:
    pod_a = RedisStatsStore(mock_redis, dashboard_settings)
    pod_b = RedisStatsStore(mock_redis, dashboard_settings)
    for _ in range(10):
        await pod_a.record(_sample(dur_ms=5.0))  # 0.005s bucket
    for _ in range(10):
        await pod_b.record(_sample(dur_ms=2000.0))  # 2.5s bucket
    agg = await pod_a.aggregate()
    assert agg.latency_buckets["+Inf"] == 20  # total observations, both pods


# ---------------------------------------------------------------------------
# Time-series bucketing
# ---------------------------------------------------------------------------


def test_bucket_series_zero_fills_and_buckets() -> None:
    now = 10_000.0
    # two events in the same 120s bucket, one in another
    pts = [(9800, 3.0), (9820, 2.0), (9600, 1.0)]
    times, values = bucket_series(pts, window_seconds=600, bucket_seconds=120, now=now)
    assert len(times) == len(values) == 5
    assert sum(values) == 6.0  # nothing dropped inside the window
    # continuous axis (monotonic, fixed step)
    assert times == sorted(times)
    assert times[1] - times[0] == 120


def test_bucket_series_drops_out_of_window() -> None:
    now = 10_000.0
    pts = [(1000, 99.0)]  # far outside a 600s window
    _, values = bucket_series(pts, 600, 120, now=now)
    assert sum(values) == 0.0


async def test_redis_series_buckets_recorded_requests(redis_store) -> None:
    for _ in range(3):
        await redis_store.record(
            RequestSample(time.time(), "GET", "GetObject", "b", "k", 200, 1, 1, "ip")
        )
    times, values = await redis_store.series("requests", "1h")
    assert sum(values) == 3.0
    assert len(times) == len(values)


async def test_redis_throughput_split_by_direction(redis_store) -> None:
    # PUT bytes -> bytes_put series; GET bytes -> bytes_get series.
    await redis_store.record(
        RequestSample(time.time(), "PUT", "PutObject", "b", "k", 200, 1, 4096, "ip")
    )
    await redis_store.record(
        RequestSample(time.time(), "GET", "GetObject", "b", "k", 200, 1, 1024, "ip")
    )
    _, put_vals = await redis_store.series("bytes_put", "1h")
    _, get_vals = await redis_store.series("bytes_get", "1h")
    assert sum(put_vals) == 4096.0
    assert sum(get_vals) == 1024.0


# ---------------------------------------------------------------------------
# Templates + routes
# ---------------------------------------------------------------------------


def _make_app(settings: Settings, store=None):
    from fastapi import FastAPI

    app = FastAPI()
    router = create_dashboard_router(settings, credentials_store={}, version="1.2.3")
    app.include_router(router, prefix=settings.dashboard_path)
    app.state.settings = settings
    app.state.start_time = time.monotonic()
    app.state.stats_store = store or MemoryStatsStore(settings)
    from s3proxy.dashboard.auth import RedisSessionStore
    from s3proxy.state.redis import get_redis

    app.state.session_store = RedisSessionStore(get_redis())
    return app


def test_status_api_redirects_unauthenticated_to_401(dashboard_settings) -> None:
    client = TestClient(_make_app(dashboard_settings), follow_redirects=False)
    r = client.get("/dashboard/api/status")
    assert r.status_code == 401


def test_login_post_sets_cookie_and_redirects(dashboard_settings) -> None:
    client = TestClient(_make_app(dashboard_settings), follow_redirects=False)
    r = client.post("/dashboard/api/login", data={"username": "creds", "password": "secret"})
    assert r.status_code == 303
    assert r.headers["location"].endswith("/dashboard/")
    assert "s3proxy_session=" in r.headers.get("set-cookie", "")


def test_login_post_rejects_bad_credentials(dashboard_settings) -> None:
    client = TestClient(_make_app(dashboard_settings), follow_redirects=False)
    r = client.post("/dashboard/api/login", data={"username": "creds", "password": "wrong"})
    assert r.status_code == 303
    assert "error=1" in r.headers["location"]


def test_session_cookie_authenticates_api(dashboard_settings) -> None:
    client = TestClient(_make_app(dashboard_settings), follow_redirects=False)
    r = client.post("/dashboard/api/login", data={"username": "creds", "password": "secret"})
    cookie = r.headers["set-cookie"].split(";")[0]
    r2 = client.get("/dashboard/api/status", headers={"Cookie": cookie})
    assert r2.status_code == 200


def test_logout_clears_cookie(dashboard_settings) -> None:
    client = TestClient(_make_app(dashboard_settings), follow_redirects=False)
    r = client.get("/dashboard/api/logout")
    assert r.status_code == 303
    assert r.headers["location"].endswith("/dashboard/login")
    assert "s3proxy_session=" in r.headers.get("set-cookie", "")


def test_status_api_returns_expected_shape(dashboard_settings) -> None:
    client = TestClient(_make_app(dashboard_settings))
    r = client.get("/dashboard/api/status", auth=("creds", "secret"))
    assert r.status_code == 200
    payload = r.json()
    assert payload["header"]["status"] == "Running"
    assert payload["footer"]["version"] == "1.2.3"
    for key in ("requests", "data_encrypted", "errors", "active_buckets"):
        assert key in payload["cards"]
        assert "breakdown" in payload["cards"][key]


def test_series_api_returns_shape(dashboard_settings) -> None:
    client = TestClient(_make_app(dashboard_settings))
    r = client.get("/dashboard/api/series?metric=requests&range=3h", auth=("creds", "secret"))
    assert r.status_code == 200
    payload = r.json()
    assert payload["metric"] == "requests"
    assert payload["range"] == "3h"
    assert "spark" in payload and "spark_times" in payload


def test_throughput_api_returns_two_series(dashboard_settings) -> None:
    client = TestClient(_make_app(dashboard_settings))
    r = client.get("/dashboard/api/throughput?range=24h", auth=("creds", "secret"))
    assert r.status_code == 200
    payload = r.json()
    assert payload["range"] == "24h"
    labels = [s["label"] for s in payload["series"]]
    assert labels == ["Encrypted (PUT)", "Decrypted (GET)"]
    for s in payload["series"]:
        assert "spark" in s and "spark_times" in s


def test_logs_api_paginates(dashboard_settings) -> None:
    client = TestClient(_make_app(dashboard_settings))
    r = client.get("/dashboard/api/logs?limit=10&offset=0", auth=("creds", "secret"))
    assert r.status_code == 200
    payload = r.json()
    for key in ("entries", "count", "offset", "limit", "total", "has_more", "operations"):
        assert key in payload


def test_status_api_401_without_auth(dashboard_settings) -> None:
    client = TestClient(_make_app(dashboard_settings))
    r = client.get("/dashboard/api/status")
    assert r.status_code == 401


def test_auth_uses_explicit_credentials_not_aws() -> None:
    settings = Settings(
        host="http://localhost:9000",
        dashboard_ui=True,
        dashboard_username="admin",
        dashboard_password="admin",
    )
    creds = DashboardCredentials(settings, {"AKIAEXAMPLE": "secret-key"})
    assert creds.valid("admin", "admin")
    # The AWS access key / secret must NOT work as dashboard credentials.
    assert not creds.valid("AKIAEXAMPLE", "secret-key")


def test_auth_raises_when_credentials_blank() -> None:
    settings = Settings(
        host="http://localhost:9000",
        dashboard_ui=True,
        dashboard_username="",
        dashboard_password="",
    )
    with pytest.raises(RuntimeError):
        create_auth_dependency(settings, {})
