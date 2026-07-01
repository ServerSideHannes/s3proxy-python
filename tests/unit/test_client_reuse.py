"""S3Client must reuse one aiobotocore client per credential set.

Creating a client per request builds a fresh aiohttp connector + SSLContext that
loads the whole CA store each time -- large native allocations (invisible to
tracemalloc) that pile up under concurrency and drive RSS. memray showed millions
of allocations in _create_connector / load_default_certs. Clients are pool-safe
and meant to be long-lived, so the wrapper caches one per (endpoint, key, region)
and reuses it. These tests pin that: same creds => one underlying client; distinct
creds => distinct clients; and they're closed on shutdown.
"""

import pytest

from s3proxy.client import s3
from s3proxy.client.types import S3Credentials


class _FakeCtx:
    def __init__(self, client):
        self._client = client

    async def __aenter__(self):
        return self._client

    async def __aexit__(self, *a):
        self._client.closed = True
        return False


class _FakeClient:
    def __init__(self, n):
        self.n = n
        self.closed = False


class _FakeSession:
    def __init__(self):
        self.calls = 0

    def client(self, *a, **k):
        self.calls += 1
        return _FakeCtx(_FakeClient(self.calls))


class _Settings:
    s3_endpoint = "http://minio:9000"


def _creds(key="AKIA1"):
    return S3Credentials(access_key=key, secret_key="s", region="us-east-1")


@pytest.fixture(autouse=True)
async def _clean(monkeypatch):
    fake = _FakeSession()
    monkeypatch.setattr(s3, "get_shared_session", lambda: fake)
    await s3.close_cached_clients()
    yield fake
    await s3.close_cached_clients()


@pytest.mark.asyncio
async def test_same_credentials_reuse_one_client(_clean):
    settings = _Settings()
    async with s3.S3Client(settings, _creds()) as c1:
        first = c1._cached_client
    async with s3.S3Client(settings, _creds()) as c2:
        second = c2._cached_client
    assert first is second  # reused
    assert _clean.calls == 1  # session.client() called once, not per request
    assert first.closed is False  # not torn down between requests


@pytest.mark.asyncio
async def test_distinct_credentials_distinct_clients(_clean):
    settings = _Settings()
    async with s3.S3Client(settings, _creds("AKIA1")) as c1:
        a = c1._cached_client
    async with s3.S3Client(settings, _creds("AKIA2")) as c2:
        b = c2._cached_client
    assert a is not b
    assert _clean.calls == 2


@pytest.mark.asyncio
async def test_close_cached_clients_tears_down(_clean):
    settings = _Settings()
    async with s3.S3Client(settings, _creds()) as c:
        client = c._cached_client
    await s3.close_cached_clients()
    assert client.closed is True
    assert len(s3._client_cache) == 0
