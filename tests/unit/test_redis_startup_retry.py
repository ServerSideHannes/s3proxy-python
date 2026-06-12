"""Startup Redis connectivity retry.

`init_redis` pings Redis once on boot. Previously a transient outage (e.g. the
Redis pod restarting) raised straight out of the FastAPI lifespan and crashed
the container. `_ping_with_retry` now retries with backoff until a deadline, so
a brief outage is tolerated while a persistent one still fails loudly.
"""

from __future__ import annotations

import pytest
import redis.asyncio as redis

from s3proxy.state import redis as redis_module
from s3proxy.state.redis import _ping_with_retry


class _FakeRedis:
    def __init__(self, fail_times: int) -> None:
        self._fail_times = fail_times
        self.calls = 0

    async def ping(self) -> bool:
        self.calls += 1
        if self.calls <= self._fail_times:
            raise redis.ConnectionError("Error 111 connecting to redis:6379. Connection refused.")
        return True


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _instant(_seconds: float) -> None:
        return None

    monkeypatch.setattr(redis_module.asyncio, "sleep", _instant)


async def test_ping_retries_then_succeeds() -> None:
    client = _FakeRedis(fail_times=3)
    await _ping_with_retry(client, "redis://redis:6379")
    assert client.calls == 4


async def test_ping_raises_after_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    # Force the deadline to already be in the past so the first failure gives up.
    monkeypatch.setattr(redis_module, "STARTUP_PING_DEADLINE_SECONDS", -1.0)
    client = _FakeRedis(fail_times=99)
    with pytest.raises(redis.ConnectionError):
        await _ping_with_retry(client, "redis://redis:6379")
    assert client.calls == 1
