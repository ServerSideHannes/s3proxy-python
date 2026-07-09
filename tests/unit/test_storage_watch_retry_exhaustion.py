"""RedisStateStore optimistic-locking retry — give up after MAX_WATCH_RETRIES.

These drive every retry attempt; production exponential backoff would sleep for
hours if left unmocked (0.005 * 2**attempt across 25 attempts).
"""

from __future__ import annotations

import pytest
from redis.asyncio import WatchError

from s3proxy.state import storage as storage_module
from s3proxy.state.storage import MAX_WATCH_RETRIES, RedisStateStore
from tests.unit.storage_watch_retry_fakes import FakeWatchRedis


@pytest.fixture(autouse=True)
def _no_watch_retry_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _instant(_seconds: float) -> None:
        return None

    monkeypatch.setattr(storage_module.asyncio, "sleep", _instant)


class TestWatchRetryExhaustion:
    async def test_get_and_delete_gives_up_after_max_retries(self):
        fake = FakeWatchRedis(value=b"payload", fail_times=MAX_WATCH_RETRIES + 5)
        store = RedisStateStore(fake)

        with pytest.raises(WatchError):
            await store.get_and_delete("k")

        assert fake.attempts == MAX_WATCH_RETRIES

    async def test_update_gives_up_after_max_retries(self):
        fake = FakeWatchRedis(value=b"abc", fail_times=MAX_WATCH_RETRIES + 5)
        store = RedisStateStore(fake)

        with pytest.raises(WatchError):
            await store.update("k", lambda data: data, ttl_seconds=10)

        assert fake.attempts == MAX_WATCH_RETRIES
