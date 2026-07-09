"""RedisStateStore optimistic-locking retry — success after transient conflicts."""

from __future__ import annotations

from s3proxy.state.storage import RedisStateStore
from tests.unit.storage_watch_retry_fakes import FakeWatchRedis


class TestWatchRetryLoop:
    async def test_get_and_delete_retries_then_succeeds(self):
        fake = FakeWatchRedis(value=b"payload", fail_times=2)
        store = RedisStateStore(fake)

        result = await store.get_and_delete("k")

        assert result == b"payload"
        assert fake.attempts == 3  # two conflicts + one success

    async def test_update_retries_then_succeeds(self):
        fake = FakeWatchRedis(value=b"abc", fail_times=1)
        store = RedisStateStore(fake)

        result = await store.update("k", lambda data: data + b"-z", ttl_seconds=10)

        assert result == b"abc-z"
        assert fake.attempts == 2
