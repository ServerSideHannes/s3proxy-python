"""RedisStateStore optimistic-locking retry (issue #66 item 3).

The WATCH/MULTI/EXEC retry was rewritten from self-recursion (with a leaking
``_retries`` kwarg) to a bounded ``for`` loop. These tests drive the loop with a
fake pipeline whose ``execute`` raises ``WatchError`` a controlled number of
times, pinning that it retries-then-succeeds and gives up after
``MAX_WATCH_RETRIES`` attempts.
"""

from __future__ import annotations

import pytest
from redis.asyncio import WatchError

from s3proxy.state.storage import MAX_WATCH_RETRIES, RedisStateStore


class _FakePipe:
    def __init__(self, parent: _FakeRedis) -> None:
        self._parent = parent

    async def __aenter__(self) -> _FakePipe:
        return self

    async def __aexit__(self, *exc) -> bool:
        return False

    async def watch(self, *keys) -> None:
        pass

    async def unwatch(self) -> None:
        pass

    def multi(self) -> None:
        pass

    def delete(self, *args) -> None:
        pass

    def set(self, *args, **kwargs) -> None:
        pass

    async def execute(self):
        if self._parent.fail_remaining > 0:
            self._parent.fail_remaining -= 1
            raise WatchError("simulated optimistic-lock conflict")
        return [True]


class _FakeRedis:
    def __init__(self, value: bytes | None, fail_times: int) -> None:
        self._value = value
        self.fail_remaining = fail_times
        self.attempts = 0

    async def get(self, key):
        return self._value

    def pipeline(self, transaction: bool = True) -> _FakePipe:
        self.attempts += 1
        return _FakePipe(self)


class TestWatchRetryLoop:
    async def test_get_and_delete_retries_then_succeeds(self):
        fake = _FakeRedis(value=b"payload", fail_times=2)
        store = RedisStateStore(fake)

        result = await store.get_and_delete("k")

        assert result == b"payload"
        assert fake.attempts == 3  # two conflicts + one success

    async def test_get_and_delete_gives_up_after_max_retries(self):
        fake = _FakeRedis(value=b"payload", fail_times=MAX_WATCH_RETRIES + 5)
        store = RedisStateStore(fake)

        with pytest.raises(WatchError):
            await store.get_and_delete("k")

        assert fake.attempts == MAX_WATCH_RETRIES

    async def test_update_retries_then_succeeds(self):
        fake = _FakeRedis(value=b"abc", fail_times=1)
        store = RedisStateStore(fake)

        result = await store.update("k", lambda data: data + b"-z", ttl_seconds=10)

        assert result == b"abc-z"
        assert fake.attempts == 2

    async def test_update_gives_up_after_max_retries(self):
        fake = _FakeRedis(value=b"abc", fail_times=MAX_WATCH_RETRIES + 5)
        store = RedisStateStore(fake)

        with pytest.raises(WatchError):
            await store.update("k", lambda data: data, ttl_seconds=10)

        assert fake.attempts == MAX_WATCH_RETRIES
