"""Shared fakes for RedisStateStore WATCH/MULTI/EXEC retry unit tests."""

from __future__ import annotations

from redis.asyncio import WatchError


class FakeWatchPipe:
    def __init__(self, parent: FakeWatchRedis) -> None:
        self._parent = parent

    async def __aenter__(self) -> FakeWatchPipe:
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


class FakeWatchRedis:
    def __init__(self, value: bytes | None, fail_times: int) -> None:
        self._value = value
        self.fail_remaining = fail_times
        self.attempts = 0

    async def get(self, key):
        return self._value

    def pipeline(self, transaction: bool = True) -> FakeWatchPipe:
        self.attempts += 1
        return FakeWatchPipe(self)
