"""Abstract state storage backends.

This module provides a Strategy Pattern for state storage, decoupling
the MultipartStateManager from the concrete storage implementation.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import TYPE_CHECKING

import structlog
from structlog.stdlib import BoundLogger

if TYPE_CHECKING:
    from redis.asyncio import Redis

logger: BoundLogger = structlog.get_logger(__name__)

# Type alias for updater function: takes bytes, returns bytes
Updater = Callable[[bytes], bytes]

# Maximum retries for Redis optimistic locking (WATCH/MULTI/EXEC)
MAX_WATCH_RETRIES = 5


class StateStore(ABC):
    """Abstract interface for state storage backends."""

    @abstractmethod
    async def get(self, key: str) -> bytes | None:
        """Get value by key. Returns None if not found."""
        ...

    @abstractmethod
    async def set(self, key: str, value: bytes, ttl_seconds: int) -> None:
        """Set value with TTL."""
        ...

    @abstractmethod
    async def delete(self, key: str) -> None:
        """Delete value by key."""
        ...

    @abstractmethod
    async def get_and_delete(self, key: str) -> bytes | None:
        """Atomically get and delete value. Returns None if not found."""
        ...

    @abstractmethod
    async def list_keys(self) -> list[str]:
        """List all stored keys."""
        ...

    @abstractmethod
    async def update(self, key: str, updater: Updater, ttl_seconds: int) -> bytes | None:
        """Atomically update value using updater function.

        Args:
            key: Storage key
            updater: Function that takes current bytes and returns new bytes
            ttl_seconds: TTL for the updated value

        Returns:
            Updated value bytes, or None if key not found
        """
        ...


class MemoryStateStore(StateStore):
    """In-memory state storage for single-instance deployments."""

    def __init__(self) -> None:
        self._store: dict[str, bytes] = {}

    async def list_keys(self) -> list[str]:
        return list(self._store.keys())

    async def get(self, key: str) -> bytes | None:
        return self._store.get(key)

    async def set(self, key: str, value: bytes, ttl_seconds: int) -> None:
        # Note: TTL not enforced in memory store (uploads complete or timeout)
        self._store[key] = value

    async def delete(self, key: str) -> None:
        self._store.pop(key, None)

    async def get_and_delete(self, key: str) -> bytes | None:
        return self._store.pop(key, None)

    async def update(self, key: str, updater: Updater, ttl_seconds: int) -> bytes | None:
        data = self._store.get(key)
        if data is None:
            return None
        new_data = updater(data)
        self._store[key] = new_data
        return new_data


class RedisStateStore(StateStore):
    """Redis-backed state storage for HA deployments."""

    def __init__(self, client: Redis, key_prefix: str = "s3proxy:upload:") -> None:
        """Initialize Redis store.

        Args:
            client: Redis async client (from redis.asyncio)
            key_prefix: Prefix for all keys in Redis
        """
        self._client: Redis = client
        self._prefix = key_prefix

    def _key(self, key: str) -> str:
        """Get prefixed key."""
        return f"{self._prefix}{key}"

    async def list_keys(self) -> list[str]:
        keys: list[str] = []
        async for key in self._client.scan_iter(match=f"{self._prefix}*", count=100):
            k = key.decode() if isinstance(key, bytes) else key
            keys.append(k.removeprefix(self._prefix))
        return keys

    async def get(self, key: str) -> bytes | None:
        return await self._client.get(self._key(key))

    async def set(self, key: str, value: bytes, ttl_seconds: int) -> None:
        await self._client.set(self._key(key), value, ex=ttl_seconds)

    async def delete(self, key: str) -> None:
        await self._client.delete(self._key(key))

    async def get_and_delete(self, key: str, _retries: int = 0) -> bytes | None:
        """Atomically get and delete using Redis transaction."""
        import redis.asyncio as redis

        pk = self._key(key)
        async with self._client.pipeline(transaction=True) as pipe:
            try:
                await pipe.watch(pk)
                data = await self._client.get(pk)
                if data is None:
                    await pipe.unwatch()
                    return None

                pipe.multi()
                pipe.delete(pk)
                await pipe.execute()
                return data

            except redis.WatchError:
                if _retries >= MAX_WATCH_RETRIES:
                    logger.error(
                        "REDIS_WATCH_RETRIES_EXHAUSTED",
                        key=key,
                        operation="get_and_delete",
                    )
                    raise
                logger.debug("REDIS_WATCH_RETRY", key=key, attempt=_retries + 1)
                return await self.get_and_delete(key, _retries=_retries + 1)

    async def update(
        self, key: str, updater: Updater, ttl_seconds: int, _retries: int = 0
    ) -> bytes | None:
        """Atomically update using Redis WATCH/MULTI/EXEC."""
        import redis.asyncio as redis

        pk = self._key(key)
        async with self._client.pipeline(transaction=True) as pipe:
            try:
                await pipe.watch(pk)
                data = await self._client.get(pk)
                if data is None:
                    await pipe.unwatch()
                    return None

                new_data = updater(data)

                pipe.multi()
                pipe.set(pk, new_data, ex=ttl_seconds)
                await pipe.execute()
                return new_data

            except redis.WatchError:
                if _retries >= MAX_WATCH_RETRIES:
                    logger.error("REDIS_WATCH_RETRIES_EXHAUSTED", key=key, operation="update")
                    raise
                logger.debug("REDIS_WATCH_RETRY", key=key, attempt=_retries + 1)
                return await self.update(key, updater, ttl_seconds, _retries=_retries + 1)
