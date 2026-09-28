"""Distributed lock for CompleteMultipartUpload.

HA deployments load-balance across many pods. Without a per-upload lock, two pods
can both call upstream CompleteMultipartUpload for the same upload_id (one after
recovering state the other already deleted), which surfaces as "multipart
completion is already in progress" and can leave the client with NoSuchUpload.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import structlog
from structlog.stdlib import BoundLogger

from ..errors import S3Error

if TYPE_CHECKING:
    from redis.asyncio import Redis

logger: BoundLogger = structlog.get_logger(__name__)

COMPLETE_LOCK_TTL_SECONDS = int(os.environ.get("S3PROXY_COMPLETE_LOCK_TTL_SECONDS", "7200"))
COMPLETE_LOCK_ACQUIRE_TIMEOUT_SECONDS = float(
    os.environ.get("S3PROXY_COMPLETE_LOCK_ACQUIRE_TIMEOUT_SECONDS", "7200")
)
COMPLETE_LOCK_POLL_INTERVAL_SECONDS = float(
    os.environ.get("S3PROXY_COMPLETE_LOCK_POLL_INTERVAL_SECONDS", "0.25")
)

_REDIS_PREFIX = "s3proxy:complete-lock:"


class CompleteUploadLock:
    """Serialize CompleteMultipartUpload per (bucket, key, upload_id)."""

    def __init__(
        self,
        redis_client: Redis | None = None,
        *,
        ttl_seconds: int = COMPLETE_LOCK_TTL_SECONDS,
        acquire_timeout_seconds: float = COMPLETE_LOCK_ACQUIRE_TIMEOUT_SECONDS,
        poll_interval_seconds: float = COMPLETE_LOCK_POLL_INTERVAL_SECONDS,
    ) -> None:
        self._redis = redis_client
        self._ttl = ttl_seconds
        self._acquire_timeout = acquire_timeout_seconds
        self._poll_interval = poll_interval_seconds
        self._memory_locks: dict[str, list] = {}
        self._memory_guard = asyncio.Lock()

    def _storage_key(self, bucket: str, key: str, upload_id: str) -> str:
        return f"{bucket}:{key}:{upload_id}"

    def _redis_key(self, bucket: str, key: str, upload_id: str) -> str:
        return f"{_REDIS_PREFIX}{bucket}:{key}:{upload_id}"

    @asynccontextmanager
    async def hold(self, bucket: str, key: str, upload_id: str) -> AsyncIterator[None]:
        if self._redis is not None:
            async with self._redis_hold(bucket, key, upload_id):
                yield
        else:
            async with self._memory_hold(bucket, key, upload_id):
                yield

    @asynccontextmanager
    async def _memory_hold(self, bucket: str, key: str, upload_id: str) -> AsyncIterator[None]:
        lk = self._storage_key(bucket, key, upload_id)
        async with self._memory_guard:
            entry = self._memory_locks.setdefault(lk, [asyncio.Lock(), 0])
            entry[1] += 1
        try:
            async with entry[0]:
                yield
        finally:
            async with self._memory_guard:
                entry[1] -= 1
                if entry[1] == 0:
                    del self._memory_locks[lk]

    @asynccontextmanager
    async def _redis_hold(self, bucket: str, key: str, upload_id: str) -> AsyncIterator[None]:
        redis_key = self._redis_key(bucket, key, upload_id)
        token = uuid.uuid4().hex
        deadline = asyncio.get_running_loop().time() + self._acquire_timeout

        while True:
            acquired = await self._redis.set(redis_key, token, nx=True, ex=self._ttl)
            if acquired:
                logger.debug(
                    "COMPLETE_LOCK_ACQUIRED",
                    bucket=bucket,
                    key=key,
                    upload_id=upload_id[:20] + "..." if len(upload_id) > 20 else upload_id,
                    backend="redis",
                )
                break

            if asyncio.get_running_loop().time() >= deadline:
                logger.warning(
                    "COMPLETE_LOCK_ACQUIRE_TIMEOUT",
                    bucket=bucket,
                    key=key,
                    upload_id=upload_id[:20] + "..." if len(upload_id) > 20 else upload_id,
                    timeout_seconds=self._acquire_timeout,
                )
                raise S3Error.slow_down(
                    "CompleteMultipartUpload is in progress on another instance; retry later"
                )

            await asyncio.sleep(self._poll_interval)

        owner = asyncio.current_task()
        lost = False

        async def renew():
            nonlocal lost
            try:
                while True:
                    await asyncio.sleep(max(0.1, self._ttl / 3))
                    if not await self._renew_redis_lock(redis_key, token):
                        raise RuntimeError("Completion lease was lost")
            except Exception:
                lost = True
                owner.cancel()

        renewal = asyncio.create_task(renew())
        try:
            yield
        except asyncio.CancelledError:
            if lost:
                raise S3Error.slow_down("Completion lease lost; retry the upload") from None
            raise
        finally:
            renewal.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await renewal
            await self._release_redis_lock(redis_key, token)

    async def _renew_redis_lock(self, redis_key, token):
        async with self._redis.pipeline(transaction=True) as pipe:
            await pipe.watch(redis_key)
            current = await pipe.get(redis_key)
            current = current.decode() if isinstance(current, bytes) else current
            if current != token:
                await pipe.unwatch()
                return False
            pipe.multi()
            pipe.expire(redis_key, self._ttl)
            await pipe.execute()
            return True

    async def _release_redis_lock(self, redis_key: str, token: str) -> None:
        import redis.asyncio as redis

        from .storage import MAX_WATCH_RETRIES, WATCH_RETRY_BASE_DELAY_SEC

        for attempt in range(MAX_WATCH_RETRIES):
            async with self._redis.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(redis_key)
                    current = await self._redis.get(redis_key)
                    if current is None:
                        await pipe.unwatch()
                        return
                    current_token = current.decode() if isinstance(current, bytes) else current
                    if current_token != token:
                        await pipe.unwatch()
                        return
                    pipe.multi()
                    pipe.delete(redis_key)
                    await pipe.execute()
                    return
                except redis.WatchError:
                    if attempt == MAX_WATCH_RETRIES - 1:
                        logger.warning(
                            "COMPLETE_LOCK_RELEASE_CONFLICT",
                            redis_key=redis_key,
                        )
                        return
                    await asyncio.sleep(WATCH_RETRY_BASE_DELAY_SEC * (2**attempt))


def create_complete_upload_lock() -> CompleteUploadLock:
    """Build a lock using Redis when the HA client is initialized."""
    from .redis import _redis_client

    return CompleteUploadLock(redis_client=_redis_client)
