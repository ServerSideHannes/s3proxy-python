"""Redis client management for state storage."""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

import redis.asyncio as redis
import structlog
from redis.asyncio import Redis
from structlog.stdlib import BoundLogger

if TYPE_CHECKING:
    from .storage import StateStore

logger: BoundLogger = structlog.get_logger(__name__)

# Redis key prefix for upload state
REDIS_KEY_PREFIX = "s3proxy:upload:"

# Bounded retry budget for the startup connectivity check. Lets the app survive
# a Redis restart during boot instead of crash-looping; still fails loudly if
# Redis stays unreachable past the deadline.
STARTUP_PING_DEADLINE_SECONDS = 60.0
STARTUP_PING_MAX_BACKOFF_SECONDS = 5.0

# Module-level Redis client (initialized by init_redis)
_redis_client: Redis | None = None

# Flag to track if we're using Redis or in-memory storage
_use_redis: bool = False


async def init_redis(redis_url: str | None, redis_password: str | None = None) -> Redis | None:
    """Initialize Redis connection pool if URL is provided."""
    global _redis_client, _use_redis

    if not redis_url:
        logger.info("Redis URL not configured, using in-memory storage (single-instance mode)")
        _use_redis = False
        return None

    # Pass password separately if provided (overrides URL password)
    if redis_password:
        _redis_client = redis.from_url(redis_url, password=redis_password, decode_responses=False)
    else:
        _redis_client = redis.from_url(redis_url, decode_responses=False)

    # Test connection, tolerating a transient outage (e.g. Redis restarting)
    await _ping_with_retry(_redis_client, redis_url)
    _use_redis = True
    logger.info("Redis connected (HA mode)", url=redis_url)
    return _redis_client


async def _ping_with_retry(client: Redis, redis_url: str) -> None:
    """Ping Redis, retrying with backoff until the startup deadline."""
    deadline = time.monotonic() + STARTUP_PING_DEADLINE_SECONDS
    backoff = 0.5
    while True:
        try:
            await client.ping()
            return
        except (redis.ConnectionError, redis.TimeoutError, OSError) as exc:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                logger.error(
                    "Redis unreachable at startup, giving up",
                    url=redis_url,
                    deadline_seconds=STARTUP_PING_DEADLINE_SECONDS,
                    error=str(exc),
                )
                raise
            sleep_for = min(backoff, STARTUP_PING_MAX_BACKOFF_SECONDS, remaining)
            logger.warning(
                "Redis not ready, retrying",
                url=redis_url,
                retry_in_seconds=round(sleep_for, 2),
                error=str(exc),
            )
            await asyncio.sleep(sleep_for)
            backoff = min(backoff * 2, STARTUP_PING_MAX_BACKOFF_SECONDS)


async def close_redis() -> None:
    """Close Redis connection."""
    global _redis_client
    if _redis_client:
        await _redis_client.aclose()
        _redis_client = None
        logger.info("Redis connection closed")


def get_redis() -> Redis:
    """Get Redis client (must be initialized first)."""
    if _redis_client is None:
        raise RuntimeError("Redis not initialized. Call init_redis() first.")
    return _redis_client


def is_using_redis() -> bool:
    """Check if we're using Redis or in-memory storage."""
    return _use_redis


def create_state_store() -> StateStore:
    """Create the appropriate StateStore based on Redis configuration.

    Call this AFTER init_redis() to get the correct store type.
    Returns RedisStateStore if Redis is configured, MemoryStateStore otherwise.
    """
    from .storage import MemoryStateStore, RedisStateStore

    if _use_redis and _redis_client is not None:
        return RedisStateStore(_redis_client)
    return MemoryStateStore()
