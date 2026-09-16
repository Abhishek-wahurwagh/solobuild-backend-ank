"""
Async Redis connection pool — singleton for the application lifetime.

Usage in routes:  ``request.app.state.redis``
Usage in worker:  call ``get_redis_pool()`` directly.
"""

from redis.asyncio import Redis, ConnectionPool

from app.core.config import settings

_pool: ConnectionPool | None = None


def _build_pool() -> ConnectionPool:
    global _pool
    if _pool is None:
        _pool = ConnectionPool.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            max_connections=20,
        )
    return _pool


async def get_redis_pool() -> Redis:
    """Return a Redis client backed by the shared connection pool."""
    return Redis(connection_pool=_build_pool())


async def close_redis_pool() -> None:
    """Drain all connections — call during application shutdown."""
    global _pool
    if _pool is not None:
        await _pool.aclose()
        _pool = None
