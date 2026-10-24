"""Application shutdown joins singleton work and releases before closing Redis."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI

from app import main
from app.leader import LeaderSupervisor

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    """Run async tests on asyncio, matching the application runtime."""
    return "asyncio"


async def test_lifespan_shutdown_order_and_cancellation(monkeypatch):
    """Preserve cancellation while stopping services, releasing, and closing Redis in order."""
    operations = []
    entered = asyncio.Event()
    store = AsyncMock(ttl_ms=300)
    store.acquire.return_value = True

    async def service(_lease):
        """Record when the supervised service finishes cancellation cleanup."""
        try:
            entered.set()
            await asyncio.Future()
        finally:
            operations.append("service-stopped")

    async def release(_value):
        """Record ownership release in the shared shutdown sequence."""
        operations.append("released")
        return True

    async def close():
        """Record Redis closure so its position in shutdown can be asserted."""
        operations.append("redis-closed")

    store.release.side_effect = release
    redis = AsyncMock()
    redis.aclose.side_effect = close
    supervisor = LeaderSupervisor(store, [service])
    monkeypatch.setattr(main, "new_redis", lambda **_kwargs: redis)
    monkeypatch.setattr(main, "build_container", lambda *_args: SimpleNamespace(leader=supervisor))

    async def application():
        """Keep the real lifespan open until the test cancels the application task."""
        async with main.lifespan(FastAPI()):
            await asyncio.Future()

    task = asyncio.create_task(application())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert operations == ["service-stopped", "released", "redis-closed"]


async def test_startup_failure_closes_client_and_propagates(monkeypatch):
    """Close Redis and surface startup errors before the lifespan can serve requests."""
    redis = AsyncMock()
    redis.ping.side_effect = RuntimeError("startup failed")
    monkeypatch.setattr(main, "new_redis", lambda **_kwargs: redis)
    with pytest.raises(RuntimeError, match="startup failed"):
        async with main.lifespan(FastAPI()):
            pytest.fail("lifespan should not yield")
    redis.aclose.assert_awaited_once()
