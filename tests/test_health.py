"""API readiness reflects the current Redis connection."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import FastAPI

from app.api.router import configure_api

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.parametrize(
    ("ping_result", "expected_status", "expected_body"),
    [
        (True, 200, {"status": "healthy", "redis": "connected"}),
        (False, 503, {"status": "unhealthy", "redis": "unavailable"}),
    ],
)
async def test_health_checks_redis(
    ping_result: bool, expected_status: int, expected_body: dict[str, str],
) -> None:
    app = FastAPI()
    configure_api(app)
    redis = SimpleNamespace(ping=AsyncMock(return_value=ping_result))
    app.state.container = SimpleNamespace(redis=redis)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test",
    ) as client:
        response = await client.get("/api/health")

    assert response.status_code == expected_status
    assert response.json() == expected_body
    redis.ping.assert_awaited_once()


async def test_health_reports_redis_connection_failure() -> None:
    app = FastAPI()
    configure_api(app)
    redis = SimpleNamespace(ping=AsyncMock(side_effect=ConnectionError("down")))
    app.state.container = SimpleNamespace(redis=redis)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test",
    ) as client:
        response = await client.get("/api/health")

    assert response.status_code == 503
    assert response.json() == {"status": "unhealthy", "redis": "unavailable"}


async def test_health_reports_uninitialized_application() -> None:
    app = FastAPI()
    configure_api(app)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test",
    ) as client:
        response = await client.get("/api/health")

    assert response.status_code == 503
