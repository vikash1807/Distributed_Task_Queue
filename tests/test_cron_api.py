"""HTTP contracts for cron and leader inspection."""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import FastAPI
from redis.exceptions import ConnectionError as RedisConnectionError

from app.api.router import configure_api
from app.model.cron import CronJob
from app.store.cron import CronNotFound, DuplicateCronError

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    """Run async tests on asyncio, matching the application runtime."""
    return "asyncio"


@pytest.fixture
async def api():
    """Provide HTTP routes with controllable cron and leader service doubles."""
    job = CronJob(
        id="job",
        schedule="* * * * *",
        type="hash",
        created_at=datetime.now(UTC),
        eligible_after_unix=0,
        revision="secret",
    )
    service = AsyncMock()
    service.create.return_value = job
    service.list.return_value = [job]
    service.set_enabled.return_value = job
    leader = AsyncMock()
    leader.status.return_value = {"leader_id": None, "is_self": False}
    app = FastAPI()
    configure_api(app)
    app.state.container = SimpleNamespace(
        cron_service=service, leader=leader, task_service=AsyncMock()
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        yield client, service, leader, app


async def test_crud_and_leader(api):
    """Exercise cron CRUD, hidden revisions, leader inspection, and CORS."""
    client, service, _leader, _app = api
    response = await client.post("/api/cron", json={"schedule": "* * * * *", "type": "hash"})
    assert response.status_code == 201
    assert "revision" not in response.json()
    assert (await client.get("/api/cron")).json()[0]["id"] == "job"
    assert (await client.patch("/api/cron/job", json={"enabled": False})).status_code == 200
    service.set_enabled.assert_awaited_once_with("job", False)
    assert (await client.delete("/api/cron/job")).status_code == 204
    assert (await client.get("/api/leader")).json() == {"leader_id": None, "is_self": False}
    methods = (await client.options("/api/cron/job")).headers["access-control-allow-methods"]
    assert "PATCH" in methods and "DELETE" in methods


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"schedule": "* * * * *", "type": " "},
        {"schedule": "* * * * *", "type": "hash", "max_retries": -1},
        {"schedule": "* * * * *", "type": "hash", "enabled": "true"},
        {"schedule": "* * * * *", "type": "hash", "id": "bad id"},
        {"schedule": "* * * * *", "type": "hash", "payload": []},
        {"schedule": "* * * * *", "type": "hash", "delay": 5},
    ],
)
async def test_input_errors_are_400(api, body):
    """Map invalid cron request bodies to HTTP 400 before service calls."""
    client, service, _leader, _app = api
    assert (await client.post("/api/cron", json=body)).status_code == 400
    service.create.assert_not_awaited()


async def test_malformed_json_and_patch_errors(api):
    """Reject malformed cron input while preserving existing task validation."""
    client, _service, _leader, _app = api
    assert (
        await client.post(
            "/api/cron",
            content="{",
            headers={"Content-Type": "application/json"},
        )
    ).status_code == 400
    assert (await client.patch("/api/cron/job", json={})).status_code == 400
    assert (
        await client.patch("/api/cron/job", json={"enabled": True, "type": "hash"})
    ).status_code == 400
    # Existing task validation retains FastAPI's original response.
    assert (await client.post("/api/tasks", json={"priority": []})).status_code == 422


async def test_duplicate_missing_and_unavailable(api):
    """Distinguish duplicate, missing, unavailable, and uninitialized services."""
    client, service, leader, app = api
    body = {"schedule": "* * * * *", "type": "hash"}
    service.create.side_effect = DuplicateCronError("job")
    assert (await client.post("/api/cron", json=body)).status_code == 409
    service.delete.side_effect = CronNotFound("job")
    service.set_enabled.side_effect = CronNotFound("job")
    assert (await client.delete("/api/cron/job")).status_code == 404
    assert (await client.patch("/api/cron/job", json={"enabled": False})).status_code == 404
    for method in (service.create, service.list, service.delete, service.set_enabled):
        method.side_effect = RedisConnectionError("offline")
    assert (await client.post("/api/cron", json=body)).status_code == 503
    assert (await client.get("/api/cron")).status_code == 503
    assert (await client.delete("/api/cron/job")).status_code == 503
    assert (await client.patch("/api/cron/job", json={"enabled": False})).status_code == 503
    leader.status.side_effect = RedisConnectionError("offline")
    assert (await client.get("/api/leader")).status_code == 503
    del app.state.container
    assert (await client.get("/api/cron")).status_code == 503
    assert (await client.get("/api/leader")).status_code == 503


async def test_failed_supervisor_is_unhealthy(api):
    """Make readiness fail when background supervision has failed."""
    client, _service, leader, app = api
    leader.failed = True
    app.state.container.redis = AsyncMock()
    assert (await client.get("/api/health")).status_code == 503
