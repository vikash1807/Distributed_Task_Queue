"""Atomic worker failure routing against a dedicated Redis database."""

import os
import time
from collections.abc import AsyncIterator

import pytest
import redis.asyncio as redis

from app.broker import LeaseNotHeld, RedisBroker
from app.model import FailedTask, Task, TaskStatus
from app.queue import PriorityQueue
from app.store import (
    KEY_DEADLETTER,
    KEY_DELAYED,
    KEY_METRICS,
    KEY_PROCESSING,
    DeadLetterStore,
    TaskStore,
    node_tasks_key,
)

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
async def failure_case() -> AsyncIterator[tuple[redis.Redis, RedisBroker, TaskStore]]:
    url = os.getenv("WORKER_TEST_REDIS_URL")
    if not url:
        pytest.skip("requires WORKER_TEST_REDIS_URL for a dedicated Redis DB")

    client = redis.from_url(url, decode_responses=True)
    try:
        await client.flushdb()
        tasks = TaskStore(client)
        broker = RedisBroker(client, tasks, PriorityQueue(client, tasks), 30, "node")
        yield client, broker, tasks
    finally:
        await client.flushdb()
        await client.aclose()


async def lease(client: redis.Redis, tasks: TaskStore, task_id: str, retries: int) -> None:
    await tasks.save(Task(
        id=task_id, type="sleep", status=TaskStatus.PROCESSING,
        owner="node", retries=retries, max_retries=1,
    ))
    await client.zadd(KEY_PROCESSING, {task_id: time.time() * 1000 + 30_000})
    await client.sadd(node_tasks_key("node"), task_id)


async def test_failure_schedules_retry_once(
    failure_case: tuple[redis.Redis, RedisBroker, TaskStore],
) -> None:
    client, broker, tasks = failure_case
    await lease(client, tasks, "retry", 0)

    assert await broker.fail("retry", "handler failed", time.time() + 2, "{}") == 1
    with pytest.raises(LeaseNotHeld):
        await broker.fail("retry", "handler failed", time.time() + 2, "{}")

    task = await tasks.get("retry")
    assert (task.status, task.retries, task.owner, task.error) == (
        TaskStatus.PENDING, 1, "", "handler failed",
    )
    assert await client.zscore(KEY_PROCESSING, "retry") is None
    assert await client.zscore(KEY_DELAYED, "retry") is not None
    assert await client.scard(node_tasks_key("node")) == 0
    assert await client.hget(KEY_METRICS, "failed") == "1"
    assert await client.hget(KEY_METRICS, "retries") == "1"


async def test_exhausted_failure_is_dead_lettered_once(
    failure_case: tuple[redis.Redis, RedisBroker, TaskStore],
) -> None:
    client, broker, tasks = failure_case
    await lease(client, tasks, "exhausted", 1)
    failed = FailedTask(
        id="exhausted", type="sleep", owner="", error="handler failed",
        reason="handler failed",
    )

    assert await broker.fail(
        "exhausted", "handler failed", time.time() + 2, failed.model_dump_json(),
    ) == 2
    with pytest.raises(LeaseNotHeld):
        await broker.fail(
            "exhausted", "handler failed", time.time() + 2, failed.model_dump_json(),
        )

    task = await tasks.get("exhausted")
    assert (task.status, task.owner, task.error) == (TaskStatus.FAILED, "", "handler failed")
    assert await client.zscore(KEY_PROCESSING, "exhausted") is None
    assert await client.scard(node_tasks_key("node")) == 0
    assert await client.llen(KEY_DEADLETTER) == 1
    assert (await DeadLetterStore(client).list(0, 10))[0].reason == "handler failed"
    assert await client.hget(KEY_METRICS, "failed") == "1"
