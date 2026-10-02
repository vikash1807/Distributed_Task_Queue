"""Redis integration tests for the reaper. Set REAPER_TEST_REDIS_URL to run."""

from __future__ import annotations

import asyncio
import os
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass

import pytest
import redis.asyncio as redis

from app.model import Node, Task, TaskEventType, TaskStatus
from app.reaper import Reaper
from app.store import (
    KEY_DEADLETTER,
    KEY_METRICS,
    KEY_NODES,
    KEY_PROCESSING,
    KEY_READY,
    KEY_READY_SIGNAL,
    DeadLetterStore,
    EventStore,
    NodeStore,
    TaskStore,
    key_task,
    node_dead_key,
    node_heartbeat_key,
    node_tasks_key,
)

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@dataclass
class ReaperCase:
    client: redis.Redis
    nodes: NodeStore
    events: EventStore
    tasks: TaskStore
    reaper: Reaper
    now_ms: int

    async def lease(
        self, task_id: str, owner: str, retries: int,
        max_retries: int, deadline: int,
    ) -> None:
        await self.tasks.save(Task(
            id=task_id,
            type="sleep",
            payload={"duration_ms": 1},
            priority=5,
            retries=retries,
            max_retries=max_retries,
            status=TaskStatus.PROCESSING,
            owner=owner,
        ))
        await self.client.zadd(KEY_PROCESSING, {task_id: deadline})
        await self.client.sadd(node_tasks_key(owner), task_id)


@pytest.fixture
async def reaper_case() -> AsyncIterator[ReaperCase]:
    url = os.getenv("REAPER_TEST_REDIS_URL")
    if not url:
        pytest.skip("requires REAPER_TEST_REDIS_URL for a dedicated Redis DB")

    client = redis.from_url(url, decode_responses=True)
    try:
        await client.flushdb()
        nodes = NodeStore(client, heartbeat_ttl_ms=10_000)
        events = EventStore(client)
        tasks = TaskStore(client)
        yield ReaperCase(
            client=client,
            nodes=nodes,
            events=events,
            tasks=tasks,
            reaper=Reaper(
                client, nodes, events,
                interval=0.01, dead_node_grace_ms=1_000, signal_cap=10,
            ),
            now_ms=int(time.time() * 1_000),
        )
    finally:
        await client.flushdb()
        await client.aclose()


async def test_expired_lease_requeues_once_and_wakes_worker(reaper_case: ReaperCase) -> None:
    case = reaper_case
    await case.lease("expired", "node-a", 0, 2, case.now_ms - 1)
    await case.reaper._sweep_expired(case.now_ms)
    await case.reaper._sweep_expired(case.now_ms)

    task = await case.tasks.get("expired")
    assert (task.status, task.retries, task.owner) == (TaskStatus.PENDING, 1, "")
    assert await case.client.zscore(KEY_READY, "expired") == -5
    assert await case.client.zscore(KEY_PROCESSING, "expired") is None
    assert await case.client.scard(node_tasks_key("node-a")) == 0
    assert await case.client.llen(KEY_READY_SIGNAL) == 1
    assert await case.client.hget(KEY_METRICS, "reaper_reclaims") == "1"
    assert await case.client.hget(KEY_METRICS, "retries") == "1"
    assert await case.client.hget(KEY_METRICS, "failed") == "1"
    events = await case.events.list_cluster(10)
    assert [event.type for event in events] == [TaskEventType.RECLAIMED]


async def test_dead_node_reclaims_before_expiry_and_prunes_after_grace(
    reaper_case: ReaperCase,
) -> None:
    case = reaper_case
    await case.nodes.register(Node(id="node-dead", hostname="host", role="worker"))
    await case.lease("owned", "node-dead", 0, 1, case.now_ms + 60_000)
    await case.client.delete(node_heartbeat_key("node-dead"))

    await case.reaper._sweep_dead_nodes(case.now_ms)
    assert await case.client.zscore(KEY_READY, "owned") == -5
    nodes = await case.nodes.list_nodes()
    assert len(nodes) == 1
    assert nodes[0].alive is False
    assert nodes[0].hostname == "host"
    await case.reaper._sweep_dead_nodes(case.now_ms + 999)
    assert len(await case.nodes.list_nodes()) == 1
    await case.reaper._sweep_dead_nodes(case.now_ms + 1_000)
    assert await case.nodes.list_nodes() == []
    events = await case.events.list_cluster(10)
    assert [event.type for event in events].count(TaskEventType.NODE_DEAD) == 1
    assert [event.type for event in events].count(TaskEventType.RECLAIMED) == 1


async def test_exhausted_task_goes_to_dlq_and_orphan_is_removed(
    reaper_case: ReaperCase,
) -> None:
    case = reaper_case
    await case.lease("exhausted", "node-a", 1, 1, case.now_ms - 1)
    await case.client.hset(key_task("exhausted"), "payload", "{}")
    await case.client.zadd(KEY_PROCESSING, {"orphan": case.now_ms - 1})
    await case.reaper._sweep_expired(case.now_ms)

    task = await case.tasks.get("exhausted")
    assert task.status == TaskStatus.FAILED
    assert task.retries == 1
    assert task.owner == ""
    assert await case.client.zcard(KEY_READY) == 0
    assert await case.client.zcard(KEY_PROCESSING) == 0
    assert not await case.client.exists(key_task("orphan"))
    assert await case.client.llen(KEY_DEADLETTER) == 1
    failed = (await DeadLetterStore(case.client).list(0, 10))[0]
    assert (failed.id, failed.status, failed.reason) == (
        "exhausted", TaskStatus.FAILED, "task lease expired"
    )
    assert failed.payload == {}
    assert failed.error == "task lease expired"
    assert failed.owner == ""
    assert failed.failed_at is not None
    assert await case.client.hget(KEY_METRICS, "reaper_reclaims") == "1"
    assert await case.client.hget(KEY_METRICS, "failed") == "1"


async def test_extended_lease_and_changed_owner_are_not_reclaimed(
    reaper_case: ReaperCase,
) -> None:
    case = reaper_case
    await case.nodes.register(Node(id="node-dead"))
    await case.lease("extended", "node-live", 0, 1, case.now_ms + 60_000)
    await case.lease("moved", "node-dead", 0, 1, case.now_ms + 60_000)
    await case.client.hset(key_task("moved"), "owner", "node-live")
    await case.client.delete(node_heartbeat_key("node-dead"))

    await case.reaper._sweep_dead_nodes(case.now_ms)
    await case.reaper._sweep_expired(case.now_ms)
    assert await case.client.zcard(KEY_PROCESSING) == 2
    assert await case.client.scard(node_tasks_key("node-dead")) == 0
    assert await case.client.zcard(KEY_READY) == 0
    assert not await case.client.exists(KEY_METRICS)


async def test_dead_node_orphan_is_removed_before_lease_expiry(
    reaper_case: ReaperCase,
) -> None:
    case = reaper_case
    await case.nodes.register(Node(id="node-dead"))
    await case.client.zadd(KEY_PROCESSING, {"orphan": case.now_ms + 60_000})
    await case.client.sadd(node_tasks_key("node-dead"), "orphan")
    await case.client.delete(node_heartbeat_key("node-dead"))

    await case.reaper._sweep_dead_nodes(case.now_ms)
    assert await case.client.zcard(KEY_PROCESSING) == 0
    assert await case.client.scard(node_tasks_key("node-dead")) == 0


async def test_restored_heartbeat_fences_dead_node_reclaim(reaper_case: ReaperCase) -> None:
    case = reaper_case
    await case.nodes.register(Node(id="node-back"))
    await case.lease("still-owned", "node-back", 0, 1, case.now_ms + 60_000)

    await case.reaper._reclaim_one("still-owned", case.now_ms, "node-back")
    assert await case.client.zcard(KEY_PROCESSING) == 1
    assert await case.client.zcard(KEY_READY) == 0
    assert await case.client.scard(node_tasks_key("node-back")) == 1


async def test_graceful_deregistration_is_not_marked_dead(reaper_case: ReaperCase) -> None:
    case = reaper_case
    await case.nodes.register(Node(id="node-stopped"))
    await case.nodes.deregister("node-stopped")

    result = await case.reaper._mark_dead(
        keys=[KEY_NODES, node_heartbeat_key("node-stopped"), node_dead_key("node-stopped")],
        args=["node-stopped", case.now_ms],
    )
    assert result == 0
    assert not await case.client.exists(node_dead_key("node-stopped"))


async def test_background_loop_cancels_cleanly(reaper_case: ReaperCase) -> None:
    task = asyncio.create_task(reaper_case.reaper.run())
    await asyncio.sleep(0.02)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
