"""Worker node and pool lifecycle without a Redis dependency."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from app.worker import Node, NodeConfig, Pool

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def make_node() -> tuple[Node, AsyncMock, AsyncMock, asyncio.Event]:
    store = AsyncMock()
    pool = AsyncMock()
    started = asyncio.Event()

    async def start(_node_id: str) -> None:
        started.set()

    pool.start.side_effect = start
    pool.wait_for_unexpected_exit.side_effect = asyncio.Event().wait
    node = Node(
        NodeConfig(
            node_store=store,
            node_id="test-node",
            hostname="host",
            role="worker",
            capacity=1,
            heartbeat_interval=0.01,
        ),
        event_store=AsyncMock(),
        pool=pool,
    )
    return node, store, pool, started


async def test_node_stops_pool_once_then_deregisters() -> None:
    node, store, pool, started = make_node()
    stop = asyncio.Event()
    task = asyncio.create_task(node.run(stop))
    await asyncio.wait_for(started.wait(), 1)

    stop.set()
    await asyncio.wait_for(task, 1)

    pool.stop.assert_awaited_once()
    store.deregister.assert_awaited_once_with("test-node")


async def test_node_cancellation_cleans_up() -> None:
    node, store, pool, started = make_node()
    task = asyncio.create_task(node.run(asyncio.Event()))
    await asyncio.wait_for(started.wait(), 1)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    pool.stop.assert_awaited_once()
    store.deregister.assert_awaited_once_with("test-node")


async def test_registration_failure_prevents_worker_start() -> None:
    node, store, pool, _ = make_node()
    store.register.side_effect = ConnectionError("Redis unavailable")

    with pytest.raises(ConnectionError):
        await node.run(asyncio.Event())

    pool.start.assert_not_awaited()
    store.deregister.assert_not_awaited()


async def test_unexpected_worker_exit_leaves_node_for_reaper() -> None:
    node, store, pool, _ = make_node()
    pool.wait_for_unexpected_exit.side_effect = RuntimeError("worker died")

    with pytest.raises(RuntimeError, match="worker died"):
        await node.run(asyncio.Event())

    pool.stop.assert_awaited_once()
    store.deregister.assert_not_awaited()


async def test_heartbeat_continues_while_pool_drains() -> None:
    node, store, pool, started = make_node()
    draining = asyncio.Event()
    finish_drain = asyncio.Event()

    async def stop_pool() -> None:
        draining.set()
        await finish_drain.wait()

    pool.stop.side_effect = stop_pool
    stop = asyncio.Event()
    task = asyncio.create_task(node.run(stop))
    await asyncio.wait_for(started.wait(), 1)
    stop.set()
    await asyncio.wait_for(draining.wait(), 1)

    async def wait_for_heartbeat() -> None:
        while store.refresh_heartbeat.await_count == 0:
            await asyncio.sleep(0.001)

    await asyncio.wait_for(wait_for_heartbeat(), 1)
    store.deregister.assert_not_awaited()
    finish_drain.set()
    await asyncio.wait_for(task, 1)
    store.deregister.assert_awaited_once_with("test-node")


async def test_failed_drain_leaves_node_for_reaper() -> None:
    node, store, pool, started = make_node()
    pool.stop.side_effect = RuntimeError("drain failed")
    stop = asyncio.Event()
    task = asyncio.create_task(node.run(stop))
    await asyncio.wait_for(started.wait(), 1)
    stop.set()

    with pytest.raises(RuntimeError, match="drain failed"):
        await task

    store.deregister.assert_not_awaited()


async def test_pool_start_propagates_state_write_failure() -> None:
    worker_state = AsyncMock()
    worker_state.set.side_effect = ConnectionError("Redis unavailable")
    pool = Pool(
        broker=AsyncMock(), executor=AsyncMock(), worker_count=1,
        poll_interval=0.01, worker_state=worker_state,
    )

    with pytest.raises(ConnectionError):
        await pool.start("node")

    assert pool._workers == []


async def test_pool_removes_worker_state_after_shutdown() -> None:
    worker_state = AsyncMock()
    broker = AsyncMock()
    broker.dequeue.return_value = None

    async def wait_for_work(_timeout: float) -> None:
        await asyncio.sleep(0.01)

    broker.wait_for_work.side_effect = wait_for_work
    pool = Pool(
        broker=broker, executor=AsyncMock(), worker_count=1,
        poll_interval=0.01, worker_state=worker_state,
    )

    await pool.start("node")
    await asyncio.wait_for(pool.stop(), 1)

    worker_state.delete.assert_awaited_once_with("node:1")
