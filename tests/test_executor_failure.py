"""Executor behavior when handlers, leases, or monitoring calls fail."""

from unittest.mock import AsyncMock, Mock

import pytest

from app.broker import LeaseNotHeld
from app.model import Task
from app.worker import Executor, ExecutorDeps

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


async def test_lost_lease_clears_processing_state() -> None:
    broker = AsyncMock()
    broker.fail.side_effect = LeaseNotHeld("task")
    handlers = Mock()
    handlers.get.return_value = AsyncMock(side_effect=ValueError("handler failed"))
    worker_state = AsyncMock()
    executor = Executor(ExecutorDeps(
        broker=broker,
        handlers=handlers,
        event_store=AsyncMock(),
        metric_store=AsyncMock(),
        worker_state=worker_state,
    ))

    await executor.execute(Task(id="task", type="sleep"), "node:1")

    assert worker_state.set.await_count == 2
    assert worker_state.set.await_args.args[0].status == "idle"


async def test_metric_failure_after_ack_does_not_fail_completed_task() -> None:
    broker = AsyncMock()
    handlers = Mock()
    handlers.get.return_value = AsyncMock(return_value=Mock(detail="done"))
    metrics = AsyncMock()
    metrics.incr_processed.side_effect = ConnectionError("metric unavailable")
    executor = Executor(ExecutorDeps(
        broker=broker,
        handlers=handlers,
        event_store=AsyncMock(),
        metric_store=metrics,
        worker_state=AsyncMock(),
    ))

    await executor.execute(Task(id="task", type="sleep"), "node:1")

    broker.ack.assert_awaited_once_with("task")
    broker.fail.assert_not_awaited()


async def test_ack_error_leaves_lease_for_reaper() -> None:
    broker = AsyncMock()
    broker.ack.side_effect = ConnectionError("Redis unavailable")
    handlers = Mock()
    handlers.get.return_value = AsyncMock(return_value=Mock(detail="done"))
    executor = Executor(ExecutorDeps(
        broker=broker,
        handlers=handlers,
        event_store=AsyncMock(),
        metric_store=AsyncMock(),
        worker_state=AsyncMock(),
    ))

    with pytest.raises(ConnectionError):
        await executor.execute(Task(id="task", type="sleep"), "node:1")

    broker.fail.assert_not_awaited()
