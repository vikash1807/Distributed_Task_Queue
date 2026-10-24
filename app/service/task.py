from __future__ import annotations

import secrets
from datetime import UTC, datetime
from typing import Any

from app.core import settings
from app.model import FailedTask, Task, TaskEvent, TaskEventType, TaskStatus
from app.queue import DelayedScheduler, PriorityQueue
from app.store import DeadLetterStore, EventStore, MetricStore, TaskStore


class DuplicateTaskError(Exception):
    """Raised when a caller submits a task ID that already exists."""


def generate_task_id() -> str:
    """
    Returns a unique task ID. Every submission gets its own identity so that
    two tasks with otherwise-identical fields are distinct entries (the identity
    bug fix). A client-supplied ID is honored and becomes an idempotency key.
    """
    return f"task-{secrets.token_hex(12)}"


def utc_now() -> datetime:
    """Return an aware UTC timestamp for task and event records."""
    return datetime.now(UTC)


class TaskService:
    """Coordinate submission, inspection, and redrive of queued tasks."""

    def __init__(
        self,
        task_queue: PriorityQueue,
        delayed_queue: DelayedScheduler,
        dead_letter: DeadLetterStore,
        task_store: TaskStore,
        metric_store: MetricStore,
        event_store: EventStore,
        signal_cap: int | None = None,
    ) -> None:
        """Bind task persistence, queues, monitoring, and the wake-up token cap."""
        self.task_store = task_store
        self.task_queue = task_queue
        self.dead_letter = dead_letter
        self.delayed_queue = delayed_queue
        self.metric_store = metric_store
        self.event_store = event_store
        self.signal_cap = settings.signal_cap if signal_cap is None else signal_cap

    async def submit_task(
        self,
        *,
        id: str = "",
        type: str = "",
        payload: Any = None,
        priority: int = 0,
        delay: int = 0,
        max_retries: int = 0,
    ) -> Task:
        """Build a pending task and atomically submit it exactly once per ID."""
        task_id = id or generate_task_id()

        task = Task(
            id=task_id,
            type=type,
            payload=payload,
            priority=priority,
            delay=delay,
            max_retries=max_retries,
            status=TaskStatus.PENDING,
            created_at=utc_now(),
        )

        event = TaskEvent(
            id=f"evt-{secrets.token_hex(12)}",
            task_id=task_id,
            type=TaskEventType.SUBMITTED,
            worker_id="-1",
            detail=f"Priority={priority}, Delay={delay}s, MaxRetries={max_retries}",
            timestamp=utc_now(),
        )
        if await self.task_store.submit(task, event, self.signal_cap) == 0:
            raise DuplicateTaskError(task_id)

        return task

    async def get_task(self, task_id: str) -> Task:
        """Read a task from its canonical Redis record."""
        return await self.task_store.get(task_id)

    async def get_failed_tasks(self, offset: int, limit: int) -> list[FailedTask]:
        """Return a page of retained dead-letter records."""
        return await self.dead_letter.list(offset, limit)

    async def redrive_failed_tasks(self) -> dict[str, Any]:
        """
        Pops all tasks from deadletter queue, resets their retries and status,
        updates their canonical record and re-enqueues them to READY.
        """

        failed_tasks: list[FailedTask] = await self.dead_letter.drain_all()
        redriven = 0

        if not failed_tasks:
            return {"redriven": 0, "message": "dead-letter queue is empty"}

        for failed_task in failed_tasks:
            task_data = {
                k: v for k, v in failed_task.model_dump().items() if k in Task.model_fields
            }

            task = Task(**task_data)

            task.retries = 0
            task.status = TaskStatus.PENDING
            task.error = ""

            try:
                # update canonical record
                await self.task_store.save(task)

                # re-enqueue task into READY queue
                await self.task_queue.enqueue(task)
            except Exception:
                continue

            redriven += 1

            # Emit event
            event = TaskEvent(
                id=f"evt-{secrets.token_hex(12)}",
                task_id=task.id,
                type=TaskEventType.REDRIVEN,
                worker_id="-1",
                detail="Moved from dead-letter queue back to ready",
                timestamp=utc_now(),
            )
            await self.event_store.push(event)

        return {
            "redriven": redriven,
            "total": len(failed_tasks),
        }
