from __future__ import annotations

import secrets
from datetime import datetime, timezone
from typing import Any

from app.model import Task, TaskStatus, FailedTask, TaskEvent, TaskEventType
from app.queue import PriorityQueue, DelayedScheduler
from app.store import TaskStore, DeadLetterStore, MetricStore, EventStore


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
    return datetime.now(timezone.utc)


class TaskService:
    def __init__(
        self,
        task_queue: PriorityQueue,
        delayed_queue: DelayedScheduler,
        dead_letter: DeadLetterStore,
        task_store: TaskStore,
        metric_store: MetricStore,
        event_store: EventStore,
    ) -> None:
        self.task_store = task_store
        self.task_queue = task_queue
        self.dead_letter = dead_letter
        self.delayed_queue = delayed_queue
        self.metric_store = metric_store
        self.event_store = event_store

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
        task_id = id or generate_task_id()

        # Client-supplied ID: check for duplicates (idempotent enqueue. If a record 
        # already exists, reject to prevent double-submission from network retries.
        if id and await self.task_store.exists(task_id):
            raise DuplicateTaskError(task_id)

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

        # Persist the record before referencing it's ID from any queue.
        await self.task_store.save(task)

        # Emit submitted Event
        event = TaskEvent(
            id=f"evt-{secrets.token_hex(12)}",
            task_id=task_id,
            type=TaskEventType.SUBMITTED,
            worker_id="-1",
            detail=f"Priority={priority}, Delay={delay}s, MaxRetries={max_retries}",
            timestamp=utc_now()
        )
        await self.event_store.push(event)

        # Increment submitted tasks count in metrics
        await self.metric_store.incr_submitted()

        if delay > 0:
            await self.delayed_queue.schedule(task, delay)
        else:
            await self.task_queue.enqueue(task)

        return task

    async def get_task(self, task_id: str) -> Task:
        return await self.task_store.get(task_id)

    async def get_failed_tasks(self, offset: int, limit: int) -> list[FailedTask]:
        return await self.dead_letter.list(offset, limit)
    
    async def redrive_failed_tasks(self) -> dict[str, Any]:
        """
        Pops all tasks from deadletter queue, resets their retries and status,
        updates their canonical record and re-enqueues them to READY.
        """

        failed_tasks: list[FailedTask] = self.dead_letter.drain_all()

        if not failed_tasks:
            return {
                "redriven": 0,
                "message":  "dead-letter queue is empty"
            }

        for failed_task in failed_tasks:
            task_data = {
                k: v for k, v in failed_task.model_dump().items()
                if k in Task.model_fields
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
                timestamp=utc_now()
            )
            await self.event_store.push(event)
        
        return {
        "redriven": redriven,
        "total": len(failed_tasks),
    }
