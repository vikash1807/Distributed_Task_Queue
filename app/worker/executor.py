# app/worker/executor.py

"""Execute a single claimed task and route it's outcome to ack, retry or DLQ. """

from __future__ import annotations

import asyncio
import logging
import time
import secrets
from dataclasses import dataclass
from datetime import datetime, timezone

from app.broker import LeaseNotHeld, RedisBroker
from app.handler import Registry
from app.model import Task, FailedTask, TaskEvent, TaskEventType, WorkerState
from app.store import MetricStore, EventStore, WorkerStateStore



logger = logging.getLogger(__name__)
DEFAULT_DRAIN_TIMEOUT = 5.0  # seconds

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def backoff_delay(retries: int) -> float:
    """
    Returns retry delay for a given (post-increment) retry count: 
    exponential 2^retries seconds, capped at 60s.
    """
    return float(min(2 ** retries, 60))


@dataclass
class ExecutorDeps:
    broker: RedisBroker
    handlers: Registry
    event_store: EventStore
    metric_store: MetricStore
    worker_state: WorkerStateStore
    drain_timeout: float = DEFAULT_DRAIN_TIMEOUT


class Executor:
    def __init__(self, deps: ExecutorDeps) -> None:
        self.broker = deps.broker
        self.handlers = deps.handlers

        self.event_store = deps.event_store
        self.metric_store = deps.metric_store
        self.worker_state = deps.worker_state
        self.drain_timeout = deps.drain_timeout

    
    async def execute(self, task: Task, worker_id: int) -> None:
        """Run a task and atomically route success or failure."""

        logger.info(
            "executing task task_id=%s priority=%d attempt=%d max_attempt=%d",
            task.id, task.priority, task.retries + 1, task.max_retries + 1
        )

	    # Mark worker state as processing 
        try:
            await self.worker_state.set(
                WorkerState(
                    id=worker_id,
                    status="processing",
                    task_id=task.id,
                    started_at=utc_now(),
                )
            )
        except Exception:
            logger.exception("failed to update worker state worker_id=%s", worker_id)

        try:
            await self._emit_event(
                task_id=task.id,
                event_type=TaskEventType.STARTED,
                worker_id=worker_id,
                detail=f"Worker {worker_id} picked up task",
            )

            try:
                handler = self.handlers.get(task.type)
                result = await asyncio.wait_for(handler(task), timeout=self.drain_timeout)
            except Exception as exc:
                logger.exception("task handler failed task_id=%s", task.id)
                task.error = str(exc) or exc.__class__.__name__

                retry_number = task.retries + 1
                delay = backoff_delay(retry_number)
                failed_task = FailedTask(
                    **(task.model_dump() | {"owner": ""}),
                    failed_at=utc_now(),
                    reason=task.error,
                )
                try:
                    route = await self.broker.fail(
                        task.id, task.error, time.time() + delay,
                        failed_task.model_dump_json(),
                    )
                except LeaseNotHeld:
                    logger.warning("lease lost while failing task_id=%s", task.id)
                    return

                await self._emit_event(
                    task_id=task.id,
                    event_type=TaskEventType.FAILED,
                    worker_id=worker_id,
                    detail=task.error,
                )
                if route == 1:
                    logger.info("task retry scheduled task_id=%s retry=%d", task.id, retry_number)
                    await self._emit_event(
                        task_id=task.id,
                        event_type=TaskEventType.RETRYING,
                        worker_id=worker_id,
                        detail=f"Retry {retry_number}/{task.max_retries} in {int(delay)}s",
                    )
                else:
                    logger.warning("task moved to dead-letter task_id=%s", task.id)
                    await self._emit_event(
                        task_id=task.id,
                        event_type=TaskEventType.DEAD_LETTERED,
                        worker_id=worker_id,
                        detail=f"task moved to dead-letter task_id={task.id}",
                    )
                return

            try:
                await self.broker.ack(task.id)
            except LeaseNotHeld:
                logger.warning("lease lost while completing task_id=%s", task.id)
                return

            # Once ACK succeeds, monitoring errors must not turn a completed
            # task into a failure or attempt to release its former lease.
            try:
                await self.metric_store.incr_processed()
            except Exception:
                logger.exception("failed to update processed metric task_id=%s", task.id)
            await self._emit_event(
                task_id=task.id,
                event_type=TaskEventType.COMPLETED,
                worker_id=worker_id,
                detail=f"task completed successfully. Result - {result.detail}",
            )
            logger.info("task completed task_id=%s detail=%s", task.id, result.detail)

        finally:
            # A lost lease or an error while routing a failure must not leave
            # the worker's displayed state stuck at "processing".
            try:
                await self.worker_state.set(
                    WorkerState(id=worker_id, status="idle")
                )
            except Exception:
                logger.exception("failed to clear worker state worker_id=%s", worker_id)

    async def _emit_event(
            self,
            task_id: str,
            event_type: TaskEventType,
            worker_id: int,
            detail: str
        ) -> None:
        event = TaskEvent(
            id=f"evt-{secrets.token_hex(12)}",
            task_id=task_id,
            type=event_type,
            worker_id=worker_id,
            detail=detail,
            timestamp=utc_now(),
        )
        try:
            await self.event_store.push(event)
        except Exception:
            logger.exception("error pushing event for task_id=%s", task_id)
