# app/queue/delayed.py

from __future__ import annotations

import asyncio
import logging
import secrets
import time
from datetime import UTC, datetime
from pathlib import Path

import redis.asyncio as redis

from app.core import settings as config
from app.core.leadership import LeaderLease, LeadershipLost
from app.model import Task, TaskEvent, TaskEventType
from app.queue.queue import PriorityQueue
from app.store import (
    KEY_DELAYED,
    KEY_READY,
    KEY_READY_SIGNAL,
    KEY_TASK_PREFIX,
    EventStore,
    TaskStore,
    key_task,
)
from app.store.redis import KEY_LEADER

logger = logging.getLogger(__name__)

SCRIPTS_DIR = Path(__file__).parent / "scripts"


def load_script(name: str) -> str:
    """Read a Lua script from the delayed queue's scripts directory."""
    return (SCRIPTS_DIR / name).read_text(encoding="utf-8")


class DelayedScheduler:
    """Store delayed work and promote due tasks during leader ownership."""

    def __init__(
        self,
        client: redis.Redis,
        queue: PriorityQueue,
        task_store: TaskStore,
        event_store: EventStore,
    ) -> None:
        """Bind queue dependencies and register promotion and retry scripts."""
        self.client = client
        self.queue = queue
        self.task_store = task_store
        self.event_store = event_store

        self._promote = client.register_script(load_script("promote.lua"))
        self._retry = client.register_script(load_script("retry.lua"))

    async def schedule(self, task: Task, delay: float) -> None:
        """schedule a task to become ready at execute_at time."""
        execute_at = int(time.time() + delay)
        await self.client.zadd(KEY_DELAYED, {task.id: float(execute_at)})

    async def schedule_retry(self, task: Task, execute_at: float) -> None:
        """Atomically update retry state and add the task to delayed."""

        await self._retry(
            keys=[
                key_task(task.id),
                KEY_DELAYED,
            ],
            args=[
                task.id,
                str(task.retries),
                task.status.value,
                task.error or "",
                str(execute_at),
            ],
        )

    async def run(self, lease: LeaderLease) -> None:
        """Continuously promote due tasks from delayed to ready every second."""

        logger.info("delayed scheduler started")
        try:
            while True:
                await asyncio.sleep(1)

                try:
                    promoted = await self._promote_due_tasks(lease)

                    if promoted:
                        logger.info("promoted %d delayed tasks", promoted)

                        # Emit events for promoted tasks. Since the Lua script
                        # handles the batch atomically, we just emit a summary event.
                        event = TaskEvent(
                            id=f"evt-{secrets.token_hex(12)}",
                            task_id="",
                            type=TaskEventType.PROMOTED,
                            worker_id="-1",
                            detail=f"Promoted {promoted} task(s) from delayed to ready",
                            timestamp=datetime.now(UTC),
                        )
                        await self.event_store.push(event)

                except LeadershipLost:
                    raise
                except Exception:
                    logger.exception("error promoting delayed tasks.")

        except asyncio.CancelledError:
            logger.info("Delayed scheduler stopped")
            raise

    async def _promote_due_tasks(self, lease: LeaderLease | None = None):
        """Promote one due batch, checking leadership again inside Redis when supplied."""
        # Direct maintenance/tests may omit the lease; supervised runtime always supplies it.
        if lease is not None:
            lease.check()
        now = str(int(time.time()))
        batch_size = 100

        promoted = await self._promote(
            keys=[
                KEY_DELAYED,
                KEY_READY,
                KEY_TASK_PREFIX,
                KEY_READY_SIGNAL,
                KEY_LEADER,
            ],
            args=[
                now,
                str(batch_size),
                str(config.signal_cap),
                *([lease.value] if lease is not None else []),
            ],
        )

        if int(promoted) == -1:
            # Escape the loop's ordinary error handler so the supervisor can stop siblings.
            raise LeadershipLost("delayed promotion rejected")
        return int(promoted or 0)
