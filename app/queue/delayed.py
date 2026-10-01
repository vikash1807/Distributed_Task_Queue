# app/queue/delayed.py

from __future__ import annotations

import time
import logging
import secrets
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import asyncio
import redis.asyncio as redis

from app.core import settings as config
from app.model import Task, TaskEvent, TaskEventType
from app.queue.queue import PriorityQueue
from app.store import (
    EventStore,
    TaskStore,
    key_task,
    KEY_DELAYED,
    KEY_READY,
    KEY_TASK_PREFIX,
    KEY_READY_SIGNAL,
)

logger = logging.getLogger(__name__)

SCRIPTS_DIR = Path(__file__).parent / "scripts"


def load_script(name: str) -> str:
    """Read a Lua script from the broker's scripts directory."""
    return (SCRIPTS_DIR / name).read_text(encoding="utf-8")


class DelayedScheduler:
    def __init__(
        self,
        client: redis.Redis,
        queue: PriorityQueue,
        task_store: TaskStore,
        event_store: EventStore
    ) -> None:
        self.client = client
        self.queue = queue
        self.task_store = task_store
        self.event_store = event_store

        self._promote = client.register_script(load_script("promote.lua"))
        self._retry = client.register_script(load_script("retry.lua"))

    async def schedule(self, task: Task, delay: float) -> None:
        """schedule a task to become ready at execute_at time."""
        execute_at = int(time.time() + delay)
        await self.client.zadd(KEY_DELAYED, {task.id : float(execute_at)})

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
            ]
        )

    async def run(self) -> None:
        """Continuously promote due tasks from delayed to ready every second."""

        logger.info("delayed scheduler started")
        try:
            while True:
                await asyncio.sleep(1)

                try:
                    promoted = await self._promote_due_tasks()

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
                            timestamp=datetime.now(timezone.utc)
                        )
                        await self.event_store.push(event)
                
                except Exception:
                    logger.exception("error promoting delayed tasks.")

        except asyncio.CancelledError:
            logger.info("Delayed scheduler stopped")
            raise
    
    async def _promote_due_tasks(self):
        """promote dues tasks from delayed to ready."""
        now = str(int(time.time()))
        batch_size = 100

        promoted = await self._promote(
            keys=[
                KEY_DELAYED,
                KEY_READY,
                KEY_TASK_PREFIX,
                KEY_READY_SIGNAL,
            ],
            args=[
                now,
                str(batch_size),
                str(config.signal_cap)
            ],
        )

        return int(promoted or 0)

