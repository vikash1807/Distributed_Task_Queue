# app/store/task.py

from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import redis.asyncio as redis

from app.model import Task, TaskEvent, TaskNotFound, TaskStatus
from app.store.redis import (
    KEY_DELAYED,
    KEY_EVENTS,
    KEY_METRICS,
    KEY_READY,
    KEY_READY_SIGNAL,
    key_task,
)


def task_to_hash(task: Task) -> dict[str, Any]:
    "Convert a task into redis hash field."
    return {
        "id": task.id,
        "type": task.type,
        "payload": json.dumps(task.payload) if task.payload is not None else "",
        "priority": task.priority,
        "delay": task.delay,
        "max_retries": task.max_retries,
        "retries": task.retries,
        "status": task.status.value,
        "created_at": (task.created_at.isoformat() if task.created_at is not None else ""),
        "error": task.error,
        "owner": task.owner,
    }


def hash_to_task(data: dict[str, Any]) -> Task:
    """Convert a Redis hash back into a Task."""
    payload: Any = None

    if data.get("payload"):
        try:
            payload = json.loads(data["payload"])
        except (json.JSONDecodeError, TypeError):
            payload = data["payload"]

    created_at: datetime | None = None

    if data.get("created_at"):
        try:
            created_at = datetime.fromisoformat(data["created_at"])
        except ValueError:
            created_at = None

    try:
        task_status = TaskStatus(data.get("status", "pending"))
    except ValueError:
        task_status = TaskStatus.PENDING

    return Task(
        id=data.get("id", ""),
        type=data.get("type", ""),
        payload=payload,
        priority=int(data.get("priority") or 0),
        delay=int(data.get("delay") or 0),
        max_retries=int(data.get("max_retries") or 0),
        retries=int(data.get("retries") or 0),
        status=task_status,
        created_at=created_at,
        error=data.get("error", ""),
        owner=data.get("owner", ""),
    )


class TaskStore:
    """Persist canonical task records and atomically create queue entries."""

    def __init__(self, client: redis.Redis) -> None:
        """Bind Redis and register the atomic submission script."""
        self.client = client
        self._submit = client.register_script(
            (Path(__file__).parent / "scripts" / "submit.lua").read_text()
        )

    async def submit(self, task: Task, event: TaskEvent, signal_cap: int) -> int:
        """Create a task atomically, returning 0 when its ID already exists."""
        # One Redis call commits the record, queue membership, metric, and event together.
        return int(await self._submit(
            keys=[key_task(task.id), KEY_READY, KEY_DELAYED, KEY_READY_SIGNAL,
                  KEY_METRICS, KEY_EVENTS],
            args=[
                # Pass integers as strings so Lua's JSON decoder cannot round their values.
                json.dumps({key: str(value) for key, value in task_to_hash(task).items()}),
                event.model_dump_json(),
                int(time.time() + task.delay) if task.delay > 0 else 0,
                signal_cap,
            ],
        ))

    async def save(self, task: Task) -> None:
        """Create or fully overwrite a task's canonical record."""
        await self.client.hset(key_task(task.id), mapping=task_to_hash(task))

    async def get(self, task_id: str):
        """Returns a task by id."""
        data = await self.client.hgetall(key_task(task_id))

        if not data:
            raise TaskNotFound(task_id)

        task = hash_to_task(data)
        return task

    async def exists(self, task_id: str) -> bool:
        """Return True if a record with this id already exists."""

        return bool(await self.client.exists(key_task(task_id)))

    async def get_many(self, task_ids: list[str]) -> list[Task]:
        """Pipeline task reads and skip IDs whose canonical record is absent."""
        if not task_ids:
            return []

        async with self.client.pipeline(transaction=False) as pipe:
            for task_id in task_ids:
                pipe.hgetall(key_task(task_id))

            results = await pipe.execute()

        return [hash_to_task(data) for data in results if data]
