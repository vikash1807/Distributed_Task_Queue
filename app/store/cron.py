"""Persistent cron definitions and atomic management operations."""

import json
import secrets
from datetime import UTC, datetime
from pathlib import Path

from redis.asyncio import Redis

from app.model.cron import CreateCron, CronJob
from app.store.redis import KEY_CRON_JOBS, key_cron


class DuplicateCronError(Exception):
    """A cron definition already uses this ID."""


class CronNotFound(Exception):
    """No cron definition exists for this ID."""


def decode_job(data: dict[str, str]) -> CronJob:
    """Restore typed fields and UTC timestamps from a Redis definition hash."""
    return CronJob(
        id=data["id"],
        schedule=data["schedule"],
        type=data["type"],
        payload=json.loads(data["payload"]),
        priority=int(data["priority"]),
        max_retries=int(data["max_retries"]),
        enabled=data["enabled"] == "1",
        created_at=datetime.fromtimestamp(int(data["created_unix"]), UTC),
        eligible_after_unix=int(data["eligible_after_unix"]),
        last_materialized_slot_unix=(
            int(data["last_materialized_slot_unix"])
            if data["last_materialized_slot_unix"]
            else None
        ),
        revision=data["revision"],
    )


class CronStore:
    """Persist cron definitions and their ID index with atomic Lua operations."""

    def __init__(self, client: Redis) -> None:
        """Bind Redis and register the definition-management script."""
        self.client = client
        self._manage = client.register_script(
            (Path(__file__).parent / "scripts" / "cron.lua").read_text()
        )

    async def create(self, request: CreateCron) -> CronJob:
        """Create a unique ID, revision, and server-time scheduling baseline."""
        cron_id = request.id or f"cron-{secrets.token_hex(12)}"
        data = {
            "id": cron_id,
            "schedule": request.schedule,
            "type": request.type,
            "payload": json.dumps(request.payload),
            "priority": str(request.priority),
            "max_retries": str(request.max_retries),
            "enabled": "1" if request.enabled else "0",
        }
        result = await self._manage(
            keys=[key_cron(cron_id), KEY_CRON_JOBS],
            # The creation revision changes even when a deleted ID is reused.
            args=["create", cron_id, json.dumps(data), secrets.token_hex(16)],
        )
        if not result:
            raise DuplicateCronError(cron_id)
        return decode_job(json.loads(result))

    async def list(self) -> list[CronJob]:
        """Read definitions in ID order and omit concurrently deleted records."""
        ids = sorted(await self.client.smembers(KEY_CRON_JOBS))
        if not ids:
            return []
        async with self.client.pipeline(transaction=False) as pipe:
            for cron_id in ids:
                pipe.hgetall(key_cron(cron_id))
            records = await pipe.execute()
        # Deletion can occur between reading the ID index and fetching its records.
        return [decode_job(record) for record in records if record]

    async def delete(self, cron_id: str) -> None:
        """Atomically remove a definition and its index membership."""
        result = await self._manage(
            keys=[key_cron(cron_id), KEY_CRON_JOBS],
            args=["delete", cron_id, "", ""],
        )
        if not result:
            raise CronNotFound(cron_id)

    async def set_enabled(self, cron_id: str, enabled: bool) -> CronJob:
        """Change enabled state and invalidate stale materializer snapshots."""
        result = await self._manage(
            keys=[key_cron(cron_id), KEY_CRON_JOBS],
            args=["enabled", cron_id, "1" if enabled else "0", secrets.token_hex(16)],
        )
        if not result:
            raise CronNotFound(cron_id)
        return decode_job(json.loads(result))
