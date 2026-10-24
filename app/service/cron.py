"""Cron management and leader-owned latest-slot materialization."""

import asyncio
import logging
import secrets
from datetime import UTC, datetime

from croniter import croniter

from app.core.leadership import LeaderLease, LeadershipLost
from app.model import Task, TaskEvent, TaskEventType
from app.model.cron import CreateCron, CronJob
from app.store.cron import CronStore
from app.store.task import TaskStore

logger = logging.getLogger(__name__)


def latest_slot(schedule: str, now: int) -> int:
    """Find the latest matching UTC slot at or before the supplied Unix second."""
    # get_prev is exclusive. +1 includes an exact matching minute boundary.
    return int(
        croniter(
            schedule,
            datetime.fromtimestamp(now + 1, UTC),
            max_years_between_matches=8,
        ).get_prev(float)
    )


class CronService:
    """Expose cron management operations to any API instance."""

    def __init__(self, store: CronStore) -> None:
        """Bind the persistent definition store used by HTTP operations."""
        self.store = store

    async def create(self, request: CreateCron) -> CronJob:
        """Create a job with its initial scheduling baseline."""
        return await self.store.create(request)

    async def list(self) -> list[CronJob]:
        """List persisted jobs, including paused definitions."""
        return await self.store.list()

    async def delete(self, cron_id: str) -> None:
        """Remove future scheduling without deleting materialized tasks."""
        await self.store.delete(cron_id)

    async def set_enabled(self, cron_id: str, enabled: bool) -> CronJob:
        """Pause a job or resume it from a fresh future-only baseline."""
        return await self.store.set_enabled(cron_id, enabled)


class CronMaterializer:
    """Submit only the latest due instance for each enabled cron definition."""

    def __init__(
        self,
        store: CronStore,
        tasks: TaskStore,
        *,
        tick: float,
        signal_cap: int,
    ) -> None:
        """Bind definition/task stores and the leader scan cadence."""
        self.store = store
        self.tasks = tasks
        self.tick = tick
        self.signal_cap = signal_cap

    async def materialize(self, job: CronJob, now: int, lease: LeaderLease) -> int | None:
        """Commit a deterministic task and its cursor under ownership guards."""
        lease.check()
        if not job.enabled:
            return None
        slot = latest_slot(job.schedule, now)
        # Jump straight to the latest slot; never replay every missed occurrence.
        if slot <= max(job.eligible_after_unix, job.last_materialized_slot_unix or 0):
            return None
        created_at = datetime.fromtimestamp(now, UTC)
        # The same job/slot always produces the same ID, including after failover.
        task = Task(
            id=f"{job.id}-{slot}",
            type=job.type,
            payload=job.payload,
            priority=job.priority,
            max_retries=job.max_retries,
            created_at=created_at,
        )
        event = TaskEvent(
            id=f"evt-{secrets.token_hex(12)}",
            task_id=task.id,
            type=TaskEventType.SUBMITTED,
            worker_id="-1",
            detail=f"Cron={job.id}, Slot={slot}",
            timestamp=created_at,
        )
        # Lua rechecks ownership/revision and commits task creation with cursor advancement.
        return await self.tasks.submit(
            task,
            event,
            self.signal_cap,
            lease=lease,
            cron_id=job.id,
            revision=job.revision,
            cursor=job.last_materialized_slot_unix,
            slot=slot,
        )

    async def tick_once(self, lease: LeaderLease) -> None:
        """Use one Redis timestamp to scan the current job snapshots."""
        lease.check()
        now = int((await self.store.client.time())[0])
        # Every job in this scan uses Redis time rather than the API host's wall clock.
        for job in await self.store.list():
            try:
                await self.materialize(job, now, lease)
            except (asyncio.CancelledError, LeadershipLost):
                raise
            except Exception:
                logger.exception("cron materialization failed cron_id=%s", job.id)

    async def run(self, lease: LeaderLease) -> None:
        """Repeat scans, propagating cancellation and leadership loss."""
        while True:
            try:
                await self.tick_once(lease)
            except (asyncio.CancelledError, LeadershipLost):
                raise
            except Exception:
                logger.exception("cron scan failed")
            await asyncio.sleep(self.tick)
