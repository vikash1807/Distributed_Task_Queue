"""Periodic recovery of expired leases and tasks from dead worker nodes."""

from __future__ import annotations

import asyncio
import logging
import secrets
import time
from datetime import UTC, datetime
from pathlib import Path

import redis.asyncio as redis

from app.core.leadership import LeaderLease, LeadershipLost
from app.model import TaskEvent, TaskEventType
from app.store import (
    KEY_DEADLETTER,
    KEY_METRICS,
    KEY_NODES,
    KEY_PROCESSING,
    KEY_READY,
    KEY_READY_SIGNAL,
    EventStore,
    NodeStore,
    key_task,
    node_dead_key,
    node_heartbeat_key,
    node_meta_key,
    node_tasks_key,
)
from app.store.redis import KEY_LEADER

logger = logging.getLogger(__name__)
SCRIPTS_DIR = Path(__file__).parent / "scripts"
BATCH_SIZE = 100


class Reaper:
    """Recover abandoned tasks while preserving lease and node race guards."""

    def __init__(
        self,
        client: redis.Redis,
        node_store: NodeStore,
        event_store: EventStore,
        interval: float,
        dead_node_grace_ms: int,
        signal_cap: int,
    ) -> None:
        """Bind recovery stores, scan timings, and atomic Redis transitions."""
        self.client = client
        self.node_store = node_store
        self.event_store = event_store
        self.interval = interval
        self.dead_node_grace_ms = dead_node_grace_ms
        self.signal_cap = signal_cap
        self._reclaim = client.register_script(
            (SCRIPTS_DIR / "reclaim.lua").read_text(encoding="utf-8")
        )
        self._mark_dead = client.register_script(
            (SCRIPTS_DIR / "mark_dead.lua").read_text(encoding="utf-8")
        )
        self._prune = client.register_script(
            (SCRIPTS_DIR / "prune.lua").read_text(encoding="utf-8")
        )

    async def run(self, lease: LeaderLease | None = None) -> None:
        """Repeat recovery sweeps until cancelled or leadership is lost."""
        logger.info("reaper started interval=%ss", self.interval)
        try:
            while True:
                try:
                    await self.sweep(lease)
                except (asyncio.CancelledError, LeadershipLost):
                    raise
                except Exception:
                    logger.exception("reaper sweep failed")
                await asyncio.sleep(self.interval)
        except (asyncio.CancelledError, LeadershipLost):
            logger.info("reaper stopped")
            raise

    async def sweep(self, lease: LeaderLease | None = None) -> None:
        """Recover dead-node work first, then expired leases."""
        now_ms = int(time.time() * 1000)
        await self._sweep_dead_nodes(now_ms, lease)
        await self._sweep_expired(now_ms, lease)

    async def _sweep_dead_nodes(self, now_ms: int, lease: LeaderLease | None = None) -> None:
        """Mark dead nodes, reclaim their work, then prune after the grace period."""
        for node_id in await self.node_store.get_dead_node_ids():
            try:
                mark = int(
                    await self._transition(
                        self._mark_dead,
                        lease,
                        keys=[KEY_NODES, node_heartbeat_key(node_id), node_dead_key(node_id)],
                        args=[node_id, now_ms],
                    )
                )
                if mark == 0:
                    continue
                if mark == 1:
                    await self._emit(TaskEventType.NODE_DEAD, "", f"Node {node_id} died")

                # Repeat unfinished work after an interrupted sweep. ZREM makes
                # this safe when several API instances run reapers concurrently.
                for task_id in await self.node_store.owned_task_ids(node_id):
                    try:
                        await self._reclaim_one(task_id, now_ms, node_id, lease)
                    except (asyncio.CancelledError, LeadershipLost):
                        raise
                    except Exception:
                        logger.exception(
                            "dead-node reclaim failed node_id=%s task_id=%s",
                            node_id,
                            task_id,
                        )

                await self._transition(
                    self._prune,
                    lease,
                    keys=[
                        KEY_NODES,
                        node_heartbeat_key(node_id),
                        node_meta_key(node_id),
                        node_tasks_key(node_id),
                        node_dead_key(node_id),
                    ],
                    args=[node_id, now_ms - self.dead_node_grace_ms],
                )
            except (asyncio.CancelledError, LeadershipLost):
                raise
            except Exception:
                logger.exception("dead-node sweep failed node_id=%s", node_id)

    async def _sweep_expired(self, now_ms: int, lease: LeaderLease | None = None) -> None:
        """Read a bounded batch of expired leases and attempt guarded recovery."""
        ids = await self.client.zrangebyscore(
            KEY_PROCESSING, "-inf", now_ms, start=0, num=BATCH_SIZE
        )
        logger.info("reaper: found expired leases to reclaim count = %d", len(ids))

        for task_id in ids:
            try:
                await self._reclaim_one(task_id, now_ms, "", lease)
            except (asyncio.CancelledError, LeadershipLost):
                raise
            except Exception:
                logger.exception("expired-lease reclaim failed task_id=%s", task_id)

    async def _reclaim_one(
        self,
        task_id: str,
        now_ms: int,
        dead_node_id: str,
        lease: LeaderLease | None = None,
    ) -> None:
        """Recover one lease atomically and report the resulting transition."""
        outcome = int(
            await self._transition(
                self._reclaim,
                lease,
                keys=[
                    KEY_PROCESSING,
                    KEY_READY,
                    key_task(task_id),
                    KEY_READY_SIGNAL,
                    KEY_DEADLETTER,
                    KEY_METRICS,
                    node_heartbeat_key(dead_node_id) if dead_node_id else KEY_PROCESSING,
                ],
                args=[
                    task_id,
                    now_ms,
                    dead_node_id,
                    self.signal_cap,
                    datetime.now(UTC).isoformat(),
                ],
            )
        )
        if outcome == 1:
            await self._emit(TaskEventType.RECLAIMED, task_id, "Task returned to ready queue")
        elif outcome == 2:
            await self._emit(TaskEventType.RECLAIMED, task_id, "Task moved to dead-letter queue")
        elif outcome == 3:
            logger.warning("removed orphaned processing lease task_id=%s", task_id)

    async def _transition(self, script, lease: LeaderLease | None, *, keys, args) -> int:
        """Check local ownership and pass its token to the Redis race guard."""
        # The optional context preserves direct maintenance calls; runtime supplies a lease.
        if lease is not None:
            lease.check()
        result = int(
            await script(
                keys=[*keys, KEY_LEADER],
                args=[*args, *([lease.value] if lease is not None else [])],
            )
        )
        if result == -1:
            # Propagate ownership loss rather than treating it as a recoverable per-task error.
            raise LeadershipLost("reaper transition rejected")
        return result

    async def _emit(self, event_type: TaskEventType, task_id: str, detail: str) -> None:
        """Publish recovery events without letting monitoring errors undo recovery."""
        event = TaskEvent(
            id=f"evt-{secrets.token_hex(12)}",
            task_id=task_id,
            type=event_type,
            worker_id="-1",
            detail=detail,
            timestamp=datetime.now(UTC),
        )
        try:
            await self.event_store.push(event)
        except (asyncio.CancelledError, LeadershipLost):
            raise
        except Exception:
            logger.exception("failed to emit reaper event type=%s task_id=%s", event_type, task_id)
