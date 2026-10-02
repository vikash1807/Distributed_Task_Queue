# app/worker/node.py

from __future__ import annotations

import asyncio
import logging
import secrets
import socket

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from app.model import (
    Node as NodeRecord,
    TaskEvent,
    TaskEventType,
)
from app.store import EventStore, NodeStore
from app.worker.pool import Pool


logger = logging.getLogger(__name__)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def get_hostname() -> str:
    """The OS hostname, or "node" if it can't be determined."""
    try:
        return socket.gethostname()
    except OSError:
        return "node"


def _short_uuid() -> str:
    """Generate a short random suffix for node identity."""
    return secrets.token_hex(6)


# create_node_id() returns a cluster-unique node identity of the form
# {hostname}-{shortuuid}. The hostname makes IDs human-readable in the
# dashboard/logs; the random suffix guarantees uniqueness across replicas that
# share a hostname (e.g. multiple containers, or restarts reusing a name).
def create_node_id() -> str:
    return f"{get_hostname()}-{_short_uuid()}"


@dataclass
class NodeConfig:
    node_store: NodeStore
    node_id: str
    hostname: str
    role: str
    capacity: int # pool worker count; 0 for a presence-only node
    heartbeat_interval: float # how often to refresh hb key


class Node:
    """
    Represents one worker process in cluster.
    A node:
    - registers itself in Redis,
    - maintains a TTL-based heartbeat,
    - emits a node_joined event,
    - deregisters on graceful shutdown.

    Pool executes tasks. A nil Pool makes a presence-only node (role "server")

    """
    def __init__(
        self,
        node_config: NodeConfig,
        event_store: Optional[EventStore],
        pool: Optional[Pool]
    ) -> None:
        self.node_cfg = node_config
        self.event_store = event_store
        self.pool = pool
    
    async def run(self, stop: asyncio.Event) -> None:
        """Register the node, run workers and heartbeat until shutdown."""
        role = self.node_cfg.role or "worker"
        node = NodeRecord(
            id=self.node_cfg.node_id,
            hostname=self.node_cfg.hostname,
            role=role,
            capacity=self.node_cfg.capacity,
            started_at=utc_now()
        )

        await self.node_cfg.node_store.register(node)
        logger.info("node registered node_id=%s", node.id)

        # Keep the heartbeat alive while in-flight work drains. If shutdown
        # fails, leave the node registered so the reaper can recover its leases.
        heartbeat_stop = asyncio.Event()
        hb_task = asyncio.create_task(self._heartbeat_loop(node, heartbeat_stop))
        stop_waiter = None
        worker_waiter = None
        graceful = False
        drained = False

        try:
            await self._emit_joined_event()
            await self.pool.start(node.id)
            logger.info("node started node_id=%s capacity=%d", node.id, node.capacity)

            stop_waiter = asyncio.create_task(stop.wait())
            worker_waiter = asyncio.create_task(self.pool.wait_for_unexpected_exit())
            done, _ = await asyncio.wait(
                (stop_waiter, worker_waiter, hb_task),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if stop_waiter in done:
                graceful = True
            else:
                for task in done:
                    if task.cancelled():
                        raise RuntimeError("worker or heartbeat cancelled unexpectedly")
                    task.result()
                raise RuntimeError("worker heartbeat stopped unexpectedly")

        except asyncio.CancelledError:
            logger.info("worker node cancellation requested node_id=%s", node.id)
            graceful = True
            raise

        finally:
            stop.set()
            for task in (stop_waiter, worker_waiter):
                if task is not None:
                    task.cancel()
            await asyncio.gather(
                *(task for task in (stop_waiter, worker_waiter) if task is not None),
                return_exceptions=True,
            )

            try:
                await self.pool.stop()
                drained = True
            finally:
                heartbeat_stop.set()
                heartbeat_result = await asyncio.gather(hb_task, return_exceptions=True)
                if isinstance(heartbeat_result[0], BaseException):
                    logger.error("heartbeat stopped with error: %s", heartbeat_result[0])

                if graceful and drained:
                    try:
                        await self.node_cfg.node_store.deregister(node.id)
                        logger.info("node deregistered node_id=%s", node.id)
                    except Exception:
                        logger.exception("node deregistration failed node_id=%s", node.id)

    async def _heartbeat_loop(self, node: NodeRecord, stop: asyncio.Event) -> None:
        """Refresh the node heartbeat until shutdown."""
        while not stop.is_set():
            try:
                await asyncio.wait_for(
                    stop.wait(),
                    timeout=self.node_cfg.heartbeat_interval
                )
                return
            except asyncio.TimeoutError:
                pass # interval elapsed - time to beat
            
            try:
                await self.node_cfg.node_store.refresh_heartbeat(node)
            except Exception:
                logger.exception("node heartbeat failed node_id=%s", node.id)
    
    async def _emit_joined_event(self) -> None:
        """Emit a node-joined cluster event."""

        event = TaskEvent(
            id=f"evt-{secrets.token_hex(12)}",
            type=TaskEventType.NODE_JOINED,
            task_id="",
            worker_id="-1",
            detail=(
                f"Node {self.node_cfg.node_id} joined \
                (host={self.node_cfg.hostname}, \
                capacity={self.node_cfg.capacity})"
            ),
            timestamp=utc_now(),
        )

        try:
            await self.event_store.push(event)
        except Exception:
            # Event failure should not prevent the worker from running.
            logger.exception(
                "failed to emit node_joined event node_id=%s",
                self.node_cfg.node_id,
            )
