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
    except:
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

        try:
            await self.node_cfg.node_store.register(node)

            logger.info(
                "node registered node_id=%s, host=%s, capacity=%d",
                self.node_cfg.node_id,
                self.node_cfg.hostname,
                self.node_cfg.capacity
            )

            # emit node_joined event
            await self._emit_joined_event()

        except Exception:
            logger.exception("node registration failed.")
        
        # heartbet runs independently of task execution so a busy node still refreshes.
        # it stop when `stop` is set.
        hb_task = asyncio.create_task(
            self._heartbeat_loop(node, stop)
        )
        
        try:
            await self.pool.start(self.node_cfg.node_id)
            logger.info(
                "node started node_id = %s, role = %s, capacity = %d",
                self.node_cfg.node_id,
                self.node_cfg.role,
                self.node_cfg.capacity
            )
            # Keep the process alive until it receives SIGINT/SIGTERM.
            await stop.wait()

        except asyncio.CancelledError:
            # Propagate cancellation after cleanup in finally blocks.
            logger.info("worker process cancellation requested")
            raise
        
        except Exception:
            logger.exception("worker process failed")
            raise

        finally:
            # stop workers before removing the node from cluster.
            await self.pool.stop()
        
            # wait for hb loop to observe cancellation before de-register.
            # so it can't re-create the hb key after deletion.
            await hb_task

            try:
                await self.node_cfg.node_store.deregister(self.node_cfg.node_id)
                logger.info("node de-registered node_id = %s", self.node_cfg.node_id)
            except Exception:
                logger.exception(
                    "node de-registeration failed node_id = %s",
                    self.node_cfg.node_id
                )
            await self.pool.stop()

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
