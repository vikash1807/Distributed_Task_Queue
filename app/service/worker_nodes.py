# app/service/worker_nodes.py

from __future__ import annotations

from app.store import NodeStore, WorkerStateStore

class WorkerNodeService:
    """Service for retrieving worker and cluster-node state."""

    def __init__(
        self,
        worker_state : WorkerStateStore,
        node_store : NodeStore,
    ) -> None:
        self.worker_state = worker_state
        self.node_store = node_store

    async def get_workers(self):
        """Return the current state of all local/discovered workers."""
        return await self.worker_state.get_all()

    async def get_nodes(self):
        """Return hydrated cluster node information."""
        return await self.node_store.list_nodes()