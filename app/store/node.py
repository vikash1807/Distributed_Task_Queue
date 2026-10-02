# app/store/node.py

from __future__ import annotations

from contextlib import suppress

import redis.asyncio as redis

from app.model import Node
from app.store.redis import (
    KEY_NODES,
    node_dead_key,
    node_heartbeat_key,
    node_meta_key,
    node_tasks_key,
)


# NodeStore manages cluster memberships. Each Node is tracked by three keys:
# - taskqueue:nodes -> a SET of all known node IDs.
# - taskqueue:node:{id}:hb -> a string key with TTL, it's existence == node is
# alive.
# - taskqueue:node:{id}:tasks -> a SET of task ids the node currently leases.
#  A node is considered alive if it refreshes its hearbeat before TTL expires.
class NodeStore:
    def __init__(self, client: redis.Redis, heartbeat_ttl_ms: int) -> None:
        self.client = client
        self.ttl_ms = heartbeat_ttl_ms

    async def register(self, node: Node) -> None:
        """
        Register records a new node in the registry and writes its first
        heartbeat.
        """
        payload = node.model_dump_json()
        node_id = node.id

        async with self.client.pipeline(transaction=True) as pipe:
            pipe.sadd(KEY_NODES, node_id)
            pipe.set(node_heartbeat_key(node_id), payload, px=self.ttl_ms)

            # Persist a non-TTL descriptor so a dead node still resolves its
            # metadata, and clear any stale dead-tombstone so a re-registering
            # node returns to life.
            pipe.set(node_meta_key(node_id), payload)
            pipe.delete(node_dead_key(node_id))

            await pipe.execute()

    async def refresh_heartbeat(self, node: Node) -> None:
        """
        Refresh the node's TTL. A briefly stalled node can rejoin after its
        heartbeat key expires.
        """
        payload = node.model_dump_json()
        node_id = node.id

        async with self.client.pipeline(transaction=True) as pipe:
            pipe.sadd(KEY_NODES, node_id)
            pipe.set(node_heartbeat_key(node_id), payload, px=self.ttl_ms)

            # Refresh the descriptor and clear any dead-tombstone: a stalled
            # node whose heartbeat returns must go back to alive:true and stop
            # being pruned.
            pipe.set(node_meta_key(node_id), payload)
            pipe.delete(node_dead_key(node_id))

            await pipe.execute()

    async def deregister(self, node_id: str) -> None:
        """
        Removes a node cleanly on graceful shutdown. Drop it from registry, delete
        it's heartbeat and tasks set.
        """
        async with self.client.pipeline(transaction=False) as pipe:
            pipe.srem(KEY_NODES, node_id)
            pipe.delete(node_heartbeat_key(node_id))
            pipe.delete(node_meta_key(node_id))
            pipe.delete(node_tasks_key(node_id))
            pipe.delete(node_dead_key(node_id))

            await pipe.execute()

    async def get_registered_ids(self) -> list[str]:
        """Returns every node ID in the registry set (alive or not)."""
        return list(await self.client.smembers(KEY_NODES))

    async def is_alive(self, node_id: str) -> bool:
        """Checks whether a node's hearbeat still exists."""
        return (await self.client.exists(node_heartbeat_key(node_id))) > 0

    async def list_nodes(self) -> list[Node]:
        """
        Returns every registered node with liveness and in-flight task count.
        Dead nodes (hearbeat expired but still in the registry) are included
        with alive=false.
        """
        ids = await self.get_registered_ids()

        if not ids:
            return []

        async with self.client.pipeline(transaction=False) as pipe:
            for node_id in ids:
                pipe.get(node_heartbeat_key(node_id))
                pipe.get(node_meta_key(node_id))
                pipe.scard(node_tasks_key(node_id))

            result = await pipe.execute()

        nodes : list[Node] = []
        for i, node_id in enumerate(ids):
            hb, meta, count = result[i*3], result[i*3 + 1], result[i*3 + 2]

            if hb is None:
                # heartbeat expired - node is dead.
                node = Node(id=node_id)
                if meta is not None:
                    with suppress(ValueError, TypeError):
                        node = Node.model_validate_json(meta)
                node.alive = False
            else:
                try:
                    node = Node.model_validate_json(hb)
                except (ValueError, TypeError):
                    node = Node(id=node_id)
                node.alive = True

            node.in_flight_tasks = int(count or 0)
            nodes.append(node)

        return nodes

    async def get_dead_node_ids(self):
        """
        Returns registered node IDs whose hearbeat key has expired.
        """
        ids = await self.get_registered_ids()
        if not ids:
            return []

        async with self.client.pipeline(transaction=False) as pipe:
            for node_id in ids:
                pipe.exists(node_heartbeat_key(node_id))

            res = await pipe.execute()

        return [node_id for node_id, exists in zip(ids, res) if not exists]

    async def owned_task_ids(self, node_id: str) -> list[str]:
        """Returns the task IDs a node currently holds in flight."""
        return list(await self.client.smembers(node_tasks_key(node_id)))
