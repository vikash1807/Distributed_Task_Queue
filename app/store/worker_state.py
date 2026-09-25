from __future__ import annotations

import json

import redis.asyncio as redis

from app.store.redis import KEY_WORKERS
from app.model import WorkerState

class WorkerStateStore:
    def __init__(self, client: redis.Redis):
        self.client = client

    async def set(self, state: WorkerState) -> None:
        """Updates a worker's state in the hash."""
        await self.client.hset(
            KEY_WORKERS,
            state.id,
            json.dumps(state.to_dict())
        )
    
    async def get_all(self) -> list[WorkerState]:
        """Returns all worker states."""

        data = await self.client.hgetall(KEY_WORKERS)

        states : list[WorkerState] = []

        for value in data.values():
            try:
                states.append(
                    WorkerState.model_validate_json(value)
                )
            except (ValueError, TypeError):
                continue

        return states
        


        