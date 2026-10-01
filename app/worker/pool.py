# app/worker/pool.py

"""Async worker pool for claiming and executing tasks."""

from __future__ import annotations

import asyncio
import logging

from app.broker import RedisBroker
from app.core import settings as config
from app.model import WorkerState
from app.store import WorkerStateStore
from app.worker.executor import Executor

logger = logging.getLogger(__name__)


class Pool:
    def __init__(
        self,
        broker: RedisBroker,
        executor: Executor,
        worker_count: int,
        poll_interval: float,
        worker_state: WorkerStateStore
    ) -> None:
        
        self.broker = broker
        self.executor = executor
        self.worker_count = worker_count
        self.poll_interval = poll_interval

        self.worker_state = worker_state

        self._stop = asyncio.Event()
        self._workers: list[asyncio.Task] = []
    
    async def start(self, node_id: str) -> None:
        """Launch the worker tasks. They run until ``stop`` is called."""
        self._stop.clear()

        for i in range(self.worker_count):

            # Create unique worker ids
            worker_id = f"{node_id}:{i+1}"
            
            try:
                await self.worker_state.set(
                    WorkerState(
                        id = worker_id,
                        status = "idle",
                    )
                )
            except Exception as exc:
                pass

            self._workers.append(
                asyncio.create_task(self._worker(worker_id))
            )

        logger.info(
            "worker pool started workers=%d",
            self.worker_count,
        )
    
    async def stop(self) -> None:
        """Signal shutdown and wait for all workers to finish in-flight work."""

        self._stop.set()
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)

        self._workers.clear()
        logger.info("worker pool stopped")
    
    async def _worker(self, worker_id: int) -> None:
        """Claim and execute tasks until shutdown.
        Workers use the ready ZSET as the source of truth. When the queue is
        empty, they block on the Redis doorbell instead of continuously polling.
        The doorbell only wakes a worker; after waking, the worker checks the
        ready queue again.
        """

        logger.info("worker started worker=%s", worker_id)

        while not self._stop.is_set():
            try:
                task = await self.broker.dequeue()

                if task is None:
                    # No task is currently ready. Wait for a doorbell signal
                    # The timeout is bounded by signal_block so the worker can
                    # periodically check _stop and exit during shutdown.
                    await self.broker.wait_for_work(config.signal_block)
                    continue

                # A task was successfully claimed; execute it. 
                await self.executor.execute(task, worker_id)
            
            except asyncio.CancelledError:
                # Worker cancellation is expected during shutdown.
                break

            except Exception:
                # A broker/worker error should not terminate the worker.
                # Wait briefly before trying again to avoid a tight error loop.

                logger.exception("worker error worker_id=%d", worker_id)
                await asyncio.sleep(self.poll_interval)
        
        logger.info("worker stopped worker_id = %s", worker_id)
