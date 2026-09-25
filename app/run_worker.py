# app/run_worker.py

from __future__ import annotations

import asyncio
import logging

from app.broker import RedisBroker
from app.core.config import load_config
from app.core.logging import setup_logging
from app.handler import create_registry
from app.queue import DelayedScheduler, PriorityQueue
from app.store import (
    DeadLetterStore,
    EventStore,
    MetricStore,
    NodeStore,
    TaskStore,
    WorkerStateStore,
    new_redis,
)
from app.worker import Executor, ExecutorDeps, Node, NodeConfig, Pool
from app.worker.node import create_node_id, get_hostname

setup_logging()
logger = logging.getLogger(__name__)


async def run() -> None:
    config = load_config()

    redis = new_redis(
        addr=config.redis_addr,
        password=config.redis_pass,
        worker_count=config.worker_count
    )

    try:
        # Fail fast if Redis is unavailable. There is no point starting
        # workers or the delayed scheduler without a working Redis connection.
        try:
            await redis.ping()
        except Exception:
            logger.exception("failed to connect to redis: %s", config.redis_addr)
            raise
        
        logger.info("connected to redis: %s", config.redis_addr)

        # Build application dependencies.
        event_store = EventStore(redis)
        task_store = TaskStore(redis)
        metric_store = MetricStore(redis)
        node_store = NodeStore(redis, config.heartbeat_ttl_ms)
        worker_state = WorkerStateStore(redis)

        task_queue = PriorityQueue(redis, task_store)

        delayed = DelayedScheduler(
            client=redis,
            queue=task_queue,
            event_store=event_store,
            task_store=task_store,
        )

        dead_letter = DeadLetterStore(redis)

        # One node ID represents this worker process.
        node_id = create_node_id()

        redis_broker = RedisBroker(
            client=redis,
            task_store=task_store,
            queue_ready=task_queue,
            visibility_timeout=config.visibility_timeout,
            node_id=node_id
        )

        executor = Executor(
            ExecutorDeps(
                broker=redis_broker,
                handlers=create_registry(),
                delayed=delayed,
                event_store=event_store,
                metric_store=metric_store,
                worker_state=worker_state,
                task_store=task_store,
                dead_letter=dead_letter,
                drain_timeout=config.drain_timeout
            )
        )

        pool = Pool(
            broker=redis_broker,
            executor=executor,
            worker_count=config.worker_count,
            poll_interval=config.poll_interval,
            worker_state=worker_state,
        )

        node = Node(
            node_config=NodeConfig(
                node_store=node_store,
                node_id=node_id,
                hostname=get_hostname(),
                role="worker",
                capacity=config.worker_count,
                heartbeat_interval=config.heartbeat_interval,
            ),
            event_store=event_store,
            pool=pool,
        )

        # Application-level shutdown signal.
        stop = asyncio.Event()

        try:
            await node.run(stop)

        except asyncio.CancelledError:
            logger.info("worker process cancellation requested")
            stop.set()
            raise

        except Exception:
            logger.exception("worker process failed")
            raise

    finally:
        await redis.aclose()
        logger.info("redis connection closed")


def main() -> None:
    """Run the worker application."""

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        # Ctrl+C is an expected way to stop the worker process.
        logger.info("worker process interrupted")

if __name__ == "__main__":
    main()