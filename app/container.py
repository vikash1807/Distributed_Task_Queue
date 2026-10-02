# app/container.py

from __future__ import annotations

from dataclasses import dataclass

import redis.asyncio as redis

from app.core.config import Config
from app.queue import DelayedScheduler, PriorityQueue
from app.reaper import Reaper
from app.service import (
    EventService,
    MetricService,
    TaskService,
    WorkerNodeService,
)
from app.store import (
    DeadLetterStore,
    EventStore,
    MetricStore,
    NodeStore,
    TaskStore,
    WorkerStateStore,
)


@dataclass
class AppContainer:
    redis: redis.Redis
    task_queue: PriorityQueue
    delayed_scheduler: DelayedScheduler
    dead_letter: DeadLetterStore
    event_store: EventStore
    metric_store: MetricStore
    node_store: NodeStore
    worker_state_store: WorkerStateStore
    task_store: TaskStore
    event_service: EventService
    metric_service: MetricService
    task_service: TaskService
    worker_node_service: WorkerNodeService
    reaper: Reaper


def build_container(client: redis.Redis, config: Config) -> AppContainer:
    event_store = EventStore(client)
    metric_store = MetricStore(client)
    node_store = NodeStore(client, config.heartbeat_ttl_ms)
    task_store = TaskStore(client)
    worker_state_store = WorkerStateStore(client)

    task_queue = PriorityQueue(client, task_store)
    delayed_scheduler = DelayedScheduler(
        client=client,
        queue=task_queue,
        task_store=task_store,
        event_store=event_store
    )
    dead_letter = DeadLetterStore(client)

    event_service = EventService(event_store)
    task_service = TaskService(
        task_queue=task_queue,
        delayed_queue=delayed_scheduler,
        dead_letter=dead_letter,
        event_store=event_store,
        metric_store=metric_store,
        task_store=task_store,
    )
    metric_service = MetricService(
        client=client,
        task_queue=task_queue,
        metric_store=metric_store
    )
    worker_node_service = WorkerNodeService(
        worker_state=worker_state_store,
        node_store=node_store
    )
    reaper = Reaper(
        client=client,
        node_store=node_store,
        event_store=event_store,
        interval=config.reaper_interval,
        dead_node_grace_ms=config.dead_node_grace_ms,
        signal_cap=config.signal_cap,
    )

    return AppContainer(
        redis=client,
        task_queue=task_queue,
        delayed_scheduler=delayed_scheduler,
        dead_letter=dead_letter,
        event_store=event_store,
        metric_store=metric_store,
        node_store=node_store,
        task_store=task_store,
        worker_state_store=worker_state_store,
        event_service=event_service,
        metric_service=metric_service,
        task_service=task_service,
        worker_node_service=worker_node_service,
        reaper=reaper,
    )
