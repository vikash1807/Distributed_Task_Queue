# app/api/dependencies.py

from __future__ import annotations

from fastapi import Request

from app.container import AppContainer
from app.service import (
    TaskService,
    MetricService,
    EventService,
    WorkerNodeService,
) 


def get_container(request: Request) -> AppContainer:
    container = getattr(request.app.state, "container", None)

    if container is None:
        raise RuntimeError("application container is not initialized")

    return container


def get_task_service(request: Request) -> TaskService:
    return get_container(request).task_service

def get_metrics_service(request: Request) -> MetricService:
    return get_container(request).metric_service

def get_event_service(request: Request) -> EventService:
    return get_container(request).event_service

def get_worker_node_service(request: Request) -> EventService:
    return get_container(request).worker_node_service
