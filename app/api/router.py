# app/api/router.py

from __future__ import annotations


from fastapi import APIRouter, FastAPI

from app.api.middleware import (
    CorsMiddleware,
    ExceptionRecoveryMiddleware,
    RequestLoggingMiddleware,
)
from app.api.routes.task import create_task_routes
from app.api.routes.metrics import create_metrics_routes
from app.api.routes.events import create_event_routes
from app.api.routes.health import create_health_routes
from app.api.routes.worker_nodes import create_worker_node_routes


def create_api_router() -> APIRouter:
    api_router = APIRouter(prefix="/api")
    api_router.include_router(create_health_routes())
    api_router.include_router(create_task_routes())
    api_router.include_router(create_metrics_routes())
    api_router.include_router(create_event_routes())
    api_router.include_router(create_worker_node_routes())

    return api_router


def configure_api(application: FastAPI) -> None:
    """
    Configure API routes and HTTP middleware for the application.
    """

    application.include_router(create_api_router())

    application.add_middleware(CorsMiddleware)
    application.add_middleware(RequestLoggingMiddleware)
    application.add_middleware(ExceptionRecoveryMiddleware)
