"""Readiness check for the API and its Redis dependency."""

from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)


def create_health_routes() -> APIRouter:
    """Register readiness checks for Redis and background supervision."""
    router = APIRouter(tags=["health"])

    @router.get("/health")
    async def health(request: Request) -> JSONResponse:
        """Report unavailable dependencies or a failed leader supervisor."""
        container = getattr(request.app.state, "container", None)
        if container is None:
            return JSONResponse(
                {"status": "unhealthy", "redis": "unavailable"},
                status_code=503,
            )

        supervisor = getattr(container, "leader", None)
        if supervisor is not None and supervisor.failed:
            return JSONResponse(
                {"status": "unhealthy", "background_services": "failed"},
                status_code=503,
            )

        try:
            if await asyncio.wait_for(container.redis.ping(), timeout=1.0):
                return JSONResponse({"status": "healthy", "redis": "connected"})
        except Exception:
            logger.warning("health check could not reach Redis", exc_info=True)

        return JSONResponse(
            {"status": "unhealthy", "redis": "unavailable"},
            status_code=503,
        )

    return router
