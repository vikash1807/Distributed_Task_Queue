"""Inspect the authoritative lease and local leadership state."""

import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)


def create_leader_routes() -> APIRouter:
    """Register the endpoint for inspecting current leader ownership."""
    router = APIRouter(tags=["leader"])

    @router.get("/leader", responses={503: {}})
    async def leader(request: Request):
        """Return Redis ownership and whether this API holds a valid lease."""
        container = getattr(request.app.state, "container", None)
        if container is None:
            return JSONResponse({"error": "leader service unavailable"}, status_code=503)
        try:
            return await container.leader.status()
        except (RedisError, TimeoutError, ValueError, KeyError, TypeError):
            logger.exception("leader query unavailable")
            return JSONResponse({"error": "leader service unavailable"}, status_code=503)

    return router
