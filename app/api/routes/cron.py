"""Cron management endpoints with their explicit 400/503 contract."""

import logging

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from redis.exceptions import RedisError

from app.model.cron import CreateCron, CronJob, UpdateCron
from app.service.cron import CronService
from app.store.cron import CronNotFound, DuplicateCronError

logger = logging.getLogger(__name__)


class CronRoute(APIRoute):
    """Translate cron validation and storage failures into API responses."""

    def get_route_handler(self):
        """Wrap FastAPI handling with the cron-specific error contract."""
        handler = super().get_route_handler()

        async def handle(request: Request):
            """Run the endpoint and map expected failures to HTTP status codes."""
            try:
                return await handler(request)
            except RequestValidationError:
                return JSONResponse({"error": "invalid cron input"}, status_code=400)
            except DuplicateCronError:
                return JSONResponse({"error": "cron ID already exists"}, status_code=409)
            except CronNotFound:
                return JSONResponse({"error": "cron job not found"}, status_code=404)
            except (RedisError, TimeoutError, ValueError, KeyError, TypeError):
                logger.exception("cron operation unavailable")
                return JSONResponse({"error": "cron service unavailable"}, status_code=503)

        return handle


def get_cron_service(request: Request) -> CronService:
    """Resolve cron operations from the initialized application container."""
    container = getattr(request.app.state, "container", None)
    if container is None:
        raise HTTPException(503, "cron service unavailable")
    return container.cron_service


def create_cron_routes() -> APIRouter:
    """Register cron creation, listing, deletion, and enabled-state routes."""
    router = APIRouter(prefix="/cron", tags=["cron"], route_class=CronRoute)

    @router.post("", status_code=201, response_model=CronJob, responses={400: {}, 409: {}, 503: {}})
    async def create(request: CreateCron, service: CronService = Depends(get_cron_service)):
        """Persist a validated job and return it with HTTP 201."""
        job = await service.create(request)
        return job

    @router.get("", response_model=list[CronJob], responses={503: {}})
    async def list_jobs(service: CronService = Depends(get_cron_service)):
        """Return stored cron definitions in their stable ID order."""
        return await service.list()

    @router.delete("/{cron_id}", status_code=204, responses={404: {}, 503: {}})
    async def delete(cron_id: str, service: CronService = Depends(get_cron_service)):
        """Delete future scheduling for a job while retaining its tasks."""
        await service.delete(cron_id)
        return Response(status_code=204)

    @router.patch("/{cron_id}", response_model=CronJob, responses={400: {}, 404: {}, 503: {}})
    async def update(
        cron_id: str,
        request: UpdateCron,
        service: CronService = Depends(get_cron_service),
    ):
        """Pause or resume an existing job without changing its schedule."""
        job = await service.set_enabled(cron_id, request.enabled)
        return job

    return router
