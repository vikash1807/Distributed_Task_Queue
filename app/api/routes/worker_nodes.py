from __future__ import annotations

import logging

from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, Response

from app.api.dependencies import get_worker_node_service
from app.service import WorkerNodeService

logger = logging.getLogger(__name__)


def error_response(status_code: int, message: str) -> JSONResponse:
    return JSONResponse(
        content={"error": message},
        status_code=status_code,
    )


def create_worker_node_routes() -> APIRouter:
    router = APIRouter(
        prefix="",
        tags=["worker, nodes"]
    )

    @router.get("/workers")
    async def get_workers(
        service: WorkerNodeService = Depends(get_worker_node_service),
    ) -> Response:
        try:
            workers = await service.get_workers()
        except Exception:
            logger.exception("failed to retrieve workers")
            return error_response(
                500,
                "failed to retrieve workers",
            )

        return JSONResponse(
            content=jsonable_encoder(
                [worker.to_dict() for worker in workers]
            ),
            status_code=200,
        )

    @router.get("/nodes")
    async def get_nodes(
        service: WorkerNodeService = Depends(get_worker_node_service),
    ) -> Response:
        try:
            nodes = await service.get_nodes()
        except Exception:
            logger.exception("failed to retrieve nodes")
            return error_response(
                500,
                "failed to retrieve nodes",
            )

        return JSONResponse(
            content=jsonable_encoder(
                [node.to_dict() for node in nodes]
            ),
            status_code=200,
        )

    return router