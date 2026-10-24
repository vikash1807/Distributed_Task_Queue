# app/api/middleware.py

from __future__ import annotations

import logging
import time

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import PlainTextResponse, Response

logger = logging.getLogger(__name__)


class CorsMiddleware(BaseHTTPMiddleware):
    """Handles CORS headers and browser prefilght requests."""

    async def dispatch(self, request: Request, call_next):
        """Handle preflight requests and attach the API CORS headers."""
        if request.method == "OPTIONS":
            response = Response(status_code=200)
        else:
            response = await call_next(request)

        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, DELETE, PATCH, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type"

        return response


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Log each request - method, path, status and duration."""

    async def dispatch(self, request: Request, call_next):
        """Log the response status and elapsed time for each request."""
        start_time = time.perf_counter()

        response = await call_next(request)

        elapsed = time.perf_counter() - start_time

        logger.info(
            "request method=%s path=%s status=%d duration=%.3fs",
            request.method,
            request.url.path,
            response.status_code,
            elapsed,
        )

        return response


class ExceptionRecoveryMiddleware(BaseHTTPMiddleware):
    """Catch unexpected application errors and convert them into HTTP 500 response."""

    async def dispatch(self, request: Request, call_next):
        """Log uncaught request errors and return an HTTP 500 response."""
        try:
            return await call_next(request)

        except Exception:
            logger.exception(
                "unhandled exception method=%s path=%s",
                request.method,
                request.url.path,
            )

            return PlainTextResponse(
                "internal server error",
                status_code=500,
            )
