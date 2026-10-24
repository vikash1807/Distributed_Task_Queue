# app/main.py
import asyncio
import logging
import sys
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from app.api.router import configure_api
from app.container import build_container
from app.core import load_config, setup_logging
from app.store import new_redis

setup_logging()
logger = logging.getLogger(__name__)


def build_config():
    """Load process settings and terminate startup on invalid configuration."""
    try:
        cfg = load_config()
    except Exception as exc:
        logger.error("config error: %s", exc)
        sys.exit(1)

    return cfg


_cfg = build_config()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start supervision and finish its cleanup before closing Redis."""
    leader_task = None
    redis = new_redis(
        addr=_cfg.redis_addr,
        password=_cfg.redis_pass,
        worker_count=_cfg.worker_count,
    )

    try:
        await redis.ping()
        logger.info("connected to redis: %s", _cfg.redis_addr)

        app.state.container = build_container(redis, _cfg)

        supervisor = app.state.container.leader
        leader_task = asyncio.create_task(supervisor.run(), name="leader-supervisor")

        # Observe unexpected failure immediately; health also exposes failed state.
        def observe_failure(task):
            """Retrieve and log unexpected supervisor task failures promptly."""
            if not task.cancelled() and task.exception() is not None:
                logger.error("leader supervisor stopped: %s", task.exception())

        leader_task.add_done_callback(observe_failure)
        await supervisor.started.wait()

        yield

    except Exception as exc:
        logger.exception("application startup or runtime failed: %s", exc)
        raise

    finally:
        # The supervisor joins its services and attempts release while Redis is still open.
        tasks = [leader_task] if leader_task is not None else []
        for task in tasks:
            task.cancel()
        try:
            for result in await asyncio.gather(*tasks, return_exceptions=True):
                if isinstance(result, BaseException) and not isinstance(
                    result, asyncio.CancelledError
                ):
                    logger.error("background service stopped with error: %s", result)
        finally:
            await redis.aclose()
            logger.info("redis connection closed")


app = FastAPI(lifespan=lifespan)

configure_api(app)


def main() -> None:
    """Start the HTTP server with the application lifecycle attached."""
    uvicorn.run(app, host="0.0.0.0", port=_cfg.server_port, log_config=None, access_log=False)


if __name__ == "__main__":
    main()
