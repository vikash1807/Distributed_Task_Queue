# app/main.py
import logging
import sys
from contextlib import asynccontextmanager

import asyncio
import uvicorn
from fastapi import FastAPI

from app.api.router import configure_api
from app.container import build_container
from app.core import load_config, setup_logging
from app.store import new_redis


setup_logging()
logger = logging.getLogger(__name__)


def build_config():
    try:
        cfg = load_config()
    except Exception as exc:
        logger.error("config error: %s", exc)
        sys.exit(1)

    return cfg


_cfg = build_config()

schedule_task = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    redis = new_redis(
        addr=_cfg.redis_addr,
        password=_cfg.redis_pass,
        worker_count=_cfg.worker_count,
    )

    try:
        await redis.ping()
        logger.info("connected to redis: %s", _cfg.redis_addr)

        app.state.container = build_container(redis, _cfg)

        scheduler = app.state.container.delayed_scheduler
        schedule_task = asyncio.create_task(scheduler.run())

        yield

    except Exception as exc:
        logger.error("redis connection error: %s", exc)
    
    finally:

        if schedule_task is not None:
            schedule_task.cancel()
            try:
                await schedule_task
            except asyncio.CancelledError:
                pass

        await redis.aclose()
        logger.info("redis connection closed")


app = FastAPI(lifespan=lifespan)

configure_api(app)


def main() -> None:
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=_cfg.server_port,
        log_config=None,
        access_log=False
    )


if __name__ == "__main__":
    main()
