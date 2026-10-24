"""Leader fencing and cron materialization on an isolated, disposable Redis DB."""

import asyncio
import os
import time
import pytest
from redis.asyncio import Redis
from redis.exceptions import ResponseError
from app.container import build_container
from app.core.config import Config
from app.service.task import DuplicateTaskError
from app.store.leader import LeaderStore
from app.store.redis import KEY_DELAYED, KEY_EVENTS, KEY_LEADER, KEY_METRICS, KEY_READY, key_task

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    """Run async tests on asyncio, matching the application runtime."""
    return "asyncio"


@pytest.fixture
async def redis():
    """Flush only the explicitly supplied disposable database around each test."""
    url = os.getenv("LEADER_CRON_TEST_REDIS_URL")
    if not url:
        pytest.skip("requires LEADER_CRON_TEST_REDIS_URL for a disposable Redis DB")
    client = Redis.from_url(url, decode_responses=True)
    try:
        await client.flushdb()
        yield client
    finally:
        await client.flushdb()
        await client.aclose()


async def test_owner_only_renew_release_and_expiry(redis):
    """Verify lease exclusion, expiry, and protection against an old owner."""
    store = LeaderStore(redis, 300)
    assert await store.acquire("first")
    assert not await store.acquire("second")
    assert not await store.renew("second")
    assert not await store.release("second")
    assert await store.renew("first")
    assert 0 < await redis.pttl(KEY_LEADER) <= 300
    await asyncio.sleep(0.31)
    assert await store.acquire("second")
    assert not await store.release("first")
    assert not await store.renew("first")
    assert await store.current() == "second"
    assert await store.release("second")
    assert not await store.release("second")


async def test_atomic_ordinary_submission_and_preflight(redis):
    """Check duplicate exclusion, delayed indexing, exact integers, and failed preflight."""
    container = build_container(redis, Config())

    async def submit():
        """Submit a shared ID and classify duplicate rejections without aborting the race."""
        try:
            return await container.task_service.submit_task(id="same", type="hash")
        except DuplicateTaskError:
            return None

    results = await asyncio.gather(*(submit() for _ in range(8)))
    assert sum(result is not None for result in results) == 1
    assert await redis.hget(KEY_METRICS, "submitted") == "1"
    assert await redis.llen(KEY_EVENTS) == 1
    delayed = await container.task_service.submit_task(id="later", type="hash", delay=10)
    assert await redis.zscore(KEY_DELAYED, delayed.id) > time.time()
    assert await redis.zscore(KEY_READY, delayed.id) is None
    large_priority = 2**53 + 1
    exact = await container.task_service.submit_task(
        id="exact-priority",
        type="hash",
        priority=large_priority,
    )
    assert (await container.task_store.get(exact.id)).priority == large_priority
    await redis.hset(KEY_METRICS, "submitted", "not-an-integer")
    with pytest.raises(ResponseError):
        await container.task_service.submit_task(id="invalid-counter", type="hash")
    assert not await redis.exists(key_task("invalid-counter"))
