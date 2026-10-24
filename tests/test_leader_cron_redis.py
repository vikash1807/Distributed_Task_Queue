"""Leader fencing and cron materialization on an isolated, disposable Redis DB."""

import asyncio
import os
import time
from unittest.mock import AsyncMock
import pytest
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError
from app.container import build_container
from app.core.config import Config
from app.leader import LeaderSupervisor
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


async def wait_until(predicate, timeout=3):
    """Poll an async condition under a bounded timeout."""
    async with asyncio.timeout(timeout):
        while not await predicate():
            await asyncio.sleep(0.01)


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


async def test_two_supervisors_failover_and_same_process_new_tenure(redis):
    """Transfer ownership after isolation and reject tokens from earlier tenures."""
    store = LeaderStore(redis, 300)
    active = set()
    seen = set()

    def service(name):
        """Build a named singleton to track exclusive activity and cleanup."""

        async def run(lease):
            """Track the active owner and clear it when the lease is invalidated."""
            assert len(active) == 0
            active.add(name)
            seen.add(name)
            try:
                await asyncio.Future()
            finally:
                assert not lease.valid
                active.remove(name)

        return run

    first = LeaderSupervisor(store, [service("first")], identity="first")
    second = LeaderSupervisor(store, [service("second")], identity="second")
    task1 = asyncio.create_task(first.run())
    task2 = asyncio.create_task(second.run())
    try:

        async def leader_exists():
            """Wait until one contender has started its singleton work."""
            return len(active) == 1

        await wait_until(leader_exists)
        winner, winner_task = (first, task1) if first.lease else (second, task2)
        loser = second if winner is first else first
        old_value = winner.lease.value
        assert (await winner.status())["is_self"]
        assert not (await loser.status())["is_self"]
        # Force renew failure while release is unavailable, simulating isolation.
        real_store = winner.store
        winner.store = AsyncMock(ttl_ms=300)
        winner.store.renew.side_effect = RedisConnectionError("partition")
        winner.store.release.side_effect = RedisConnectionError("partition")
        winner.store.acquire.return_value = False

        async def takeover():
            """Detect a valid lease on the contender that was previously a follower."""
            return loser.lease is not None and loser.lease.valid

        await wait_until(takeover)
        assert len(seen) == 2
        assert winner.lease is None
        assert not await real_store.renew(old_value)
        assert not await real_store.release(old_value)
        winner_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await winner_task
        # A process reacquisition gets a different token even with the same ID.
        loser_task = task2 if winner is first else task1
        old_loser = loser.lease.value
        loser_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await loser_task
        restarted = LeaderSupervisor(store, [service("restart")], identity=loser.identity)
        new_task = asyncio.create_task(restarted.run())
        try:

            async def reacquired():
                """Wait for the restarted identity to obtain a fresh acquisition."""
                return restarted.lease is not None

            await wait_until(reacquired)
            assert restarted.lease.value != old_loser
            assert not await store.release(old_loser)
        finally:
            new_task.cancel()
            await asyncio.gather(new_task, return_exceptions=True)
    finally:
        task1.cancel()
        task2.cancel()
        await asyncio.gather(task1, task2, return_exceptions=True)
