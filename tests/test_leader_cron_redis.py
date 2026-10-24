"""Leader fencing and cron materialization on an isolated, disposable Redis DB."""

import asyncio
import json
import os
import time
from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError

from app.container import build_container
from app.core.config import Config
from app.core.leadership import LeaderLease, LeadershipLost
from app.leader import LeaderSupervisor
from app.model import FailedTask, Node, Task, TaskEvent, TaskEventType, TaskStatus
from app.model.cron import CreateCron
from app.service.cron import CronMaterializer, latest_slot
from app.service.task import DuplicateTaskError
from app.store.cron import CronNotFound, CronStore, DuplicateCronError
from app.store.leader import LeaderStore
from app.store.redis import (
    KEY_DEADLETTER,
    KEY_DELAYED,
    KEY_EVENTS,
    KEY_LEADER,
    KEY_METRICS,
    KEY_PROCESSING,
    KEY_READY,
    KEY_READY_SIGNAL,
    key_cron,
    key_task,
    node_dead_key,
    node_heartbeat_key,
    node_tasks_key,
)
from app.store.task import TaskStore

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


async def own(client, ttl=3000):
    """Acquire a real Redis lease and construct its local validity context."""
    value = json.dumps({"leader_id": "test", "token": "tenure"})
    assert await LeaderStore(client, ttl).acquire(value)
    return LeaderLease(value, time.monotonic() + ttl / 1000)


async def due_job(client, cron_id="job"):
    """Create a job with an old baseline to simulate missed schedule slots."""
    store = CronStore(client)
    job = await store.create(CreateCron(id=cron_id, schedule="* * * * *", type="hash"))
    now = int((await client.time())[0])
    await client.hset(key_cron(job.id), "eligible_after_unix", now - 600)
    return (await store.list())[0], now


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


async def test_cron_management_resume_and_reserved_looking_id(redis):
    """Keep IDs collision-free and resume from a fresh baseline without losing the cursor."""
    store = CronStore(redis)
    job = await store.create(CreateCron(id="jobs", schedule="* * * * *", type="hash"))
    assert job.last_materialized_slot_unix is None
    with pytest.raises(DuplicateCronError):
        await store.create(CreateCron(id="jobs", schedule="* * * * *", type="hash"))
    paused = await store.set_enabled(job.id, False)
    assert paused.revision != job.revision and not paused.enabled
    await redis.hset(
        key_cron(job.id),
        mapping={
            "eligible_after_unix": 0,
            "last_materialized_slot_unix": 60,
        },
    )
    resumed = await store.set_enabled(job.id, True)
    assert resumed.eligible_after_unix > 60
    assert resumed.last_materialized_slot_unix == 60
    repeated = await store.set_enabled(job.id, True)
    assert repeated.revision == resumed.revision
    assert repeated.eligible_after_unix == resumed.eligible_after_unix
    await store.delete(job.id)
    assert await store.list() == []
    with pytest.raises(CronNotFound):
        await store.delete(job.id)
    with pytest.raises(CronNotFound):
        await store.set_enabled(job.id, True)


async def test_latest_slot_concurrency_and_lost_response(redis):
    """Create one instance per slot despite concurrent calls and a lost response."""
    lease = await own(redis)
    job, now = await due_job(redis)
    tasks = TaskStore(redis)
    materializer = CronMaterializer(CronStore(redis), tasks, tick=0.1, signal_cap=10)
    outcomes = await asyncio.gather(*(materializer.materialize(job, now, lease) for _ in range(8)))
    assert outcomes.count(1) == 1
    assert outcomes.count(-2) == 7
    slot = latest_slot(job.schedule, now)
    task_id = f"{job.id}-{slot}"
    assert await redis.zrange(KEY_READY, 0, -1) == [task_id]
    assert await redis.hget(KEY_METRICS, "submitted") == "1"
    assert await redis.llen(KEY_EVENTS) == 1
    assert await redis.llen(KEY_READY_SIGNAL) == 1
    assert (await tasks.get(task_id)).payload is None
    assert (await CronStore(redis).list())[0].last_materialized_slot_unix == slot

    # A second slot commits, but the client loses Redis's successful response.
    updated = (await CronStore(redis).list())[0]
    execute = tasks._submit

    async def lose_response(**kwargs):
        """Execute the Redis commit, then simulate losing its reply."""
        await execute(**kwargs)
        raise RedisConnectionError("response lost")

    with (
        patch.object(tasks, "_submit", side_effect=lose_response),
        pytest.raises(RedisConnectionError),
    ):
        await materializer.materialize(updated, now + 60, lease)
    assert await materializer.materialize(updated, now + 60, lease) == -2
    assert await redis.hget(KEY_METRICS, "submitted") == "2"
    assert await redis.llen(KEY_EVENTS) == 2


async def test_existing_instance_advances_cursor_without_overwriting(redis):
    """Treat an existing task as materialized without resetting or re-enqueuing it."""
    lease = await own(redis)
    job, now = await due_job(redis)
    slot = latest_slot(job.schedule, now)
    task = Task(id=f"{job.id}-{slot}", type="hash", status=TaskStatus.COMPLETED)
    tasks = TaskStore(redis)
    await tasks.save(task)
    materializer = CronMaterializer(CronStore(redis), tasks, tick=0.1, signal_cap=10)
    assert await materializer.materialize(job, now, lease) == 0
    assert (await tasks.get(task.id)).status == TaskStatus.COMPLETED
    assert await redis.zcard(KEY_READY) == 0
    assert await redis.llen(KEY_EVENTS) == 0
    assert (await CronStore(redis).list())[0].last_materialized_slot_unix == slot


async def test_failed_submission_leaves_cursor_and_task_absent(redis):
    """Leave no task or cursor update when Redis preflight or submission fails."""
    lease = await own(redis)
    job, now = await due_job(redis)
    tasks = TaskStore(redis)
    materializer = CronMaterializer(CronStore(redis), tasks, tick=0.1, signal_cap=10)
    await redis.set(KEY_READY_SIGNAL, "wrong type")
    with pytest.raises(ResponseError):
        await materializer.materialize(job, now, lease)
    assert (await CronStore(redis).list())[0].last_materialized_slot_unix is None
    assert not await tasks.exists(f"{job.id}-{latest_slot(job.schedule, now)}")
    assert not await redis.exists(KEY_METRICS)
    await redis.delete(KEY_READY_SIGNAL)
    with (
        patch.object(tasks, "_submit", side_effect=RedisConnectionError("offline")),
        pytest.raises(RedisConnectionError),
    ):
        await materializer.materialize(job, now, lease)
    assert (await CronStore(redis).list())[0].last_materialized_slot_unix is None
    assert await materializer.materialize(job, now, lease) == 1


@pytest.mark.parametrize("change", ["pause", "delete", "recreate"])
async def test_management_races_reject_stale_snapshot(redis, change):
    """Reject materialization after a snapshot is paused, deleted, or replaced."""
    lease = await own(redis)
    job, now = await due_job(redis)
    store = CronStore(redis)
    if change == "pause":
        await store.set_enabled(job.id, False)
    else:
        await store.delete(job.id)
        if change == "recreate":
            await due_job(redis)
    tasks = TaskStore(redis)
    materializer = CronMaterializer(store, tasks, tick=0.1, signal_cap=10)
    assert await materializer.materialize(job, now, lease) == -2
    assert await redis.zcard(KEY_READY) == 0


async def test_creation_pause_resume_and_backward_clock_skip(redis):
    """Skip slots before creation/resume and avoid replay when time moves backward."""
    lease = await own(redis)
    job, now = await due_job(redis)
    store = CronStore(redis)
    materializer = CronMaterializer(store, TaskStore(redis), tick=0.1, signal_cap=10)
    fresh = await store.create(CreateCron(id="fresh", schedule="* * * * *", type="hash"))
    assert await materializer.materialize(fresh, now, lease) is None
    assert await materializer.materialize(job, now, lease) == 1
    updated = (await store.list())[1]
    assert updated.id == "job"
    assert await materializer.materialize(updated, now - 60, lease) is None
    paused = await store.set_enabled(job.id, False)
    assert await materializer.materialize(paused, now + 600, lease) is None
    resumed = await store.set_enabled(job.id, True)
    assert await materializer.materialize(resumed, now, lease) is None


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


async def test_stale_leader_rejected_by_all_mutation_scripts(redis):
    """Reject a stale Redis owner even when its local lease appears valid."""
    lease = await own(redis)
    job, now = await due_job(redis)
    container = build_container(redis, Config())
    await container.task_store.save(Task(id="delayed", type="hash"))
    await redis.zadd(KEY_DELAYED, {"delayed": 0})
    node = Node(id="dead", hostname="test", started_at=datetime.now(UTC))
    await container.node_store.register(node)
    await redis.delete(node_heartbeat_key(node.id))
    await container.task_store.save(
        Task(
            id="abandoned",
            type="hash",
            status=TaskStatus.PROCESSING,
            owner=node.id,
        )
    )
    await redis.zadd(KEY_PROCESSING, {"abandoned": 0})
    await redis.sadd(node_tasks_key(node.id), "abandoned")
    await redis.set(KEY_LEADER, "successor", px=3000)
    # Local lease remains valid: only Redis ownership can detect this takeover.
    with pytest.raises(LeadershipLost):
        await container.delayed_scheduler._promote_due_tasks(lease)
    with pytest.raises(LeadershipLost):
        await container.reaper.sweep(lease)
    with pytest.raises(LeadershipLost):
        await container.reaper._reclaim_one("abandoned", now * 1000, node.id, lease)
    with pytest.raises(LeadershipLost):
        await container.reaper._transition(
            container.reaper._prune,
            lease,
            keys=[
                "taskqueue:nodes",
                node_heartbeat_key(node.id),
                f"taskqueue:node:{node.id}:meta",
                node_tasks_key(node.id),
                node_dead_key(node.id),
            ],
            args=[node.id, now * 1000],
        )
    with pytest.raises(LeadershipLost):
        await container.cron_materializer.materialize(job, now, lease)
    assert await redis.zscore(KEY_DELAYED, "delayed") == 0
    assert await redis.zscore(KEY_PROCESSING, "abandoned") == 0
    assert not await redis.exists(node_dead_key(node.id))
    assert await redis.zcard(KEY_READY) == 0


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


async def test_supervisor_runs_all_actual_loops(redis):
    """Run promotion, recovery, and cron through one elected supervisor."""
    config = Config(leader_ttl_ms=600, cron_tick_ms=20, reaper_interval_ms=20)
    first = build_container(redis, config)
    second = build_container(redis, config)
    job, now = await due_job(redis)
    await first.task_service.submit_task(id="delay", type="hash", delay=1)
    await first.task_store.save(Task(id="expired", type="hash", max_retries=1))
    await redis.zadd(KEY_PROCESSING, {"expired": 0})
    tasks = [asyncio.create_task(c.leader.run()) for c in (first, second)]
    try:

        async def all_materialized():
            """Wait for cron, delayed promotion, and recovery to produce ready work."""
            return await redis.zcard(KEY_READY) == 3

        await wait_until(all_materialized)
        assert await redis.exists(key_task(f"{job.id}-{latest_slot(job.schedule, now)}"))
        assert sum(bool(c.leader.lease) for c in (first, second)) == 1
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    assert await redis.get(KEY_LEADER) is None


async def test_cron_submission_requires_ownership(redis):
    """Reject cron submission without a leader lease before writing task state."""
    container = build_container(redis, Config())
    job, now = await due_job(redis)
    event = TaskEvent(type=TaskEventType.SUBMITTED, worker_id="-1")
    with pytest.raises(ValueError, match="leader lease"):
        await container.task_store.submit(
            Task(id=f"{job.id}-{now}", type="hash"),
            event,
            10,
            cron_id=job.id,
            revision=job.revision,
        )
    assert await redis.zcard(KEY_READY) == 0


async def test_redrive_materialized_failed_instance(redis):
    """Redrive a failed cron task while preserving its scheduling cursor."""
    container = build_container(redis, Config())
    lease = await own(redis)
    job, now = await due_job(redis)
    assert await container.cron_materializer.materialize(job, now, lease) == 1
    task_id = f"{job.id}-{latest_slot(job.schedule, now)}"
    failed = FailedTask(id=task_id, type="hash", retries=1, max_retries=1)
    await container.task_store.save(failed)
    await redis.zrem(KEY_READY, task_id)
    await redis.lpush(KEY_DEADLETTER, failed.model_dump_json())
    assert await container.task_service.redrive_failed_tasks() == {"redriven": 1, "total": 1}
    restored = await container.task_store.get(task_id)
    assert restored.status == TaskStatus.PENDING and restored.retries == 0
    assert (await container.cron_service.list())[0].last_materialized_slot_unix == latest_slot(
        job.schedule,
        now,
    )
    assert (await container.task_service.redrive_failed_tasks())["redriven"] == 0
