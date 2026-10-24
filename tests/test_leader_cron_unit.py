"""Validation, lease failure handling, and supervisor lifecycle without Redis."""

import asyncio
import time
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError
from redis.exceptions import ConnectionError as RedisConnectionError

from app.core.config import Config
from app.core.leadership import LeaderLease, LeadershipLost
from app.leader import LeaderSupervisor
from app.model.cron import CreateCron
from app.service.cron import latest_slot

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    """Run async tests on asyncio, matching the application runtime."""
    return "asyncio"


@pytest.mark.parametrize(
    "expression",
    [
        "* * * * * *",
        "@daily",
        "R * * * *",
        "H * * * *",
        "0 0 31 2 *",
        "bad",
    ],
)
def test_invalid_cron(expression):
    """Reject unsupported syntax and schedules that cannot occur."""
    with pytest.raises(ValidationError):
        CreateCron(schedule=expression, type="hash")


@pytest.mark.parametrize("expression", ["* * * * *", "0 0 29 2 *", "0 2 * * MON-FRI"])
def test_valid_cron(expression):
    """Accept ordinary UTC, leap-day, and named-weekday schedules."""
    assert CreateCron(schedule=expression, type="hash").schedule == expression


@pytest.mark.parametrize("now, expected", [(119, 60), (120, 120), (179, 120), (180, 180)])
def test_latest_slot_includes_exact_boundary(now, expected):
    """Include an exact minute boundary without selecting a future slot."""
    assert latest_slot("* * * * *", now) == expected


def test_config_validation():
    """Enforce the minimum lease TTL and positive cron scan interval."""
    with pytest.raises(ValidationError):
        Config(leader_ttl_ms=299)
    with pytest.raises(ValidationError):
        Config(cron_tick_ms=0)
    assert Config(leader_ttl_ms=300).leader_ttl_ms == 300


@pytest.mark.parametrize("failure", [False, RedisConnectionError("offline"), "timeout"])
async def test_renewal_failure_invalidates_lease_and_cancels_services(failure):
    """Stop every singleton on rejected, failed, or timed-out renewal."""
    store = AsyncMock(ttl_ms=300)
    if failure == "timeout":

        async def hang(_value):
            """Simulate a Redis renewal that never responds."""
            await asyncio.Future()

        store.renew.side_effect = hang
    elif isinstance(failure, Exception):
        store.renew.side_effect = failure
    else:
        store.renew.return_value = failure
    stopped = [asyncio.Event() for _ in range(3)]

    def service(index):
        """Build a singleton whose cleanup records cancellation for this index."""

        async def run(lease):
            """Remain active until cancellation and assert ownership was invalidated first."""
            try:
                assert lease.valid
                await asyncio.Future()
            finally:
                assert not lease.valid
                stopped[index].set()

        return run

    supervisor = LeaderSupervisor(store, [service(i) for i in range(3)])
    lease = LeaderLease("owner", time.monotonic() + 0.3)
    with pytest.raises(LeadershipLost):
        await asyncio.wait_for(supervisor._lead(lease), 1)
    assert all(event.is_set() for event in stopped)
    assert not lease.valid
    assert supervisor.interval == 0.1


async def test_expired_local_lease_rejects_successful_late_renewal():
    """Reject a renewal response that arrives after local lease validity ends."""
    store = AsyncMock(ttl_ms=300)

    async def late(_value):
        """Return renewal success too late for the original local deadline."""
        await asyncio.sleep(0.05)
        return True

    store.renew.side_effect = late
    supervisor = LeaderSupervisor(store, [])
    lease = LeaderLease("owner", time.monotonic() + 0.12)
    with pytest.raises(LeadershipLost):
        await supervisor._renew(lease)
    assert not lease.valid


async def test_unexpected_service_exit_marks_failure_and_releases():
    """Expose unexpected singleton exit as failure and release its lease."""
    store = AsyncMock(ttl_ms=300)
    store.acquire.return_value = True

    async def exits(_lease):
        """Simulate a singleton returning normally when it should run continuously."""
        return

    supervisor = LeaderSupervisor(store, [exits])
    with pytest.raises(RuntimeError, match="exited unexpectedly"):
        await supervisor.run()
    assert supervisor.failed
    assert supervisor.lease is None
    store.release.assert_awaited_once()


async def test_ineligible_and_uncertain_acquisition_start_no_services():
    """Keep singleton work stopped when ineligible or acquisition is uncertain."""
    store = AsyncMock(ttl_ms=300)
    service = AsyncMock()
    supervisor = LeaderSupervisor(store, [service], eligible=False)
    task = asyncio.create_task(supervisor.run())
    await supervisor.started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    store.acquire.assert_not_awaited()
    service.assert_not_awaited()

    store.acquire.side_effect = RedisConnectionError("lost response")
    supervisor = LeaderSupervisor(store, [service])
    task = asyncio.create_task(supervisor.run())
    await supervisor.started.wait()
    await asyncio.sleep(0.02)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    service.assert_not_awaited()


async def test_cancel_joins_services_before_release():
    """Require service cleanup to finish before ownership is released."""
    store = AsyncMock(ttl_ms=300)
    store.acquire.return_value = True
    entered = asyncio.Event()
    stopped = asyncio.Event()

    async def service(_lease):
        """Signal startup and record that cancellation cleanup finished."""
        try:
            entered.set()
            await asyncio.Future()
        finally:
            stopped.set()

    async def release(_value):
        """Assert service cleanup completed before releasing Redis ownership."""
        assert stopped.is_set()
        return True

    store.release.side_effect = release
    supervisor = LeaderSupervisor(store, [service])
    task = asyncio.create_task(supervisor.run())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert stopped.is_set()
    store.release.assert_awaited_once()
