"""Supervise one set of background services while a Redis lease is valid."""

import asyncio
import json
import logging
import secrets
import time
from collections.abc import Awaitable, Callable

from redis.exceptions import RedisError

from app.core.leadership import LeaderLease, LeadershipLost
from app.store.leader import LeaderStore
from app.worker.node import create_node_id

logger = logging.getLogger(__name__)
Service = Callable[[LeaderLease], Awaitable[None]]


class LeaderSupervisor:
    """Run singleton services only while this API owns the Redis lease."""

    def __init__(
        self,
        store: LeaderStore,
        services: list[Service],
        *,
        eligible: bool = True,
        identity: str | None = None,
    ) -> None:
        """Set the process identity, service entrypoints, and TTL/3 cadence."""
        self.store = store
        self.services = services
        self.eligible = eligible
        self.identity = identity or create_node_id()
        # Convert the millisecond TTL into a renewal/retry interval of TTL/3 seconds.
        self.interval = store.ttl_ms / 3000
        self.lease: LeaderLease | None = None
        self.failed = False
        self.started = asyncio.Event()

    async def status(self) -> dict[str, str | bool | None]:
        """Combine authoritative Redis ownership with local lease validity."""
        value = await asyncio.wait_for(self.store.current(), self.interval)
        owner = json.loads(value)["leader_id"] if value else None
        return {
            "leader_id": owner,
            "is_self": bool(self.lease and self.lease.valid and value == self.lease.value),
        }

    async def run(self) -> None:
        """Acquire leadership, supervise each tenure, and retry after step-down."""
        # Startup waits for the election loop to start, not for this API to win.
        self.started.set()
        try:
            if not self.eligible:
                # Stay cancellable without competing or starting singleton work.
                await asyncio.Future()
            while True:
                # A new token prevents an old tenure from acting on a later acquisition.
                value = json.dumps({"leader_id": self.identity, "token": secrets.token_hex(16)})
                attempt = time.monotonic()
                try:
                    acquired = await asyncio.wait_for(self.store.acquire(value), self.interval)
                except (RedisError, TimeoutError):
                    # Redis may have acquired the key despite a lost reply; do not assume ownership.
                    logger.warning("leader acquisition unavailable", exc_info=True)
                    acquired = False
                if acquired:
                    # Count from the request start so network delay adds no extra lease time.
                    lease = LeaderLease(value, attempt + self.store.ttl_ms / 1000)
                    self.lease = lease
                    try:
                        lease.check()
                        await self._lead(lease)
                    except LeadershipLost:
                        logger.info("leader stepped down identity=%s", self.identity)
                    finally:
                        # Report local step-down even if Redis release is temporarily unavailable.
                        lease.active = False
                        self.lease = None
                        try:
                            await asyncio.wait_for(self.store.release(value), self.interval)
                        except (RedisError, TimeoutError):
                            logger.warning("leader release unavailable; lease will expire")
                await asyncio.sleep(self.interval)
        except asyncio.CancelledError:
            raise
        except Exception:
            self.failed = True
            logger.exception("leader supervisor failed")
            raise

    async def _renew(self, lease: LeaderLease) -> None:
        """Extend the owned lease; invalidate it immediately on uncertainty."""
        while True:
            await asyncio.sleep(self.interval)
            try:
                lease.check()
                attempt = time.monotonic()
                # Never wait for renewal beyond the currently known local validity window.
                timeout = min(self.interval, lease.deadline - attempt)
                renewed = await asyncio.wait_for(self.store.renew(lease.value), timeout)
                # A late successful response must not revive an already expired local lease.
                lease.check()
                if not renewed:
                    raise LeadershipLost("renewal rejected")
                lease.deadline = attempt + self.store.ttl_ms / 1000
            except (RedisError, TimeoutError) as exc:
                lease.active = False
                raise LeadershipLost("renewal unavailable") from exc
            except LeadershipLost:
                lease.active = False
                raise

    async def _lead(self, lease: LeaderLease) -> None:
        """Monitor renewal and services together, then join them on exit."""
        tasks = []
        try:
            tasks.append(asyncio.create_task(self._renew(lease), name="leader-renewal"))
            tasks.extend(asyncio.create_task(service(lease)) for service in self.services)
            logger.info("leadership acquired identity=%s", self.identity)
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            # Prefer a leadership-loss outcome over cancellation of sibling work.
            for task in done:
                error = None if task.cancelled() else task.exception()
                if isinstance(error, LeadershipLost):
                    raise error
            for task in done:
                if task.cancelled():
                    raise RuntimeError("singleton cancelled unexpectedly")
                task.result()
            # Each service is expected to run continuously; normal return is still a failure.
            raise RuntimeError("singleton exited unexpectedly")
        finally:
            # Invalidate first so siblings stop protected work while cancellation is delivered.
            lease.active = False
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
