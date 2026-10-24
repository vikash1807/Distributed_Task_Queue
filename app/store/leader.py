"""Redis lease primitives; ownership always includes an acquisition token."""

from pathlib import Path

from redis.asyncio import Redis

from app.store.redis import KEY_LEADER


class LeaderStore:
    """Own Redis lease acquisition, renewal, release, and inspection."""

    def __init__(self, client: Redis, ttl_ms: int) -> None:
        """Bind the lease TTL and register owner-only renewal/release scripts."""
        self.client = client
        self.ttl_ms = ttl_ms
        scripts = Path(__file__).parent / "scripts"
        self._renew = client.register_script((scripts / "renew_leader.lua").read_text())
        self._release = client.register_script((scripts / "release_leader.lua").read_text())

    async def acquire(self, value: str) -> bool:
        """Create the lease only if absent, with a millisecond expiry."""
        return bool(await self.client.set(KEY_LEADER, value, nx=True, px=self.ttl_ms))

    async def renew(self, value: str) -> bool:
        """Extend the TTL only when the complete acquisition value still matches."""
        return bool(await self._renew(keys=[KEY_LEADER], args=[value, self.ttl_ms]))

    async def release(self, value: str) -> bool:
        """Remove only this acquisition, leaving any successor lease intact."""
        return bool(await self._release(keys=[KEY_LEADER], args=[value]))

    async def current(self) -> str | None:
        """Read the current identity/token value, or None after expiry."""
        return await self.client.get(KEY_LEADER)
