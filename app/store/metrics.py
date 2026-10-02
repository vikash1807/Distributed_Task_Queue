# app/store/metrics.py

from __future__ import annotations

import redis.asyncio as redis

from app.model import EnhancedMetrics, Metrics
from app.store.redis import KEY_METRICS


def _parse_i64(s: object) -> int:
    try:
        return int(s)  # type: ignore[arg-type]
    except (ValueError, TypeError):
        return 0


class MetricStore:
    def __init__(self, client: redis.Redis) -> None:
        self.client = client

    async def incr_processed(self) -> None:
        await self.client.hincrby(KEY_METRICS, "processed", 1)

    async def incr_failed(self) -> None:
        await self.client.hincrby(KEY_METRICS, "failed", 1)

    async def incr_retries(self) -> None:
        await self.client.hincrby(KEY_METRICS, "retries", 1)

    async def incr_submitted(self) -> None:
        await self.client.hincrby(KEY_METRICS, "submitted", 1)

    async def get_metrics(self, queue_size: int, active_workers: int) -> Metrics:
        data = await self.client.hgetall(KEY_METRICS)

        return Metrics(
            total_processed=_parse_i64(data.get("processed")),
            total_failed=_parse_i64(data.get("failed")),
            total_retries=_parse_i64(data.get("retries")),
            reaper_reclaims=_parse_i64(data.get("reaper_reclaims")),
            queue_size=queue_size,
            active_workers=active_workers,
        )

    async def get_enhanced_metrics(
        self,
        queue_size: int,
        active_workers: int,
        delayed_size: int,
        dead_letter_size: int,
    ) -> EnhancedMetrics:
        data = await self.client.hgetall(KEY_METRICS)

        processed = _parse_i64(data.get("processed"))
        failed = _parse_i64(data.get("failed"))
        submitted = _parse_i64(data.get("submitted"))
        retries = _parse_i64(data.get("retries"))

        total = processed + failed
        success_rate = (processed / total) * 100 if total else 0.0

        return EnhancedMetrics(
            total_processed=processed,
            total_failed=failed,
            total_retries=retries,
            reaper_reclaims=_parse_i64(data.get("reaper_reclaims")),
            queue_size=queue_size,
            active_workers=active_workers,
            success_rate=success_rate,
            delayed_queue_size=delayed_size,
            dead_letter_size=dead_letter_size,
            total_submitted=submitted,
        )

