from __future__ import annotations

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    """All configurations, read from environment with defaults."""

    model_config = SettingsConfigDict(
        case_sensitive=False,
        extra='ignore',
        populate_by_name=True
    )

    redis_addr: str = Field("localhost:6379", validation_alias="REDIS_ADDR")
    redis_pass: str = Field("", validation_alias="REDIS_PASSWORD")
    server_port: int = Field(8080, validation_alias="SERVER_PORT")
    metrics_port: int = Field(9100, validation_alias="METRICS_PORT")

    worker_count: int = Field(5, validation_alias="WORKER_COUNT")
    poll_interval_ms: int = Field(500, validation_alias="POLL_INTERVAL_MS")
    # Per-handler execution limit, which also bounds in-flight shutdown drain.
    drain_timeout_ms: int = Field(5000, validation_alias="DRAIN_TIMEOUT_MS")

    visibility_timeout_ms: int = Field(30000, validation_alias="VISIBILITY_TIMEOUT_MS")
    reaper_interval_ms: int = Field(5000, validation_alias="REAPER_INTERVAL_MS")

    signal_block_ms: int = Field(1000, validation_alias="SIGNAL_BLOCK_MS")
    signal_cap: int = Field(1024, validation_alias="SIGNAL_CAP")

    # distribution / cluster membership
    heartbeat_interval_ms: int = Field(3000, validation_alias="HEARTBEAT_INTERVAL_MS")
    heartbeat_ttl_ms: int = Field(10000, validation_alias="HEARTBEAT_TTL_MS")
    dead_node_grace_ms: int = Field(30000, validation_alias="DEAD_NODE_GRACE_MS")

    # duration helpers (seconds)
    @property
    def visibility_timeout(self) -> float:
        return self.visibility_timeout_ms / 1000

    @property
    def reaper_interval(self) -> float:
        return self.reaper_interval_ms / 1000

    @property
    def poll_interval(self) -> float:
        return self.poll_interval_ms / 1000

    @property
    def drain_timeout(self) -> float:
        return self.drain_timeout_ms / 1000

    @property
    def signal_block(self) -> float:
        return self.signal_block_ms / 1000

    @property
    def heartbeat_interval(self) -> float:
        return self.heartbeat_interval_ms / 1000

    @property
    def heartbeat_ttl(self) -> float:
        return self.heartbeat_ttl_ms / 1000

    @property
    def node_grace_window(self) -> float:
        return self.dead_node_grace_ms / 1000



    # Validation
    @model_validator(mode="after")
    def _validate(self) -> Config:
        if not self.redis_addr:
            raise ValueError("config: REDIS_ADDR must not be empty")
        if not self.server_port:
            raise ValueError("config: SERVER_PORT must not be empty")
        if not self.metrics_port:
            raise ValueError("config: METRICS_PORT must not be empty")
        if self.worker_count <= 0:
            raise ValueError(f"config: WORKER_COUNT must be > 0, got {self.worker_count}")
        if self.visibility_timeout_ms <= 0:
            raise ValueError("config: VISIBILITY_TIMEOUT_MS must be > 0")
        if self.reaper_interval_ms <= 0:
            raise ValueError("config: REAPER_INTERVAL_MS must be > 0")
        if self.poll_interval_ms <= 0:
            raise ValueError("config: POLL_INTERVAL_MS must be > 0")
        if self.drain_timeout_ms <= 0:
            raise ValueError("config: DRAIN_TIMEOUT_MS must be > 0")
        if self.signal_block_ms <= 0:
            raise ValueError("config: SIGNAL_BLOCK_MS must be > 0")
        if self.signal_cap <= 0:
            raise ValueError(f"config: SIGNAL_CAP must be > 0, got {self.signal_cap}")
        if self.heartbeat_interval_ms <= 0:
            raise ValueError("config: HEARTBEAT_INTERVAL_MS must be > 0")
        if self.heartbeat_ttl_ms <= self.heartbeat_interval_ms:
            raise ValueError(
                f"config: HEARTBEAT_TTL_MS ({self.heartbeat_ttl_ms}) must exceed "
                f"HEARTBEAT_INTERVAL_MS ({self.heartbeat_interval_ms}) to tolerate a missed beat"
            )
        if self.dead_node_grace_ms <= 0:
            raise ValueError("config: DEAD_NODE_GRACE_MS must be > 0")
        return self


def load_config() -> Config:
    """Read configuration from enviornment, apply defaults, and validate."""
    return Config()

settings = load_config()
