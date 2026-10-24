"""UTC cron definitions and their API validation rules."""

from datetime import UTC, datetime
from typing import Any

from croniter import CroniterBadCronError, CroniterBadDateError, croniter
from pydantic import BaseModel, ConfigDict, Field, field_validator


class CreateCron(BaseModel):
    """Validate a deterministic five-field UTC schedule and task template."""

    model_config = ConfigDict(extra="forbid", strict=True)

    id: str | None = Field(None, pattern=r"^[A-Za-z0-9_-]{1,128}$")
    schedule: str
    type: str = Field(min_length=1)
    payload: dict[str, Any] | None = None
    priority: int = 0
    max_retries: int = Field(0, ge=0)
    enabled: bool = True

    @field_validator("type")
    @classmethod
    def validate_type(cls, value: str) -> str:
        """Reject blank handler names before storing the task template."""
        if not value.strip():
            raise ValueError("type must not be blank")
        return value

    @field_validator("schedule")
    @classmethod
    def validate_schedule(cls, value: str) -> str:
        """Reject unsupported expressions and schedules with no reachable slot."""
        value = " ".join(value.split())
        if len(value.split()) != 5:
            raise ValueError("schedule requires five UTC cron fields")
        # Ordinary name fields contain neither H nor R as standalone syntax.
        # Try deterministic expansion without hash_id; explicitly reject R.
        if any(word.lower()[:1] == "r" for field in value.split() for word in field.split(",")):
            raise ValueError("random cron expressions are unsupported")
        try:
            iterator = croniter(value, datetime.now(UTC), max_years_between_matches=8)
            iterator.get_next(datetime)
        except (CroniterBadCronError, CroniterBadDateError, ValueError) as exc:
            raise ValueError("invalid or unreachable cron schedule") from exc
        return value


class UpdateCron(BaseModel):
    """Allow only a strictly typed enabled-state change."""

    model_config = ConfigDict(extra="forbid", strict=True)
    enabled: bool


class CronJob(CreateCron):
    """Represent a stored definition, scheduling baseline, and success cursor."""

    id: str
    created_at: datetime
    # Creation/resume baseline skips paused slots; the cursor tracks successful materialization.
    eligible_after_unix: int
    last_materialized_slot_unix: int | None = None
    # A fresh internal revision rejects snapshots read before pause or delete/recreate.
    revision: str = Field(exclude=True)
