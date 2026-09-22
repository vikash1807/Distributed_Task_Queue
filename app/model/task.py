# app/model/task.py

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field, field_validator


class TaskNotFound(Exception):
    """Raised by ``TaskStore.get`` when no record exists for the given id."""


class TaskStatus(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class Task(BaseModel):
    id: str = ""
    type: str = ""
    payload: dict[str, Any] | None = None
    priority: int = 0
    delay: int = 0
    max_retries: int = 0  # 0 = no retries
    retries: int = 0
    status: TaskStatus = TaskStatus.PENDING
    created_at: datetime | None = None
    error: str = ""
    owner: str = ""  # node ID currently leasing the task

    @field_validator("status", mode="before")
    @classmethod
    def parse_status(cls, value: Any) -> TaskStatus:
        if isinstance(value, TaskStatus):
            return value

        if not value or not isinstance(value, str):
            return TaskStatus.PENDING

        # If it's a string, clean it and try to parse it
        try:
            return TaskStatus(value.lower())
        except ValueError:
            return TaskStatus.PENDING

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

    @classmethod
    def from_json_dict(cls, d: dict[str, Any]) -> Task:
        return cls.model_validate(d)


class FailedTask(Task):
    status: TaskStatus = TaskStatus.FAILED
    failed_at: datetime | None = None
    reason: str = ""

    @field_validator("status", mode="before")
    @classmethod
    def force_failed_status(cls, value: Any) -> TaskStatus:
        """Always force status to FAILED for any FailedTask instance."""
        return TaskStatus.FAILED


class TaskEventType(StrEnum):
    SUBMITTED = "submitted"
    STARTED = "started"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    DEAD_LETTERED = "dead_lettered"
    PROMOTED = "promoted"
    RECLAIMED = "reclaimed"
    NODE_JOINED = "node_joined"
    NODE_DEAD = "node_dead"
    REDRIVEN = "redriven"

class TaskEvent(BaseModel):
    id: str = ""
    task_id: str = ""
    type: TaskEventType | str = ""
    worker_id: int = 0  # -1 for submit/scheduler/reaper (non-worker) events
    detail: str = ""
    timestamp: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

    @classmethod
    def from_json_dict(cls, d: dict[str, Any]) -> TaskEvent:
        return cls.model_validate(d)


class Metrics(BaseModel):
    total_processed: int = 0
    total_failed: int = 0
    total_retries: int = 0
    queue_size: int = 0
    active_workers: int = 0

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

class EnhancedMetrics(Metrics):
    """Extends Metrics directly to combine base and extended parameters into a single model."""
    success_rate: float = 0.0
    delayed_queue_size: int = 0
    dead_letter_size: int = 0
    total_submitted: int = 0


class WorkerState(BaseModel):
    id: int = -1
    status: str = "" #idle, processing
    task_id: str = ""
    started_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")
    
    @classmethod
    def from_json_dict(cls, d: dict[str, Any]) -> WorkerState:
        return cls.model_validate(d)


# Node is a standalone worker process that joins the cluster. Identity is
# {hostname}-{shortuuid}. Presence is tracked by a heartbeat TTL key; liveness
# is derived from whether that key still exists.
class Node(BaseModel):
    id: str = ""
    hostname: str = ""
    role: str = ""
    capacity: int = 0 # No. of executor tasks
    started_at = datetime | None = None
    alive: bool = False
    in_flight_tasks: int = 0

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")
    
    @classmethod
    def from_json_dict(cls, d: dict[str, Any]) -> Node:
        return cls.model_validate(d)
