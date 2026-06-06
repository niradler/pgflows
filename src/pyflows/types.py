from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class WorkflowState(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SUSPENDED = "suspended"


class WorkflowStatus(BaseModel):
    workflow_id: str
    name: str
    state: WorkflowState
    created_at: datetime
    updated_at: datetime
    error: str | None = None
    output: Any | None = None


class QueueMessage(BaseModel):
    message_id: str
    queue: str
    payload: dict[str, Any]
    enqueued_at: datetime
    read_count: int = 0


class ScheduledJob(BaseModel):
    job_id: int
    job_name: str
    cron: str
    command: str
    active: bool


class RetryConfig(BaseModel):
    max_retries: int = 3
    backoff: str = "exponential"
    initial_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0
    jitter: bool = True


class StepConfig(BaseModel):
    name: str
    retry: RetryConfig = Field(default_factory=RetryConfig)
    timeout_seconds: float | None = None
