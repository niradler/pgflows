from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pyflows.types import QueueMessage, ScheduledJob, WorkflowStatus


class OrchestratorBackend(ABC):
    """Drives durable workflow execution (start, signal, query, cancel)."""

    @abstractmethod
    async def initialize(self) -> None:
        """Set up extensions, schemas, and connection pools."""

    @abstractmethod
    async def start_workflow(
        self,
        workflow_id: str,
        name: str,
        payload: dict[str, Any],
    ) -> str:
        """Enqueue a new workflow run. Returns the workflow_id."""

    @abstractmethod
    async def signal_workflow(
        self,
        workflow_id: str,
        signal: str,
        data: dict[str, Any] | None = None,
    ) -> None:
        """Send a signal to a running workflow (e.g. step-completed)."""

    @abstractmethod
    async def get_workflow_status(self, workflow_id: str) -> WorkflowStatus:
        """Return the current status of a workflow."""

    @abstractmethod
    async def cancel_workflow(self, workflow_id: str) -> None:
        """Request cancellation of a running workflow."""

    @abstractmethod
    async def close(self) -> None:
        """Release connections and clean up resources."""


class QueueBackend(ABC):
    """Manages the Python step queue (enqueue, dequeue, ack/nack, listen)."""

    @abstractmethod
    async def initialize(self) -> None:
        """Create queues and install extensions if needed."""

    @abstractmethod
    async def enqueue(
        self,
        queue: str,
        message: dict[str, Any],
        delay_seconds: int = 0,
    ) -> str:
        """Push a message onto the queue. Returns the message_id."""

    @abstractmethod
    async def dequeue(self, queue: str, batch_size: int = 1) -> list[QueueMessage]:
        """Pull up to batch_size messages from the queue."""

    @abstractmethod
    async def ack(self, queue: str, message_id: str) -> None:
        """Acknowledge successful processing of a message."""

    @abstractmethod
    async def nack(self, queue: str, message_id: str) -> None:
        """Return a message to the queue for redelivery."""

    @abstractmethod
    async def close(self) -> None:
        """Release connections and clean up resources."""


class SchedulerBackend(ABC):
    """Manages recurring workflow triggers via cron scheduling."""

    @abstractmethod
    async def initialize(self) -> None:
        """Install pg_cron extension and create schema if needed."""

    @abstractmethod
    async def schedule(
        self,
        job_name: str,
        cron: str,
        command: str,
    ) -> str:
        """Register a cron job. Returns the job_id (pg_durable instance_id)."""

    @abstractmethod
    async def unschedule(self, job_id: str) -> None:
        """Remove a previously registered cron job."""

    @abstractmethod
    async def list_jobs(self) -> list[ScheduledJob]:
        """Return all registered cron jobs."""

    @abstractmethod
    async def close(self) -> None:
        """Release connections and clean up resources."""
