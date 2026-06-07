from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
from pydantic import BaseModel

from pgflows.registry import WorkflowRegistry
from pgflows.telemetry import PgflowsTelemetry
from pgflows.types import QueueMessage, WorkflowState
from pgflows.worker import WorkflowWorker


class WInput(BaseModel):
    x: int


class WOutput(BaseModel):
    y: int


async def simple_workflow(ctx, input: WInput) -> WOutput:
    return WOutput(y=input.x + 1)


def _make_queue_msg(payload: dict) -> QueueMessage:
    return QueueMessage(
        message_id="1",
        queue="pgflows_workflows",
        payload=payload,
        enqueued_at=datetime.now(UTC),
    )


@pytest.mark.asyncio
async def test_worker_processes_task():
    registry = WorkflowRegistry()
    registry.register_workflow(simple_workflow, name="simple_workflow")

    state = AsyncMock()
    state.try_claim_instance.return_value = True
    state.get_step_result.return_value = None
    queue = AsyncMock()
    queue.dequeue.return_value = [
        _make_queue_msg(
            {"workflow_name": "simple_workflow", "instance_id": "inst-001", "input": {"x": 5}}
        )
    ]

    worker = WorkflowWorker(
        registry=registry,
        state_backend=state,
        queue_backend=queue,
        telemetry=PgflowsTelemetry.noop(),
        queue_name="pgflows_workflows",
    )
    await worker.process_batch()

    state.try_claim_instance.assert_called_once_with("inst-001")
    last_call = state.update_instance_state.call_args_list[-1]
    assert last_call[0][1] == WorkflowState.COMPLETED


@pytest.mark.asyncio
async def test_worker_marks_failed_on_exception():
    registry = WorkflowRegistry()

    async def failing_workflow(ctx, input: WInput) -> WOutput:
        raise ValueError("boom")

    registry.register_workflow(failing_workflow, name="failing_workflow")

    state = AsyncMock()
    state.try_claim_instance.return_value = True
    state.get_step_result.return_value = None
    queue = AsyncMock()
    queue.dequeue.return_value = [
        _make_queue_msg(
            {"workflow_name": "failing_workflow", "instance_id": "inst-002", "input": {"x": 1}}
        )
    ]

    worker = WorkflowWorker(
        registry=registry,
        state_backend=state,
        queue_backend=queue,
        telemetry=PgflowsTelemetry.noop(),
        queue_name="pgflows_workflows",
    )
    await worker.process_batch()

    last_call = state.update_instance_state.call_args_list[-1]
    assert last_call[0][1] == WorkflowState.FAILED
    queue.archive.assert_called_once()
    queue.nack.assert_not_called()


@pytest.mark.asyncio
async def test_worker_skips_non_pending_instance():
    registry = WorkflowRegistry()
    registry.register_workflow(simple_workflow, name="simple_workflow")

    state = AsyncMock()
    state.try_claim_instance.return_value = False  # already running/cancelled/completed
    queue = AsyncMock()
    queue.dequeue.return_value = [
        _make_queue_msg(
            {"workflow_name": "simple_workflow", "instance_id": "inst-taken", "input": {"x": 5}}
        )
    ]

    worker = WorkflowWorker(
        registry=registry,
        state_backend=state,
        queue_backend=queue,
        telemetry=PgflowsTelemetry.noop(),
        queue_name="pgflows_workflows",
    )
    await worker.process_batch()

    queue.ack.assert_called_once_with("pgflows_workflows", "1")
    state.update_instance_state.assert_not_called()


@pytest.mark.asyncio
async def test_worker_skips_unknown_workflow():
    registry = WorkflowRegistry()
    state = AsyncMock()
    queue = AsyncMock()
    queue.dequeue.return_value = [
        _make_queue_msg(
            {"workflow_name": "nonexistent", "instance_id": "inst-003", "input": {}}
        )
    ]

    worker = WorkflowWorker(
        registry=registry,
        state_backend=state,
        queue_backend=queue,
        telemetry=PgflowsTelemetry.noop(),
        queue_name="pgflows_workflows",
    )
    await worker.process_batch()
    queue.ack.assert_called_once()
    state.update_instance_state.assert_not_called()
