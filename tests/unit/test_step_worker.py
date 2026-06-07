from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
from pydantic import BaseModel

from pgflows.registry import WorkflowRegistry
from pgflows.step_worker import StepWorker
from pgflows.telemetry import PgflowsTelemetry
from pgflows.types import QueueMessage


class SInput(BaseModel):
    x: int


class SOutput(BaseModel):
    y: int


async def add_one(ctx, input: SInput) -> SOutput:
    return SOutput(y=input.x + 1)


def _msg(payload: dict, message_id: str = "1") -> QueueMessage:
    return QueueMessage(
        message_id=message_id,
        queue="pgflows_steps",
        payload=payload,
        enqueued_at=datetime.now(UTC),
    )


def _worker(registry: WorkflowRegistry, queue: AsyncMock, pg_durable: AsyncMock) -> StepWorker:
    return StepWorker(
        registry=registry,
        queue_backend=queue,
        pg_durable=pg_durable,
        telemetry=PgflowsTelemetry.noop(),
        step_queue="pgflows_steps",
    )


@pytest.mark.asyncio
async def test_runs_step_signals_result_and_acks():
    registry = WorkflowRegistry()
    registry.register_step(add_one)
    queue = AsyncMock()
    queue.dequeue.return_value = [
        _msg({"step": "add_one", "instance_id": "inst-1", "signal": "done_0", "input": {"x": 5}})
    ]
    pg_durable = AsyncMock()

    count = await _worker(registry, queue, pg_durable).process_batch()

    assert count == 1
    pg_durable.signal.assert_awaited_once_with("inst-1", "done_0", {"y": 6})
    queue.ack.assert_awaited_once_with("pgflows_steps", "1")
    queue.nack.assert_not_called()


@pytest.mark.asyncio
async def test_unknown_step_is_archived_not_signalled():
    registry = WorkflowRegistry()
    queue = AsyncMock()
    queue.dequeue.return_value = [
        _msg({"step": "nope", "instance_id": "i", "signal": "s", "input": {}})
    ]
    pg_durable = AsyncMock()

    await _worker(registry, queue, pg_durable).process_batch()

    queue.archive.assert_awaited_once_with("pgflows_steps", "1")
    pg_durable.signal.assert_not_called()


@pytest.mark.asyncio
async def test_invalid_input_signals_error_and_archives():
    registry = WorkflowRegistry()
    registry.register_step(add_one)
    queue = AsyncMock()
    queue.dequeue.return_value = [
        _msg({"step": "add_one", "instance_id": "i", "signal": "s", "input": {"wrong": 1}})
    ]
    pg_durable = AsyncMock()

    await _worker(registry, queue, pg_durable).process_batch()

    pg_durable.signal.assert_awaited_once()
    assert pg_durable.signal.await_args[0][2] == {"__error__": "invalid input"}
    queue.archive.assert_awaited_once()


@pytest.mark.asyncio
async def test_malformed_message_archived():
    registry = WorkflowRegistry()
    registry.register_step(add_one)
    queue = AsyncMock()
    queue.dequeue.return_value = [_msg({"instance_id": "i"})]  # no step/signal
    pg_durable = AsyncMock()

    await _worker(registry, queue, pg_durable).process_batch()

    queue.archive.assert_awaited_once()
    pg_durable.signal.assert_not_called()


@pytest.mark.asyncio
async def test_step_exception_nacks_for_redelivery():
    registry = WorkflowRegistry()

    async def boom(ctx, input: SInput) -> SOutput:
        raise ValueError("kaboom")

    registry.register_step(boom)
    queue = AsyncMock()
    queue.dequeue.return_value = [
        _msg({"step": "boom", "instance_id": "i", "signal": "s", "input": {"x": 1}})
    ]
    pg_durable = AsyncMock()

    await _worker(registry, queue, pg_durable).process_batch()

    queue.nack.assert_awaited_once_with("pgflows_steps", "1")
    queue.ack.assert_not_called()
    pg_durable.signal.assert_not_called()


@pytest.mark.asyncio
async def test_empty_queue_returns_zero():
    registry = WorkflowRegistry()
    queue = AsyncMock()
    queue.dequeue.return_value = []
    pg_durable = AsyncMock()

    count = await _worker(registry, queue, pg_durable).process_batch()
    assert count == 0


@pytest.mark.asyncio
async def test_signal_skipped_when_pg_durable_absent():
    registry = WorkflowRegistry()
    registry.register_step(add_one)
    queue = AsyncMock()
    queue.dequeue.return_value = [
        _msg({"step": "add_one", "instance_id": "i", "signal": "s", "input": {"x": 1}})
    ]

    worker = StepWorker(
        registry=registry,
        queue_backend=queue,
        pg_durable=None,
        telemetry=PgflowsTelemetry.noop(),
        step_queue="pgflows_steps",
    )
    # Should not raise even though there is no pg_durable client to signal.
    await worker.process_batch()
    queue.ack.assert_awaited_once()
