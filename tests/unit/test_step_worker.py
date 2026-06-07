from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

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


def _make_pool():
    """Minimal asyncpg.Pool mock that records execute() calls."""
    conn = AsyncMock()
    conn.execute = AsyncMock(return_value=None)
    pool = MagicMock()
    pool.acquire = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    return pool, conn


def _worker(registry: WorkflowRegistry, queue: AsyncMock, pool) -> StepWorker:
    return StepWorker(
        registry=registry,
        queue_backend=queue,
        pool=pool,
        telemetry=PgflowsTelemetry.noop(),
        step_queue="pgflows_steps",
        results_table="pgflows.pgmq_step_results",
    )


@pytest.mark.asyncio
async def test_runs_step_writes_result_and_acks():
    registry = WorkflowRegistry()
    registry.register_step(add_one)
    queue = AsyncMock()
    queue.dequeue.return_value = [
        _msg({"step": "add_one", "instance_id": "i", "result_key": "k1", "input": {"x": 5}})
    ]
    pool, conn = _make_pool()

    count = await _worker(registry, queue, pool).process_batch()

    assert count == 1
    # result written to the poll table keyed by result_key
    args = conn.execute.await_args[0]
    assert "INSERT INTO pgflows.pgmq_step_results" in args[0]
    assert args[1] == "k1"
    assert args[2] == {"y": 6}   # raw dict — pool's jsonb codec serializes it
    queue.ack.assert_awaited_once_with("pgflows_steps", "1")
    queue.nack.assert_not_called()


@pytest.mark.asyncio
async def test_unknown_step_is_archived():
    registry = WorkflowRegistry()
    queue = AsyncMock()
    queue.dequeue.return_value = [
        _msg({"step": "nope", "instance_id": "i", "result_key": "k", "input": {}})
    ]
    pool, conn = _make_pool()

    await _worker(registry, queue, pool).process_batch()

    queue.archive.assert_awaited_once_with("pgflows_steps", "1")
    conn.execute.assert_not_called()


@pytest.mark.asyncio
async def test_invalid_input_writes_error_and_archives():
    registry = WorkflowRegistry()
    registry.register_step(add_one)
    queue = AsyncMock()
    queue.dequeue.return_value = [
        _msg({"step": "add_one", "instance_id": "i", "result_key": "k", "input": {"wrong": 1}})
    ]
    pool, conn = _make_pool()

    await _worker(registry, queue, pool).process_batch()

    args = conn.execute.await_args[0]
    assert args[2] == {"__error__": "invalid input"}
    queue.archive.assert_awaited_once()


@pytest.mark.asyncio
async def test_malformed_message_archived():
    registry = WorkflowRegistry()
    registry.register_step(add_one)
    queue = AsyncMock()
    queue.dequeue.return_value = [_msg({"instance_id": "i"})]  # no step/result_key
    pool, conn = _make_pool()

    await _worker(registry, queue, pool).process_batch()

    queue.archive.assert_awaited_once()
    conn.execute.assert_not_called()


@pytest.mark.asyncio
async def test_step_exception_nacks_for_redelivery():
    registry = WorkflowRegistry()

    async def boom(ctx, input: SInput) -> SOutput:
        raise ValueError("kaboom")

    registry.register_step(boom)
    queue = AsyncMock()
    queue.dequeue.return_value = [
        _msg({"step": "boom", "instance_id": "i", "result_key": "k", "input": {"x": 1}})
    ]
    pool, conn = _make_pool()

    await _worker(registry, queue, pool).process_batch()

    queue.nack.assert_awaited_once_with("pgflows_steps", "1")
    queue.ack.assert_not_called()
    conn.execute.assert_not_called()


@pytest.mark.asyncio
async def test_empty_queue_returns_zero():
    registry = WorkflowRegistry()
    queue = AsyncMock()
    queue.dequeue.return_value = []
    pool, _ = _make_pool()

    count = await _worker(registry, queue, pool).process_batch()
    assert count == 0


@pytest.mark.asyncio
async def test_run_requires_pool():
    registry = WorkflowRegistry()
    queue = AsyncMock()
    worker = StepWorker(
        registry=registry,
        queue_backend=queue,
        pool=None,
        telemetry=PgflowsTelemetry.noop(),
    )
    with pytest.raises(RuntimeError, match="pool"):
        await worker.run()
