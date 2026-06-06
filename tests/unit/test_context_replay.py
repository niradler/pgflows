from unittest.mock import AsyncMock

import pytest
from pydantic import BaseModel

from pyflows.context import WorkflowContext
from pyflows.telemetry import PyflowsTelemetry


class NumberInput(BaseModel):
    value: int


class NumberOutput(BaseModel):
    result: int


async def double_step(ctx, input: NumberInput) -> NumberOutput:
    return NumberOutput(result=input.value * 2)


@pytest.mark.asyncio
async def test_step_executes_and_caches():
    state = AsyncMock()
    state.get_step_result.return_value = None
    ctx = WorkflowContext(
        instance_id="test-001",
        workflow_name="test_wf",
        state_backend=state,
        telemetry=PyflowsTelemetry.noop(),
    )
    result = await ctx.step(double_step, NumberInput(value=5))
    assert result.result == 10
    state.save_step_result.assert_called_once()


@pytest.mark.asyncio
async def test_step_replays_from_cache():
    state = AsyncMock()
    state.get_step_result.return_value = {"result": 99}
    ctx = WorkflowContext(
        instance_id="test-002",
        workflow_name="test_wf",
        state_backend=state,
        telemetry=PyflowsTelemetry.noop(),
    )
    result = await ctx.step(double_step, NumberInput(value=5))
    assert result.result == 99
    state.save_step_result.assert_not_called()


@pytest.mark.asyncio
async def test_step_counter_increments_per_name():
    state = AsyncMock()
    state.get_step_result.return_value = None
    ctx = WorkflowContext(
        instance_id="test-003",
        workflow_name="test_wf",
        state_backend=state,
        telemetry=PyflowsTelemetry.noop(),
    )
    await ctx.step(double_step, NumberInput(value=1))
    await ctx.step(double_step, NumberInput(value=2))
    calls = state.get_step_result.call_args_list
    assert calls[0][0][2] == 0  # first call: step_index=0
    assert calls[1][0][2] == 1  # second call: step_index=1
