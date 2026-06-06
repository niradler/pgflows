from unittest.mock import AsyncMock

import pytest
from pydantic import BaseModel

from pyflows.context import StepContext, WorkflowContext
from pyflows.exceptions import StepExecutionError
from pyflows.telemetry import PyflowsTelemetry
from pyflows.types import RetryConfig


class NumberInput(BaseModel):
    value: int


class NumberOutput(BaseModel):
    result: int


async def double_step(ctx: StepContext, input: NumberInput) -> NumberOutput:
    return NumberOutput(result=input.value * 2)


async def always_fails(ctx: StepContext, input: NumberInput) -> NumberOutput:
    raise ValueError("deliberate failure")


def make_state(cached=None):
    state = AsyncMock()
    state.get_step_result.return_value = cached
    state.save_step_result = AsyncMock()
    state.save_step_error = AsyncMock()
    return state


def make_ctx(state, step_defaults=None):
    return WorkflowContext(
        instance_id="inst-001",
        workflow_name="test_wf",
        state_backend=state,
        telemetry=PyflowsTelemetry.noop(),
        step_defaults=step_defaults,
    )


async def test_step_executes_and_saves():
    state = make_state(cached=None)
    ctx = make_ctx(state)
    result = await ctx.step(double_step, NumberInput(value=5))
    assert result.result == 10
    state.save_step_result.assert_called_once()
    # Verify correct arguments: (instance_id, step_name, step_index, input_dict, output_dict)
    args = state.save_step_result.call_args[0]
    assert args[1] == "double_step"
    assert args[4] == {"result": 10}


async def test_step_replays_from_cache():
    state = make_state(cached={"result": 99})
    ctx = make_ctx(state)
    result = await ctx.step(double_step, NumberInput(value=5))
    assert result.result == 99  # cached, not re-executed
    state.save_step_result.assert_not_called()


async def test_step_counter_increments_per_call():
    state = make_state(cached=None)
    ctx = make_ctx(state)
    await ctx.step(double_step, NumberInput(value=1))
    await ctx.step(double_step, NumberInput(value=2))
    calls = state.get_step_result.call_args_list
    # First call: index 0, second: index 1
    assert calls[0][0][2] == 0
    assert calls[1][0][2] == 1


async def test_step_retries_on_failure():
    attempt_log = []

    async def flaky(ctx, input: NumberInput) -> NumberOutput:
        attempt_log.append(1)
        if len(attempt_log) < 3:
            raise ValueError("not yet")
        return NumberOutput(result=42)

    state = make_state(cached=None)
    retry = RetryConfig(max_retries=3, initial_delay_seconds=0.001)
    ctx = make_ctx(state, step_defaults=retry)
    result = await ctx.step(flaky, NumberInput(value=0))
    assert result.result == 42
    assert len(attempt_log) == 3


async def test_step_raises_after_max_retries():
    state = make_state(cached=None)
    retry = RetryConfig(max_retries=1, initial_delay_seconds=0.001)
    ctx = make_ctx(state, step_defaults=retry)
    with pytest.raises(StepExecutionError, match="always_fails"):
        await ctx.step(always_fails, NumberInput(value=0))
    # save_step_error called for each attempt
    assert state.save_step_error.call_count == 2  # max_retries=1 → 2 attempts total


async def test_step_name_override():
    state = make_state(cached=None)
    ctx = make_ctx(state)
    await ctx.step(double_step, NumberInput(value=3), name="custom_name")
    args = state.get_step_result.call_args[0]
    assert args[1] == "custom_name"
