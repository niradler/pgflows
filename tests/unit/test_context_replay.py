from unittest.mock import AsyncMock

import pytest
from pydantic import BaseModel

from pyflows.context import StepContext, WorkflowContext
from pyflows.exceptions import StepExecutionError
from pyflows.plugins import PyflowsPlugin, StepEvent
from pyflows.registry import WorkflowRegistry
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


class TrackerPlugin(PyflowsPlugin):
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, int]] = []

    async def before_step(self, event: StepEvent, input_model: BaseModel) -> None:
        self.calls.append(("before_step", event.step_name, event.attempt))

    async def after_step(self, event: StepEvent, result: object) -> None:
        self.calls.append(("after_step", event.step_name, event.attempt))

    async def on_step_error(self, event: StepEvent, error: Exception) -> None:
        self.calls.append(("on_step_error", event.step_name, event.attempt))


def make_state(cached=None):
    state = AsyncMock()
    state.get_step_result.return_value = cached
    state.save_step_result = AsyncMock()
    state.save_step_error = AsyncMock()
    return state


def make_ctx(state, step_defaults=None, plugins=None, registry=None):
    return WorkflowContext(
        instance_id="inst-001",
        workflow_name="test_wf",
        state_backend=state,
        telemetry=PyflowsTelemetry.noop(),
        step_defaults=step_defaults,
        plugins=plugins,
        registry=registry,
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


async def test_step_execution_error_chains_original_exception():
    state = make_state(cached=None)
    retry = RetryConfig(max_retries=0, initial_delay_seconds=0.001)
    ctx = make_ctx(state, step_defaults=retry)

    with pytest.raises(StepExecutionError) as exc_info:
        await ctx.step(always_fails, NumberInput(value=0))

    assert isinstance(exc_info.value.__cause__, ValueError)
    assert str(exc_info.value.__cause__) == "deliberate failure"


async def test_step_name_override():
    state = make_state(cached=None)
    ctx = make_ctx(state)
    await ctx.step(double_step, NumberInput(value=3), name="custom_name")
    args = state.get_step_result.call_args[0]
    assert args[1] == "custom_name"


async def test_step_fires_plugin_hooks_on_success():
    plugin = TrackerPlugin()
    state = make_state(cached=None)
    ctx = make_ctx(state, plugins=[plugin])

    await ctx.step(double_step, NumberInput(value=3))

    assert plugin.calls == [
        ("before_step", "double_step", 1),
        ("after_step", "double_step", 1),
    ]


async def test_step_fires_plugin_error_hook_for_each_failed_attempt():
    plugin = TrackerPlugin()
    state = make_state(cached=None)
    retry = RetryConfig(max_retries=1, initial_delay_seconds=0.001)
    ctx = make_ctx(state, step_defaults=retry, plugins=[plugin])

    with pytest.raises(StepExecutionError):
        await ctx.step(always_fails, NumberInput(value=0))

    assert plugin.calls == [
        ("before_step", "always_fails", 1),
        ("on_step_error", "always_fails", 1),
        ("before_step", "always_fails", 2),
        ("on_step_error", "always_fails", 2),
    ]


async def test_step_uses_registered_retry_config():
    counter = {"n": 0}

    async def flaky(ctx: StepContext, input: NumberInput) -> NumberOutput:
        counter["n"] += 1
        if counter["n"] < 3:
            raise ValueError("not yet")
        return NumberOutput(result=42)

    registry = WorkflowRegistry()
    registry.register_step(flaky, retry=RetryConfig(max_retries=2, initial_delay_seconds=0.001))
    state = make_state(cached=None)
    ctx = make_ctx(state, step_defaults=RetryConfig(max_retries=0), registry=registry)

    result = await ctx.step(flaky, NumberInput(value=0))

    assert result.result == 42
    assert counter["n"] == 3


async def test_default_registered_retry_does_not_override_workflow_default():
    registry = WorkflowRegistry()
    registry.register_step(always_fails)
    state = make_state(cached=None)
    ctx = make_ctx(state, step_defaults=RetryConfig(max_retries=0), registry=registry)

    with pytest.raises(StepExecutionError):
        await ctx.step(always_fails, NumberInput(value=0))

    assert state.save_step_error.call_count == 1
