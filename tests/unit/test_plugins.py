from __future__ import annotations

import logging

import pytest
from pydantic import BaseModel

from pgflows.plugins import LoggingPlugin, PgflowsPlugin, StepEvent, WorkflowEvent, fire


class SampleInput(BaseModel):
    x: int


class TrackerPlugin(PgflowsPlugin):
    """Records which hooks were called and with what args."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []

    async def before_workflow(self, event: WorkflowEvent) -> None:
        self.calls.append(("before_workflow", event.workflow_name))

    async def after_workflow(self, event: WorkflowEvent, result: object) -> None:
        self.calls.append(("after_workflow", event.workflow_name))

    async def on_workflow_error(self, event: WorkflowEvent, error: Exception) -> None:
        self.calls.append(("on_workflow_error", str(error)))

    async def before_step(self, event: StepEvent, input_model: BaseModel) -> None:
        self.calls.append(("before_step", event.step_name, event.attempt))

    async def after_step(self, event: StepEvent, result: object) -> None:
        self.calls.append(("after_step", event.step_name))

    async def on_step_error(self, event: StepEvent, error: Exception) -> None:
        self.calls.append(("on_step_error", event.step_name, event.attempt))


# --- fire() helper ---

@pytest.mark.asyncio
async def test_fire_calls_all_plugins() -> None:
    p1, p2 = TrackerPlugin(), TrackerPlugin()
    event = WorkflowEvent(instance_id="i1", workflow_name="wf")
    await fire([p1, p2], "before_workflow", event)
    assert ("before_workflow", "wf") in p1.calls
    assert ("before_workflow", "wf") in p2.calls


@pytest.mark.asyncio
async def test_fire_swallows_plugin_errors() -> None:
    class BrokenPlugin(PgflowsPlugin):
        async def before_workflow(self, event: WorkflowEvent) -> None:
            raise RuntimeError("boom")

    good = TrackerPlugin()
    event = WorkflowEvent(instance_id="i1", workflow_name="wf")
    # Should not raise even though BrokenPlugin explodes
    await fire([BrokenPlugin(), good], "before_workflow", event)
    assert ("before_workflow", "wf") in good.calls


@pytest.mark.asyncio
async def test_fire_empty_plugins_is_noop() -> None:
    event = WorkflowEvent(instance_id="i1", workflow_name="wf")
    await fire([], "before_workflow", event)  # must not raise


# --- PgflowsPlugin base class ---

@pytest.mark.asyncio
async def test_base_plugin_all_hooks_are_noop() -> None:
    class MinimalPlugin(PgflowsPlugin):
        pass

    plugin = MinimalPlugin()
    wf_event = WorkflowEvent(instance_id="x", workflow_name="y")
    step_event = StepEvent(instance_id="x", workflow_name="y", step_name="s", step_index=0)
    await plugin.before_workflow(wf_event)
    await plugin.after_workflow(wf_event, None)
    await plugin.on_workflow_error(wf_event, ValueError())
    await plugin.before_step(step_event, SampleInput(x=1))
    await plugin.after_step(step_event, None)
    await plugin.on_step_error(step_event, ValueError())


# --- LoggingPlugin ---

@pytest.mark.asyncio
async def test_logging_plugin_before_workflow(caplog: pytest.LogCaptureFixture) -> None:
    plugin = LoggingPlugin()
    event = WorkflowEvent(instance_id="abc-123", workflow_name="my_workflow")
    with caplog.at_level(logging.INFO, logger="pgflows.plugins"):
        await plugin.before_workflow(event)
    assert "my_workflow" in caplog.text
    assert "abc-123" in caplog.text


@pytest.mark.asyncio
async def test_logging_plugin_on_workflow_error(caplog: pytest.LogCaptureFixture) -> None:
    plugin = LoggingPlugin()
    event = WorkflowEvent(instance_id="abc-123", workflow_name="my_workflow")
    with caplog.at_level(logging.ERROR, logger="pgflows.plugins"):
        await plugin.on_workflow_error(event, ValueError("something broke"))
    assert "something broke" in caplog.text


@pytest.mark.asyncio
async def test_logging_plugin_step_hooks(caplog: pytest.LogCaptureFixture) -> None:
    plugin = LoggingPlugin()
    event = StepEvent(
        instance_id="abc-123", workflow_name="wf", step_name="my_step", step_index=0, attempt=2
    )
    with caplog.at_level(logging.INFO, logger="pgflows.plugins"):
        await plugin.before_step(event, SampleInput(x=5))
    assert "my_step" in caplog.text
    assert "2" in caplog.text  # attempt number


@pytest.mark.asyncio
async def test_logging_plugin_custom_level(caplog: pytest.LogCaptureFixture) -> None:
    plugin = LoggingPlugin(level=logging.DEBUG)
    event = WorkflowEvent(instance_id="x", workflow_name="wf")
    with caplog.at_level(logging.DEBUG, logger="pgflows.plugins"):
        await plugin.before_workflow(event)
    assert caplog.records  # at least one record emitted


# --- StepEvent defaults ---

def test_step_event_default_attempt() -> None:
    event = StepEvent(instance_id="i", workflow_name="w", step_name="s", step_index=0)
    assert event.attempt == 1


def test_step_event_is_frozen() -> None:
    event = StepEvent(instance_id="i", workflow_name="w", step_name="s", step_index=0)
    with pytest.raises((AttributeError, TypeError)):
        event.attempt = 99  # type: ignore[misc]
