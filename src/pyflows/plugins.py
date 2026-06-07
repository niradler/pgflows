from __future__ import annotations

import asyncio
import logging
from abc import ABC
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel

from pyflows.logger import get_logger

logger = get_logger("plugins")


@dataclass(frozen=True)
class WorkflowEvent:
    instance_id: str
    workflow_name: str


@dataclass(frozen=True)
class StepEvent:
    instance_id: str
    workflow_name: str
    step_name: str
    step_index: int
    attempt: int = 1


class PyflowsPlugin(ABC):
    """Base class for pyflows plugins. Override any hooks you need."""

    async def before_workflow(self, event: WorkflowEvent) -> None:
        pass

    async def after_workflow(self, event: WorkflowEvent, result: Any) -> None:
        pass

    async def on_workflow_error(self, event: WorkflowEvent, error: Exception) -> None:
        pass

    async def before_step(self, event: StepEvent, input_model: BaseModel) -> None:
        pass

    async def after_step(self, event: StepEvent, result: Any) -> None:
        pass

    async def on_step_error(self, event: StepEvent, error: Exception) -> None:
        pass


class LoggingPlugin(PyflowsPlugin):
    """Logs workflow and step lifecycle events using Python's logging module."""

    def __init__(self, level: int = logging.INFO) -> None:
        self._level = level

    async def before_workflow(self, event: WorkflowEvent) -> None:
        logger.log(self._level, "workflow started: %s [%s]", event.workflow_name, event.instance_id)

    async def after_workflow(self, event: WorkflowEvent, result: Any) -> None:
        logger.log(
            self._level, "workflow completed: %s [%s]", event.workflow_name, event.instance_id
        )

    async def on_workflow_error(self, event: WorkflowEvent, error: Exception) -> None:
        logger.error("workflow failed: %s [%s] — %s", event.workflow_name, event.instance_id, error)

    async def before_step(self, event: StepEvent, input_model: BaseModel) -> None:
        logger.log(
            self._level,
            "step started: %s (attempt %d) [%s]",
            event.step_name,
            event.attempt,
            event.instance_id,
        )

    async def after_step(self, event: StepEvent, result: Any) -> None:
        logger.log(self._level, "step completed: %s [%s]", event.step_name, event.instance_id)

    async def on_step_error(self, event: StepEvent, error: Exception) -> None:
        logger.warning(
            "step failed: %s (attempt %d) [%s] — %s",
            event.step_name,
            event.attempt,
            event.instance_id,
            error,
        )


async def fire(plugins: list[PyflowsPlugin], hook: str, *args: Any, **kwargs: Any) -> None:
    """Call a hook on all plugins concurrently, swallowing individual plugin errors."""
    if not plugins:
        return

    async def _call(plugin: PyflowsPlugin) -> None:
        try:
            await getattr(plugin, hook)(*args, **kwargs)
        except Exception:
            logger.warning("plugin %s.%s raised", type(plugin).__name__, hook, exc_info=True)

    await asyncio.gather(*[_call(p) for p in plugins])
