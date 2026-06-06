from __future__ import annotations

import asyncio
import traceback
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from pyflows.exceptions import StepExecutionError
from pyflows.telemetry import PyflowsTelemetry
from pyflows.types import RetryConfig

if TYPE_CHECKING:
    from pyflows.backends.pg_state import PgStateBackend


class WorkflowContext:
    """Passed to workflow functions. Drives step execution with checkpoint replay."""

    def __init__(
        self,
        instance_id: str,
        workflow_name: str,
        state_backend: PgStateBackend,
        telemetry: PyflowsTelemetry,
        step_defaults: RetryConfig | None = None,
    ) -> None:
        self.instance_id = instance_id
        self.workflow_name = workflow_name
        self._state = state_backend
        self._telemetry = telemetry
        self._step_defaults = step_defaults or RetryConfig()
        self._step_counter: dict[str, int] = {}

    async def step(
        self,
        fn: Callable,
        input_model: BaseModel,
        *,
        name: str | None = None,
        retry: RetryConfig | None = None,
    ) -> Any:
        step_name = name or fn.__name__
        step_index = self._step_counter.get(step_name, 0)
        self._step_counter[step_name] = step_index + 1

        cached = await self._state.get_step_result(self.instance_id, step_name, step_index)
        if cached is not None:
            return_type = fn.__annotations__.get("return")
            if return_type and issubclass(return_type, BaseModel):
                return return_type.model_validate(cached)
            return cached

        retry_cfg = retry or self._step_defaults
        last_error: Exception | None = None

        with self._telemetry.step_span(self.instance_id, step_name, step_index):
            for attempt in range(1, retry_cfg.max_retries + 2):
                try:
                    result = await fn(StepContext(self.instance_id, step_name), input_model)
                    output = result.model_dump() if isinstance(result, BaseModel) else result
                    await self._state.save_step_result(
                        self.instance_id, step_name, step_index,
                        input_model.model_dump(), output,
                    )
                    return result
                except Exception as exc:
                    last_error = exc
                    await self._state.save_step_error(
                        self.instance_id, step_name, step_index,
                        input_model.model_dump(), traceback.format_exc(), attempt,
                    )
                    if attempt <= retry_cfg.max_retries:
                        delay = min(
                            retry_cfg.initial_delay_seconds * (2 ** (attempt - 1)),
                            retry_cfg.max_delay_seconds,
                        )
                        await asyncio.sleep(delay)

        raise StepExecutionError(step_name, last_error)  # type: ignore[arg-type]


class StepContext:
    """Passed to step functions — provides workflow context without step primitives."""

    def __init__(self, workflow_id: str, step_name: str) -> None:
        self.workflow_id = workflow_id
        self.step_name = step_name
