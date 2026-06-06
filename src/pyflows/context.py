from __future__ import annotations

import asyncio
import traceback
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TypeVar

from pydantic import BaseModel

from pyflows.exceptions import StepExecutionError
from pyflows.logger import get_logger
from pyflows.telemetry import PyflowsTelemetry
from pyflows.types import RetryConfig

if TYPE_CHECKING:
    from pyflows.backends.pg_state import PgStateBackend

_log = get_logger("context")
T = TypeVar("T", bound=BaseModel)


class WorkflowContext:
    """Passed to workflow functions. Drives step execution with checkpoint replay.

    On the first run, ctx.step() executes the step function and persists its
    output. On replay (e.g. after a worker crash), it returns the cached output
    without re-executing the function. This makes workflows idempotent.
    """

    def __init__(
        self,
        instance_id: str,
        workflow_name: str,
        state_backend: PgStateBackend,
        telemetry: PyflowsTelemetry,
        step_defaults: RetryConfig | None = None,
        plugins: list | None = None,  # accepted for backward-compat; not used here
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
        """Execute a step with replay-based checkpointing.

        If this step already completed in a prior execution, returns the
        cached output immediately (replay). Otherwise, executes fn and
        persists the result before returning.
        """
        step_name = name or fn.__name__
        step_index = self._step_counter.get(step_name, 0)
        self._step_counter[step_name] = step_index + 1

        cached = await self._state.get_step_result(self.instance_id, step_name, step_index)
        if cached is not None:
            _log.debug(
                "step replay: instance=%s step=%s[%d]", self.instance_id, step_name, step_index
            )
            return_hint = fn.__annotations__.get("return")
            if return_hint and isinstance(return_hint, type) and issubclass(return_hint, BaseModel):
                return return_hint.model_validate(cached)
            return cached

        retry_cfg = retry or self._step_defaults
        last_error: Exception | None = None

        with self._telemetry.step_span(self.instance_id, step_name, step_index):
            for attempt in range(1, retry_cfg.max_retries + 2):
                try:
                    _log.debug(
                        "step execute: instance=%s step=%s[%d] attempt=%d",
                        self.instance_id, step_name, step_index, attempt,
                    )
                    ctx = StepContext(self.instance_id, step_name)
                    result = await fn(ctx, input_model)
                    output = result.model_dump() if isinstance(result, BaseModel) else result
                    await self._state.save_step_result(
                        self.instance_id, step_name, step_index,
                        input_model.model_dump(), output,
                    )
                    return result
                except Exception as exc:
                    last_error = exc
                    _log.warning(
                        "step failed: instance=%s step=%s attempt=%d error=%s",
                        self.instance_id, step_name, attempt, exc,
                    )
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
    """Passed to step functions — provides workflow metadata without step primitives."""

    def __init__(self, workflow_id: str, step_name: str) -> None:
        self.workflow_id = workflow_id
        self.step_name = step_name
