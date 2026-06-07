from __future__ import annotations

import asyncio
import random
import traceback
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TypeVar, get_type_hints

from pydantic import BaseModel

from pgflows.exceptions import StepExecutionError
from pgflows.logger import get_logger
from pgflows.plugins import PgflowsPlugin, StepEvent, fire
from pgflows.telemetry import PgflowsTelemetry
from pgflows.types import RetryConfig

if TYPE_CHECKING:
    from pgflows.backends.pg_state import PgStateBackend
    from pgflows.registry import WorkflowRegistry

_log = get_logger("context")
T = TypeVar("T", bound=BaseModel)


class WorkflowContext:
    """Passed to workflow functions. Drives step execution with checkpoint replay.

    On replay (e.g. after a worker crash), a completed step returns its cached
    output without re-executing, making workflows idempotent.
    """

    def __init__(
        self,
        instance_id: str,
        workflow_name: str,
        state_backend: PgStateBackend,
        telemetry: PgflowsTelemetry,
        step_defaults: RetryConfig | None = None,
        plugins: list[PgflowsPlugin] | None = None,
        registry: WorkflowRegistry | None = None,
    ) -> None:
        self.instance_id = instance_id
        self.workflow_name = workflow_name
        self._state = state_backend
        self._telemetry = telemetry
        self._step_defaults = step_defaults or RetryConfig()
        self._step_counter: dict[str, int] = {}
        self._plugins = plugins or []
        self._registry = registry

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
            _log.debug(
                "step replay: instance=%s step=%s[%d]", self.instance_id, step_name, step_index
            )
            try:
                hints = get_type_hints(fn)
            except Exception:
                hints = {}
            return_hint = hints.get("return")
            if return_hint and isinstance(return_hint, type) and issubclass(return_hint, BaseModel):
                return return_hint.model_validate(cached)
            return cached

        retry_cfg = retry or self._get_registered_retry(fn) or self._step_defaults
        last_error: Exception | None = None

        with self._telemetry.step_span(self.instance_id, step_name, step_index):
            for attempt in range(1, retry_cfg.max_retries + 2):
                event = StepEvent(
                    instance_id=self.instance_id,
                    workflow_name=self.workflow_name,
                    step_name=step_name,
                    step_index=step_index,
                    attempt=attempt,
                )
                try:
                    _log.debug(
                        "step execute: instance=%s step=%s[%d] attempt=%d",
                        self.instance_id,
                        step_name,
                        step_index,
                        attempt,
                    )
                    await fire(self._plugins, "before_step", event, input_model)
                    ctx = StepContext(self.instance_id, step_name)
                    timeout = self._get_step_timeout(fn)
                    if timeout is not None:
                        result = await asyncio.wait_for(fn(ctx, input_model), timeout=timeout)
                    else:
                        result = await fn(ctx, input_model)
                    output = result.model_dump() if isinstance(result, BaseModel) else result
                    await self._state.save_step_result(
                        self.instance_id,
                        step_name,
                        step_index,
                        input_model.model_dump(),
                        output,
                    )
                    await fire(self._plugins, "after_step", event, result)
                    return result
                except Exception as exc:
                    last_error = exc
                    _log.warning(
                        "step failed: instance=%s step=%s attempt=%d error=%s",
                        self.instance_id,
                        step_name,
                        attempt,
                        exc,
                    )
                    await self._state.save_step_error(
                        self.instance_id,
                        step_name,
                        step_index,
                        input_model.model_dump(),
                        traceback.format_exc(),
                        attempt,
                    )
                    await fire(self._plugins, "on_step_error", event, exc)
                    if attempt <= retry_cfg.max_retries:
                        base = retry_cfg.initial_delay_seconds
                        if retry_cfg.backoff == "exponential":
                            delay = base * (2 ** (attempt - 1))
                        else:
                            delay = base * attempt
                        delay = min(delay, retry_cfg.max_delay_seconds)
                        if retry_cfg.jitter:
                            delay *= random.uniform(0.5, 1.0)
                        await asyncio.sleep(delay)
            assert last_error is not None
            raise StepExecutionError(step_name, last_error) from last_error

    def _get_registered_retry(self, fn: Callable) -> RetryConfig | None:
        if self._registry is None:
            return None
        step_defn = self._registry.get_step_by_function(fn)
        if step_defn is None or not step_defn.retry_overridden:
            return None
        return step_defn.retry

    def _get_step_timeout(self, fn: Callable) -> float | None:
        if self._registry is None:
            return None
        step_defn = self._registry.get_step_by_function(fn)
        return step_defn.timeout_seconds if step_defn else None


class StepContext:
    """Passed to step functions — provides workflow metadata without step primitives."""

    def __init__(self, instance_id: str, step_name: str) -> None:
        self.instance_id = instance_id
        self.step_name = step_name
