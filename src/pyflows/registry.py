from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import get_type_hints

from pyflows.types import RetryConfig


@dataclass
class StepDefinition:
    name: str
    fn: Callable
    input_type: type
    output_type: type
    retry: RetryConfig = field(default_factory=RetryConfig)
    retry_overridden: bool = False
    timeout_seconds: float | None = None


@dataclass
class WorkflowDefinition:
    name: str
    fn: Callable
    input_type: type
    output_type: type
    step_defaults: RetryConfig = field(default_factory=RetryConfig)


class WorkflowRegistry:
    def __init__(self) -> None:
        self._workflows: dict[str, WorkflowDefinition] = {}
        self._steps: dict[str, StepDefinition] = {}

    @staticmethod
    def _extract_types(fn: Callable) -> tuple[type, type]:
        """Return (input_type, output_type) from a workflow/step function signature."""
        hints = get_type_hints(fn)
        params = list(inspect.signature(fn).parameters.values())
        # params[0] = ctx, params[1] = input
        input_type = hints.get(params[1].name) if len(params) > 1 else dict
        return input_type, hints.get("return", dict)

    def register_step(
        self,
        fn: Callable,
        name: str | None = None,
        retry: RetryConfig | None = None,
        timeout_seconds: float | None = None,
    ) -> StepDefinition:
        step_name = name or fn.__name__
        input_type, output_type = self._extract_types(fn)
        defn = StepDefinition(
            name=step_name,
            fn=fn,
            input_type=input_type,
            output_type=output_type,
            retry=retry or RetryConfig(),
            retry_overridden=retry is not None,
            timeout_seconds=timeout_seconds,
        )
        self._steps[step_name] = defn
        return defn

    def register_workflow(
        self,
        fn: Callable,
        name: str | None = None,
        step_defaults: RetryConfig | None = None,
    ) -> WorkflowDefinition:
        wf_name = name or fn.__name__
        input_type, output_type = self._extract_types(fn)
        defn = WorkflowDefinition(
            name=wf_name,
            fn=fn,
            input_type=input_type,
            output_type=output_type,
            step_defaults=step_defaults or RetryConfig(),
        )
        self._workflows[wf_name] = defn
        return defn

    def get_step(self, name: str) -> StepDefinition:
        if name not in self._steps:
            raise KeyError(f"Step '{name}' not registered")
        return self._steps[name]

    def get_step_by_function(self, fn: Callable) -> StepDefinition | None:
        for defn in self._steps.values():
            if defn.fn is fn:
                return defn
        return None

    def get_workflow(self, name: str) -> WorkflowDefinition:
        if name not in self._workflows:
            raise KeyError(f"Workflow '{name}' not registered")
        return self._workflows[name]

    def list_workflows(self) -> list[str]:
        return list(self._workflows.keys())

    def list_steps(self) -> list[str]:
        return list(self._steps.keys())
