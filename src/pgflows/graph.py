from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class StepNode(BaseModel):
    type: Literal["step"] = "step"
    step: str
    input: str | None = None
    capture: str | None = None


class SleepNode(BaseModel):
    type: Literal["sleep"] = "sleep"
    seconds: int


class WaitSignalNode(BaseModel):
    type: Literal["wait_signal"] = "wait_signal"
    signal: str
    timeout: int | None = None


class WaitScheduleNode(BaseModel):
    type: Literal["wait_schedule"] = "wait_schedule"
    cron: str


class Condition(BaseModel):
    step: str
    input: str | None = None


class SequenceNode(BaseModel):
    type: Literal["sequence"] = "sequence"
    nodes: list[Node]


class ParallelNode(BaseModel):
    type: Literal["parallel"] = "parallel"
    branches: list[Node]
    mode: Literal["all", "race"] = "all"


class BranchNode(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    type: Literal["branch"] = "branch"
    condition: Condition
    then: Node
    else_: Node | None = Field(default=None, alias="else")


class LoopNode(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    type: Literal["loop"] = "loop"
    body: Node
    while_: Condition | None = Field(default=None, alias="while")


Node = Annotated[
    StepNode
    | SleepNode
    | WaitSignalNode
    | WaitScheduleNode
    | SequenceNode
    | ParallelNode
    | BranchNode
    | LoopNode,
    Field(discriminator="type"),
]


class GraphSpec(BaseModel):
    """A data-driven workflow definition compiled to a pg_durable DSL graph."""

    version: int = 1
    # Seeds the single {input} durable var. Exactly one var is set on purpose —
    # >1 durable var triggers nondeterministic replay; all other data flows via captures.
    input: dict[str, Any] | None = None
    root: Node


for _model in (SequenceNode, ParallelNode, BranchNode, LoopNode, GraphSpec):
    _model.model_rebuild()
