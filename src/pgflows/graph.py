from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

# A typed, extensible workflow spec compiled to a pg_durable DSL graph (see
# graph_compiler.compile_graph). Each node is a member of a discriminated union on
# `type`; adding a capability is one new class here plus one compile case there.


class StepNode(BaseModel):
    """Run a registered Python step via the pgmq+NOTIFY worker binding."""

    type: Literal["step"] = "step"
    step: str
    # Optional jsonb SQL expression for this step's input (may reference $captures).
    # When omitted, the step receives its upstream node's output (see compiler threading).
    input: str | None = None
    # Optional explicit capture name for this step's output; auto-generated when omitted.
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
    """A registered step whose truthy output drives a branch/loop condition."""

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
