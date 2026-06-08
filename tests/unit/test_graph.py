from __future__ import annotations

import pytest
from pydantic import ValidationError

from pgflows.graph import (
    BranchNode,
    GraphSpec,
    LoopNode,
    ParallelNode,
    SequenceNode,
    StepNode,
)


def test_discriminated_union_parses_each_node_type():
    spec = GraphSpec.model_validate(
        {
            "root": {
                "type": "sequence",
                "nodes": [
                    {"type": "step", "step": "a"},
                    {"type": "sleep", "seconds": 5},
                    {"type": "wait_signal", "signal": "ok", "timeout": 60},
                    {"type": "wait_schedule", "cron": "0 * * * *"},
                ],
            }
        }
    )
    seq = spec.root
    assert isinstance(seq, SequenceNode)
    assert isinstance(seq.nodes[0], StepNode)
    assert seq.nodes[0].step == "a"


def test_unknown_node_type_rejected():
    with pytest.raises(ValidationError):
        GraphSpec.model_validate({"root": {"type": "bogus"}})


def test_branch_else_alias_round_trips():
    node = BranchNode.model_validate(
        {
            "type": "branch",
            "condition": {"step": "is_big"},
            "then": {"type": "step", "step": "big"},
            "else": {"type": "step", "step": "small"},
        }
    )
    assert isinstance(node.else_, StepNode)
    assert node.else_.step == "small"
    # serializes back to the JSON key "else", not "else_"
    assert "else" in node.model_dump(by_alias=True)


def test_loop_while_alias_round_trips():
    node = LoopNode.model_validate(
        {
            "type": "loop",
            "body": {"type": "step", "step": "tick"},
            "while": {"step": "again"},
        }
    )
    assert node.while_ is not None
    assert node.while_.step == "again"


def test_populate_by_name_allows_python_attr():
    # constructing in Python with else_/while_ also works (populate_by_name)
    node = BranchNode(
        condition={"step": "c"},
        then=StepNode(step="t"),
        else_=StepNode(step="e"),
    )
    assert node.else_.step == "e"


def test_parallel_defaults_to_all():
    node = ParallelNode.model_validate(
        {
            "type": "parallel",
            "branches": [{"type": "step", "step": "a"}, {"type": "step", "step": "b"}],
        }
    )
    assert node.mode == "all"


def test_json_schema_uses_aliases():
    schema = GraphSpec.model_json_schema(by_alias=True)
    assert schema["title"] == "GraphSpec"
    branch_props = schema["$defs"]["BranchNode"]["properties"]
    assert "else" in branch_props
    assert "else_" not in branch_props
