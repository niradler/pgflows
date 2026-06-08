from __future__ import annotations

import pytest

from pgflows.graph import GraphSpec
from pgflows.graph_compiler import GraphCompileError, compile_graph


def _compile(spec: dict) -> str:
    return str(
        compile_graph(
            GraphSpec.model_validate(spec),
            step_queue="q",
            notify_channel="ch",
        )
    )


def test_single_step_seeds_input_var_and_captures():
    sql = _compile({"root": {"type": "step", "step": "double_it"}})
    # first step receives the {input} durable var
    assert "''input'',''{input}''::jsonb" in sql
    # enqueues on the configured queue/channel
    assert "pgmq.send(''q''" in sql
    assert "pg_notify(''ch''" in sql
    # captures its output
    assert "|=> 'pgflows_double_it_1'" in sql


def test_sequence_threads_previous_capture_into_next_input():
    sql = _compile(
        {
            "root": {
                "type": "sequence",
                "nodes": [
                    {"type": "step", "step": "double_it"},
                    {"type": "step", "step": "add_ten"},
                ],
            }
        }
    )
    # second step's input is the first step's capture, not {input}
    assert "''input'',$pgflows_double_it_1::jsonb" in sql
    assert "\n    ~> " in sql  # sequenced


def test_explicit_input_overrides_threading():
    sql = _compile(
        {
            "root": {
                "type": "sequence",
                "nodes": [
                    {"type": "step", "step": "a"},
                    {"type": "step", "step": "b", "input": "'{\"k\":1}'::jsonb"},
                ],
            }
        }
    )
    assert "''input'','{\"k\":1}''::jsonb" not in sql  # sanity: quoting handled by builder
    assert "k" in sql


def test_custom_capture_name_used():
    sql = _compile({"root": {"type": "step", "step": "a", "capture": "myout"}})
    assert "|=> 'myout'" in sql


def test_parallel_two_branches_join_and_merge():
    sql = _compile(
        {
            "root": {
                "type": "sequence",
                "nodes": [
                    {
                        "type": "parallel",
                        "branches": [
                            {"type": "step", "step": "a"},
                            {"type": "step", "step": "b"},
                        ],
                    },
                    {"type": "step", "step": "merge"},
                ],
            }
        }
    )
    assert ") & (" in sql  # pairwise join
    # downstream step receives a merged object of both branch captures
    assert "jsonb_build_object(''b0'', ($pgflows_a_1::jsonb), ''b1'', ($pgflows_b_2::jsonb))" in sql


def test_parallel_three_branches_uses_join3():
    sql = _compile(
        {
            "root": {
                "type": "parallel",
                "branches": [
                    {"type": "step", "step": "a"},
                    {"type": "step", "step": "b"},
                    {"type": "step", "step": "c"},
                ],
            }
        }
    )
    assert "df.join3(" in sql


def test_race_uses_or_operator():
    sql = _compile(
        {
            "root": {
                "type": "parallel",
                "mode": "race",
                "branches": [
                    {"type": "step", "step": "a"},
                    {"type": "step", "step": "b"},
                ],
            }
        }
    )
    assert ") | (" in sql


def test_branch_emits_conditional_with_else():
    sql = _compile(
        {
            "root": {
                "type": "branch",
                "condition": {"step": "is_big"},
                "then": {"type": "step", "step": "big"},
                "else": {"type": "step", "step": "small"},
            }
        }
    )
    assert "?>" in sql and "!>" in sql
    # condition truthiness reads ->>'result' then falls back to whole-value text
    assert "->>''result''" in sql


def test_branch_without_else_omits_else_arm():
    sql = _compile(
        {
            "root": {
                "type": "branch",
                "condition": {"step": "c"},
                "then": {"type": "step", "step": "t"},
            }
        }
    )
    assert "?>" in sql
    assert "!>" not in sql


def test_loop_with_while_uses_df_loop():
    sql = _compile(
        {
            "root": {
                "type": "loop",
                "body": {"type": "step", "step": "tick"},
                "while": {"step": "again"},
            }
        }
    )
    assert "df.loop(" in sql


def test_infinite_loop_uses_at_operator():
    sql = _compile({"root": {"type": "loop", "body": {"type": "step", "step": "tick"}}})
    assert sql.lstrip().startswith("@>")


def test_sleep_and_wait_nodes():
    sql = _compile(
        {
            "root": {
                "type": "sequence",
                "nodes": [
                    {"type": "sleep", "seconds": 3},
                    {"type": "wait_schedule", "cron": "* * * * *"},
                    {"type": "wait_signal", "signal": "go"},
                ],
            }
        }
    )
    assert "df.sleep(3)" in sql
    assert "df.wait_for_schedule('* * * * *')" in sql
    assert "df.wait_for_signal('go')" in sql


# ---------------------------------------------------------------------------
# Composition-limit guard
# ---------------------------------------------------------------------------


def test_loop_and_parallel_coexistence_rejected():
    with pytest.raises(GraphCompileError, match="loop and a parallel"):
        _compile(
            {
                "root": {
                    "type": "sequence",
                    "nodes": [
                        {"type": "loop", "body": {"type": "step", "step": "x"}},
                        {
                            "type": "parallel",
                            "branches": [
                                {"type": "step", "step": "a"},
                                {"type": "step", "step": "b"},
                            ],
                        },
                    ],
                }
            }
        )


def test_non_terminal_race_rejected():
    with pytest.raises(GraphCompileError, match="race.*terminal"):
        _compile(
            {
                "root": {
                    "type": "sequence",
                    "nodes": [
                        {
                            "type": "parallel",
                            "mode": "race",
                            "branches": [
                                {"type": "step", "step": "a"},
                                {"type": "step", "step": "b"},
                            ],
                        },
                        {"type": "step", "step": "after"},
                    ],
                }
            }
        )


def test_terminal_race_allowed():
    # race as the root (tail) compiles fine
    _compile(
        {
            "root": {
                "type": "parallel",
                "mode": "race",
                "branches": [
                    {"type": "step", "step": "a"},
                    {"type": "step", "step": "b"},
                ],
            }
        }
    )


def test_empty_sequence_rejected():
    with pytest.raises(GraphCompileError, match="at least one"):
        _compile({"root": {"type": "sequence", "nodes": []}})


def test_parallel_single_branch_rejected():
    with pytest.raises(GraphCompileError, match="at least 2"):
        _compile({"root": {"type": "parallel", "branches": [{"type": "step", "step": "a"}]}})
