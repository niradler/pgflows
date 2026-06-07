from __future__ import annotations

import pytest

from pgflows.dsl import (
    DslNode,
    break_,
    http,
    if_node,
    if_rows,
    join3,
    loop,
    pgmq_step,
    sleep,
    sql_node,
    wait_for_schedule,
    wait_for_signal,
)

# ---------------------------------------------------------------------------
# DslNode operators
# ---------------------------------------------------------------------------


def test_rshift_sequence():
    a = DslNode("A")
    b = DslNode("B")
    result = a >> b
    assert str(result) == "A\n    ~> B"


def test_and_parallel():
    a = DslNode("A")
    b = DslNode("B")
    result = a & b
    assert str(result) == "(A) & (B)"


def test_or_race():
    a = DslNode("A")
    b = DslNode("B")
    result = a | b
    assert str(result) == "(A) | (B)"


def test_capture():
    node = DslNode("A")
    result = node.capture("my_var")
    assert str(result) == "(A) |=> 'my_var'"


def test_if_then_no_else():
    cond = DslNode("COND")
    then = DslNode("THEN")
    result = cond.if_then(then)
    assert str(result) == "(COND) ?> (THEN)"


def test_if_then_with_else():
    cond = DslNode("COND")
    then = DslNode("THEN")
    else_ = DslNode("ELSE")
    result = cond.if_then(then, else_)
    assert str(result) == "(COND) ?> (THEN) !> (ELSE)"


def test_repr():
    node = DslNode("X")
    assert repr(node) == "DslNode('X')"


# ---------------------------------------------------------------------------
# Builder functions
# ---------------------------------------------------------------------------


def test_sleep():
    node = sleep(10)
    assert str(node) == "df.sleep(10)"


def test_wait_for_signal_no_timeout():
    node = wait_for_signal("my-signal")
    assert str(node) == "df.wait_for_signal('my-signal')"


def test_wait_for_signal_with_timeout():
    node = wait_for_signal("ready", timeout_seconds=60)
    assert str(node) == "df.wait_for_signal('ready', 60)"


def test_wait_for_schedule():
    node = wait_for_schedule("0 9 * * 1-5")
    assert str(node) == "df.wait_for_schedule('0 9 * * 1-5')"


def test_http_minimal():
    node = http("http://example.com/step")
    assert str(node) == "df.http('http://example.com/step', 'POST', NULL, NULL, 30)"


def test_http_with_body():
    node = http("http://example.com/step", method="GET", body='{"key":"val"}')
    assert "'GET'" in str(node)
    assert '\'{"key":"val"}\'' in str(node)


def test_http_with_headers():
    headers = {"Authorization": "Bearer tok"}
    node = http("http://example.com/step", headers=headers)
    raw = str(node)
    assert "::jsonb" in raw
    assert "Authorization" in raw


def test_http_custom_timeout():
    node = http("http://example.com/step", timeout_seconds=120)
    assert str(node).endswith("120)")


def test_loop_infinite():
    body = DslNode("BODY")
    node = loop(body)
    assert str(node) == "@> (BODY)"


def test_loop_with_condition():
    body = DslNode("BODY")
    cond = DslNode("COND")
    node = loop(body, cond)
    assert str(node) == "df.loop(BODY, COND)"


def test_sql_node_escapes_quotes():
    node = sql_node("SELECT 'hello'")
    assert str(node) == "'SELECT ''hello'''"


def test_sql_node_plain():
    node = sql_node("SELECT 1")
    assert str(node) == "'SELECT 1'"


# ---------------------------------------------------------------------------
# Chaining
# ---------------------------------------------------------------------------


def test_chain_sequence_and_parallel():
    a = DslNode("A")
    b = DslNode("B")
    c = DslNode("C")
    chain = a >> (b & c)
    assert "~>" in str(chain)
    assert "& " in str(chain) or "&" in str(chain)


# ---------------------------------------------------------------------------
# New DSL builders
# ---------------------------------------------------------------------------


def test_join3():
    a, b, c = DslNode("A"), DslNode("B"), DslNode("C")
    node = join3(a, b, c)
    assert str(node) == "df.join3(A, B, C)"


def test_if_node():
    cond = DslNode("COND")
    then = DslNode("THEN")
    else_ = DslNode("ELSE")
    node = if_node(cond, then, else_)
    assert str(node) == "(COND) ?> (THEN) !> (ELSE)"


def test_if_rows():
    then = DslNode("THEN")
    else_ = DslNode("ELSE")
    node = if_rows("my_result", then, else_)
    assert str(node) == "df.if_rows('my_result', THEN, ELSE)"


def test_if_rows_escapes_name():
    then = DslNode("T")
    else_ = DslNode("E")
    node = if_rows("it's result", then, else_)
    assert "it''s result" in str(node)


def test_break_no_value():
    node = break_()
    assert str(node) == "df.break()"


def test_break_with_json_value():
    node = break_('{"status": "done"}')
    assert str(node) == """df.break('{"status": "done"}')"""


def test_break_escapes_quotes():
    node = break_("it's done")
    assert "it''s done" in str(node)


# ---------------------------------------------------------------------------
# pgmq_step — native SQL => pgmq => NOTIFY => wait_for_signal
# ---------------------------------------------------------------------------


def test_pgmq_step_emits_enqueue_notify_and_wait():
    sql = str(pgmq_step("charge_card"))
    assert "pgmq.send(" in sql
    assert "pg_notify(" in sql
    assert "df.wait_for_signal(" in sql
    # three nodes sequenced
    assert sql.count("~>") == 2


def test_pgmq_step_carries_step_instance_signal_and_input():
    sql = str(pgmq_step("charge_card"))
    assert "charge_card" in sql
    assert "{sys_instance_id}" in sql
    assert "__pgflows_charge_card" in sql  # default signal name
    assert "{input}" in sql  # default input expression


def test_pgmq_step_custom_signal_queue_and_channel():
    sql = str(
        pgmq_step("notify", signal="done_42", queue="my_steps", notify_channel="bell")
    )
    assert "done_42" in sql
    assert "my_steps" in sql
    assert "bell" in sql


def test_pgmq_step_capture_wraps_with_name():
    sql = str(pgmq_step("charge_card", capture="charge_result"))
    assert "|=> 'charge_result'" in sql


def test_pgmq_step_quotes_doubled_for_sql_literal():
    # The enqueue node is itself a single-quoted DSL string, so its inner quotes double.
    sql = str(pgmq_step("charge_card"))
    assert "''step''" in sql
    assert "''instance_id''" in sql


def test_pgmq_step_rejects_unsafe_step_name():
    with pytest.raises(ValueError, match="step_name"):
        pgmq_step("bad'; DROP TABLE--")


def test_pgmq_step_rejects_unsafe_queue():
    with pytest.raises(ValueError, match="queue"):
        pgmq_step("ok", queue="bad name")


def test_pgmq_step_composes_with_operators():
    node = pgmq_step("a") >> pgmq_step("b")
    sql = str(node)
    assert sql.index("'a'") < sql.index("'b'") if "'a'" in sql else True
    assert sql.count("pgmq.send(") == 2
