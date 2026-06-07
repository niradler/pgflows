from __future__ import annotations

from pgflows.dsl import DslNode, http, loop, sleep, sql_node, wait_for_schedule, wait_for_signal

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
