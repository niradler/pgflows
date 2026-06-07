from __future__ import annotations

import json
import re

_VALID_HTTP_METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE"}
_SAFE_IDENT = re.compile(r"^[A-Za-z0-9_]+$")


def _q(s: str) -> str:
    """Escape a string for safe embedding in a PostgreSQL single-quoted literal."""
    return "'" + s.replace("'", "''") + "'"


def _require_ident(value: str, label: str) -> None:
    if not _SAFE_IDENT.fullmatch(value):
        raise ValueError(
            f"{label} must contain only letters, digits, and underscores; got {value!r}"
        )


class DslNode:
    """Represents a pg_durable DSL expression (a SQL-level TEXT value)."""

    def __init__(self, sql: str) -> None:
        self._sql = sql

    def __str__(self) -> str:
        return self._sql

    def __repr__(self) -> str:
        return f"DslNode({self._sql!r})"

    def __rshift__(self, other: DslNode) -> DslNode:
        """Sequence: self ~> other"""
        return DslNode(f"{self._sql}\n    ~> {other._sql}")

    def __and__(self, other: DslNode) -> DslNode:
        """Parallel join (wait for ALL): (self) & (other)"""
        return DslNode(f"({self._sql}) & ({other._sql})")

    def __or__(self, other: DslNode) -> DslNode:
        """Race (first wins): (self) | (other)"""
        return DslNode(f"({self._sql}) | ({other._sql})")

    def capture(self, name: str) -> DslNode:
        """Capture result as named variable: (self) |=> 'name'"""
        return DslNode(f"({self._sql}) |=> {_q(name)}")

    def if_then(self, then: DslNode, else_: DslNode | None = None) -> DslNode:
        """Conditional: self ?> then  or  self ?> then !> else_"""
        if else_ is None:
            return DslNode(f"({self._sql}) ?> ({then._sql})")
        return DslNode(f"({self._sql}) ?> ({then._sql}) !> ({else_._sql})")


def sql_node(query: str) -> DslNode:
    """Wrap a SQL query as a DSL node (auto-escapes single quotes)."""
    escaped = query.replace("'", "''")
    return DslNode(f"'{escaped}'")


def sleep(seconds: int) -> DslNode:
    """Sleep for a fixed number of seconds inside a pg_durable flow."""
    return DslNode(f"df.sleep({seconds})")


def wait_for_signal(name: str, timeout_seconds: int | None = None) -> DslNode:
    """Pause execution until the named signal arrives."""
    if timeout_seconds is not None:
        if not isinstance(timeout_seconds, int):
            raise TypeError("timeout_seconds must be int")
        return DslNode(f"df.wait_for_signal({_q(name)}, {timeout_seconds})")
    return DslNode(f"df.wait_for_signal({_q(name)})")


def wait_for_schedule(cron: str) -> DslNode:
    """Pause until the next tick of a cron expression."""
    return DslNode(f"df.wait_for_schedule({_q(cron)})")


def http(
    url: str,
    method: str = "POST",
    body: str | None = None,
    headers: dict[str, str] | None = None,
    timeout_seconds: int = 30,
) -> DslNode:
    """Build a df.http() DSL node."""
    if method.upper() not in _VALID_HTTP_METHODS:
        raise ValueError(f"method must be one of {_VALID_HTTP_METHODS}, got {method!r}")
    if not isinstance(timeout_seconds, int):
        raise TypeError("timeout_seconds must be int")
    body_arg = _q(body) if body is not None else "NULL"
    headers_arg = f"{_q(json.dumps(headers))}::jsonb" if headers is not None else "NULL"
    return DslNode(
        f"df.http({_q(url)}, {_q(method.upper())}, {body_arg}, {headers_arg}, {timeout_seconds})"
    )


_RESULTS_TABLE = re.compile(r"^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)?$")


def pgmq_step(
    step_name: str,
    *,
    result_key: str | None = None,
    input_expr: str = "'{input}'::jsonb",
    queue: str = "pgflows_steps",
    notify_channel: str | None = None,
    capture: str | None = None,
    results_table: str = "pgflows.pgmq_step_results",
    poll_seconds: int = 1,
) -> DslNode:
    """Build a native SQL → pgmq → NOTIFY → poll-result step.

    Emits a race-free request/response against a Python StepWorker:

      1. ``pgmq.send`` — durably enqueues ``{step, instance_id, result_key, input}``
         onto ``queue`` for a StepWorker to pick up.
      2. ``pg_notify`` — rings the doorbell on ``notify_channel`` so a listening
         StepWorker wakes immediately instead of waiting for its poll tick.
      3. ``df.loop(df.sleep, …)`` — polls ``results_table`` until the worker inserts
         the result row keyed by ``result_key`` (the row persists, so the worker can
         never "win the race" and drop the result, unlike a fire-and-forget signal).
      4. ``SELECT result FROM results_table`` — reads the output back.

    When captured, ``df`` substitutes ``$capture`` with the read node's first-column
    value — which is exactly the step's output — so a later step can thread it as
    ``input_expr="$capture::jsonb"``.
    """
    _require_ident(step_name, "step_name")
    _require_ident(queue, "queue")
    chan = notify_channel or queue
    _require_ident(chan, "notify_channel")
    if capture is not None:
        _require_ident(capture, "capture")
    if not _RESULTS_TABLE.fullmatch(results_table):
        raise ValueError(
            f"results_table must be a [schema.]table identifier; got {results_table!r}"
        )
    if not isinstance(poll_seconds, int):
        raise TypeError("poll_seconds must be int")
    key = result_key or f"{{sys_instance_id}}:{step_name}"

    enqueue_sql = (
        f"SELECT pgmq.send('{queue}', json_build_object("
        f"'step','{step_name}',"
        f"'instance_id','{{sys_instance_id}}',"
        f"'result_key','{key}',"
        f"'input',{input_expr})::jsonb)"
    )
    notify_sql = f"SELECT pg_notify('{chan}','{{sys_instance_id}}')"
    poll = loop(
        sleep(poll_seconds),
        sql_node(f"SELECT NOT EXISTS(SELECT 1 FROM {results_table} WHERE key = '{key}')"),
    )
    read = sql_node(f"SELECT result FROM {results_table} WHERE key = '{key}'")
    node = sql_node(enqueue_sql) >> sql_node(notify_sql) >> poll >> read
    if capture is not None:
        node = node.capture(capture)
    return node


def loop(body: DslNode, condition: DslNode | None = None) -> DslNode:
    """Infinite loop (@>) or while-loop (df.loop with condition)."""
    if condition is None:
        return DslNode(f"@> ({body._sql})")
    return DslNode(f"df.loop({body._sql}, {condition._sql})")


def join3(a: DslNode, b: DslNode, c: DslNode) -> DslNode:
    """Three-way parallel join — waits for all three to complete."""
    return DslNode(f"df.join3({a._sql}, {b._sql}, {c._sql})")


def if_node(condition: DslNode, then: DslNode, else_: DslNode) -> DslNode:
    """Conditional: condition ?> then !> else_ (standalone, both branches required)."""
    return DslNode(f"({condition._sql}) ?> ({then._sql}) !> ({else_._sql})")


def if_rows(result_name: str, then: DslNode, else_: DslNode) -> DslNode:
    """Branch on whether a captured result has rows — no SQL executed, in-memory check."""
    return DslNode(f"df.if_rows({_q(result_name)}, {then._sql}, {else_._sql})")


def break_(value: str | None = None) -> DslNode:
    """Exit the enclosing loop. value is literal JSON (not auto-wrapped)."""
    if value is None:
        return DslNode("df.break()")
    return DslNode(f"df.break({_q(value)})")


__all__ = [
    "DslNode",
    "break_",
    "http",
    "if_node",
    "if_rows",
    "join3",
    "loop",
    "pgmq_step",
    "sleep",
    "sql_node",
    "wait_for_schedule",
    "wait_for_signal",
]
