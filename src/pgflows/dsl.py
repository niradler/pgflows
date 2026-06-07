from __future__ import annotations

import json

_VALID_HTTP_METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE"}


def _q(s: str) -> str:
    """Escape a string for safe embedding in a PostgreSQL single-quoted literal."""
    return "'" + s.replace("'", "''") + "'"


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
    "sleep",
    "sql_node",
    "wait_for_schedule",
    "wait_for_signal",
]
