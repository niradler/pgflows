from __future__ import annotations

import json


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
        return DslNode(f"({self._sql}) |=> '{name}'")

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
        return DslNode(f"df.wait_for_signal('{name}', {timeout_seconds})")
    return DslNode(f"df.wait_for_signal('{name}')")


def wait_for_schedule(cron: str) -> DslNode:
    """Pause until the next tick of a cron expression."""
    return DslNode(f"df.wait_for_schedule('{cron}')")


def http(
    url: str,
    method: str = "POST",
    body: str | None = None,
    headers: dict[str, str] | None = None,
    timeout_seconds: int = 30,
) -> DslNode:
    """Build a df.http() DSL node."""
    body_arg = f"'{body}'" if body is not None else "NULL"
    headers_arg = f"'{json.dumps(headers)}'::jsonb" if headers is not None else "NULL"
    return DslNode(
        f"df.http('{url}', '{method}', {body_arg}, {headers_arg}, {timeout_seconds})"
    )


def loop(body: DslNode, condition: DslNode | None = None) -> DslNode:
    """Infinite loop (@>) or while-loop (df.loop with condition)."""
    if condition is None:
        return DslNode(f"@> ({body._sql})")
    return DslNode(f"df.loop({body._sql}, {condition._sql})")


__all__ = [
    "DslNode",
    "http",
    "loop",
    "sleep",
    "sql_node",
    "wait_for_schedule",
    "wait_for_signal",
]
