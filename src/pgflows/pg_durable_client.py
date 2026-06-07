from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import asyncpg
from pydantic import BaseModel

from pgflows.dsl import DslNode


def _loads(value: Any) -> Any:
    """Best-effort decode of a pg_durable JSON column.

    df.* monitoring functions hand JSON back as text; asyncpg may already have
    decoded it. Tolerate str / already-parsed / None without raising — these are
    read-only audit reads where a raw fallback is more useful than a hard failure.
    """
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, ValueError):
            return value
    return value


class InstanceInfo(BaseModel):
    """Header record for one durable instance (df.instance_info)."""

    instance_id: str
    label: str | None = None
    function_name: str | None = None
    function_version: str | None = None
    current_execution_id: int | None = None
    status: str
    output: Any | None = None


class InstanceNode(BaseModel):
    """One node in the per-execution graph trail (df.instance_nodes)."""

    execution_id: int | None = None
    node_id: str
    node_type: str
    query: str | None = None
    result_name: str | None = None
    left_node: str | None = None
    right_node: str | None = None
    status: str
    result: Any | None = None
    updated_at: datetime | None = None


class ExecutionRecord(BaseModel):
    """One replay execution of an instance (df.instance_executions)."""

    execution_id: int | None = None
    status: str
    event_count: int | None = None
    duration_ms: int | None = None
    output: Any | None = None


class Metrics(BaseModel):
    """Cluster-wide durable-function counters (df.metrics)."""

    total_instances: int = 0
    running_instances: int = 0
    completed_instances: int = 0
    failed_instances: int = 0
    total_executions: int = 0
    total_events: int = 0


class PgDurableClient:
    """Execute pg_durable SQL functions against a connected Postgres pool.

    Requires the pg_durable (df) extension to be installed.
    Obtain via app.pg_durable after app.initialize().
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def setvar(self, name: str, value: str) -> None:
        """Set a durable function variable (captured at df.start() time)."""
        async with self._pool.acquire() as conn:
            await conn.execute("SELECT df.setvar($1, $2)", name, value)

    async def start(
        self,
        node: DslNode | str,
        label: str | None = None,
        database: str | None = None,
    ) -> str:
        """Start a durable function. Returns the 8-char instance ID.

        The DSL expression is interpolated into the statement (not bound) because
        ``~>``, ``|=>``, ``df.http()`` etc. are Postgres operators/functions that
        Postgres must EVALUATE to build the function graph — a bound text parameter
        would reach df as inert text and fail to parse. A plain ``str`` is treated as
        a single raw SQL query and wrapped as a SQL node literal. ``label`` and
        ``database`` are still bound parameters.
        """
        dsl = self._as_expr(node)
        async with self._pool.acquire() as conn:
            if label is not None and database is not None:
                row = await conn.fetchrow(
                    f"SELECT df.start({dsl}, $1, $2)", label, database
                )
            elif label is not None:
                row = await conn.fetchrow(f"SELECT df.start({dsl}, $1)", label)
            elif database is not None:
                row = await conn.fetchrow(
                    f"SELECT df.start({dsl}, NULL, $1)", database
                )
            else:
                row = await conn.fetchrow(f"SELECT df.start({dsl})")
        return row[0]

    @staticmethod
    def _as_expr(node: DslNode | str) -> str:
        """Render node as a SQL expression. DslNode → its operator expression;
        a plain str → a single-quoted SQL node literal."""
        if isinstance(node, DslNode):
            return str(node)
        return "'" + node.replace("'", "''") + "'"

    async def cancel(self, instance_id: str, reason: str = "Cancelled by user") -> None:
        """Cancel a running or pending durable function instance."""
        async with self._pool.acquire() as conn:
            await conn.execute("SELECT df.cancel($1, $2)", instance_id, reason)

    async def signal(
        self,
        instance_id: str,
        signal_name: str,
        data: Any | None = None,
    ) -> None:
        """Send a named signal to a waiting durable function instance."""
        payload = json.dumps(data) if data is not None else "{}"
        async with self._pool.acquire() as conn:
            await conn.execute(
                "SELECT df.signal($1, $2, $3)", instance_id, signal_name, payload
            )

    async def status(self, instance_id: str) -> str:
        """Return instance status: pending, running, completed, failed, cancelled."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT df.status($1)", instance_id)
            return row[0]

    async def result(self, instance_id: str) -> Any:
        """Return the final result of a completed instance (parsed JSON)."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT df.result($1)", instance_id)
            return json.loads(row[0]) if row and row[0] else None

    async def explain(self, input: DslNode | str) -> str:
        """Visualize a DSL expression or inspect a running instance.

        A DslNode is interpolated and evaluated (so operators build a graph); a plain
        str is bound as a parameter (e.g. an instance ID).
        """
        async with self._pool.acquire() as conn:
            if isinstance(input, DslNode):
                row = await conn.fetchrow(f"SELECT df.explain({input})")
            else:
                row = await conn.fetchrow("SELECT df.explain($1)", input)
            return row[0]

    async def list_instances(
        self, status: str | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        """List durable function instances, optionally filtered by status."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM df.list_instances($1, $2)", status, limit
            )
            return [dict(r) for r in rows]

    async def instance_info(self, instance_id: str) -> InstanceInfo | None:
        """Header record for one instance (label, function, current execution, output)."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM df.instance_info($1)", instance_id)
        if row is None:
            return None
        data = dict(row)
        data["output"] = _loads(data.get("output"))
        return InstanceInfo.model_validate(data)

    async def instance_nodes(
        self, instance_id: str, last_n_executions: int = 5
    ) -> list[InstanceNode]:
        """Per-node execution trail — the durable 'session history' of a run.

        Returns more rows than user-written nodes: the graph expands into
        structural nodes (THEN/JOIN/IF) that each get their own row.
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM df.instance_nodes($1, $2)", instance_id, last_n_executions
            )
        nodes: list[InstanceNode] = []
        for r in rows:
            data = dict(r)
            data["result"] = _loads(data.get("result"))
            nodes.append(InstanceNode.model_validate(data))
        return nodes

    async def instance_executions(
        self, instance_id: str, limit_count: int = 5
    ) -> list[ExecutionRecord]:
        """Per-execution records (status, event_count, duration_ms, output)."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM df.instance_executions($1, $2)", instance_id, limit_count
            )
        records: list[ExecutionRecord] = []
        for r in rows:
            data = dict(r)
            data["output"] = _loads(data.get("output"))
            records.append(ExecutionRecord.model_validate(data))
        return records

    async def metrics(self) -> Metrics:
        """Cluster-wide aggregate counters across all durable instances."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM df.metrics()")
        return Metrics.model_validate(dict(row)) if row is not None else Metrics()

    async def getvar(self, name: str) -> str | None:
        """Get a variable owned by the current user."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT df.getvar($1)", name)
            return row[0] if row else None

    async def unsetvar(self, name: str) -> None:
        """Remove a variable owned by the current user."""
        async with self._pool.acquire() as conn:
            await conn.execute("SELECT df.unsetvar($1)", name)

    async def clearvars(self) -> None:
        """Clear all variables for the current user."""
        async with self._pool.acquire() as conn:
            await conn.execute("SELECT df.clearvars()")

    async def grant_usage(
        self,
        role_name: str,
        include_http: bool = False,
        with_grant: bool = False,
    ) -> None:
        """Grant pg_durable usage privileges to a role."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                "SELECT df.grant_usage($1, $2, $3)", role_name, include_http, with_grant
            )

    async def revoke_usage(self, role_name: str) -> None:
        """Revoke all pg_durable privileges from a role."""
        async with self._pool.acquire() as conn:
            await conn.execute("SELECT df.revoke_usage($1)", role_name)


__all__ = [
    "PgDurableClient",
    "InstanceInfo",
    "InstanceNode",
    "ExecutionRecord",
    "Metrics",
]
