from __future__ import annotations

import json
from typing import Any

import asyncpg

from pgflows.dsl import DslNode


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


__all__ = ["PgDurableClient"]
