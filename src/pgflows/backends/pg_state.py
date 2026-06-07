from __future__ import annotations

import json
from typing import Any

import asyncpg

from pgflows.backends.base import OrchestratorBackend
from pgflows.exceptions import BackendNotInitializedError, WorkflowNotFoundError
from pgflows.types import WorkflowState, WorkflowStatus


class PgStateBackend(OrchestratorBackend):
    """Stores workflow instance and step checkpoint state in pgflows.* Postgres tables.

    Uses asyncpg connection pool. On Linux/macOS the psycopg3 AsyncConnectionPool may
    be substituted; on Windows, psycopg3 async networking is incompatible due to libpq
    returning CRT file descriptors that select.select() cannot monitor on Windows.
    """

    def __init__(self, dsn: str, min_pool: int = 2, max_pool: int = 10, ssl: bool = True) -> None:
        self._dsn = dsn
        self._min_pool = min_pool
        self._max_pool = max_pool
        self._ssl = ssl
        self._pool: asyncpg.Pool | None = None

    async def initialize(self) -> None:
        async def _init_conn(conn: asyncpg.Connection) -> None:
            await conn.set_type_codec(
                "jsonb",
                encoder=json.dumps,
                decoder=json.loads,
                schema="pg_catalog",
            )
            await conn.set_type_codec(
                "json",
                encoder=json.dumps,
                decoder=json.loads,
                schema="pg_catalog",
            )

        self._pool = await asyncpg.create_pool(
            self._dsn,
            min_size=self._min_pool,
            max_size=self._max_pool,
            init=_init_conn,
            ssl=self._ssl if self._ssl else False,
        )

    async def register_workflow(self, name: str, config: dict[str, Any]) -> None:
        await self._execute(
            """
            INSERT INTO pgflows.workflow_definitions (name, config)
            VALUES ($1, $2)
            ON CONFLICT (name) DO UPDATE SET config = EXCLUDED.config, updated_at = NOW()
            """,
            name,
            config,
        )

    async def get_workflow_definition(self, name: str) -> dict[str, Any]:
        row = await self._fetchone(
            "SELECT name, version, config FROM pgflows.workflow_definitions WHERE name = $1",
            name,
        )
        if row is None:
            raise WorkflowNotFoundError(name)
        return {"name": row["name"], "version": row["version"], "config": row["config"]}

    async def create_instance(self, workflow_name: str, input_data: dict[str, Any]) -> str:
        row = await self._fetchone(
            """
            INSERT INTO pgflows.workflow_instances (workflow_name, input)
            VALUES ($1, $2)
            RETURNING instance_id::text
            """,
            workflow_name,
            input_data,
        )
        return row["instance_id"]  # type: ignore[index]

    async def get_instance(self, instance_id: str) -> WorkflowStatus:
        row = await self._fetchone(
            """
            SELECT instance_id, workflow_name, state, output, error, created_at, updated_at
            FROM pgflows.workflow_instances
            WHERE instance_id = $1::uuid
            """,
            instance_id,
        )
        if row is None:
            raise WorkflowNotFoundError(instance_id)
        return WorkflowStatus(
            workflow_id=str(row["instance_id"]),
            name=row["workflow_name"],
            state=WorkflowState(row["state"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            output=row["output"],
            error=row["error"],
        )

    async def try_claim_instance(self, instance_id: str) -> bool:
        """Atomically transition pending→running. Returns False if already claimed."""
        self._assert_initialized()
        async with self._pool.acquire() as conn:  # type: ignore[union-attr]
            status = await conn.execute(
                "UPDATE pgflows.workflow_instances"
                " SET state='running', updated_at=NOW()"
                " WHERE instance_id=$1::uuid AND state='pending'",
                instance_id,
            )
        return status == "UPDATE 1"

    async def check_extension(self, extname: str) -> bool:
        row = await self._fetchone("SELECT 1 FROM pg_extension WHERE extname = $1", extname)
        return row is not None

    async def update_instance_state(
        self,
        instance_id: str,
        state: WorkflowState,
        output: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        await self._execute(
            """
            UPDATE pgflows.workflow_instances
            SET state = $1, output = $2, error = $3, updated_at = NOW()
            WHERE instance_id = $4::uuid
            """,
            state.value,
            output,
            error,
            instance_id,
        )

    async def get_step_result(
        self,
        instance_id: str,
        step_name: str,
        step_index: int,
    ) -> dict[str, Any] | None:
        row = await self._fetchone(
            """
            SELECT output FROM pgflows.step_results
            WHERE instance_id = $1::uuid AND step_name = $2 AND step_index = $3
              AND state = 'completed'
            """,
            instance_id,
            step_name,
            step_index,
        )
        return row["output"] if row is not None else None

    async def save_step_result(
        self,
        instance_id: str,
        step_name: str,
        step_index: int,
        input_data: dict[str, Any],
        output: dict[str, Any],
    ) -> None:
        await self._execute(
            """
            INSERT INTO pgflows.step_results
                (instance_id, step_name, step_index, state, input, output, completed_at)
            VALUES ($1::uuid, $2, $3, 'completed', $4, $5, NOW())
            ON CONFLICT (instance_id, step_name, step_index)
            DO UPDATE SET state = 'completed', output = EXCLUDED.output, completed_at = NOW()
            """,
            instance_id,
            step_name,
            step_index,
            input_data,
            output,
        )

    async def save_step_error(
        self,
        instance_id: str,
        step_name: str,
        step_index: int,
        input_data: dict[str, Any],
        error: str,
        attempt: int,
    ) -> None:
        await self._execute(
            """
            INSERT INTO pgflows.step_results
                (instance_id, step_name, step_index, state, input, error, attempt)
            VALUES ($1::uuid, $2, $3, 'failed', $4, $5, $6)
            ON CONFLICT (instance_id, step_name, step_index)
            DO UPDATE SET state = 'failed', error = EXCLUDED.error, attempt = EXCLUDED.attempt
            """,
            instance_id,
            step_name,
            step_index,
            input_data,
            error,
            attempt,
        )

    async def list_instances(
        self,
        workflow_name: str | None = None,
        state: WorkflowState | None = None,
        limit: int = 100,
    ) -> list[WorkflowStatus]:
        filters: list[str] = []
        params: list[Any] = []
        if workflow_name:
            params.append(workflow_name)
            filters.append(f"workflow_name = ${len(params)}")
        if state:
            params.append(state.value)
            filters.append(f"state = ${len(params)}")
        params.append(limit)
        where = ("WHERE " + " AND ".join(filters)) if filters else ""
        rows = await self._fetchall(
            f"""
            SELECT instance_id, workflow_name, state, output, error, created_at, updated_at
            FROM pgflows.workflow_instances
            {where}
            ORDER BY created_at DESC
            LIMIT ${len(params)}
            """,
            *params,
        )
        return [
            WorkflowStatus(
                workflow_id=str(r["instance_id"]),
                name=r["workflow_name"],
                state=WorkflowState(r["state"]),
                created_at=r["created_at"],
                updated_at=r["updated_at"],
                output=r["output"],
                error=r["error"],
            )
            for r in rows
        ]

    # --- OrchestratorBackend ABC required methods ---

    async def start_workflow(self, workflow_id: str, name: str, payload: dict[str, Any]) -> str:
        return await self.create_instance(name, payload)

    async def signal_workflow(
        self, workflow_id: str, signal: str, data: dict[str, Any] | None = None
    ) -> None:
        pass  # signals not used in state-backend mode

    async def get_workflow_status(self, workflow_id: str) -> WorkflowStatus:
        return await self.get_instance(workflow_id)

    async def cancel_workflow(self, workflow_id: str) -> None:
        await self.update_instance_state(workflow_id, WorkflowState.CANCELLED)

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    # --- internal helpers ---

    def _assert_initialized(self) -> None:
        if self._pool is None:
            raise BackendNotInitializedError("PgStateBackend")

    async def _execute(self, query: str, *params: Any) -> None:
        self._assert_initialized()
        async with self._pool.acquire() as conn:  # type: ignore[union-attr]
            await conn.execute(query, *params)

    async def _fetchone(self, query: str, *params: Any) -> asyncpg.Record | None:
        self._assert_initialized()
        async with self._pool.acquire() as conn:  # type: ignore[union-attr]
            return await conn.fetchrow(query, *params)

    async def _fetchall(self, query: str, *params: Any) -> list[asyncpg.Record]:
        self._assert_initialized()
        async with self._pool.acquire() as conn:  # type: ignore[union-attr]
            return await conn.fetch(query, *params)
