from __future__ import annotations

import json
from typing import Any

import psycopg_pool

from pyflows.backends.base import OrchestratorBackend
from pyflows.exceptions import BackendNotInitializedError, WorkflowNotFoundError
from pyflows.types import WorkflowState, WorkflowStatus


class PgStateBackend(OrchestratorBackend):
    """Stores workflow instance and step checkpoint state in pyflows.* Postgres tables."""

    def __init__(self, dsn: str, min_pool: int = 2, max_pool: int = 10) -> None:
        self._dsn = dsn
        self._min_pool = min_pool
        self._max_pool = max_pool
        self._pool: psycopg_pool.AsyncConnectionPool | None = None

    async def initialize(self) -> None:
        self._pool = psycopg_pool.AsyncConnectionPool(
            self._dsn,
            min_size=self._min_pool,
            max_size=self._max_pool,
            open=False,
        )
        await self._pool.open()

    async def register_workflow(self, name: str, config: dict[str, Any]) -> None:
        await self._execute(
            """
            INSERT INTO pyflows.workflow_definitions (name, config)
            VALUES (%s, %s)
            ON CONFLICT (name) DO UPDATE SET config = EXCLUDED.config, updated_at = NOW()
            """,
            (name, json.dumps(config)),
        )

    async def get_workflow_definition(self, name: str) -> dict[str, Any]:
        row = await self._fetchone(
            "SELECT name, version, config FROM pyflows.workflow_definitions WHERE name = %s",
            (name,),
        )
        if row is None:
            raise WorkflowNotFoundError(name)
        return {"name": row[0], "version": row[1], "config": row[2]}

    async def create_instance(self, workflow_name: str, input_data: dict[str, Any]) -> str:
        row = await self._fetchone(
            """
            INSERT INTO pyflows.workflow_instances (workflow_name, input)
            VALUES (%s, %s)
            RETURNING instance_id::text
            """,
            (workflow_name, json.dumps(input_data)),
        )
        return row[0]

    async def get_instance(self, instance_id: str) -> WorkflowStatus:
        row = await self._fetchone(
            """
            SELECT instance_id, workflow_name, state, output, error, created_at, updated_at
            FROM pyflows.workflow_instances
            WHERE instance_id = %s::uuid
            """,
            (instance_id,),
        )
        if row is None:
            raise WorkflowNotFoundError(instance_id)
        return WorkflowStatus(
            workflow_id=str(row[0]),
            name=row[1],
            state=WorkflowState(row[2]),
            created_at=row[5],
            updated_at=row[6],
            output=row[3],
            error=row[4],
        )

    async def update_instance_state(
        self,
        instance_id: str,
        state: WorkflowState,
        output: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        await self._execute(
            """
            UPDATE pyflows.workflow_instances
            SET state = %s, output = %s, error = %s, updated_at = NOW()
            WHERE instance_id = %s::uuid
            """,
            (state.value, json.dumps(output) if output else None, error, instance_id),
        )

    async def get_step_result(
        self,
        instance_id: str,
        step_name: str,
        step_index: int,
    ) -> dict[str, Any] | None:
        row = await self._fetchone(
            """
            SELECT output FROM pyflows.step_results
            WHERE instance_id = %s::uuid AND step_name = %s AND step_index = %s
              AND state = 'completed'
            """,
            (instance_id, step_name, step_index),
        )
        return row[0] if row else None

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
            INSERT INTO pyflows.step_results
                (instance_id, step_name, step_index, state, input, output, completed_at)
            VALUES (%s::uuid, %s, %s, 'completed', %s, %s, NOW())
            ON CONFLICT (instance_id, step_name, step_index)
            DO UPDATE SET state = 'completed', output = EXCLUDED.output, completed_at = NOW()
            """,
            (instance_id, step_name, step_index, json.dumps(input_data), json.dumps(output)),
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
            INSERT INTO pyflows.step_results
                (instance_id, step_name, step_index, state, input, error, attempt)
            VALUES (%s::uuid, %s, %s, 'failed', %s, %s, %s)
            ON CONFLICT (instance_id, step_name, step_index)
            DO UPDATE SET state = 'failed', error = EXCLUDED.error, attempt = EXCLUDED.attempt
            """,
            (instance_id, step_name, step_index, json.dumps(input_data), error, attempt),
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
            filters.append("workflow_name = %s")
            params.append(workflow_name)
        if state:
            filters.append("state = %s")
            params.append(state.value)
        where = ("WHERE " + " AND ".join(filters)) if filters else ""
        params.append(limit)
        rows = await self._fetchall(
            f"""
            SELECT instance_id, workflow_name, state, output, error, created_at, updated_at
            FROM pyflows.workflow_instances
            {where}
            ORDER BY created_at DESC
            LIMIT %s
            """,
            tuple(params),
        )
        return [
            WorkflowStatus(
                workflow_id=str(r[0]),
                name=r[1],
                state=WorkflowState(r[2]),
                created_at=r[5],
                updated_at=r[6],
                output=r[3],
                error=r[4],
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

    async def _execute(self, query: str, params: tuple = ()) -> None:
        self._assert_initialized()
        async with self._pool.connection() as conn:  # type: ignore[union-attr]
            await conn.execute(query, params)

    async def _fetchone(self, query: str, params: tuple = ()) -> tuple | None:
        self._assert_initialized()
        async with self._pool.connection() as conn:  # type: ignore[union-attr]
            async with conn.cursor() as cur:
                await cur.execute(query, params)
                return await cur.fetchone()

    async def _fetchall(self, query: str, params: tuple = ()) -> list[tuple]:
        self._assert_initialized()
        async with self._pool.connection() as conn:  # type: ignore[union-attr]
            async with conn.cursor() as cur:
                await cur.execute(query, params)
                return await cur.fetchall()
