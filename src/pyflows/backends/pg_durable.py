from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pyflows.backends.base import OrchestratorBackend
from pyflows.exceptions import BackendNotInitializedError
from pyflows.types import WorkflowStatus

if TYPE_CHECKING:
    pass


class PgDurableBackend(OrchestratorBackend):
    """pg_durable-backed orchestrator.

    Talks to Postgres via the pg_durable extension to start, signal,
    and inspect durable workflow runs.
    """

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._conn: Any = None  # psycopg AsyncConnection, set after initialize()

    async def initialize(self) -> None:
        # TODO(M2): open psycopg AsyncConnection, verify pg_durable extension installed
        raise NotImplementedError

    async def start_workflow(
        self,
        workflow_id: str,
        name: str,
        payload: dict[str, Any],
    ) -> str:
        self._assert_initialized()
        # TODO(M3): call pg_durable.start_workflow() via psycopg
        raise NotImplementedError

    async def signal_workflow(
        self,
        workflow_id: str,
        signal: str,
        data: dict[str, Any] | None = None,
    ) -> None:
        self._assert_initialized()
        # TODO(M4): call pg_durable.signal() via psycopg
        raise NotImplementedError

    async def get_workflow_status(self, workflow_id: str) -> WorkflowStatus:
        self._assert_initialized()
        # TODO(M2): query pg_durable.workflows table
        raise NotImplementedError

    async def cancel_workflow(self, workflow_id: str) -> None:
        self._assert_initialized()
        # TODO(M2): call pg_durable.cancel() via psycopg
        raise NotImplementedError

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    def _assert_initialized(self) -> None:
        if self._conn is None:
            raise BackendNotInitializedError("PgDurableBackend")
