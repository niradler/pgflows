from __future__ import annotations

from typing import Any

from pyflows.backends.base import OrchestratorBackend
from pyflows.exceptions import BackendNotInitializedError
from pyflows.types import WorkflowStatus


class PgDurableBackend(OrchestratorBackend):
    """pg_durable-backed orchestrator (not yet implemented).

    Placeholder for a future backend that talks directly to Postgres via the
    pg_durable extension. Use PgStateBackend for the current stable backend.
    """

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._conn: Any = None

    async def initialize(self) -> None:
        raise NotImplementedError

    async def start_workflow(
        self,
        workflow_id: str,
        name: str,
        payload: dict[str, Any],
    ) -> str:
        self._assert_initialized()
        raise NotImplementedError

    async def signal_workflow(
        self,
        workflow_id: str,
        signal: str,
        data: dict[str, Any] | None = None,
    ) -> None:
        self._assert_initialized()
        raise NotImplementedError

    async def get_workflow_status(self, workflow_id: str) -> WorkflowStatus:
        self._assert_initialized()
        raise NotImplementedError

    async def cancel_workflow(self, workflow_id: str) -> None:
        self._assert_initialized()
        raise NotImplementedError

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    def _assert_initialized(self) -> None:
        if self._conn is None:
            raise BackendNotInitializedError("PgDurableBackend")
