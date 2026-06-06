from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pyflows.backends.base import SchedulerBackend
from pyflows.exceptions import BackendNotInitializedError
from pyflows.types import ScheduledJob

if TYPE_CHECKING:
    pass


class PgCronBackend(SchedulerBackend):
    """pg_cron-backed scheduler.

    Calls cron.schedule() / cron.unschedule() in Postgres to trigger
    workflow runs on a recurring cron expression.
    """

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._conn: Any = None  # psycopg AsyncConnection, set after initialize()

    async def initialize(self) -> None:
        # TODO(M7): open psycopg AsyncConnection, verify pg_cron extension installed
        raise NotImplementedError

    async def schedule(
        self,
        job_name: str,
        cron: str,
        command: str,
    ) -> int:
        self._assert_initialized()
        # TODO(M7): SELECT cron.schedule(job_name, cron, command) RETURNING jobid
        raise NotImplementedError

    async def unschedule(self, job_id: int) -> None:
        self._assert_initialized()
        # TODO(M7): SELECT cron.unschedule(job_id); raise SchedulerJobNotFoundError if 0 rows
        raise NotImplementedError

    async def list_jobs(self) -> list[ScheduledJob]:
        self._assert_initialized()
        # TODO(M7): SELECT * FROM cron.job
        raise NotImplementedError

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    def _assert_initialized(self) -> None:
        if self._conn is None:
            raise BackendNotInitializedError("PgCronBackend")
