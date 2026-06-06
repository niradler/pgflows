from __future__ import annotations

import asyncpg

from pyflows.backends.base import SchedulerBackend
from pyflows.exceptions import BackendNotInitializedError, SchedulerJobNotFoundError
from pyflows.types import ScheduledJob


class PgCronBackend(SchedulerBackend):
    """pg_cron-backed scheduler.

    Calls cron.schedule() / cron.unschedule() in Postgres to trigger
    workflow runs on a recurring cron expression. Requires pg_cron extension.
    """

    def __init__(self, dsn: str, pool_size: int = 2) -> None:
        self._dsn = dsn
        self._pool_size = pool_size
        self._pool: asyncpg.Pool | None = None

    async def initialize(self) -> None:
        self._pool = await asyncpg.create_pool(self._dsn, min_size=1, max_size=self._pool_size)
        row = await self._pool.fetchrow(
            "SELECT 1 FROM pg_extension WHERE extname = 'pg_cron'"
        )
        if row is None:
            raise RuntimeError(
                "pg_cron extension not installed. "
                "Run: CREATE EXTENSION IF NOT EXISTS pg_cron;"
            )

    async def schedule(self, job_name: str, cron: str, command: str) -> int:
        self._assert_initialized()
        job_id: int = await self._pool.fetchval(  # type: ignore[union-attr]
            "SELECT cron.schedule($1, $2, $3)", job_name, cron, command
        )
        return job_id

    async def unschedule(self, job_id: int) -> None:
        self._assert_initialized()
        ok: bool = await self._pool.fetchval(  # type: ignore[union-attr]
            "SELECT cron.unschedule($1)", job_id
        )
        if not ok:
            raise SchedulerJobNotFoundError(str(job_id))

    async def list_jobs(self) -> list[ScheduledJob]:
        self._assert_initialized()
        rows = await self._pool.fetch(  # type: ignore[union-attr]
            "SELECT jobid, jobname, schedule, command, active FROM cron.job ORDER BY jobid"
        )
        return [
            ScheduledJob(
                job_id=r["jobid"],
                job_name=r["jobname"],
                cron=r["schedule"],
                command=r["command"],
                active=r["active"],
            )
            for r in rows
        ]

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    def _assert_initialized(self) -> None:
        if self._pool is None:
            raise BackendNotInitializedError("PgCronBackend")
