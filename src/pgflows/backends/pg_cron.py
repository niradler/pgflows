from __future__ import annotations

import asyncpg

from pgflows.backends.base import SchedulerBackend
from pgflows.exceptions import BackendNotInitializedError, SchedulerJobNotFoundError
from pgflows.logger import get_logger
from pgflows.types import ScheduledJob

_log = get_logger("scheduler")


class PgCronBackend(SchedulerBackend):
    """Recurring scheduler backed by the pg_cron extension.

    pg_cron is the right tool for recurring schedules — unlike a pg_durable
    ``@> (… ~> wait_for_schedule)`` loop, which pins a worker connection forever and
    cannot share an instance with parallel nodes. Each job is a ``cron.schedule`` entry
    whose command runs in the ``cron.database_name`` database on the cron tick; pair it
    with a command that ``pgmq.send`` + ``pg_notify`` to trigger a workflow run (see
    ``WorkflowApp.schedule_workflow``).

    Accepts either a ``dsn`` (opens its own small pool) or an existing ``pool`` to share.
    """

    def __init__(
        self,
        dsn: str | None = None,
        *,
        pool: asyncpg.Pool | None = None,
        pool_size: int = 2,
    ) -> None:
        if dsn is None and pool is None:
            raise ValueError("PgCronBackend requires either a dsn or an existing pool")
        self._dsn = dsn
        self._pool = pool
        self._owns_pool = pool is None
        self._pool_size = pool_size

    async def initialize(self) -> None:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(self._dsn, min_size=1, max_size=self._pool_size)
        row = await self._pool.fetchrow("SELECT 1 FROM pg_extension WHERE extname = 'pg_cron'")
        if row is None:
            raise RuntimeError(
                "pg_cron extension not installed. Run: CREATE EXTENSION IF NOT EXISTS pg_cron; "
                "(and add pg_cron to shared_preload_libraries)."
            )

    async def schedule(self, job_name: str, cron: str, command: str) -> str:
        """Register (or replace) a named cron job. Returns the pg_cron job id."""
        self._assert_initialized()
        job_id: int = await self._pool.fetchval(  # type: ignore[union-attr]
            "SELECT cron.schedule($1, $2, $3)", job_name, cron, command
        )
        _log.info("scheduled job=%s id=%s cron=%s", job_name, job_id, cron)
        return str(job_id)

    async def unschedule(self, job_id: str) -> None:
        """Remove a cron job by its numeric job id (as returned by schedule())."""
        self._assert_initialized()
        removed: bool = await self._pool.fetchval(  # type: ignore[union-attr]
            "SELECT cron.unschedule($1::bigint)", int(job_id)
        )
        if not removed:
            raise SchedulerJobNotFoundError(job_id)
        _log.info("unscheduled job id=%s", job_id)

    async def unschedule_by_name(self, job_name: str) -> None:
        """Remove a cron job by name — idempotent (no error if already absent)."""
        self._assert_initialized()
        await self._pool.execute(  # type: ignore[union-attr]
            "DELETE FROM cron.job WHERE jobname = $1", job_name
        )
        _log.info("unscheduled job name=%s", job_name)

    async def list_jobs(self) -> list[ScheduledJob]:
        self._assert_initialized()
        rows = await self._pool.fetch(  # type: ignore[union-attr]
            "SELECT jobid, jobname, schedule, command, active FROM cron.job"
        )
        return [
            ScheduledJob(
                job_id=str(r["jobid"]),
                job_name=r["jobname"] or str(r["jobid"]),
                cron=r["schedule"],
                command=r["command"],
                active=r["active"],
            )
            for r in rows
        ]

    async def close(self) -> None:
        if self._pool is not None and self._owns_pool:
            await self._pool.close()
        self._pool = None

    def _assert_initialized(self) -> None:
        if self._pool is None:
            raise BackendNotInitializedError("PgCronBackend")
