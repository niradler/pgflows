from __future__ import annotations

import asyncpg

from pgflows.backends.base import SchedulerBackend
from pgflows.exceptions import BackendNotInitializedError, SchedulerJobNotFoundError
from pgflows.logger import get_logger
from pgflows.types import ScheduledJob

_log = get_logger("scheduler")


class PgCronBackend(SchedulerBackend):
    """Cron-style scheduler backed by pg_durable.

    Creates long-running durable functions using @> (infinite loop) combined
    with df.wait_for_schedule() — no pg_cron extension required.

    Each scheduled job is a pg_durable instance that loops forever:
        @> (command ~> df.wait_for_schedule(cron_expr))
    """

    def __init__(self, dsn: str, pool_size: int = 2) -> None:
        self._dsn = dsn
        self._pool_size = pool_size
        self._pool: asyncpg.Pool | None = None

    async def initialize(self) -> None:
        self._pool = await asyncpg.create_pool(self._dsn, min_size=1, max_size=self._pool_size)
        row = await self._pool.fetchrow("SELECT 1 FROM pg_extension WHERE extname = 'df'")
        if row is None:
            raise RuntimeError(
                "pg_durable extension not installed. Run: CREATE EXTENSION IF NOT EXISTS df;"
            )

    async def schedule(self, job_name: str, cron: str, command: str) -> str:
        """Create an infinite-loop durable function that fires on the cron schedule.

        Returns the pg_durable instance_id which serves as the job_id.
        """
        self._assert_initialized()
        instance_id: str = await self._pool.fetchval(  # type: ignore[union-attr]
            "SELECT df.start(@> ($1 ~> df.wait_for_schedule($2)), $3)",
            command,
            cron,
            job_name,
        )
        _log.info("scheduled job=%s instance=%s cron=%s", job_name, instance_id, cron)
        return instance_id

    async def unschedule(self, job_id: str) -> None:
        self._assert_initialized()
        status = await self._pool.fetchval(  # type: ignore[union-attr]
            "SELECT df.status($1)", job_id
        )
        if status is None:
            raise SchedulerJobNotFoundError(job_id)
        await self._pool.execute(  # type: ignore[union-attr]
            "SELECT df.cancel($1, 'Unscheduled')", job_id
        )
        _log.info("unscheduled job=%s", job_id)

    async def list_jobs(self) -> list[ScheduledJob]:
        self._assert_initialized()
        rows = await self._pool.fetch(  # type: ignore[union-attr]
            "SELECT instance_id, label, status FROM df.list_instances('running')"
        )
        return [
            ScheduledJob(
                job_id=r["instance_id"],
                job_name=r["label"] or r["instance_id"],
                cron="",
                command="",
                active=r["status"] == "running",
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
