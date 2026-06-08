"""Unit tests for PgCronBackend (real pg_cron-backed scheduler)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pgflows.backends.pg_cron import PgCronBackend
from pgflows.exceptions import BackendNotInitializedError, SchedulerJobNotFoundError

_UNSET = object()


def _make_backend() -> PgCronBackend:
    return PgCronBackend(dsn="postgresql://localhost/test")


def _mock_pool(fetchrow=_UNSET, fetchval=_UNSET, fetch=_UNSET, execute=_UNSET) -> AsyncMock:
    pool = AsyncMock()
    if fetchrow is not _UNSET:
        pool.fetchrow = AsyncMock(return_value=fetchrow)
    if fetchval is not _UNSET:
        pool.fetchval = AsyncMock(return_value=fetchval)
    if fetch is not _UNSET:
        pool.fetch = AsyncMock(return_value=fetch)
    if execute is not _UNSET:
        pool.execute = AsyncMock(return_value=execute)
    return pool


def test_requires_dsn_or_pool() -> None:
    with pytest.raises(ValueError, match="dsn or an existing pool"):
        PgCronBackend()


async def test_initialize_verifies_pg_cron_extension() -> None:
    backend = _make_backend()
    mock_pool = _mock_pool(fetchrow=MagicMock())
    with patch("pgflows.backends.pg_cron.asyncpg.create_pool", AsyncMock(return_value=mock_pool)):
        await backend.initialize()
    assert backend._pool is mock_pool


async def test_initialize_raises_if_pg_cron_missing() -> None:
    backend = _make_backend()
    mock_pool = _mock_pool(fetchrow=None)
    with patch("pgflows.backends.pg_cron.asyncpg.create_pool", AsyncMock(return_value=mock_pool)):
        with pytest.raises(RuntimeError, match="pg_cron"):
            await backend.initialize()


async def test_schedule_calls_cron_schedule_and_returns_id() -> None:
    backend = _make_backend()
    backend._pool = _mock_pool(fetchval=42)
    job_id = await backend.schedule("nightly", "0 0 * * *", "SELECT cleanup()")
    assert job_id == "42"
    sql = backend._pool.fetchval.call_args[0][0]
    assert "cron.schedule" in sql
    # job name / schedule / command are bind params, not interpolated
    args = backend._pool.fetchval.call_args[0]
    assert args[1:] == ("nightly", "0 0 * * *", "SELECT cleanup()")


async def test_unschedule_by_id() -> None:
    backend = _make_backend()
    backend._pool = _mock_pool(fetchval=True)
    await backend.unschedule("42")
    sql = backend._pool.fetchval.call_args[0][0]
    assert "cron.unschedule" in sql


async def test_unschedule_unknown_id_raises() -> None:
    backend = _make_backend()
    backend._pool = _mock_pool(fetchval=False)
    with pytest.raises(SchedulerJobNotFoundError):
        await backend.unschedule("999")


async def test_unschedule_by_name_is_idempotent() -> None:
    backend = _make_backend()
    backend._pool = _mock_pool(execute="DELETE 0")
    await backend.unschedule_by_name("missing")  # no raise
    sql = backend._pool.execute.call_args[0][0]
    assert "DELETE FROM cron.job" in sql


async def test_list_jobs_maps_cron_job_rows() -> None:
    backend = _make_backend()
    backend._pool = _mock_pool(
        fetch=[
            {
                "jobid": 1,
                "jobname": "nightly",
                "schedule": "0 0 * * *",
                "command": "x",
                "active": True,
            }
        ]
    )
    jobs = await backend.list_jobs()
    assert jobs[0].job_id == "1"
    assert jobs[0].job_name == "nightly"
    assert jobs[0].cron == "0 0 * * *"
    assert jobs[0].active is True


async def test_operations_require_initialized() -> None:
    backend = _make_backend()
    with pytest.raises(BackendNotInitializedError):
        await backend.schedule("j", "* * * * *", "SELECT 1")


async def test_close_does_not_close_shared_pool() -> None:
    shared = _mock_pool()
    shared.close = AsyncMock()
    backend = PgCronBackend(pool=shared)
    await backend.close()
    shared.close.assert_not_awaited()
