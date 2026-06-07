"""Unit tests for PgCronBackend (pg_durable-backed scheduler)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pgflows.backends.pg_cron import PgCronBackend
from pgflows.exceptions import BackendNotInitializedError, SchedulerJobNotFoundError
from pgflows.types import ScheduledJob

_UNSET = object()


def _make_backend() -> PgCronBackend:
    return PgCronBackend(dsn="postgresql://localhost/test")


def _mock_pool(fetchrow=_UNSET, fetchval=_UNSET, fetch=_UNSET) -> AsyncMock:
    pool = AsyncMock()
    if fetchrow is not _UNSET:
        pool.fetchrow = AsyncMock(return_value=fetchrow)
    if fetchval is not _UNSET:
        pool.fetchval = AsyncMock(return_value=fetchval)
    if fetch is not _UNSET:
        pool.fetch = AsyncMock(return_value=fetch)
    return pool


# --- initialize ---

async def test_initialize_sets_pool_and_verifies_extension() -> None:
    backend = _make_backend()
    mock_pool = _mock_pool(fetchrow=MagicMock())  # extension present

    with patch("pgflows.backends.pg_cron.asyncpg.create_pool", AsyncMock(return_value=mock_pool)):
        await backend.initialize()

    assert backend._pool is mock_pool


async def test_initialize_raises_if_pg_durable_missing() -> None:
    backend = _make_backend()
    mock_pool = _mock_pool(fetchrow=None)  # extension absent

    with patch("pgflows.backends.pg_cron.asyncpg.create_pool", AsyncMock(return_value=mock_pool)):
        with pytest.raises(RuntimeError, match="pg_durable"):
            await backend.initialize()


# --- schedule ---

async def test_schedule_returns_string_instance_id() -> None:
    backend = _make_backend()
    backend._pool = _mock_pool(fetchval="abc12345")

    job_id = await backend.schedule("my_job", "0 * * * *", "SELECT cleanup()")

    assert job_id == "abc12345"
    call_sql: str = backend._pool.fetchval.call_args[0][0]  # type: ignore[union-attr]
    assert "df.start" in call_sql
    assert "df.wait_for_schedule" in call_sql
    args = backend._pool.fetchval.call_args[0]  # type: ignore[union-attr]
    assert args[1] == "SELECT cleanup()"
    assert args[2] == "0 * * * *"
    assert args[3] == "my_job"


async def test_schedule_passes_args_as_bind_params() -> None:
    backend = _make_backend()
    backend._pool = _mock_pool(fetchval="xyz99999")

    await backend.schedule("job's-name", "* * * * *", "SELECT 'hello'")

    args = backend._pool.fetchval.call_args[0]  # type: ignore[union-attr]
    # Parameters are passed as bind params — no manual quoting needed.
    assert args[1] == "SELECT 'hello'"
    assert args[2] == "* * * * *"
    assert args[3] == "job's-name"


async def test_schedule_raises_if_not_initialized() -> None:
    backend = _make_backend()
    with pytest.raises(BackendNotInitializedError):
        await backend.schedule("j", "* * * * *", "SELECT 1")


# --- unschedule ---

async def test_unschedule_cancels_running_instance() -> None:
    backend = _make_backend()
    backend._pool = _mock_pool(fetchval="running")

    await backend.unschedule("abc12345")

    call_sql: str = backend._pool.execute.call_args[0][0]  # type: ignore[union-attr]
    assert "df.cancel" in call_sql


async def test_unschedule_raises_not_found() -> None:
    backend = _make_backend()
    backend._pool = _mock_pool(fetchval=None)  # instance not found

    with pytest.raises(SchedulerJobNotFoundError):
        await backend.unschedule("missing-id")


# --- list_jobs ---

async def test_list_jobs_maps_running_instances() -> None:
    backend = _make_backend()
    backend._pool = _mock_pool(fetch=[
        {"instance_id": "aaa11111", "label": "daily-job", "status": "running"},
        {"instance_id": "bbb22222", "label": None, "status": "running"},
    ])

    jobs = await backend.list_jobs()

    assert len(jobs) == 2
    assert jobs[0] == ScheduledJob(
        job_id="aaa11111", job_name="daily-job", cron="", command="", active=True
    )
    assert jobs[1].job_name == "bbb22222"  # falls back to instance_id when label is None


async def test_list_jobs_empty() -> None:
    backend = _make_backend()
    backend._pool = _mock_pool(fetch=[])

    jobs = await backend.list_jobs()

    assert jobs == []


# --- close ---

async def test_close_releases_pool() -> None:
    backend = _make_backend()
    mock_pool = AsyncMock()
    backend._pool = mock_pool

    await backend.close()

    mock_pool.close.assert_awaited_once()
    assert backend._pool is None


async def test_close_noop_when_not_initialized() -> None:
    backend = _make_backend()
    await backend.close()  # should not raise
