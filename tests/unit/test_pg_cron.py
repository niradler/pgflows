from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from pyflows.backends.pg_cron import PgCronBackend
from pyflows.exceptions import BackendNotInitializedError, SchedulerJobNotFoundError
from pyflows.types import ScheduledJob

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

@pytest.mark.asyncio
async def test_initialize_sets_pool_and_verifies_extension() -> None:
    backend = _make_backend()
    mock_pool = _mock_pool(fetchrow={"1": 1})

    with patch("pyflows.backends.pg_cron.asyncpg.create_pool", AsyncMock(return_value=mock_pool)):
        await backend.initialize()

    assert backend._pool is mock_pool


@pytest.mark.asyncio
async def test_initialize_raises_if_pg_cron_missing() -> None:
    backend = _make_backend()
    mock_pool = _mock_pool(fetchrow=None)

    with patch("pyflows.backends.pg_cron.asyncpg.create_pool", AsyncMock(return_value=mock_pool)):
        with pytest.raises(RuntimeError, match="pg_cron"):
            await backend.initialize()


# --- schedule ---

@pytest.mark.asyncio
async def test_schedule_returns_job_id() -> None:
    backend = _make_backend()
    backend._pool = _mock_pool(fetchval=42)

    job_id = await backend.schedule("my_job", "* * * * *", "SELECT 1")

    assert job_id == 42
    backend._pool.fetchval.assert_awaited_once_with(  # type: ignore[union-attr]
        "SELECT cron.schedule($1, $2, $3)", "my_job", "* * * * *", "SELECT 1"
    )


@pytest.mark.asyncio
async def test_schedule_raises_if_not_initialized() -> None:
    backend = _make_backend()
    with pytest.raises(BackendNotInitializedError):
        await backend.schedule("j", "* * * * *", "SELECT 1")


# --- unschedule ---

@pytest.mark.asyncio
async def test_unschedule_success() -> None:
    backend = _make_backend()
    backend._pool = _mock_pool(fetchval=True)

    await backend.unschedule(42)


@pytest.mark.asyncio
async def test_unschedule_raises_not_found() -> None:
    backend = _make_backend()
    backend._pool = _mock_pool(fetchval=False)

    with pytest.raises(SchedulerJobNotFoundError):
        await backend.unschedule(99)


# --- list_jobs ---

@pytest.mark.asyncio
async def test_list_jobs_returns_scheduled_jobs() -> None:
    backend = _make_backend()
    backend._pool = _mock_pool(fetch=[
        {"jobid": 1, "jobname": "j1", "schedule": "0 * * * *",
         "command": "SELECT 1", "active": True},
        {"jobid": 2, "jobname": "j2", "schedule": "*/5 * * * *",
         "command": "SELECT 2", "active": False},
    ])

    jobs = await backend.list_jobs()

    assert len(jobs) == 2
    expected = ScheduledJob(
        job_id=1, job_name="j1", cron="0 * * * *", command="SELECT 1", active=True
    )
    assert jobs[0] == expected
    assert jobs[1].active is False


@pytest.mark.asyncio
async def test_list_jobs_empty() -> None:
    backend = _make_backend()
    backend._pool = _mock_pool(fetch=[])

    jobs = await backend.list_jobs()

    assert jobs == []


# --- close ---

@pytest.mark.asyncio
async def test_close_releases_pool() -> None:
    backend = _make_backend()
    mock_pool = AsyncMock()
    backend._pool = mock_pool

    await backend.close()

    mock_pool.close.assert_awaited_once()
    assert backend._pool is None


@pytest.mark.asyncio
async def test_close_noop_when_not_initialized() -> None:
    backend = _make_backend()
    await backend.close()
