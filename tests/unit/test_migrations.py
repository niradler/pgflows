from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pgflows.migrations import MIGRATIONS, run_migrations


@pytest.mark.asyncio
async def test_run_migrations_applies_all_when_fresh() -> None:
    """Fresh DB: all migrations should be applied."""
    mock_conn = AsyncMock()
    mock_conn.fetch = AsyncMock(return_value=[])  # nothing applied yet
    mock_conn.execute = AsyncMock()
    mock_conn.transaction = MagicMock(return_value=AsyncMock(
        __aenter__=AsyncMock(return_value=None),
        __aexit__=AsyncMock(return_value=False),
    ))

    with patch("pgflows.migrations.asyncpg.connect", AsyncMock(return_value=mock_conn)):
        count = await run_migrations("postgresql://localhost/test")

    assert count == len(MIGRATIONS)


@pytest.mark.asyncio
async def test_run_migrations_skips_applied() -> None:
    """DB already has migration 0001: should skip it and apply 0."""
    first_version = MIGRATIONS[0][0]
    mock_conn = AsyncMock()
    mock_conn.fetch = AsyncMock(return_value=[{"version": first_version}])
    mock_conn.execute = AsyncMock()
    mock_conn.transaction = MagicMock(return_value=AsyncMock(
        __aenter__=AsyncMock(return_value=None),
        __aexit__=AsyncMock(return_value=False),
    ))

    with patch("pgflows.migrations.asyncpg.connect", AsyncMock(return_value=mock_conn)):
        count = await run_migrations("postgresql://localhost/test")

    # All already applied → 0 new
    assert count == len(MIGRATIONS) - 1


@pytest.mark.asyncio
async def test_run_migrations_closes_connection_on_error() -> None:
    """Connection is always closed even when migration fails."""
    mock_conn = AsyncMock()
    mock_conn.fetch = AsyncMock(side_effect=RuntimeError("db error"))
    mock_conn.close = AsyncMock()

    with patch("pgflows.migrations.asyncpg.connect", AsyncMock(return_value=mock_conn)):
        with pytest.raises(RuntimeError, match="db error"):
            await run_migrations("postgresql://localhost/test")

    mock_conn.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_migrations_idempotent_when_all_applied() -> None:
    """All migrations already applied → returns 0."""
    all_versions = [{"version": v} for v, _ in MIGRATIONS]
    mock_conn = AsyncMock()
    mock_conn.fetch = AsyncMock(return_value=all_versions)
    mock_conn.execute = AsyncMock()

    with patch("pgflows.migrations.asyncpg.connect", AsyncMock(return_value=mock_conn)):
        count = await run_migrations("postgresql://localhost/test")

    assert count == 0
    # Bootstrap SQL runs but no migration SQL executed
    mock_conn.execute.assert_awaited_once()  # only the bootstrap DDL


def test_migrations_list_is_ordered() -> None:
    """Migration versions must be in ascending lexicographic order."""
    versions = [v for v, _ in MIGRATIONS]
    assert versions == sorted(versions)


def test_migrations_have_unique_versions() -> None:
    versions = [v for v, _ in MIGRATIONS]
    assert len(versions) == len(set(versions))
