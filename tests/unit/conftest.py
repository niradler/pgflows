from __future__ import annotations

import asyncio
import os

import asyncpg
import pytest

_TEST_DSN = os.getenv(
    "PYFLOWS_TEST_DSN",
    "postgresql://pyflows:pyflows@localhost:5433/pyflows_test",
)


@pytest.fixture(scope="session")
async def require_db():
    """Skip the test if Postgres is not reachable."""
    try:
        conn = await asyncio.wait_for(asyncpg.connect(_TEST_DSN), timeout=2)
        await conn.close()
    except Exception:
        pytest.skip("Postgres not available (run: docker compose up -d)")
