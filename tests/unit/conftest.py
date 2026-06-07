from __future__ import annotations

import asyncio
import os

import asyncpg
import pytest

_TEST_DSN = os.getenv(
    "PGFLOWS_TEST_DSN",
    "postgresql://pgflows:pgflows@127.0.0.1:5433/pgflows_test",
)


@pytest.fixture(scope="session")
async def require_db():
    """Skip the test if Postgres is not reachable."""
    try:
        conn = await asyncio.wait_for(asyncpg.connect(_TEST_DSN, ssl=False), timeout=2)
        await conn.close()
    except Exception:
        pytest.skip("Postgres not available (run: docker compose up -d)")
