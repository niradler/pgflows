import asyncio
import os

import asyncpg
import pytest

from pyflows.config import PyflowsConfig

TEST_DSN = os.getenv(
    "PYFLOWS_TEST_DSN",
    "postgresql://pyflows:pyflows@localhost:5433/pyflows_test",
)


@pytest.fixture(scope="session")
def pyflows_config():
    return PyflowsConfig(
        dsn=TEST_DSN,
        workflow_queue="pyflows_e2e_q",
        otel_enabled=False,
    )


@pytest.fixture(autouse=True, scope="session")
async def require_db():
    try:
        conn = await asyncio.wait_for(asyncpg.connect(TEST_DSN), timeout=2)
        await conn.close()
    except Exception:
        pytest.skip("Postgres not available (run: docker compose up -d)")
