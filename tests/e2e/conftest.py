import os

import asyncpg
import pytest

from pgflows.config import PgflowsConfig

TEST_DSN = os.getenv(
    "PGFLOWS_TEST_DSN",
    "postgresql://pgflows:pgflows@127.0.0.1:5433/pgflows_test",
)


@pytest.fixture(scope="session")
def pgflows_config():
    return PgflowsConfig(
        dsn=TEST_DSN,
        workflow_queue="pgflows_e2e_q",
        otel_enabled=False,
        db_ssl=False,
    )


@pytest.fixture(autouse=True, scope="session")
async def require_db():
    try:
        conn = await asyncpg.connect(TEST_DSN, ssl=False)
        await conn.close()
    except Exception:
        pytest.skip("Postgres not available (run: docker compose up -d)")
