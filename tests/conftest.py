import asyncio
import os
import pathlib
import sys

import asyncpg
import pytest

TEST_DSN = os.getenv(
    "PYFLOWS_TEST_DSN",
    "postgresql://pyflows:pyflows@localhost:5433/pyflows_test",
)

SCHEMA_SQL = (
    pathlib.Path(__file__).parent.parent / "src" / "pyflows" / "schema.sql"
).read_text()


if sys.platform == "win32":

    @pytest.fixture(scope="session")
    def event_loop_policy():
        """Force SelectorEventLoop on Windows — required by psycopg async."""
        return asyncio.WindowsSelectorEventLoopPolicy()


@pytest.fixture(scope="session")
async def db_pool():
    pool = await asyncpg.create_pool(TEST_DSN, min_size=2, max_size=10)
    # Apply schema once per session
    async with pool.acquire() as conn:
        await conn.execute(SCHEMA_SQL)
    yield pool
    await pool.close()
