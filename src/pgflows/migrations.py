from __future__ import annotations

import asyncpg

# Each migration: (version_key, sql). Applied once in order; never re-run.
MIGRATIONS: list[tuple[str, str]] = [
    (
        "0001_initial_schema",
        """
        CREATE TABLE IF NOT EXISTS pgflows.workflow_definitions (
            name        TEXT PRIMARY KEY,
            version     INT  NOT NULL DEFAULT 1,
            config      JSONB NOT NULL DEFAULT '{}',
            created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS pgflows.workflow_instances (
            instance_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            workflow_name TEXT NOT NULL REFERENCES pgflows.workflow_definitions(name),
            state         TEXT NOT NULL DEFAULT 'pending',
            input         JSONB NOT NULL,
            output        JSONB,
            error         TEXT,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_instances_state
            ON pgflows.workflow_instances(state);

        CREATE INDEX IF NOT EXISTS idx_instances_workflow_name
            ON pgflows.workflow_instances(workflow_name);

        CREATE TABLE IF NOT EXISTS pgflows.step_results (
            id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            instance_id  UUID NOT NULL
                REFERENCES pgflows.workflow_instances(instance_id) ON DELETE CASCADE,
            step_name    TEXT NOT NULL,
            step_index   INT  NOT NULL,
            state        TEXT NOT NULL DEFAULT 'pending',
            input        JSONB NOT NULL,
            output       JSONB,
            error        TEXT,
            attempt      INT  NOT NULL DEFAULT 1,
            created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            completed_at TIMESTAMPTZ,
            UNIQUE(instance_id, step_name, step_index)
        );
        """,
    ),
    (
        "0002_composite_index",
        """
        CREATE INDEX IF NOT EXISTS idx_instances_wf_state
            ON pgflows.workflow_instances(workflow_name, state);
        """,
    ),
    (
        "0003_worker_step_results",
        """
        -- Drop box for pgmq+NOTIFY step results. pg_durable polls this table
        -- (race-free) instead of relying on a signal that can be sent before the
        -- waiter is registered. The StepWorker inserts a row keyed by result_key.
        CREATE TABLE IF NOT EXISTS pgflows.worker_step_results (
            key         TEXT PRIMARY KEY,
            result      JSONB NOT NULL,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
    ),
]

_BOOTSTRAP_SQL = """
CREATE SCHEMA IF NOT EXISTS pgflows;
CREATE TABLE IF NOT EXISTS pgflows.schema_migrations (
    version     TEXT PRIMARY KEY,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


async def run_migrations(dsn: str, ssl: bool = False) -> int:
    """Apply pending migrations. Returns count of newly applied migrations."""
    conn = await asyncpg.connect(dsn, ssl=ssl)
    try:
        await conn.execute(_BOOTSTRAP_SQL)
        rows = await conn.fetch("SELECT version FROM pgflows.schema_migrations")
        applied = {r["version"] for r in rows}
        count = 0
        for version, sql in MIGRATIONS:
            if version in applied:
                continue
            async with conn.transaction():
                await conn.execute(sql)
                await conn.execute(
                    "INSERT INTO pgflows.schema_migrations (version) VALUES ($1)", version
                )
            count += 1
        return count
    finally:
        await conn.close()
