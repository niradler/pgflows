"""Live end-to-end tests for pg_cron-backed recurring scheduling.

Proves ``app.schedule_workflow`` registers a real pg_cron job whose command creates a
pending instance + enqueues it, and that a running pull worker then executes the run —
the durable-safe recurring path (pg_durable loops are not meant for recurring cron).

Requires the combined df + pgmq + pg_cron image (tests/e2e/docker). Auto-skips otherwise.
"""

from __future__ import annotations

import asyncio
import os

import asyncpg
import pytest
import pytest_asyncio
from pydantic import BaseModel

from pgflows.app import WorkflowApp
from pgflows.config import PgflowsConfig
from pgflows.types import WorkflowState

_TEST_DSN = os.getenv(
    "PGFLOWS_TEST_DSN",
    "postgresql://pgflows:pgflows@127.0.0.1:5433/pgflows_test",
)


class TickIn(BaseModel):
    label: str


class TickOut(BaseModel):
    seen: str


async def _has_extensions() -> bool:
    try:
        conn = await asyncio.wait_for(asyncpg.connect(_TEST_DSN, ssl=False), timeout=2)
    except Exception:
        return False
    try:
        rows = await conn.fetch(
            "SELECT extname FROM pg_extension WHERE extname IN ('pg_durable', 'pg_cron')"
        )
        return {r["extname"] for r in rows} >= {"pg_durable", "pg_cron"}
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def cron_app():
    if not await _has_extensions():
        pytest.skip("pg_durable + pg_cron required — run tests/e2e/docker compose image")

    config = PgflowsConfig(
        dsn=_TEST_DSN,
        workflow_queue="pgflows_cron_wf",
        otel_enabled=False,
        db_ssl=False,
    )
    app = WorkflowApp(config=config)

    @app.step()
    async def record(ctx, inp: TickIn) -> TickOut:
        return TickOut(seen=inp.label)

    @app.workflow(name="ticker")
    async def ticker(ctx, inp: TickIn) -> TickOut:
        return await ctx.step(record, inp)

    await app.initialize()
    # start clean — clear any cron jobs a prior run left behind
    async with app.acquire() as conn:
        await conn.execute("DELETE FROM cron.job")
    try:
        yield app
    finally:
        async with app.acquire() as conn:
            await conn.execute("DELETE FROM cron.job")
        await app.close()


async def test_pg_cron_detected(cron_app):
    assert cron_app.pg_cron_available is True


async def test_schedule_registers_and_unschedules(cron_app):
    ticker = cron_app.registry.get_workflow("ticker").fn
    job_id = await cron_app.schedule_workflow("nightly", "0 0 * * *", ticker, TickIn(label="x"))
    assert job_id.isdigit()
    jobs = await cron_app.list_schedules()
    assert any(j.job_name == "nightly" for j in jobs)

    await cron_app.unschedule_workflow("nightly")
    jobs = await cron_app.list_schedules()
    assert not any(j.job_name == "nightly" for j in jobs)


async def test_scheduled_command_starts_and_runs_workflow(cron_app):
    """The command pg_cron stores must create a pending instance + enqueue it; driving
    the worker then completes the run. Executes the stored command directly (no waiting
    on cron timing) to deterministically prove the generated SQL is correct."""
    ticker = cron_app.registry.get_workflow("ticker").fn
    await cron_app.schedule_workflow("deterministic", "0 0 * * *", ticker, TickIn(label="hello"))
    async with cron_app.acquire() as conn:
        command = await conn.fetchval(
            "SELECT command FROM cron.job WHERE jobname = 'deterministic'"
        )
        await conn.execute(command)  # run exactly what pg_cron would run on a tick

    for _ in range(50):
        await cron_app.process_once()
        done = await cron_app.list_workflows(workflow_name="ticker", state=WorkflowState.COMPLETED)
        if done:
            break
        await asyncio.sleep(0.1)

    done = await cron_app.list_workflows(workflow_name="ticker", state=WorkflowState.COMPLETED)
    assert len(done) >= 1
    assert done[0].output == {"seen": "hello"}


@pytest.mark.timeout(60)
async def test_pg_cron_actually_fires_on_interval(cron_app):
    """End-to-end: a real pg_cron tick (seconds interval) starts a run the worker completes."""
    await cron_app.schedule_workflow(
        "fast", "10 seconds", cron_app.registry.get_workflow("ticker").fn, TickIn(label="fired")
    )
    deadline = asyncio.get_event_loop().time() + 45
    while asyncio.get_event_loop().time() < deadline:
        await cron_app.process_once()
        done = await cron_app.list_workflows(workflow_name="ticker", state=WorkflowState.COMPLETED)
        if any(d.output == {"seen": "fired"} for d in done):
            return
        await asyncio.sleep(0.5)
    pytest.fail("pg_cron job did not fire a completed run within the timeout")
