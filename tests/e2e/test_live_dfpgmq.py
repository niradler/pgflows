"""Real end-to-end tests against a live Postgres with BOTH pg_durable (df) and pgmq.

These exercise the actual user flows — pull worker, push-HTTP, and the new
pgmq+NOTIFY step binding — through real df.start / df.http / pgmq.send / df.signal,
not mocks.

Requires the combined image (df + pgmq). Build + run:

    docker build -t pgflows-e2e-dfpgmq:latest tests/e2e/docker
    docker compose -f tests/e2e/docker/docker-compose.yml up -d --wait

Every test skips automatically if the pg_durable extension is not present, so the
suite stays green on the plain pgmq image.
"""

from __future__ import annotations

import asyncio
import json
import os
import socket

import asyncpg
import pytest
import pytest_asyncio
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

from pgflows.app import WorkflowApp
from pgflows.config import PgflowsConfig
from pgflows.dsl import http, pgmq_step
from pgflows.fastapi_integration import create_pgflows_router

_TEST_DSN = os.getenv(
    "PGFLOWS_TEST_DSN",
    "postgresql://pgflows:pgflows@127.0.0.1:5433/pgflows_test",
)


# --- step + workflow models -------------------------------------------------


class NameIn(BaseModel):
    name: str


class GreetOut(BaseModel):
    message: str


class NumIn(BaseModel):
    n: int


class DblOut(BaseModel):
    val: int


class AddOut(BaseModel):
    total: int


# --- fixtures ----------------------------------------------------------------


async def _has_pg_durable() -> bool:
    try:
        conn = await asyncio.wait_for(asyncpg.connect(_TEST_DSN, ssl=False), timeout=2)
    except Exception:
        return False
    try:
        row = await conn.fetchrow("SELECT 1 FROM pg_extension WHERE extname = 'pg_durable'")
        return row is not None
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def live_app():
    if not await _has_pg_durable():
        pytest.skip("pg_durable not installed — run tests/e2e/docker compose image")

    # Use the default step queue/channel so they match pgmq_step()'s defaults.
    config = PgflowsConfig(
        dsn=_TEST_DSN,
        workflow_queue="pgflows_live_wf",
        step_queue="pgflows_steps",
        step_notify_channel="pgflows_steps",
        otel_enabled=False,
        db_ssl=False,
    )
    app = WorkflowApp(config=config)

    @app.step()
    async def greet(ctx, inp: NameIn) -> GreetOut:
        return GreetOut(message=f"hi {inp.name}")

    @app.step()
    async def double_it(ctx, inp: NumIn) -> DblOut:
        return DblOut(val=inp.n * 2)

    @app.step()
    async def add_ten(ctx, inp: DblOut) -> AddOut:
        return AddOut(total=inp.val + 10)

    @app.workflow()
    async def greet_workflow(ctx, inp: NameIn) -> GreetOut:
        return await ctx.step(greet, inp)

    await app.initialize()
    yield app
    await app.close()


async def _wait_status(client, instance_id: str, timeout: float = 25.0) -> str:
    """Poll until the instance reaches a terminal state. Returns final status."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        status = await client.status(instance_id)
        if status in ("completed", "failed", "cancelled"):
            return status
        await asyncio.sleep(0.25)
    return await client.status(instance_id)


def _free_port() -> int:
    s = socket.socket()
    s.bind(("0.0.0.0", 0))
    port = s.getsockname()[1]
    s.close()
    return port


# --- detection (the fixed bug) ----------------------------------------------


async def test_extension_detected(live_app):
    # check_extension now matches extname 'pg_durable' (not the 'df' schema name).
    assert live_app.pg_durable_available is True
    client = live_app.pg_durable
    assert client is not None


async def test_client_runs_plain_sql_flow(live_app):
    client = live_app.pg_durable
    instance_id = await client.start("SELECT 1 AS x", label="live-sql")
    assert len(instance_id) > 0
    assert await _wait_status(client, instance_id) == "completed"


# --- pull mode (Python worker, in-process steps) ----------------------------


async def test_pull_mode_workflow_completes(live_app):
    instance_id = await live_app.start(
        live_app.registry.get_workflow("greet_workflow").fn, NameIn(name="pull")
    )
    # drive the worker a few batches until the instance finishes
    for _ in range(20):
        await live_app.process_once()
        status = await live_app.get_status(instance_id)
        if status.state.value in ("completed", "failed"):
            break
        await asyncio.sleep(0.1)
    status = await live_app.get_status(instance_id)
    assert status.state.value == "completed"
    assert status.output == {"message": "hi pull"}


# --- pgmq + NOTIFY binding (the new feature) --------------------------------


async def test_pgmq_step_single_roundtrip(live_app):
    client = live_app.pg_durable
    worker = asyncio.create_task(live_app.run_step_worker())
    try:
        await client.setvar("input", json.dumps({"name": "ada"}))
        node = pgmq_step("greet", capture="r")
        instance_id = await client.start(node, label="pgmq-greet")
        assert await _wait_status(client, instance_id) == "completed"
        result = await client.result(instance_id)
        # the final read envelope holds the step output at rows[0].result
        assert result["rows"][0]["result"] == {"message": "hi ada"}
    finally:
        live_app._step_worker.shutdown()
        worker.cancel()
        try:
            await worker
        except (asyncio.CancelledError, Exception):
            pass


async def test_pgmq_step_threads_output_to_next_input(live_app):
    client = live_app.pg_durable
    worker = asyncio.create_task(live_app.run_step_worker())
    try:
        await client.setvar("input", json.dumps({"n": 4}))
        node = pgmq_step("double_it", result_key="{sys_instance_id}:s1", capture="r1") >> pgmq_step(
            "add_ten",
            result_key="{sys_instance_id}:s2",
            input_expr="$r1::jsonb",
            capture="r2",
        )
        instance_id = await client.start(node, label="pgmq-thread")
        assert await _wait_status(client, instance_id) == "completed"
        result = await client.result(instance_id)
        # 4 * 2 = 8, then + 10 = 18 — proves step1 output reached step2 input.
        assert result["rows"][0]["result"] == {"total": 18}
    finally:
        live_app._step_worker.shutdown()
        worker.cancel()
        try:
            await worker
        except (asyncio.CancelledError, Exception):
            pass


async def test_pgmq_exporter_sql_is_valid_df(live_app):
    client = live_app.pg_durable
    worker = asyncio.create_task(live_app.run_step_worker())
    try:
        exporter = live_app.exporter(mode="pgmq")
        sql = exporter.compose("exp_greet", ["greet"])
        await client.setvar("input", json.dumps({"name": "exp"}))
        # Execute exactly what the exporter emits (setvar/comments + df.start).
        async with asyncpg.create_pool(_TEST_DSN, ssl=False) as pool:
            async with pool.acquire() as conn:
                await conn.execute(sql)
        # find the instance the exported SQL started, by label
        instances = await client.list_instances(limit=100)
        match = [i for i in instances if i.get("label") == "exp_greet"]
        assert match, "exported workflow did not create an instance"
        instance_id = match[0]["instance_id"]
        assert await _wait_status(client, instance_id) == "completed"
    finally:
        live_app._step_worker.shutdown()
        worker.cancel()
        try:
            await worker
        except (asyncio.CancelledError, Exception):
            pass


# --- push HTTP binding (the fixed df.http path) -----------------------------


async def test_push_http_step_roundtrip(live_app):
    port = _free_port()
    fastapi_app = FastAPI()
    fastapi_app.include_router(create_pgflows_router(live_app, prefix="/pgflows"))
    server = uvicorn.Server(
        uvicorn.Config(fastapi_app, host="0.0.0.0", port=port, log_level="warning")
    )
    server_task = asyncio.create_task(server.serve())
    try:
        for _ in range(50):
            if server.started:
                break
            await asyncio.sleep(0.1)
        assert server.started, "uvicorn did not start"

        client = live_app.pg_durable
        base_url = f"http://host.docker.internal:{port}/pgflows"
        await client.setvar("base_url", base_url)
        await client.setvar("input", json.dumps({"name": "bob"}))
        node = http(
            "{base_url}/steps/greet",
            "POST",
            "{input}",
            {"X-DF-Instance-ID": "{sys_instance_id}", "Content-Type": "application/json"},
        )
        instance_id = await client.start(node, label="http-greet")
        assert await _wait_status(client, instance_id) == "completed"
        result = await client.result(instance_id)
        # df.http result envelope: {"status":200,"body":"...","ok":true,...}
        assert result["status"] == 200
        assert json.loads(result["body"]) == {"message": "hi bob"}
    finally:
        server.should_exit = True
        try:
            await asyncio.wait_for(server_task, timeout=5)
        except (TimeoutError, asyncio.CancelledError, Exception):
            server_task.cancel()
