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
from pgflows.dsl import http, if_node, sql_node, wait_for_signal, worker_step
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

    # Use the default step queue/channel so they match worker_step()'s defaults.
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

    @app.step()
    async def add_hundred(ctx, inp: DblOut) -> AddOut:
        return AddOut(total=inp.val + 100)

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


async def test_worker_step_single_roundtrip(live_app):
    client = live_app.pg_durable
    worker = asyncio.create_task(live_app.run_step_worker())
    try:
        await client.setvar("input", json.dumps({"name": "ada"}))
        node = worker_step("greet", capture="r")
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


async def test_worker_step_threads_output_to_next_input(live_app):
    client = live_app.pg_durable
    worker = asyncio.create_task(live_app.run_step_worker())
    try:
        await client.setvar("input", json.dumps({"n": 4}))
        node = worker_step(
            "double_it", result_key="{sys_instance_id}:s1", capture="r1"
        ) >> worker_step(
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


async def test_worker_exporter_sql_is_valid_df(live_app):
    client = live_app.pg_durable
    worker = asyncio.create_task(live_app.run_step_worker())
    try:
        exporter = live_app.exporter(mode="worker")
        sql = exporter.compose("exp_greet", ["greet"])
        await client.setvar("input", json.dumps({"name": "exp"}))
        # The pgmq-mode preamble is comment-only, so the exported text is a single
        # df.start statement — run it and capture the instance id it returns.
        async with asyncpg.create_pool(_TEST_DSN, ssl=False) as pool:
            async with pool.acquire() as conn:
                instance_id = await conn.fetchval(sql)
        assert instance_id, "exported SQL did not return an instance id"
        assert await _wait_status(client, instance_id) == "completed"
    finally:
        live_app._step_worker.shutdown()
        worker.cancel()
        try:
            await worker
        except (asyncio.CancelledError, Exception):
            pass


# --- complex workflow: sequence + parallel + branch + side-effects ----------


@pytest.mark.timeout(90)
async def test_complex_workflow_parallel_branch_threading(live_app):
    """A realistic non-linear graph on real df+pgmq, end to end:

        double_it                  (pgmq)  {n:5}    -> {val:10}   capture $d
          ~> ( add_ten($d)         (pgmq, parallel) -> {total:20}  capture $a
             & add_hundred($d) )   (pgmq, parallel) -> {total:110} capture $h
          ~> INSERT audit('ten', $a)        (real side-effect, capture from a branch)
          ~> INSERT audit('hundred', $h)    (real side-effect, capture from a branch)
          ~> IF $a.total > 15 THEN audit('high') ELSE audit('low')   (real branch)

    pg_durable is the orchestrator: it runs the two pgmq steps concurrently (the
    real StepWorker services both), joins them (& waits for ALL), threads the
    captures made *inside* the parallel branches into the post-join nodes, and
    durably drives the conditional. We thread data via result captures and keep a
    single durable var — df serializes the durable-vars snapshot non-deterministically
    across a JOIN replay when >1 var is set, so prefer captures over many setvars.
    """
    import uuid

    run = uuid.uuid4().hex[:12]
    client = live_app.pg_durable

    async with asyncpg.create_pool(_TEST_DSN, ssl=False) as pool:
        async with pool.acquire() as conn:
            await conn.execute(
                "CREATE TABLE IF NOT EXISTS pgflows_audit "
                "(run text, label text, payload jsonb)"
            )

    worker = asyncio.create_task(live_app.run_step_worker())
    try:
        await client.clearvars()  # keep a single durable var (see docstring)
        await client.setvar("input", json.dumps({"n": 5}))

        def _audit(label: str, value_sql: str) -> str:
            return (
                f"INSERT INTO pgflows_audit(run,label,payload) "
                f"VALUES ('{run}','{label}',{value_sql})"
            )

        d = worker_step("double_it", result_key="{sys_instance_id}:d", capture="d")
        a = worker_step(
            "add_ten", input_expr="$d::jsonb", result_key="{sys_instance_id}:a", capture="a"
        )
        h = worker_step(
            "add_hundred", input_expr="$d::jsonb", result_key="{sys_instance_id}:h", capture="h"
        )
        audit_ten = sql_node(_audit("ten", "$a::jsonb"))
        audit_hundred = sql_node(_audit("hundred", "$h::jsonb"))
        branch = if_node(
            sql_node("SELECT ($a::jsonb->>'total')::int > 15"),
            sql_node(_audit("branch", "'\"high\"'::jsonb")),
            sql_node(_audit("branch", "'\"low\"'::jsonb")),
        )

        graph = d >> (a & h) >> audit_ten >> audit_hundred >> branch
        instance_id = await client.start(graph, label="complex")
        assert await _wait_status(client, instance_id, timeout=80) == "completed"

        # Verify the real side-effects the graph wrote.
        async with asyncpg.create_pool(_TEST_DSN, ssl=False) as pool:
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT label, payload FROM pgflows_audit WHERE run = $1 ORDER BY label",
                    run,
                )
        data = {r["label"]: json.loads(r["payload"]) for r in rows}
        # 5*2=10 ($d); parallel: 10+10=20 ($a) and 10+100=110 ($h); 20>15 → high.
        assert data["ten"] == {"total": 20}
        assert data["hundred"] == {"total": 110}
        assert data["branch"] == "high"
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


# --- execution-history surface (instance_info/nodes/executions/metrics) -----


@pytest.mark.timeout(90)
async def test_execution_history_surface(live_app):
    client = live_app.pg_durable
    graph = (
        sql_node("SELECT 21 AS v").capture("v")
        >> sql_node("SELECT 1 AS a")
        >> sql_node("SELECT 99 AS done")
    )
    instance_id = await client.start(graph, label="history-run")
    assert await _wait_status(client, instance_id, timeout=80) == "completed"

    info = await client.instance_info(instance_id)
    assert info is not None and info.status == "completed"

    nodes = await client.instance_nodes(instance_id)
    assert len(nodes) >= 3
    assert all(n.status for n in nodes)
    assert any(n.result_name == "v" for n in nodes)  # capture recorded in the trail

    execs = await client.instance_executions(instance_id)
    assert execs and execs[0].duration_ms is not None

    metrics = await client.metrics()
    assert metrics.total_instances >= 1 and metrics.completed_instances >= 1


# --- app.worker_step binds the configured queue (no manual queue= needed) ----


async def test_app_worker_step_custom_queue_roundtrip():
    if not await _has_pg_durable():
        pytest.skip("pg_durable not installed")

    config = PgflowsConfig(
        dsn=_TEST_DSN,
        workflow_queue="pgflows_custom_wf",
        step_queue="pgflows_custom_steps",
        step_notify_channel="pgflows_custom_steps",
        otel_enabled=False,
        db_ssl=False,
    )
    app = WorkflowApp(config=config)

    @app.step()
    async def greet(ctx, inp: NameIn) -> GreetOut:
        return GreetOut(message=f"hi {inp.name}")

    await app.initialize()
    worker = asyncio.create_task(app.run_step_worker())
    try:
        client = app.pg_durable
        await client.setvar("input", json.dumps({"name": "cfg"}))
        # app.worker_step injects the configured (non-default) queue/channel — the bare
        # worker_step() would enqueue to 'pgflows_steps' and hang against this worker.
        node = app.worker_step("greet", capture="r")
        instance_id = await client.start(node, label="custom-queue")
        assert await _wait_status(client, instance_id) == "completed"
        result = await client.result(instance_id)
        assert result["rows"][0]["result"] == {"message": "hi cfg"}
    finally:
        app._step_worker.shutdown()
        worker.cancel()
        try:
            await worker
        except (asyncio.CancelledError, Exception):
            pass
        await app.close()


# --- signal-approval gate reads the envelope under ->'data' (the doc fix) ----


@pytest.mark.timeout(60)
async def test_signal_approval_envelope_branch(live_app):
    import uuid

    run = uuid.uuid4().hex[:12]
    client = live_app.pg_durable

    async with live_app.acquire() as conn:
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS pgflows_approval(run text, decision text)"
        )

    decision = wait_for_signal("approval", 30).capture("decision")
    branch = if_node(
        sql_node(
            "SELECT NOT ($decision::jsonb->>'timed_out')::boolean "
            "AND coalesce(($decision::jsonb->'data'->>'approved')::boolean, false)"
        ),
        sql_node(f"INSERT INTO pgflows_approval VALUES ('{run}','approved')"),
        sql_node(f"INSERT INTO pgflows_approval VALUES ('{run}','rejected')"),
    )
    instance_id = await client.start(decision >> branch, label="approval")

    await asyncio.sleep(2)
    assert await client.status(instance_id) == "running"  # parked on the signal

    await client.signal(instance_id, "approval", {"approved": True})
    assert await _wait_status(client, instance_id, timeout=30) == "completed"

    async with live_app.acquire() as conn:
        rows = await conn.fetch(
            "SELECT decision FROM pgflows_approval WHERE run = $1", run
        )
    assert [r["decision"] for r in rows] == ["approved"]
