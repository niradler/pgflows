"""Runnable, self-verifying push-mode workflow examples (pg_durable + pgmq).

Every example below has been run end to end against a real Postgres carrying both
the `pg_durable` (`df`) and `pgmq` extensions — no mocks. Each prints PASS/FAIL by
checking the durable result (and real table side-effects for the complex one).

Run it::

    docker build -t pgflows-e2e-dfpgmq:latest tests/e2e/docker
    docker compose -f tests/e2e/docker/docker-compose.yml up -d --wait
    uv run python examples/push_mode_workflows.py

Key idea: **pg_durable is the orchestrator.** It durably drives the graph
(`~>` sequence, `&` parallel join, `?>`/`!>` branch) and calls out to Python steps;
the `StepWorker` runs those steps and writes results back through a poll table.
Data is threaded with result captures (`|=>`), and we keep a single durable var
(more than one makes pg_durable's vars snapshot serialize non-deterministically and a
parallel-join replay fails).
"""

from __future__ import annotations

import asyncio
import json
import os

import asyncpg
from pydantic import BaseModel

from pgflows import PgflowsConfig, WorkflowApp
from pgflows.dsl import if_node, sql_node, worker_step

DSN = os.getenv("PGFLOWS_DSN", "postgresql://pgflows:pgflows@127.0.0.1:5433/pgflows_test")


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


def build_app() -> WorkflowApp:
    app = WorkflowApp(
        PgflowsConfig(dsn=DSN, step_queue="pgflows_steps", step_notify_channel="pgflows_steps",
                      otel_enabled=False, db_ssl=False)
    )

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

    return app


async def _wait(client, instance_id: str, timeout: float = 60.0) -> str:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        status = await client.status(instance_id)
        if status in ("completed", "failed", "cancelled"):
            return status
        await asyncio.sleep(0.25)
    return await client.status(instance_id)


def _check(name: str, ok: bool, detail: str = "") -> None:
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}{f' — {detail}' if detail else ''}")
    if not ok:
        raise SystemExit(f"example {name!r} failed")


async def example_single_step(app: WorkflowApp) -> None:
    """One worker step: pg_durable dispatches greet, the worker runs it, result polled back."""
    client = app.pg_durable
    await client.setvar("input", json.dumps({"name": "ada"}))
    iid = await client.start(worker_step("greet", capture="r"), label="ex-single")
    status = await _wait(client, iid)
    out = (await client.result(iid))["rows"][0]["result"]
    ok = status == "completed" and out == {"message": "hi ada"}
    _check("single worker step", ok, json.dumps(out))


async def example_threaded_pipeline(app: WorkflowApp) -> None:
    """Two worker steps, output→input threaded via a capture: {n:4} -> {val:8} -> {total:18}."""
    client = app.pg_durable
    await client.setvar("input", json.dumps({"n": 4}))
    node = worker_step("double_it", capture="d") >> worker_step(
        "add_ten", input_expr="$d::jsonb", capture="a"
    )
    iid = await client.start(node, label="ex-pipeline")
    status = await _wait(client, iid)
    out = (await client.result(iid))["rows"][0]["result"]
    _check("threaded pipeline", status == "completed" and out == {"total": 18}, json.dumps(out))


async def example_parallel_branch(app: WorkflowApp) -> None:
    """The full picture — sequence + parallel join + conditional + real side-effects:

        double_it ~> ( add_ten($d) & add_hundred($d) )
                  ~> INSERT audit('ten',$a) ~> INSERT audit('hundred',$h)
                  ~> IF $a.total>15 THEN audit('high') ELSE audit('low')
    """
    import uuid

    run = uuid.uuid4().hex[:12]
    client = app.pg_durable
    async with asyncpg.create_pool(DSN, ssl=False) as pool:
        async with pool.acquire() as conn:
            await conn.execute(
                "CREATE TABLE IF NOT EXISTS pgflows_audit (run text, label text, payload jsonb)"
            )

    await client.clearvars()  # single durable var only
    await client.setvar("input", json.dumps({"n": 5}))

    def audit(label: str, value_sql: str) -> str:
        return (
            f"INSERT INTO pgflows_audit(run,label,payload) "
            f"VALUES ('{run}','{label}',{value_sql})"
        )

    d = worker_step("double_it", capture="d")
    a = worker_step("add_ten", input_expr="$d::jsonb", capture="a")
    h = worker_step("add_hundred", input_expr="$d::jsonb", capture="h")
    branch = if_node(
        sql_node("SELECT ($a::jsonb->>'total')::int > 15"),
        sql_node(audit("branch", "'\"high\"'::jsonb")),
        sql_node(audit("branch", "'\"low\"'::jsonb")),
    )
    graph = (
        d
        >> (a & h)
        >> sql_node(audit("ten", "$a::jsonb"))
        >> sql_node(audit("hundred", "$h::jsonb"))
        >> branch
    )

    iid = await client.start(graph, label="ex-complex")
    status = await _wait(client, iid, timeout=80)

    async with asyncpg.create_pool(DSN, ssl=False) as pool:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT label, payload FROM pgflows_audit WHERE run = $1", run
            )
    data = {r["label"]: json.loads(r["payload"]) for r in rows}
    ok = (
        status == "completed"
        and data.get("ten") == {"total": 20}
        and data.get("hundred") == {"total": 110}
        and data.get("branch") == "high"
    )
    _check("parallel + branch + side-effects", ok, json.dumps(data))


async def main() -> None:
    app = build_app()
    await app.initialize()
    if not app.pg_durable_available:
        print("pg_durable not installed — build tests/e2e/docker and point PGFLOWS_DSN at it.")
        await app.close()
        return

    worker = asyncio.create_task(app.run_step_worker())
    try:
        print("Running push-mode examples against", DSN)
        await example_single_step(app)
        await example_threaded_pipeline(app)
        await example_parallel_branch(app)
        print("All examples passed ✅")
    finally:
        app._step_worker.shutdown()
        worker.cancel()
        try:
            await worker
        except BaseException:
            pass
        await app.close()


if __name__ == "__main__":
    asyncio.run(main())
