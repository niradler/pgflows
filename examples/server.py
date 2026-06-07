"""Runnable pgflows app server — the Python-app half of the two-container stack.

Pairs with the df+pgmq Postgres image (tests/e2e/docker). On startup it initializes
the WorkflowApp, registers a demo step + workflow, mounts the push-mode FastAPI
router, and launches both the pull worker and the pgmq+NOTIFY step worker in the
background. Exercises all three step bindings against a real database.

Run locally:
    PGFLOWS_DSN=postgresql://pgflows:pgflows@127.0.0.1:5433/pgflows_test \
        uv run uvicorn examples.server:app --port 8000

Or via docker compose (see docker-compose.full.yml).
"""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantic import BaseModel

from pgflows import PgflowsConfig, WorkflowApp
from pgflows.fastapi_integration import create_pgflows_router

DSN = os.getenv("PGFLOWS_DSN", "postgresql://pgflows:pgflows@127.0.0.1:5433/pgflows_test")

pgflows_app = WorkflowApp(
    PgflowsConfig(dsn=DSN, otel_enabled=False, db_ssl=False)
)


class NameIn(BaseModel):
    name: str


class GreetOut(BaseModel):
    message: str


@pgflows_app.step()
async def greet(ctx, inp: NameIn) -> GreetOut:
    return GreetOut(message=f"hi {inp.name}")


@pgflows_app.workflow()
async def greet_workflow(ctx, inp: NameIn) -> GreetOut:
    return await ctx.step(greet, inp)


@asynccontextmanager
async def lifespan(_: FastAPI):
    await pgflows_app.initialize()
    tasks = [asyncio.create_task(pgflows_app.run_worker())]
    if pgflows_app.pg_durable_available:
        tasks.append(asyncio.create_task(pgflows_app.run_step_worker()))
    try:
        yield
    finally:
        for t in tasks:
            t.cancel()
        await pgflows_app.close()


app = FastAPI(lifespan=lifespan)
app.include_router(create_pgflows_router(pgflows_app, prefix="/pgflows"))


@app.get("/health")
async def health() -> dict[str, object]:
    return {"ok": True, "pg_durable": pgflows_app.pg_durable_available}
