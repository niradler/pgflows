"""E2E tests for push-mode (FastAPI + pg_durable) integration.

These tests require:
  1. Postgres running (docker compose up -d) — skipped automatically otherwise.
  2. The pg_durable (df) extension — tests that need it skip if it's absent.
"""

from __future__ import annotations

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pydantic import BaseModel

from pgflows.app import WorkflowApp
from pgflows.fastapi_integration import create_pgflows_router
from pgflows.telemetry import PgflowsTelemetry

# ---------------------------------------------------------------------------
# Shared fixtures  (function-scoped so pool and event loop stay aligned)
# ---------------------------------------------------------------------------


class GreetInput(BaseModel):
    name: str


class GreetOutput(BaseModel):
    message: str


@pytest_asyncio.fixture
async def initialized_app(pgflows_config):
    app = WorkflowApp(config=pgflows_config)

    @app.step()
    async def greet(ctx, input: GreetInput) -> GreetOutput:
        return GreetOutput(message=f"hello, {input.name}")

    @app.workflow()
    async def greet_workflow(ctx, input: GreetInput) -> GreetOutput:
        return await ctx.step(greet, input)

    await app.initialize()
    yield app
    await app.close()


@pytest_asyncio.fixture
async def async_client(initialized_app):
    fastapi_app = FastAPI()
    router = create_pgflows_router(initialized_app, prefix="/pgflows")
    fastapi_app.include_router(router)
    async with AsyncClient(
        transport=ASGITransport(app=fastapi_app), base_url="http://test"
    ) as client:
        yield client


# ---------------------------------------------------------------------------
# Push-mode step endpoint (works even without pg_durable)
# ---------------------------------------------------------------------------


async def test_push_step_executes_and_returns_result(async_client):
    resp = await async_client.post("/pgflows/steps/greet", json={"name": "world"})
    assert resp.status_code == 200
    assert resp.json() == {"message": "hello, world"}


async def test_push_step_unknown_returns_404(async_client):
    resp = await async_client.post("/pgflows/steps/unknown_step", json={})
    assert resp.status_code == 404


async def test_push_step_invalid_input_returns_422(async_client):
    resp = await async_client.post("/pgflows/steps/greet", json={"wrong": "field"})
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Workflow management over real DB
# ---------------------------------------------------------------------------


async def test_start_workflow_returns_instance_id(async_client):
    resp = await async_client.post(
        "/pgflows/workflows/greet_workflow/start",
        json={"input": {"name": "Nir"}},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "instance_id" in data
    assert len(data["instance_id"]) > 0


async def test_get_workflow_status(async_client):
    start = await async_client.post(
        "/pgflows/workflows/greet_workflow/start",
        json={"input": {"name": "Alice"}},
    )
    instance_id = start.json()["instance_id"]

    resp = await async_client.get(f"/pgflows/workflows/{instance_id}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["workflow_id"] == instance_id


async def test_list_workflows(async_client):
    resp = await async_client.get("/pgflows/workflows")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


async def test_cancel_workflow(async_client):
    start = await async_client.post(
        "/pgflows/workflows/greet_workflow/start",
        json={"input": {"name": "ToCancel"}},
    )
    instance_id = start.json()["instance_id"]
    resp = await async_client.delete(f"/pgflows/workflows/{instance_id}")
    assert resp.status_code == 200
    assert resp.json()["cancelled"] is True


# ---------------------------------------------------------------------------
# pg_durable-specific: signal (skipped if extension absent)
# ---------------------------------------------------------------------------


async def test_signal_without_pg_durable_returns_503(async_client, initialized_app):
    if initialized_app.pg_durable_available:
        pytest.skip("pg_durable is installed — this test needs it absent")
    resp = await async_client.post(
        "/pgflows/workflows/fake-id/signal",
        json={"signal_name": "approve"},
    )
    assert resp.status_code == 503


async def test_pg_durable_client_available(initialized_app):
    if not initialized_app.pg_durable_available:
        pytest.skip("pg_durable (df) extension not installed")
    client = initialized_app.pg_durable
    assert client is not None


# ---------------------------------------------------------------------------
# X-DF-Instance-ID header (works without pg_durable)
# ---------------------------------------------------------------------------


async def test_push_step_forwards_instance_id_header(async_client):
    resp = await async_client.post(
        "/pgflows/steps/greet",
        json={"name": "world"},
        headers={"X-DF-Instance-ID": "e2e-instance-42"},
    )
    assert resp.status_code == 200
    assert resp.json() == {"message": "hello, world"}


async def test_push_step_works_without_instance_id_header(async_client):
    resp = await async_client.post("/pgflows/steps/greet", json={"name": "world"})
    assert resp.status_code == 200
    assert resp.json() == {"message": "hello, world"}


# ---------------------------------------------------------------------------
# OTel span emission in push mode (real app, in-memory exporter)
# ---------------------------------------------------------------------------


async def test_push_step_emits_otel_span(pgflows_config):
    exporter = InMemorySpanExporter()
    app = WorkflowApp(config=pgflows_config)
    app._telemetry = PgflowsTelemetry.with_in_memory_exporter(exporter)

    @app.step()
    async def greet(ctx, input: GreetInput) -> GreetOutput:
        return GreetOutput(message=f"hello, {input.name}")

    await app.initialize()
    try:
        fastapi_app = FastAPI()
        router = create_pgflows_router(app, prefix="/pgflows")
        fastapi_app.include_router(router)
        async with AsyncClient(
            transport=ASGITransport(app=fastapi_app), base_url="http://test"
        ) as client:
            resp = await client.post(
                "/pgflows/steps/greet",
                json={"name": "span-test"},
                headers={"X-DF-Instance-ID": "span-e2e-99"},
            )
        assert resp.status_code == 200
        spans = exporter.get_finished_spans()
        assert len(spans) == 1
        span = spans[0]
        assert span.name == "pgflows.step.greet"
        assert span.attributes["pgflows.step.name"] == "greet"
        assert span.attributes["pgflows.workflow.id"] == "span-e2e-99"
    finally:
        await app.close()
