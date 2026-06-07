"""E2E tests for push-mode (FastAPI + pg_durable) integration.

These tests require:
  1. Postgres running (docker compose up -d) — skipped automatically otherwise.
  2. The pg_durable (df) extension — tests that need it skip if it's absent.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel

from pgflows.app import WorkflowApp
from pgflows.fastapi_integration import create_pgflows_router

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


class GreetInput(BaseModel):
    name: str


class GreetOutput(BaseModel):
    message: str


@pytest.fixture(scope="module")
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


@pytest.fixture(scope="module")
def test_client(initialized_app):
    fastapi_app = FastAPI()
    router = create_pgflows_router(initialized_app, prefix="/pgflows")
    fastapi_app.include_router(router)
    with TestClient(fastapi_app) as client:
        yield client


# ---------------------------------------------------------------------------
# Push-mode step endpoint (works even without pg_durable)
# ---------------------------------------------------------------------------


def test_push_step_executes_and_returns_result(test_client):
    resp = test_client.post("/pgflows/steps/greet", json={"name": "world"})
    assert resp.status_code == 200
    assert resp.json() == {"message": "hello, world"}


def test_push_step_unknown_returns_404(test_client):
    resp = test_client.post("/pgflows/steps/unknown_step", json={})
    assert resp.status_code == 404


def test_push_step_invalid_input_returns_422(test_client):
    resp = test_client.post("/pgflows/steps/greet", json={"wrong": "field"})
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Workflow management over real DB
# ---------------------------------------------------------------------------


def test_start_workflow_returns_instance_id(test_client):
    resp = test_client.post(
        "/pgflows/workflows/greet_workflow/start",
        json={"input": {"name": "Nir"}},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "instance_id" in data
    assert len(data["instance_id"]) > 0


def test_get_workflow_status(test_client):
    # start a workflow to get a real instance_id
    start = test_client.post(
        "/pgflows/workflows/greet_workflow/start",
        json={"input": {"name": "Alice"}},
    )
    instance_id = start.json()["instance_id"]

    resp = test_client.get(f"/pgflows/workflows/{instance_id}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["workflow_id"] == instance_id


def test_list_workflows(test_client):
    resp = test_client.get("/pgflows/workflows")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


def test_cancel_workflow(test_client):
    start = test_client.post(
        "/pgflows/workflows/greet_workflow/start",
        json={"input": {"name": "ToCancel"}},
    )
    instance_id = start.json()["instance_id"]
    resp = test_client.delete(f"/pgflows/workflows/{instance_id}")
    assert resp.status_code == 200
    assert resp.json()["cancelled"] is True


# ---------------------------------------------------------------------------
# pg_durable-specific: signal (skipped if extension absent)
# ---------------------------------------------------------------------------


def test_signal_without_pg_durable_returns_503(test_client, initialized_app):
    if initialized_app.pg_durable_available:
        pytest.skip("pg_durable is installed — this test needs it absent")
    resp = test_client.post(
        "/pgflows/workflows/fake-id/signal",
        json={"signal_name": "approve"},
    )
    assert resp.status_code == 503


def test_pg_durable_client_available(initialized_app):
    if not initialized_app.pg_durable_available:
        pytest.skip("pg_durable (df) extension not installed")
    client = initialized_app.pg_durable
    assert client is not None
