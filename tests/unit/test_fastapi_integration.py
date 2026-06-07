from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel

from pgflows.app import WorkflowApp
from pgflows.config import PgflowsConfig
from pgflows.fastapi_integration import create_pgflows_router
from pgflows.types import WorkflowState, WorkflowStatus

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


class EchoInput(BaseModel):
    message: str


class EchoOutput(BaseModel):
    echoed: str


def _make_app_with_step() -> tuple[WorkflowApp, FastAPI]:
    """Return an initialized-enough WorkflowApp + FastAPI with the router mounted."""
    app = WorkflowApp(config=PgflowsConfig(dsn="postgresql://x:x@localhost/x"))
    app._initialized = True

    # Register a step directly via the decorator
    @app.step()
    async def echo(ctx: Any, input: EchoInput) -> EchoOutput:
        return EchoOutput(echoed=input.message)

    # Register a workflow
    @app.workflow()
    async def my_workflow(ctx: Any, input: EchoInput) -> EchoOutput:
        return EchoOutput(echoed=input.message)

    # Provide minimal state mock so management endpoints work
    state_mock = AsyncMock()
    app._state = state_mock

    # Stub app.start so it doesn't need a queue
    app.start = AsyncMock(return_value="instance-abc")  # type: ignore[method-assign]

    # Stub app.get_status
    from datetime import datetime

    _status = WorkflowStatus(
        workflow_id="instance-abc",
        name="my_workflow",
        state=WorkflowState.RUNNING,
        created_at=datetime(2024, 1, 1),
        updated_at=datetime(2024, 1, 1),
    )
    app.get_status = AsyncMock(return_value=_status)  # type: ignore[method-assign]
    app.cancel = AsyncMock(return_value=None)  # type: ignore[method-assign]
    app.list_workflows = AsyncMock(return_value=[_status])  # type: ignore[method-assign]

    fastapi_app = FastAPI()
    router = create_pgflows_router(app, prefix="/pgflows")
    fastapi_app.include_router(router)

    return app, fastapi_app


# ---------------------------------------------------------------------------
# Step endpoint
# ---------------------------------------------------------------------------


def test_execute_step_unknown_returns_404():
    _, fastapi_app = _make_app_with_step()
    with TestClient(fastapi_app) as client:
        resp = client.post("/pgflows/steps/does_not_exist", json={"message": "hi"})
    assert resp.status_code == 404
    assert "not registered" in resp.json()["detail"]


def test_execute_step_valid_input_returns_200():
    _, fastapi_app = _make_app_with_step()
    with TestClient(fastapi_app) as client:
        resp = client.post("/pgflows/steps/echo", json={"message": "hello"})
    assert resp.status_code == 200
    assert resp.json() == {"echoed": "hello"}


def test_execute_step_invalid_input_returns_422():
    _, fastapi_app = _make_app_with_step()
    with TestClient(fastapi_app) as client:
        # Missing required field "message"
        resp = client.post("/pgflows/steps/echo", json={"wrong_field": 123})
    assert resp.status_code == 422


def test_execute_step_exception_returns_500():
    app, fastapi_app = _make_app_with_step()

    @app.step(name="boom")
    async def boom_step(ctx: Any, input: EchoInput) -> EchoOutput:
        raise ValueError("intentional error")

    router = create_pgflows_router(app, prefix="/test")
    app2 = FastAPI()
    app2.include_router(router)

    with TestClient(app2) as client:
        resp = client.post("/test/steps/boom", json={"message": "x"})
    assert resp.status_code == 500
    assert "intentional error" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Workflow management endpoints
# ---------------------------------------------------------------------------


def test_start_workflow_unknown_returns_404():
    _, fastapi_app = _make_app_with_step()
    with TestClient(fastapi_app) as client:
        resp = client.post("/pgflows/workflows/no_such_workflow/start", json={"input": {}})
    assert resp.status_code == 404


def test_start_workflow_returns_instance_id():
    _, fastapi_app = _make_app_with_step()
    with TestClient(fastapi_app) as client:
        resp = client.post(
            "/pgflows/workflows/my_workflow/start", json={"input": {"message": "go"}}
        )
    assert resp.status_code == 200
    assert resp.json()["instance_id"] == "instance-abc"


def test_start_workflow_invalid_input_returns_422():
    _, fastapi_app = _make_app_with_step()
    with TestClient(fastapi_app) as client:
        resp = client.post(
            "/pgflows/workflows/my_workflow/start",
            json={"input": {"wrong": "field"}},
        )
    assert resp.status_code == 422


def test_get_workflow_status_returns_200():
    _, fastapi_app = _make_app_with_step()
    with TestClient(fastapi_app) as client:
        resp = client.get("/pgflows/workflows/instance-abc")
    assert resp.status_code == 200
    data = resp.json()
    assert data["workflow_id"] == "instance-abc"
    assert data["state"] == "running"


def test_get_workflow_status_not_found_returns_404():
    app, fastapi_app = _make_app_with_step()
    app.get_status = AsyncMock(  # type: ignore[method-assign]
        side_effect=Exception("not found")
    )
    with TestClient(fastapi_app) as client:
        resp = client.get("/pgflows/workflows/bad-id")
    assert resp.status_code == 404


def test_cancel_workflow_returns_cancelled():
    _, fastapi_app = _make_app_with_step()
    with TestClient(fastapi_app) as client:
        resp = client.delete("/pgflows/workflows/instance-abc")
    assert resp.status_code == 200
    assert resp.json() == {"cancelled": True, "instance_id": "instance-abc"}


def test_list_workflows_returns_list():
    _, fastapi_app = _make_app_with_step()
    with TestClient(fastapi_app) as client:
        resp = client.get("/pgflows/workflows")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)
    assert len(resp.json()) == 1


# ---------------------------------------------------------------------------
# Signal endpoint
# ---------------------------------------------------------------------------


def test_signal_without_pg_durable_returns_503():
    app, fastapi_app = _make_app_with_step()
    app._pg_durable_available = False
    with TestClient(fastapi_app) as client:
        resp = client.post(
            "/pgflows/workflows/inst-1/signal",
            json={"signal_name": "approve", "data": None},
        )
    assert resp.status_code == 503


def test_signal_with_pg_durable_returns_200():
    app, fastapi_app = _make_app_with_step()
    app._pg_durable_available = True
    pg_client_mock = AsyncMock()
    pg_client_mock.signal = AsyncMock(return_value=None)
    app._pg_durable_client = pg_client_mock

    with TestClient(fastapi_app) as client:
        resp = client.post(
            "/pgflows/workflows/inst-1/signal",
            json={"signal_name": "approve", "data": {"ok": True}},
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["signalled"] is True
    assert body["signal"] == "approve"
    pg_client_mock.signal.assert_awaited_once_with("inst-1", "approve", {"ok": True})
