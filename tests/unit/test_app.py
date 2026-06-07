
import pytest
from pydantic import BaseModel

from pgflows.app import WorkflowApp
from pgflows.config import PgflowsConfig


class GreetInput(BaseModel):
    name: str


class GreetOutput(BaseModel):
    message: str


def _make_app() -> WorkflowApp:
    return WorkflowApp(config=PgflowsConfig(dsn="postgresql://x:x@localhost/x"))


def test_app_registers_workflow():
    app = _make_app()

    @app.workflow()
    async def greet(ctx, input: GreetInput) -> GreetOutput:
        return GreetOutput(message=f"hi {input.name}")

    assert "greet" in app.registry.list_workflows()


def test_app_registers_step():
    app = _make_app()

    @app.step()
    async def process(ctx, input: GreetInput) -> GreetOutput:
        return GreetOutput(message="done")

    assert "process" in app.registry.list_steps()


def test_app_not_initialized_raises():
    app = _make_app()
    with pytest.raises(RuntimeError, match="not initialized"):
        app._assert_initialized()


def test_app_worker_step_injects_configured_queue_and_channel():
    app = WorkflowApp(
        config=PgflowsConfig(
            dsn="postgresql://x:x@localhost/x",
            step_queue="orders_steps",
            step_notify_channel="orders_steps",
        )
    )
    sql = str(app.worker_step("charge_card"))
    assert "pgmq.send(''orders_steps''" in sql
    assert "pg_notify(''orders_steps''" in sql


def test_app_worker_step_kwargs_override_config():
    app = WorkflowApp(
        config=PgflowsConfig(dsn="postgresql://x:x@localhost/x", step_queue="orders_steps")
    )
    sql = str(app.worker_step("charge_card", queue="explicit_q", notify_channel="bell"))
    assert "pgmq.send(''explicit_q''" in sql
    assert "pg_notify(''bell''" in sql


def test_app_acquire_requires_initialized():
    app = _make_app()
    with pytest.raises(RuntimeError, match="not initialized"):
        app.acquire()


def test_app_workflow_decorator_preserves_name():
    app = _make_app()

    @app.workflow(name="custom_name")
    async def some_fn(ctx, input: GreetInput) -> GreetOutput:
        return GreetOutput(message="x")

    assert "custom_name" in app.registry.list_workflows()
    assert "some_fn" not in app.registry.list_workflows()
