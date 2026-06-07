
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


def test_app_workflow_decorator_preserves_name():
    app = _make_app()

    @app.workflow(name="custom_name")
    async def some_fn(ctx, input: GreetInput) -> GreetOutput:
        return GreetOutput(message="x")

    assert "custom_name" in app.registry.list_workflows()
    assert "some_fn" not in app.registry.list_workflows()
