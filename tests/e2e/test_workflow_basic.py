import pytest
from pydantic import BaseModel

from pyflows.app import WorkflowApp
from pyflows.types import WorkflowState


class GreetInput(BaseModel):
    name: str


class GreetOutput(BaseModel):
    message: str


@pytest.mark.asyncio
async def test_workflow_completes(pyflows_config):
    app = WorkflowApp(config=pyflows_config)

    @app.workflow()
    async def greet_workflow(ctx, input: GreetInput) -> GreetOutput:
        return GreetOutput(message=f"Hello, {input.name}!")

    await app.initialize()
    try:
        instance_id = await app.start(greet_workflow, GreetInput(name="World"))
        assert instance_id is not None
        processed = await app.process_once()
        assert processed >= 1
        status = await app.get_status(instance_id)
        assert status.state == WorkflowState.COMPLETED
        assert status.output["message"] == "Hello, World!"
    finally:
        await app.close()


@pytest.mark.asyncio
async def test_workflow_with_step(pyflows_config):
    app = WorkflowApp(config=pyflows_config)

    @app.step()
    async def double_value(ctx, input: GreetInput) -> GreetOutput:
        return GreetOutput(message=f"{input.name}{input.name}")

    @app.workflow()
    async def step_workflow(ctx, input: GreetInput) -> GreetOutput:
        return await ctx.step(double_value, input)

    await app.initialize()
    try:
        instance_id = await app.start(step_workflow, GreetInput(name="hi"))
        await app.process_once()
        status = await app.get_status(instance_id)
        assert status.state == WorkflowState.COMPLETED
        assert "hihi" in status.output["message"]
    finally:
        await app.close()


@pytest.mark.asyncio
async def test_list_workflows(pyflows_config):
    app = WorkflowApp(config=pyflows_config)

    @app.workflow()
    async def list_test_wf(ctx, input: GreetInput) -> GreetOutput:
        return GreetOutput(message="done")

    await app.initialize()
    try:
        await app.start(list_test_wf, GreetInput(name="test"))
        await app.process_once()
        instances = await app.list_workflows(workflow_name="list_test_wf")
        assert len(instances) >= 1
        assert instances[0].state == WorkflowState.COMPLETED
    finally:
        await app.close()
