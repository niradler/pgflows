import pytest
from pydantic import BaseModel

from pgflows.app import WorkflowApp
from pgflows.types import WorkflowState


class SimpleInput(BaseModel):
    x: int


class SimpleOutput(BaseModel):
    y: int


@pytest.mark.asyncio
async def test_monitor_pending_then_completed(pgflows_config):
    app = WorkflowApp(config=pgflows_config)

    @app.workflow()
    async def monitor_wf(ctx, input: SimpleInput) -> SimpleOutput:
        return SimpleOutput(y=input.x + 1)

    await app.initialize()
    try:
        instance_id = await app.start(monitor_wf, SimpleInput(x=10))
        status = await app.get_status(instance_id)
        assert status.state in {WorkflowState.PENDING, WorkflowState.RUNNING}

        await app.process_once()

        status = await app.get_status(instance_id)
        assert status.state == WorkflowState.COMPLETED
        assert status.output["y"] == 11
    finally:
        await app.close()


@pytest.mark.asyncio
async def test_cancel_workflow(pgflows_config):
    app = WorkflowApp(config=pgflows_config)

    @app.workflow()
    async def cancel_wf(ctx, input: SimpleInput) -> SimpleOutput:
        return SimpleOutput(y=input.x)

    await app.initialize()
    try:
        instance_id = await app.start(cancel_wf, SimpleInput(x=5))
        await app.cancel(instance_id)
        status = await app.get_status(instance_id)
        assert status.state == WorkflowState.CANCELLED
    finally:
        await app.close()
