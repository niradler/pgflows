import pytest
from pydantic import BaseModel

from pgflows.app import WorkflowApp
from pgflows.types import RetryConfig, WorkflowState


class NumInput(BaseModel):
    value: int


class NumOutput(BaseModel):
    result: int


@pytest.mark.asyncio
async def test_step_retries_on_failure(pgflows_config):
    counter = {"n": 0}
    app = WorkflowApp(config=pgflows_config)

    @app.step(retry=RetryConfig(max_retries=2, initial_delay_seconds=0.01))
    async def flaky_step(ctx, input: NumInput) -> NumOutput:
        counter["n"] += 1
        if counter["n"] < 3:
            raise ValueError("transient")
        return NumOutput(result=input.value * 2)

    @app.workflow()
    async def retry_workflow(ctx, input: NumInput) -> NumOutput:
        return await ctx.step(flaky_step, input)

    await app.initialize()
    try:
        instance_id = await app.start(retry_workflow, NumInput(value=5))
        await app.process_once()
        status = await app.get_status(instance_id)
        assert status.state == WorkflowState.COMPLETED
        assert status.output["result"] == 10
        assert counter["n"] == 3
    finally:
        await app.close()


@pytest.mark.asyncio
async def test_step_fails_after_max_retries(pgflows_config):
    app = WorkflowApp(config=pgflows_config)

    @app.step(retry=RetryConfig(max_retries=1, initial_delay_seconds=0.01))
    async def always_fails(ctx, input: NumInput) -> NumOutput:
        raise ValueError("always")

    @app.workflow()
    async def fail_workflow(ctx, input: NumInput) -> NumOutput:
        return await ctx.step(always_fails, input)

    await app.initialize()
    try:
        instance_id = await app.start(fail_workflow, NumInput(value=1))
        await app.process_once()
        status = await app.get_status(instance_id)
        assert status.state == WorkflowState.FAILED
    finally:
        await app.close()
