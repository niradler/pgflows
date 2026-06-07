import pytest
from pydantic import BaseModel

from pgflows.registry import StepDefinition, WorkflowDefinition, WorkflowRegistry
from pgflows.types import RetryConfig


class MyInput(BaseModel):
    name: str


class MyOutput(BaseModel):
    message: str


async def my_step(ctx, input: MyInput) -> MyOutput:
    return MyOutput(message=f"hello {input.name}")


async def my_workflow(ctx, input: MyInput) -> MyOutput:
    return MyOutput(message="done")


def test_register_step_captures_types():
    reg = WorkflowRegistry()
    defn = reg.register_step(my_step, name="my_step")
    assert isinstance(defn, StepDefinition)
    assert defn.name == "my_step"
    assert defn.input_type is MyInput
    assert defn.output_type is MyOutput


def test_register_workflow_captures_types():
    reg = WorkflowRegistry()
    defn = reg.register_workflow(my_workflow, name="my_workflow")
    assert isinstance(defn, WorkflowDefinition)
    assert defn.name == "my_workflow"
    assert reg.get_workflow("my_workflow") is defn


def test_get_step_missing_raises_key_error():
    reg = WorkflowRegistry()
    with pytest.raises(KeyError, match="nonexistent"):
        reg.get_step("nonexistent")


def test_get_workflow_missing_raises_key_error():
    reg = WorkflowRegistry()
    with pytest.raises(KeyError, match="nonexistent"):
        reg.get_workflow("nonexistent")


def test_list_workflows_and_steps():
    reg = WorkflowRegistry()
    reg.register_step(my_step)
    reg.register_workflow(my_workflow)
    assert "my_step" in reg.list_steps()
    assert "my_workflow" in reg.list_workflows()


def test_step_uses_default_retry_config():
    reg = WorkflowRegistry()
    defn = reg.register_step(my_step)
    assert defn.retry.max_retries == 3  # RetryConfig default


def test_step_custom_retry():
    reg = WorkflowRegistry()
    custom = RetryConfig(max_retries=5, backoff="linear")
    defn = reg.register_step(my_step, retry=custom)
    assert defn.retry.max_retries == 5
