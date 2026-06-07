import pytest

from pyflows.backends.pg_state import PgStateBackend
from pyflows.exceptions import WorkflowNotFoundError
from pyflows.types import WorkflowState

TEST_DSN = "postgresql://pyflows:pyflows@localhost:5433/pyflows_test"


@pytest.fixture
async def state(require_db):
    backend = PgStateBackend(dsn=TEST_DSN)
    await backend.initialize()
    yield backend
    await backend.close()


async def test_register_and_get_definition(state):
    await state.register_workflow("test_wf_def", config={"version": 1})
    defn = await state.get_workflow_definition("test_wf_def")
    assert defn["name"] == "test_wf_def"
    assert defn["version"] == 1


async def test_create_and_get_instance(state):
    await state.register_workflow("test_wf_inst", config={})
    instance_id = await state.create_instance("test_wf_inst", {"key": "value"})
    assert instance_id is not None
    status = await state.get_instance(instance_id)
    assert status.state == WorkflowState.PENDING
    assert status.workflow_id == instance_id
    assert status.name == "test_wf_inst"


async def test_update_instance_state(state):
    await state.register_workflow("test_wf_update", config={})
    instance_id = await state.create_instance("test_wf_update", {})
    await state.update_instance_state(
        instance_id, WorkflowState.COMPLETED, output={"result": "ok"}
    )
    status = await state.get_instance(instance_id)
    assert status.state == WorkflowState.COMPLETED
    assert status.output["result"] == "ok"


async def test_step_checkpoint_and_replay(state):
    await state.register_workflow("test_wf_step", config={})
    instance_id = await state.create_instance("test_wf_step", {})
    # No result yet
    result = await state.get_step_result(instance_id, "my_step", 0)
    assert result is None
    # Save result
    await state.save_step_result(instance_id, "my_step", 0, {"x": 1}, {"y": 2})
    # Retrieve cached result
    cached = await state.get_step_result(instance_id, "my_step", 0)
    assert cached == {"y": 2}


async def test_list_instances(state):
    await state.register_workflow("test_wf_list", config={})
    await state.create_instance("test_wf_list", {"n": 1})
    await state.create_instance("test_wf_list", {"n": 2})
    instances = await state.list_instances(workflow_name="test_wf_list")
    assert len(instances) >= 2


async def test_cancel_workflow(state):
    await state.register_workflow("test_wf_cancel", config={})
    instance_id = await state.create_instance("test_wf_cancel", {})
    await state.cancel_workflow(instance_id)
    status = await state.get_instance(instance_id)
    assert status.state == WorkflowState.CANCELLED


async def test_get_missing_instance_raises(state):
    with pytest.raises(WorkflowNotFoundError):
        await state.get_instance("00000000-0000-0000-0000-000000000000")
