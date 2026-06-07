import pytest
from pydantic import BaseModel

from pgflows.registry import WorkflowRegistry
from pgflows.sql_exporter import DryRunResult, SqlExporter


class CheckInput(BaseModel):
    url: str


class CheckOutput(BaseModel):
    healthy: bool


class NotifyInput(BaseModel):
    message: str


class NotifyOutput(BaseModel):
    sent: bool


async def check_service(ctx, input: CheckInput) -> CheckOutput:
    return CheckOutput(healthy=True)


async def notify(ctx, input: NotifyInput) -> NotifyOutput:
    return NotifyOutput(sent=True)


async def two_step_workflow(ctx, input: CheckInput) -> CheckOutput:
    result = await ctx.step(check_service, input)
    await ctx.step(notify, NotifyInput(message="done"))
    return result


async def no_step_workflow(ctx, input: CheckInput) -> CheckOutput:
    return CheckOutput(healthy=True)


def _make_exporter() -> tuple[WorkflowRegistry, SqlExporter]:
    reg = WorkflowRegistry()
    reg.register_step(check_service)
    reg.register_step(notify)
    reg.register_workflow(two_step_workflow)
    reg.register_workflow(no_step_workflow)
    exporter = SqlExporter(registry=reg, base_url="http://localhost:8000")
    return reg, exporter


def test_export_workflow_contains_dsl_markers():
    _, exporter = _make_exporter()
    sql = exporter.export_workflow("two_step_workflow")
    assert "df.start(" in sql
    assert "df.http(" in sql
    assert "check_service" in sql
    assert "notify" in sql


def test_export_workflow_step_order():
    _, exporter = _make_exporter()
    sql = exporter.export_workflow("two_step_workflow")
    assert sql.index("check_service") < sql.index("notify")


def test_dry_run_returns_correct_structure():
    _, exporter = _make_exporter()
    result: DryRunResult = exporter.dry_run("two_step_workflow", {"url": "http://example.com"})
    assert result.workflow_name == "two_step_workflow"
    assert len(result.steps) == 2
    assert result.steps[0].step_name == "check_service"
    assert result.steps[1].step_name == "notify"
    assert result.sql is not None
    assert "df.start(" in result.sql


def test_dry_run_step_http_urls():
    _, exporter = _make_exporter()
    result = exporter.dry_run("two_step_workflow")
    assert result.steps[0].http_url == "http://localhost:8000/steps/check_service"
    assert result.steps[1].http_url == "http://localhost:8000/steps/notify"


def test_export_workflow_no_steps():
    _, exporter = _make_exporter()
    sql = exporter.export_workflow("no_step_workflow")
    assert "no steps" in sql


def test_export_all_includes_all_workflows():
    _, exporter = _make_exporter()
    sql = exporter.export_all()
    assert "two_step_workflow" in sql
    assert "no_step_workflow" in sql


def test_base_url_trailing_slash_stripped():
    reg = WorkflowRegistry()
    reg.register_workflow(no_step_workflow)
    exporter = SqlExporter(registry=reg, base_url="http://localhost:8000/")
    result = exporter.dry_run("no_step_workflow")
    assert result.sql is not None


# --- compose() tests ---


def test_compose_produces_dsl():
    _, exporter = _make_exporter()
    sql = exporter.compose("my_runtime_wf", ["check_service", "notify"])
    assert "df.start(" in sql
    assert "df.http(" in sql
    assert "check_service" in sql
    assert "notify" in sql
    assert "my_runtime_wf" in sql


def test_compose_step_order():
    _, exporter = _make_exporter()
    sql = exporter.compose("ordered_wf", ["check_service", "notify"])
    assert sql.index("check_service") < sql.index("notify")


def test_compose_single_step():
    _, exporter = _make_exporter()
    sql = exporter.compose("single_step_wf", ["check_service"])
    assert "check_service" in sql
    assert "~>" not in sql


def test_compose_unregistered_step_raises():
    _, exporter = _make_exporter()
    with pytest.raises(KeyError):
        exporter.compose("bad_wf", ["check_service", "nonexistent_step"])


def test_compose_empty_steps():
    _, exporter = _make_exporter()
    sql = exporter.compose("empty_wf", [])
    assert "no steps" in sql


def test_compose_rejects_unsafe_workflow_name():
    _, exporter = _make_exporter()
    with pytest.raises(ValueError, match="workflow_name"):
        exporter.compose("bad'); DROP TABLE df.instances; --", ["check_service"])


def test_export_workflow_rejects_unsafe_name():
    _, exporter = _make_exporter()
    with pytest.raises(ValueError, match="workflow_name"):
        exporter.export_workflow("bad'name")


def test_compose_rejects_unsafe_step_name():
    reg = WorkflowRegistry()
    # register with a safe Python name, but attempt compose with injected name
    reg.register_step(check_service, name="safe_step")
    exporter = SqlExporter(registry=reg, base_url="http://localhost:8000")
    with pytest.raises(ValueError, match="step_name"):
        exporter.compose("my_wf", ["safe_step'; DROP TABLE--"])


# --- HTTP binding correctness (fixed) ---


def test_http_fragment_includes_instance_id_header():
    _, exporter = _make_exporter()
    sql = exporter.export_workflow("two_step_workflow")
    assert "X-DF-Instance-ID" in sql
    assert "{sys_instance_id}" in sql


def test_http_fragment_forwards_input_body():
    _, exporter = _make_exporter()
    sql = exporter.export_workflow("two_step_workflow")
    # Body forwards the {input} durable var, not the old dummy {"step": ...} payload.
    assert "'{input}'" in sql
    assert '{"step":' not in sql


def test_http_mode_requires_base_url():
    reg = WorkflowRegistry()
    with pytest.raises(ValueError, match="base_url"):
        SqlExporter(registry=reg, mode="http")


# --- pgmq mode (native SQL => pgmq => NOTIFY => signal) ---


def test_pgmq_mode_emits_pgmq_and_poll_not_http():
    reg, _ = _make_exporter()
    exporter = SqlExporter(registry=reg, mode="pgmq", step_queue="pgflows_steps")
    sql = exporter.export_workflow("two_step_workflow")
    assert "pgmq.send(" in sql
    assert "pg_notify(" in sql
    assert "df.loop(df.sleep(" in sql              # race-free poll, not a signal
    assert "df.wait_for_signal(" not in sql
    assert "df.http(" not in sql


def test_pgmq_mode_no_base_url_needed():
    reg, _ = _make_exporter()
    exporter = SqlExporter(registry=reg, mode="pgmq")
    sql = exporter.export_workflow("two_step_workflow")
    assert "df.start(" in sql
    assert "df.setvar('base_url'" not in sql


def test_pgmq_mode_unique_result_key_per_step():
    reg, _ = _make_exporter()
    exporter = SqlExporter(registry=reg, mode="pgmq")
    sql = exporter.export_workflow("two_step_workflow")
    assert "pgflows_check_service_0" in sql
    assert "pgflows_notify_1" in sql


def test_pgmq_mode_threads_prev_output_to_next_input():
    reg, _ = _make_exporter()
    exporter = SqlExporter(registry=reg, mode="pgmq")
    sql = exporter.export_workflow("two_step_workflow")
    # second step's input is the first step's captured output (df substitutes
    # $capture with the read node's first-column value = the step output).
    assert "$pgflows_check_service_0::jsonb" in sql


def test_pgmq_mode_dry_run_step_urls():
    reg, _ = _make_exporter()
    exporter = SqlExporter(registry=reg, mode="pgmq", step_queue="pgflows_steps")
    result = exporter.dry_run("two_step_workflow")
    assert result.steps[0].http_url == "pgmq://pgflows_steps/check_service"


def test_pgmq_mode_compose():
    reg, _ = _make_exporter()
    exporter = SqlExporter(registry=reg, mode="pgmq")
    sql = exporter.compose("runtime_wf", ["check_service", "notify"])
    assert "pgmq.send(" in sql
    assert sql.index("check_service") < sql.index("notify")
