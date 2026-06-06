from pydantic import BaseModel

from pyflows.registry import WorkflowRegistry
from pyflows.sql_exporter import DryRunResult, SqlExporter


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
