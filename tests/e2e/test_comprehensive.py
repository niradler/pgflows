from __future__ import annotations

from typing import Any

import asyncpg
import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pydantic import BaseModel

from pyflows.app import WorkflowApp
from pyflows.context import StepContext
from pyflows.plugins import PyflowsPlugin, StepEvent, WorkflowEvent
from pyflows.sql_exporter import SqlExporter
from pyflows.telemetry import PyflowsTelemetry
from pyflows.types import RetryConfig, WorkflowState

# ---------------------------------------------------------------------------
# Test 1 — multi-step data pipeline
# ---------------------------------------------------------------------------


class RawInput(BaseModel):
    raw: str


class IntValue(BaseModel):
    value: int


class MessageOutput(BaseModel):
    message: str


@pytest.mark.asyncio
async def test_multi_step_data_pipeline(pyflows_config):
    app = WorkflowApp(config=pyflows_config)

    @app.step()
    async def parse_step(ctx: StepContext, input: RawInput) -> IntValue:
        return IntValue(value=int(input.raw))

    @app.step()
    async def double_step(ctx: StepContext, input: IntValue) -> IntValue:
        return IntValue(value=input.value * 2)

    @app.step()
    async def format_step(ctx: StepContext, input: IntValue) -> MessageOutput:
        return MessageOutput(message=f"Result: {input.value}")

    @app.workflow()
    async def pipeline_workflow(ctx, input: RawInput) -> MessageOutput:
        parsed = await ctx.step(parse_step, input)
        doubled = await ctx.step(double_step, parsed)
        return await ctx.step(format_step, doubled)

    await app.initialize()
    try:
        instance_id = await app.start(pipeline_workflow, RawInput(raw="42"))
        await app.process_once()
        status = await app.get_status(instance_id)
        assert status.state == WorkflowState.COMPLETED
        assert status.output["message"] == "Result: 84"
    finally:
        await app.close()


# ---------------------------------------------------------------------------
# Test 2 — checkpoint replay skips already-executed steps
# ---------------------------------------------------------------------------


class CountInput(BaseModel):
    x: int


class CountOutput(BaseModel):
    result: int


@pytest.mark.asyncio
async def test_checkpoint_replay_skips_executed_steps(pyflows_config):
    step1_calls = {"n": 0}
    step2_calls = {"n": 0}
    app = WorkflowApp(config=pyflows_config)

    @app.step()
    async def replay_step_one(ctx: StepContext, input: CountInput) -> CountOutput:
        step1_calls["n"] += 1
        return CountOutput(result=input.x + 100)

    @app.step()
    async def replay_step_two(ctx: StepContext, input: CountOutput) -> CountOutput:
        step2_calls["n"] += 1
        return CountOutput(result=input.result + 1)

    @app.workflow()
    async def replay_workflow(ctx, input: CountInput) -> CountOutput:
        out1 = await ctx.step(replay_step_one, input)
        return await ctx.step(replay_step_two, out1)

    await app.initialize()
    try:
        # Create the instance and inject step 1's result BEFORE processing.
        instance_id = await app.start(replay_workflow, CountInput(x=5))

        # Manually save step 1's result — simulates a previous partial run.
        await app._state.save_step_result(
            instance_id,
            "replay_step_one",
            0,
            {"x": 5},
            {"result": 999},  # injected — NOT 5+100
        )

        await app.process_once()

        status = await app.get_status(instance_id)
        assert status.state == WorkflowState.COMPLETED

        # Step 1 must NOT have run (was replayed from cache).
        assert step1_calls["n"] == 0

        # Step 2 must have run exactly once.
        assert step2_calls["n"] == 1

        # Final output uses the injected value (999 + 1 = 1000), not 5+100.
        assert status.output["result"] == 1000
    finally:
        await app.close()


# ---------------------------------------------------------------------------
# Test 3 — plugin hooks fire in order
# ---------------------------------------------------------------------------


class CapturePlugin(PyflowsPlugin):
    def __init__(self) -> None:
        self.events: list[str] = []

    async def before_workflow(self, event: WorkflowEvent) -> None:
        self.events.append("before_workflow")

    async def after_workflow(self, event: WorkflowEvent, result: Any) -> None:
        self.events.append("after_workflow")

    async def before_step(self, event: StepEvent, input_model: BaseModel) -> None:
        self.events.append("before_step")

    async def after_step(self, event: StepEvent, result: Any) -> None:
        self.events.append("after_step")


class HookInput(BaseModel):
    v: int


class HookOutput(BaseModel):
    v: int


@pytest.mark.asyncio
async def test_plugin_hooks_fire_in_order(pyflows_config):
    capture = CapturePlugin()
    app = WorkflowApp(config=pyflows_config)
    app.register_plugin(capture)

    @app.step()
    async def hook_step_a(ctx: StepContext, input: HookInput) -> HookOutput:
        return HookOutput(v=input.v + 1)

    @app.step()
    async def hook_step_b(ctx: StepContext, input: HookOutput) -> HookOutput:
        return HookOutput(v=input.v + 1)

    @app.workflow()
    async def hook_workflow(ctx, input: HookInput) -> HookOutput:
        r = await ctx.step(hook_step_a, input)
        return await ctx.step(hook_step_b, r)

    await app.initialize()
    try:
        instance_id = await app.start(hook_workflow, HookInput(v=0))
        await app.process_once()
        status = await app.get_status(instance_id)
        assert status.state == WorkflowState.COMPLETED

        assert capture.events == [
            "before_workflow",
            "before_step",
            "after_step",
            "before_step",
            "after_step",
            "after_workflow",
        ]
    finally:
        await app.close()


# ---------------------------------------------------------------------------
# Test 4 — concurrent workflows all complete
# ---------------------------------------------------------------------------


class ConcInput(BaseModel):
    n: int


class ConcOutput(BaseModel):
    doubled: int


@pytest.mark.asyncio
async def test_concurrent_workflows_all_complete(pyflows_config):
    app = WorkflowApp(config=pyflows_config)

    @app.step()
    async def conc_double(ctx: StepContext, input: ConcInput) -> ConcOutput:
        return ConcOutput(doubled=input.n * 2)

    @app.workflow()
    async def conc_workflow(ctx, input: ConcInput) -> ConcOutput:
        return await ctx.step(conc_double, input)

    await app.initialize()
    try:
        ids = []
        for i in range(5):
            iid = await app.start(conc_workflow, ConcInput(n=i))
            ids.append((i, iid))

        # batch_size default is 5 — one call processes all five.
        processed = await app.process_once()
        assert processed == 5

        for n, iid in ids:
            status = await app.get_status(iid)
            assert status.state == WorkflowState.COMPLETED, f"n={n} not COMPLETED"
            assert status.output["doubled"] == n * 2, f"n={n} wrong output"
    finally:
        await app.close()


# ---------------------------------------------------------------------------
# Test 5 — same step called multiple times (step_index 0, 1, 2)
# ---------------------------------------------------------------------------


class MultiInput(BaseModel):
    val: int


class MultiOutput(BaseModel):
    val: int


@pytest.mark.asyncio
async def test_same_step_called_multiple_times(pyflows_config):
    call_log: list[int] = []
    app = WorkflowApp(config=pyflows_config)

    @app.step()
    async def increment_step(ctx: StepContext, input: MultiInput) -> MultiOutput:
        call_log.append(input.val)
        return MultiOutput(val=input.val + 10)

    @app.workflow()
    async def multi_call_workflow(ctx, input: MultiInput) -> MultiOutput:
        r0 = await ctx.step(increment_step, input)
        r1 = await ctx.step(increment_step, r0)
        r2 = await ctx.step(increment_step, r1)
        return r2

    await app.initialize()
    try:
        instance_id = await app.start(multi_call_workflow, MultiInput(val=0))
        await app.process_once()

        status = await app.get_status(instance_id)
        assert status.state == WorkflowState.COMPLETED

        # Step ran 3 times with distinct inputs.
        assert call_log == [0, 10, 20]
        assert status.output["val"] == 30

        # Each call stored under a different step_index.
        r0 = await app._state.get_step_result(instance_id, "increment_step", 0)
        r1 = await app._state.get_step_result(instance_id, "increment_step", 1)
        r2 = await app._state.get_step_result(instance_id, "increment_step", 2)

        assert r0 is not None and r0["val"] == 10
        assert r1 is not None and r1["val"] == 20
        assert r2 is not None and r2["val"] == 30
    finally:
        await app.close()


# ---------------------------------------------------------------------------
# Test 6 — SqlExporter dry_run on a real registered workflow
# ---------------------------------------------------------------------------


class ExportInput(BaseModel):
    text: str


class ExportOutput(BaseModel):
    text: str


@pytest.mark.asyncio
async def test_sql_exporter_dry_run_real_workflow(pyflows_config):
    app = WorkflowApp(config=pyflows_config)

    @app.step()
    async def export_step_alpha(ctx: StepContext, input: ExportInput) -> ExportOutput:
        return ExportOutput(text=input.text.upper())

    @app.step()
    async def export_step_beta(ctx: StepContext, input: ExportOutput) -> ExportOutput:
        return ExportOutput(text=f"[{input.text}]")

    @app.workflow()
    async def export_workflow(ctx, input: ExportInput) -> ExportOutput:
        r = await ctx.step(export_step_alpha, input)
        return await ctx.step(export_step_beta, r)

    await app.initialize()
    try:
        exporter = SqlExporter(app.registry, "http://localhost:8000")
        result = exporter.dry_run("export_workflow")

        assert result.workflow_name == "export_workflow"
        assert len(result.steps) == 2

        # Each step has an http_url.
        for step_sql in result.steps:
            assert step_sql.http_url.startswith("http://localhost:8000")

        # The SQL must contain the expected pg_durable DSL fragments.
        assert "df.start" in result.sql
        assert "df.http" in result.sql
        assert "base_url" in result.sql
    finally:
        await app.close()


# ---------------------------------------------------------------------------
# Test 7 — failed step records error in DB
# ---------------------------------------------------------------------------


class FailInput(BaseModel):
    x: int


class FailOutput(BaseModel):
    x: int


@pytest.mark.asyncio
async def test_failed_step_records_error_in_db(pyflows_config):
    app = WorkflowApp(config=pyflows_config)

    @app.step(retry=RetryConfig(max_retries=0, initial_delay_seconds=0.0))
    async def always_explodes(ctx: StepContext, input: FailInput) -> FailOutput:
        raise RuntimeError("boom")

    @app.workflow()
    async def error_recording_workflow(ctx, input: FailInput) -> FailOutput:
        return await ctx.step(always_explodes, input)

    await app.initialize()
    try:
        instance_id = await app.start(error_recording_workflow, FailInput(x=1))
        await app.process_once()

        status = await app.get_status(instance_id)
        assert status.state == WorkflowState.FAILED

        # Query step_results directly — get_step_result only returns 'completed' rows.
        conn = await asyncpg.connect(pyflows_config.dsn, ssl=False)
        try:
            row = await conn.fetchrow(
                """
                SELECT state, error FROM pyflows.step_results
                WHERE instance_id = $1::uuid AND step_name = $2 AND step_index = 0
                """,
                instance_id,
                "always_explodes",
            )
        finally:
            await conn.close()

        assert row is not None, "step result row not found in DB"
        assert row["state"] == "failed"
        assert row["error"] is not None
        assert "boom" in row["error"]
    finally:
        await app.close()


# ---------------------------------------------------------------------------
# Test 8 — workflow state transitions (PENDING → COMPLETED)
# ---------------------------------------------------------------------------


class TransInput(BaseModel):
    n: int


class TransOutput(BaseModel):
    n: int


@pytest.mark.asyncio
async def test_workflow_state_transitions(pyflows_config):
    app = WorkflowApp(config=pyflows_config)

    @app.workflow()
    async def transition_workflow(ctx, input: TransInput) -> TransOutput:
        return TransOutput(n=input.n * 3)

    await app.initialize()
    try:
        instance_id = await app.start(transition_workflow, TransInput(n=7))

        before = await app.get_status(instance_id)
        assert before.state == WorkflowState.PENDING

        await app.process_once()

        after = await app.get_status(instance_id)
        assert after.state == WorkflowState.COMPLETED
        assert after.output["n"] == 21
    finally:
        await app.close()


# ---------------------------------------------------------------------------
# Test 9 — OTel spans generated
# ---------------------------------------------------------------------------


class SpanInput(BaseModel):
    label: str


class SpanOutput(BaseModel):
    label: str


@pytest.mark.asyncio
async def test_otel_spans_generated(pyflows_config):
    exporter = InMemorySpanExporter()
    app = WorkflowApp(config=pyflows_config)

    @app.step()
    async def span_step(ctx: StepContext, input: SpanInput) -> SpanOutput:
        return SpanOutput(label=f"done:{input.label}")

    @app.workflow()
    async def span_workflow(ctx, input: SpanInput) -> SpanOutput:
        return await ctx.step(span_step, input)

    await app.initialize()
    try:
        # Replace no-op telemetry with one backed by the in-memory exporter.
        real_tel = PyflowsTelemetry.with_in_memory_exporter(exporter)
        app._telemetry = real_tel
        app._worker._telemetry = real_tel

        instance_id = await app.start(span_workflow, SpanInput(label="hello"))
        await app.process_once()

        status = await app.get_status(instance_id)
        assert status.state == WorkflowState.COMPLETED

        spans = exporter.get_finished_spans()
        assert len(spans) > 0, "no spans exported"

        span_names = [s.name for s in spans]
        workflow_spans = [n for n in span_names if n.startswith("pyflows.workflow.")]
        step_spans = [n for n in span_names if n.startswith("pyflows.step.")]

        assert workflow_spans, f"no workflow spans found; got: {span_names}"
        assert step_spans, f"no step spans found; got: {span_names}"

        # Workflow span carries the expected attributes.
        wf_span = next(s for s in spans if s.name.startswith("pyflows.workflow."))
        attrs = wf_span.attributes or {}
        assert "pyflows.workflow.name" in attrs
        assert "pyflows.workflow.id" in attrs
        assert attrs["pyflows.workflow.id"] == instance_id
    finally:
        await app.close()


# ---------------------------------------------------------------------------
# Test 10 — failed workflow message is archived (DLQ), not re-queued
# ---------------------------------------------------------------------------


class DlqInput(BaseModel):
    x: int


class DlqOutput(BaseModel):
    x: int


@pytest.mark.asyncio
async def test_failed_workflow_archived_not_requeued(pyflows_config):
    app = WorkflowApp(config=pyflows_config)

    @app.step(retry=RetryConfig(max_retries=0, initial_delay_seconds=0.0))
    async def dlq_explodes(ctx: StepContext, input: DlqInput) -> DlqOutput:
        raise RuntimeError("dlq-test")

    @app.workflow()
    async def dlq_workflow(ctx, input: DlqInput) -> DlqOutput:
        return await ctx.step(dlq_explodes, input)

    await app.initialize()
    try:
        instance_id = await app.start(dlq_workflow, DlqInput(x=1))
        await app.process_once()

        status = await app.get_status(instance_id)
        assert status.state == WorkflowState.FAILED

        # Message must be archived — a second pass must find nothing.
        second_pass = await app.process_once()
        assert second_pass == 0, "failed workflow was re-queued instead of archived"

        # Confirm the message is in the pgmq archive table.
        conn = await asyncpg.connect(pyflows_config.dsn, ssl=False)
        try:
            count = await conn.fetchval(
                f"SELECT COUNT(*) FROM pgmq.a_{pyflows_config.workflow_queue}"
                " WHERE message->>'instance_id' = $1",
                instance_id,
            )
        finally:
            await conn.close()

        assert count >= 1, "message not in pgmq archive"
    finally:
        await app.close()


# ---------------------------------------------------------------------------
# Test 11 — duplicate message processed only once (worker coordination)
# ---------------------------------------------------------------------------


class ClaimInput(BaseModel):
    v: int


class ClaimOutput(BaseModel):
    v: int


@pytest.mark.asyncio
async def test_duplicate_message_processed_only_once(pyflows_config):
    call_count = {"n": 0}
    app = WorkflowApp(config=pyflows_config)

    @app.step()
    async def claim_step(ctx: StepContext, input: ClaimInput) -> ClaimOutput:
        call_count["n"] += 1
        return ClaimOutput(v=input.v * 2)

    @app.workflow()
    async def claim_workflow(ctx, input: ClaimInput) -> ClaimOutput:
        return await ctx.step(claim_step, input)

    await app.initialize()
    try:
        instance_id = await app.start(claim_workflow, ClaimInput(v=7))

        # Simulate re-delivery: inject a duplicate message for the same instance.
        await app._queue.enqueue(
            pyflows_config.workflow_queue,
            {"workflow_name": "claim_workflow", "instance_id": instance_id, "input": {"v": 7}},
        )

        # Both messages are visible; process_batch dequeues them concurrently.
        processed = await app._worker.process_batch()
        assert processed == 2

        status = await app.get_status(instance_id)
        assert status.state == WorkflowState.COMPLETED
        assert status.output["v"] == 14

        # Step ran exactly once despite two queue messages.
        assert call_count["n"] == 1, f"step ran {call_count['n']} times — duplicate not skipped"
    finally:
        await app.close()


# ---------------------------------------------------------------------------
# Test 12 — runtime workflow composition via compose()
# ---------------------------------------------------------------------------


class ComposeInput(BaseModel):
    value: int


class ComposeOutput(BaseModel):
    value: int


@pytest.mark.asyncio
async def test_runtime_compose_produces_valid_dsl(pyflows_config):
    """compose() builds pg_durable DSL from step names registered against a real app."""
    app = WorkflowApp(config=pyflows_config)

    @app.step()
    async def compose_double(ctx: StepContext, input: ComposeInput) -> ComposeOutput:
        return ComposeOutput(value=input.value * 2)

    @app.step()
    async def compose_add_ten(ctx: StepContext, input: ComposeOutput) -> ComposeOutput:
        return ComposeOutput(value=input.value + 10)

    await app.initialize()
    try:
        exporter = SqlExporter(app.registry, "http://my-app:8000")
        sql = exporter.compose("runtime_pipeline", ["compose_double", "compose_add_ten"])

        assert "runtime_pipeline" in sql
        assert "df.start(" in sql
        assert "df.http(" in sql
        assert "compose_double" in sql
        assert "compose_add_ten" in sql
        # step order is preserved
        assert sql.index("compose_double") < sql.index("compose_add_ten")
        # base_url is wired in
        assert "my-app:8000" in sql

        # unregistered step raises KeyError immediately
        with pytest.raises(KeyError):
            exporter.compose("bad_wf", ["compose_double", "nonexistent"])

        # unsafe workflow name raises ValueError (injection guard)
        with pytest.raises(ValueError, match="workflow_name"):
            exporter.compose("bad'); DROP TABLE--", ["compose_double"])
    finally:
        await app.close()


@pytest.mark.asyncio
async def test_runtime_compose_steps_are_executable(pyflows_config):
    """Steps referenced in compose() are real, runnable Python functions."""
    app = WorkflowApp(config=pyflows_config)

    @app.step()
    async def rt_square(ctx: StepContext, input: ComposeInput) -> ComposeOutput:
        return ComposeOutput(value=input.value ** 2)

    @app.step()
    async def rt_negate(ctx: StepContext, input: ComposeOutput) -> ComposeOutput:
        return ComposeOutput(value=-input.value)

    @app.workflow()
    async def rt_composed_wf(ctx, input: ComposeInput) -> ComposeOutput:
        r = await ctx.step(rt_square, input)
        return await ctx.step(rt_negate, r)

    await app.initialize()
    try:
        # Confirm compose() produces DSL for these steps
        exporter = SqlExporter(app.registry, "http://localhost:8000")
        sql = exporter.compose("rt_pipeline", ["rt_square", "rt_negate"])
        assert "rt_square" in sql and "rt_negate" in sql

        # Confirm the underlying Python steps actually execute correctly end-to-end
        instance_id = await app.start(rt_composed_wf, ComposeInput(value=4))
        await app.process_once()
        status = await app.get_status(instance_id)
        assert status.state.value == "completed"
        assert status.output["value"] == -16  # 4² = 16, negated = -16
    finally:
        await app.close()
