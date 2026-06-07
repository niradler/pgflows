from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from pgflows.telemetry import PgflowsTelemetry


def test_workflow_span_attributes():
    exporter = InMemorySpanExporter()
    telemetry = PgflowsTelemetry.with_in_memory_exporter(exporter)
    with telemetry.workflow_span("my_workflow", "inst-001"):
        pass
    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "pgflows.workflow.my_workflow"
    assert spans[0].attributes.get("pgflows.workflow.id") == "inst-001"
    assert spans[0].attributes.get("pgflows.workflow.name") == "my_workflow"


def test_step_span_attributes():
    exporter = InMemorySpanExporter()
    telemetry = PgflowsTelemetry.with_in_memory_exporter(exporter)
    with telemetry.step_span("inst-001", "check_service", 2):
        pass
    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "pgflows.step.check_service"
    assert spans[0].attributes.get("pgflows.step.index") == 2
    assert spans[0].attributes.get("pgflows.workflow.id") == "inst-001"


def test_error_span_marks_error():
    exporter = InMemorySpanExporter()
    telemetry = PgflowsTelemetry.with_in_memory_exporter(exporter)
    try:
        with telemetry.step_span("inst-002", "fail_step", 0):
            raise ValueError("oops")
    except ValueError:
        pass
    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    from opentelemetry.trace import StatusCode
    assert spans[0].status.status_code == StatusCode.ERROR


def test_noop_telemetry_does_not_crash():
    telemetry = PgflowsTelemetry.noop()
    with telemetry.workflow_span("wf", "id"):
        pass
    with telemetry.step_span("id", "step", 0):
        pass
