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


def test_from_env_returns_noop_when_endpoint_not_set(monkeypatch):
    """from_env() falls back to no-op when OTEL_EXPORTER_OTLP_ENDPOINT is absent."""
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    telemetry = PgflowsTelemetry.from_env("test-svc")
    assert isinstance(telemetry, PgflowsTelemetry)
    # Should not crash — acts like noop
    with telemetry.workflow_span("test_wf", "inst-noop"):
        pass


def test_from_env_attaches_span_processor_when_endpoint_set(monkeypatch):
    """from_env() wires a BatchSpanProcessor when OTEL_EXPORTER_OTLP_ENDPOINT is set."""
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")

    telemetry = PgflowsTelemetry.from_env("test-svc")
    assert isinstance(telemetry, PgflowsTelemetry)

    provider = telemetry._provider
    assert isinstance(provider, TracerProvider), "from_env() must create a real TracerProvider"
    active = provider._active_span_processor  # type: ignore[attr-defined]
    processors = getattr(active, "_span_processors", None)
    if processors is None:
        processors = [active]
    assert any(isinstance(p, BatchSpanProcessor) for p in processors), (
        "from_env() must attach a BatchSpanProcessor when the endpoint env var is set"
    )


def test_from_env_creates_spans_without_error(monkeypatch):
    """from_env() telemetry must allow creating and finishing spans regardless of env."""
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    telemetry = PgflowsTelemetry.from_env("test-svc")
    with telemetry.workflow_span("test_wf", "inst-from-env"):
        pass
    with telemetry.step_span("inst-from-env", "test_step", 0):
        pass
