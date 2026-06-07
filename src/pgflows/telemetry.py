from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import Span, StatusCode


class PgflowsTelemetry:
    """OTel instrumentation for pgflows workflows and steps.

    Usage — inject into WorkflowApp:
        telemetry = PgflowsTelemetry.from_env("my-service")
        # or for tests:
        telemetry = PgflowsTelemetry.noop()
    """

    TRACER_NAME = "pgflows"

    def __init__(self, tracer: trace.Tracer) -> None:
        self._tracer = tracer

    @classmethod
    def with_provider(cls, provider: TracerProvider) -> PgflowsTelemetry:
        return cls(provider.get_tracer(cls.TRACER_NAME))

    @classmethod
    def with_in_memory_exporter(cls, exporter: InMemorySpanExporter) -> PgflowsTelemetry:
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        return cls(provider.get_tracer(cls.TRACER_NAME))

    @classmethod
    def from_env(cls, service_name: str = "pgflows") -> PgflowsTelemetry:
        """Create a real provider. Configure exporters via OTel env vars."""
        provider = TracerProvider()
        return cls(provider.get_tracer(cls.TRACER_NAME))

    @classmethod
    def noop(cls) -> PgflowsTelemetry:
        """No-op telemetry — spans are created but not exported. Safe for tests."""
        return cls(trace.get_tracer(cls.TRACER_NAME))

    @contextmanager
    def workflow_span(self, workflow_name: str, instance_id: str) -> Generator[Span]:
        with self._tracer.start_as_current_span(f"pgflows.workflow.{workflow_name}") as span:
            span.set_attribute("pgflows.workflow.name", workflow_name)
            span.set_attribute("pgflows.workflow.id", instance_id)
            try:
                yield span
            except Exception as exc:
                span.set_status(StatusCode.ERROR, str(exc))
                raise

    @contextmanager
    def step_span(self, instance_id: str, step_name: str, step_index: int) -> Generator[Span]:
        with self._tracer.start_as_current_span(f"pgflows.step.{step_name}") as span:
            span.set_attribute("pgflows.workflow.id", instance_id)
            span.set_attribute("pgflows.step.name", step_name)
            span.set_attribute("pgflows.step.index", step_index)
            try:
                yield span
            except Exception as exc:
                span.set_status(StatusCode.ERROR, str(exc))
                raise
