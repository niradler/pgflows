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

    def __init__(self, tracer: trace.Tracer, provider: TracerProvider | None = None) -> None:
        self._tracer = tracer
        self._provider = provider

    @classmethod
    def with_provider(cls, provider: TracerProvider) -> PgflowsTelemetry:
        return cls(provider.get_tracer(cls.TRACER_NAME), provider=provider)

    @classmethod
    def with_in_memory_exporter(cls, exporter: InMemorySpanExporter) -> PgflowsTelemetry:
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        return cls(provider.get_tracer(cls.TRACER_NAME), provider=provider)

    @classmethod
    def from_env(cls, service_name: str = "pgflows") -> PgflowsTelemetry:
        """Create a TracerProvider, exporting via OTLP when the endpoint is configured.

        Set OTEL_EXPORTER_OTLP_ENDPOINT to enable export:
            OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317

        When the env var is absent, returns a no-op provider (spans created but not
        exported). Use PgflowsConfig(otel_enabled=False) to skip tracing entirely.
        """
        import os

        if not os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"):
            return cls.noop()

        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import SERVICE_NAME, Resource
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        resource = Resource({SERVICE_NAME: service_name})
        provider = TracerProvider(resource=resource)
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
        return cls(provider.get_tracer(cls.TRACER_NAME), provider=provider)

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
