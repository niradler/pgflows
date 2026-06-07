from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import Span, StatusCode


class PyflowsTelemetry:
    """OTel instrumentation for pyflows workflows and steps.

    Usage — inject into WorkflowApp:
        telemetry = PyflowsTelemetry.from_env("my-service")
        # or for tests:
        telemetry = PyflowsTelemetry.noop()
    """

    TRACER_NAME = "pyflows"

    def __init__(self, tracer: trace.Tracer) -> None:
        self._tracer = tracer

    @classmethod
    def with_provider(cls, provider: TracerProvider) -> PyflowsTelemetry:
        return cls(provider.get_tracer(cls.TRACER_NAME))

    @classmethod
    def with_in_memory_exporter(cls, exporter: InMemorySpanExporter) -> PyflowsTelemetry:
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        return cls(provider.get_tracer(cls.TRACER_NAME))

    @classmethod
    def from_env(cls, service_name: str = "pyflows") -> PyflowsTelemetry:
        """Create a real provider. Configure exporters via OTel env vars."""
        provider = TracerProvider()
        return cls(provider.get_tracer(cls.TRACER_NAME))

    @classmethod
    def noop(cls) -> PyflowsTelemetry:
        """No-op telemetry — spans are created but not exported. Safe for tests."""
        return cls(trace.get_tracer(cls.TRACER_NAME))

    @contextmanager
    def workflow_span(self, workflow_name: str, instance_id: str) -> Generator[Span]:
        with self._tracer.start_as_current_span(f"pyflows.workflow.{workflow_name}") as span:
            span.set_attribute("pyflows.workflow.name", workflow_name)
            span.set_attribute("pyflows.workflow.id", instance_id)
            try:
                yield span
            except Exception as exc:
                span.set_status(StatusCode.ERROR, str(exc))
                raise

    @contextmanager
    def step_span(self, instance_id: str, step_name: str, step_index: int) -> Generator[Span]:
        with self._tracer.start_as_current_span(f"pyflows.step.{step_name}") as span:
            span.set_attribute("pyflows.workflow.id", instance_id)
            span.set_attribute("pyflows.step.name", step_name)
            span.set_attribute("pyflows.step.index", step_index)
            try:
                yield span
            except Exception as exc:
                span.set_status(StatusCode.ERROR, str(exc))
                raise
