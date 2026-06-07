"""AI SRE incident response workflow — pgflows example.

Demonstrates: multi-step workflows, retry config, typed I/O, plugin hooks.

Usage:
    docker compose up -d
    uv run python examples/ai_sre/workflow.py
"""

from __future__ import annotations

import asyncio
import logging

from pydantic import BaseModel

from pgflows import (
    LoggingPlugin,
    PgflowsConfig,
    RetryConfig,
    StepContext,
    WorkflowApp,
    WorkflowContext,
    configure_default_logging,
)

configure_default_logging(level=logging.INFO)


# --- typed I/O models ---

class AlertInput(BaseModel):
    alert_id: str
    service: str
    severity: str  # "warning" | "critical"


class ServiceStatus(BaseModel):
    service: str
    healthy: bool
    error_rate: float
    latency_p99_ms: float


class DiagnosisResult(BaseModel):
    root_cause: str
    confidence: float
    recommended_action: str


class RemediationResult(BaseModel):
    action_taken: str
    success: bool
    details: str


class IncidentReport(BaseModel):
    alert_id: str
    service: str
    diagnosis: DiagnosisResult
    remediation: RemediationResult
    resolved: bool


# --- app setup ---

config = PgflowsConfig(
    dsn="postgresql://pgflows:pgflows@127.0.0.1:5433/pgflows_test",
    otel_enabled=False,
    db_ssl=False,
)
app = WorkflowApp(config=config)
app.register_plugin(LoggingPlugin())


# --- steps ---

@app.step(retry=RetryConfig(max_retries=2, initial_delay_seconds=1.0))
async def check_service_health(ctx: StepContext, input: AlertInput) -> ServiceStatus:
    """Fetch real-time health metrics for the affected service."""
    # In production: call your metrics API (Datadog, Prometheus, etc.)
    logging.getLogger("sre").info("Checking health for %s", input.service)
    return ServiceStatus(
        service=input.service,
        healthy=False,
        error_rate=0.42,
        latency_p99_ms=850.0,
    )


@app.step(retry=RetryConfig(max_retries=1, initial_delay_seconds=2.0))
async def diagnose_incident(ctx: StepContext, status: ServiceStatus) -> DiagnosisResult:
    """Use AI to identify root cause from metrics."""
    # In production: call an LLM with context from metrics/logs
    pct = status.error_rate * 100
    logging.getLogger("sre").info("Diagnosing %s (error_rate=%.0f%%)", status.service, pct)
    return DiagnosisResult(
        root_cause="Database connection pool exhausted",
        confidence=0.87,
        recommended_action="restart_connection_pool",
    )


@app.step(retry=RetryConfig(max_retries=3, initial_delay_seconds=5.0))
async def apply_remediation(ctx: StepContext, diagnosis: DiagnosisResult) -> RemediationResult:
    """Execute the recommended remediation action."""
    logging.getLogger("sre").info("Applying: %s", diagnosis.recommended_action)
    # In production: call your infrastructure API
    return RemediationResult(
        action_taken=diagnosis.recommended_action,
        success=True,
        details="Connection pool restarted, connections now: 0/50",
    )


# --- workflow ---

@app.workflow()
async def handle_alert(ctx: WorkflowContext, input: AlertInput) -> IncidentReport:
    """Durable incident response: health check → AI diagnosis → auto-remediation."""
    status = await ctx.step(check_service_health, input)
    diagnosis = await ctx.step(diagnose_incident, status)
    remediation = await ctx.step(apply_remediation, diagnosis)

    return IncidentReport(
        alert_id=input.alert_id,
        service=input.service,
        diagnosis=diagnosis,
        remediation=remediation,
        resolved=remediation.success,
    )


# --- runner ---

async def main() -> None:
    await app.initialize()
    try:
        alert = AlertInput(alert_id="INC-1337", service="payment-api", severity="critical")
        instance_id = await app.start(handle_alert, alert)
        print(f"Started workflow: {instance_id}")

        await app.process_once()

        status = await app.get_status(instance_id)
        print(f"State: {status.state}")
        if status.output:
            print(f"Resolved: {status.output.get('resolved')}")
            print(f"Root cause: {status.output.get('diagnosis', {}).get('root_cause')}")
            print(f"Action taken: {status.output.get('remediation', {}).get('action_taken')}")
    finally:
        await app.close()


if __name__ == "__main__":
    asyncio.run(main())
