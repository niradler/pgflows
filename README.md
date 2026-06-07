# pgflows

[![PyPI](https://img.shields.io/pypi/v/pgflows)](https://pypi.org/project/pgflows/)
[![Python](https://img.shields.io/pypi/pyversions/pgflows)](https://pypi.org/project/pgflows/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> Durable workflow engine SDK for Python + Postgres

pgflows lets you write long-running, fault-tolerant workflows as plain async Python functions — backed entirely by your existing Postgres database. No extra infrastructure, no separate orchestration service, no new runtime to operate.

> [!WARNING]
> **Early development (alpha).** The core API is stabilizing but not yet 1.0. Expect breaking changes before the first stable release.

## How it works

Each workflow step is persisted to Postgres before execution. If the process crashes mid-run, the worker replays from the last checkpoint — re-executing only the steps that haven't completed. All state, retries, and scheduling live in the database.

```text
@workflow fn  →  WorkflowApp.start()
                     ↓  enqueues to pgmq
              Python async worker
                     ↓  executes steps
              PgStateBackend  ←→  Postgres
```

## Quick start

```bash
docker compose up -d   # start Postgres with pgmq
uv add pgflows
```

```python
import asyncio
from pydantic import BaseModel
from pgflows import PgflowsConfig, RetryConfig, StepContext, WorkflowApp, WorkflowContext

config = PgflowsConfig(
    dsn="postgresql://pgflows:pgflows@127.0.0.1:5433/pgflows_test",
    otel_enabled=False,
    db_ssl=False,
)
app = WorkflowApp(config=config)


class OrderInput(BaseModel):
    order_id: str
    amount: float


class OrderResult(BaseModel):
    charged: bool
    confirmation: str


@app.step(retry=RetryConfig(max_retries=3, initial_delay_seconds=1.0))
async def charge_payment(ctx: StepContext, input: OrderInput) -> OrderResult:
    # call your payment API here
    return OrderResult(charged=True, confirmation=f"CHG-{input.order_id}")


@app.workflow()
async def process_order(ctx: WorkflowContext, input: OrderInput) -> OrderResult:
    return await ctx.step(charge_payment, input)


async def main() -> None:
    await app.initialize()
    instance_id = await app.start(process_order, OrderInput(order_id="ORD-1", amount=99.0))
    await app.process_once()
    status = await app.get_status(instance_id)
    print(status.state, status.output)
    await app.close()

asyncio.run(main())
```

## Features

- **Checkpoint replay** — workflows survive crashes; completed steps are never re-executed
- **Typed end-to-end** — step inputs and outputs are Pydantic models; no `dict[str, Any]` at the boundary
- **Configurable retries** — per-step `RetryConfig` with exponential or linear backoff and jitter
- **Plugin hooks** — `before_workflow`, `after_workflow`, `on_workflow_error`, `before_step`, `after_step`, `on_step_error`
- **Automatic migrations** — `await app.initialize()` applies schema migrations; no manual SQL required
- **Cron scheduler** — trigger recurring workflows via `PgCronBackend` (backed by pg_durable `df.wait_for_schedule`)
- **Dead-letter queue** — failed workflows are archived to `pgmq.a_{queue}` instead of being re-queued indefinitely
- **Worker coordination** — atomic `pending→running` claim prevents duplicate processing when multiple workers race on the same instance
- **Swappable backends** — orchestrator, queue, and scheduler implement ABCs; swap without touching workflow code
- **OpenTelemetry** — built-in span management for workflows and steps

## Plugin system

```python
from pgflows import LoggingPlugin, PgflowsPlugin, StepEvent, WorkflowEvent

# Built-in: log all lifecycle events
app.register_plugin(LoggingPlugin())

# Custom: implement any subset of hooks
class MetricsPlugin(PgflowsPlugin):
    async def after_step(self, event: StepEvent, result: object) -> None:
        metrics.record("step.completed", tags={"step": event.step_name})

    async def on_workflow_error(self, event: WorkflowEvent, error: Exception) -> None:
        metrics.record("workflow.failed", tags={"workflow": event.workflow_name})

app.register_plugin(MetricsPlugin())
```

Plugins are called in registration order. A plugin that raises never affects other plugins or the workflow itself.

## Retry configuration

```python
from pgflows import RetryConfig

# Per-step retry (backoff can be "exponential" or "linear")
@app.step(retry=RetryConfig(max_retries=5, initial_delay_seconds=2.0, max_delay_seconds=60.0, backoff="exponential"))
async def my_step(ctx, input: MyInput) -> MyOutput: ...

# Workflow-level defaults (applied to all steps unless overridden)
@app.workflow(step_defaults=RetryConfig(max_retries=2))
async def my_workflow(ctx, input: MyInput) -> MyOutput: ...
```

## SQL export and runtime workflows

pgflows can export any registered workflow to a [pg_durable](https://github.com/microsoft/pg_durable) SQL DSL. Use this to:

- Transfer workflow definitions from dev → prod without code deployment
- Create workflows at runtime from config, API payloads, or external systems
- Inspect the step sequence of any workflow before executing it

### Export a Python workflow to SQL

```python
from pgflows import SqlExporter

exporter = SqlExporter(registry=app.registry, base_url="http://my-app:8000")

# Full SQL ready to run against a Postgres database with pg_durable
sql = exporter.export_workflow("process_order")

# Dry-run: inspect steps without producing runnable SQL
result = exporter.dry_run("process_order")
print(result.steps)   # [StepSql(step_name='charge_payment', ...)]
print(result.sql)     # pg_durable DSL
```

### Compose a workflow at runtime from step names

When you want to define a workflow without writing a Python function — from a config file, an API request, or a database record — use `compose()`. Each step name must already be registered with the app.

```python
# No Python workflow function needed — compose step sequences dynamically
sql = exporter.compose(
    workflow_name="on_call_response",
    steps=["check_service_health", "diagnose_incident", "apply_remediation"],
)

# Execute sql against Postgres with pg_durable to start the workflow
```

The `compose()` call validates that every step name is registered, so typos raise a `KeyError` immediately rather than failing silently at runtime.

### Export all workflows

```python
# All registered workflows in one SQL file (dev → prod migration)
sql = exporter.export_all()
```

## Scheduling

`PgCronBackend` schedules recurring workflows using pg_durable's `df.wait_for_schedule()` — no `pg_cron` extension required, only the `df` extension from [pg_durable](https://github.com/microsoft/pg_durable).

```python
from pgflows import PgCronBackend

scheduler = PgCronBackend(dsn=config.dsn)
await scheduler.initialize()

# Schedule a workflow to run every hour (job_id is a pg_durable instance ID string)
job_id = await scheduler.schedule(
    job_name="hourly_health_check",
    cron="0 * * * *",
    command="SELECT pgflows.enqueue_workflow('health_check', '{}')",
)

jobs = await scheduler.list_jobs()
await scheduler.unschedule(job_id)
```

Check whether pg_durable is installed at runtime:

```python
await app.initialize()
if app.pg_durable_available:
    # scheduler and push-mode SQL export are usable
    ...
```

## Backend abstraction

Every infrastructure concern is behind an ABC in `backends/base.py`. Swap backends without touching workflow code:

| Component | Default | Interface |
| --------- | ------- | --------- |
| State + checkpoints | `PgStateBackend` | `OrchestratorBackend` |
| Step queue | `PgmqBackend` | `QueueBackend` |
| Cron scheduling | `PgCronBackend` | `SchedulerBackend` |

```python
# Bring your own queue backend
class RedisQueueBackend(QueueBackend):
    ...

app = WorkflowApp(config=config)
# Use custom backend by injecting into WorkflowWorker directly
```

## Requirements

**Python:** 3.13+

**Postgres extensions** (15+):

| Extension | Purpose | Required |
| --------- | ------- | -------- |
| [`pgmq`](https://github.com/tembo-io/pgmq) | Step queue | Yes |
| [`pg_durable` (`df`)](https://github.com/microsoft/pg_durable) | Cron scheduling, push-mode SQL export | Optional |

The bundled `docker-compose.yml` starts a Postgres instance with `pgmq` pre-installed:

```bash
docker compose up -d
```

## Installation

```bash
pip install pgflows
# or
uv add pgflows
```

## Development

```bash
uv sync                          # install deps
uv run pytest tests/unit/        # unit tests (no DB needed)
docker compose up -d             # start Postgres
uv run pytest tests/e2e/         # E2E tests
uv run ruff check src/ tests/    # lint
```

## AI SRE example

See [`examples/ai_sre/workflow.py`](examples/ai_sre/workflow.py) for a full incident response workflow: health check → AI diagnosis → auto-remediation, with retries, plugin hooks, and typed I/O.

## Roadmap

- [x] M1 — Project scaffold: backend ABCs, Pydantic types, exception hierarchy
- [x] M2 — Core SDK: `WorkflowApp`, `@step`, `@workflow`, `WorkflowContext`, replay engine
- [x] M3 — SqlExporter: Python workflow → pg_durable DSL (AST-based)
- [x] M4 — E2E test suite: basic, retry, monitor/cancel (Docker-based)
- [x] M6 — Plugin system: `PgflowsPlugin` ABC, `LoggingPlugin`, lifecycle hooks
- [x] M7 — Migrations + scheduler: versioned schema migrations, `PgCronBackend` via pg_durable
- [x] M8 — AI SRE example, README, production hardening: DLQ, worker coordination, linear backoff, pg_durable detection
- [ ] M5 — FastAPI integration: push endpoint (deferred; pull mode works without it)
- [ ] M9 — PyPI release + full documentation

## License

MIT
