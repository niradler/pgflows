# pyflows

[![PyPI](https://img.shields.io/pypi/v/pyflows)](https://pypi.org/project/pyflows/)
[![Python](https://img.shields.io/pypi/pyversions/pyflows)](https://pypi.org/project/pyflows/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> Durable workflow engine SDK for Python + Postgres

pyflows lets you write long-running, fault-tolerant workflows as plain async Python functions — backed entirely by your existing Postgres database. No new infrastructure, no message broker, no separate orchestration service.

> [!WARNING]
> **Early development (alpha).** The core SDK API (`@step`, `@workflow`, worker) is under active development. The types, exceptions, and backend ABCs shown below are stable. Everything marked "planned API" reflects the target design and will change before 1.0.

## How it works

pyflows compiles your Python workflow functions into [pg_durable](https://github.com/microsoft/pg_durable) DSL — a durable execution engine that runs inside Postgres. Each step is dispatched via [pgmq](https://github.com/tembo-io/pgmq). A Python async worker processes steps and signals completion back to the orchestrator.

```text
@workflow fn   →   pg_durable (orchestration)
                        ↓
                   pgmq (step queue)
                        ↓
              Python async worker (step executor)
                        ↓
                   df.signal() (resume)
```

Durability, retries, and scheduling live at the database level — not in process memory that disappears on restart.

## Planned API

```python
# planned API — not yet implemented
@app.workflow()
async def remediate_incident(ctx: WorkflowContext, input: IncidentInput) -> RemediationResult:
    health = await ctx.step(check_service_health, input.service_id)
    if not health.ok:
        await ctx.step(restart_service, input.service_id, retry=RetryConfig(max_retries=5))
    return await ctx.step(verify_recovery, input.service_id)
```

## Features

- **Durable by default** — workflows survive restarts and crashes; all state is in Postgres
- **Pure async Python** — write steps as `async def`, compose with `await ctx.step(...)`
- **Typed end-to-end** — step inputs and outputs are Pydantic models; no `dict[str, Any]` at the boundary
- **Configurable retries** — per-step `RetryConfig` with exponential backoff and jitter
- **Built-in scheduling** — trigger workflows on a cron via `pg_cron` *(planned)*
- **LISTEN/NOTIFY wake-up** — zero-poll idle; workers wake instantly when work arrives *(planned)*
- **Swappable backends** — orchestrator, queue, and scheduler are ABCs; bring your own implementation
- **Plugin hooks** — `before_step`, `after_step`, `on_step_error`, `before_workflow`, `after_workflow` *(planned)*

## Requirements

**Python:** 3.13+

**PostgreSQL extensions** (15+):

| Extension | Purpose | Required |
| --------- | ------- | -------- |
| [`pg_durable`](https://github.com/microsoft/pg_durable) | Durable workflow orchestration | Yes |
| [`pgmq`](https://github.com/tembo-io/pgmq) | Message queue for step dispatch | Yes |
| [`pg_cron`](https://github.com/citusdata/pg_cron) | Cron-based workflow triggers | Optional |

### Installing Postgres extensions

The fastest way to get all three running locally is with [Tembo's Docker image](https://github.com/tembo-io/tembo), which ships pgmq and pg_cron pre-installed:

```bash
docker run -d \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  quay.io/tembo/standard-cnpg:latest
```

For pg_durable, follow the [installation guide](https://github.com/microsoft/pg_durable#installation) and enable it in your database:

```sql
CREATE EXTENSION pg_durable;
CREATE EXTENSION pgmq;
CREATE EXTENSION pg_cron;  -- optional
```

## Installation

```bash
pip install pyflows
```

With [uv](https://docs.astral.sh/uv/):

```bash
uv add pyflows
```

With optional FastAPI integration:

```bash
uv add "pyflows[fastapi]"
```

## What works today

The backend ABCs, types, and exceptions are stable and importable:

```python
from pyflows import (
    # Backend ABCs (implement your own or use the built-in stubs)
    OrchestratorBackend,
    QueueBackend,
    SchedulerBackend,
    # Concrete implementations (stubs — M2+ for full implementation)
    PgDurableBackend,
    PgmqBackend,
    PgCronBackend,
    # Types
    WorkflowStatus,
    WorkflowState,
    QueueMessage,
    ScheduledJob,
    RetryConfig,
    StepConfig,
    # Exceptions
    PyflowsError,
    WorkflowNotFoundError,
    WorkflowAlreadyExistsError,
    StepExecutionError,
    BackendNotInitializedError,
    SchedulerJobNotFoundError,
)
```

## Architecture

| Component     | Default implementation | Interface             |
| ------------- | ---------------------- | --------------------- |
| Orchestration | `PgDurableBackend`     | `OrchestratorBackend` |
| Step queue    | `PgmqBackend`          | `QueueBackend`        |
| Scheduling    | `PgCronBackend`        | `SchedulerBackend`    |

All three interfaces are abstract base classes — you can swap in any implementation.

### Execution modes

**Pull (default):** pg_durable enqueues the step → pgmq.send() → LISTEN/NOTIFY wakes the Python worker → step runs → `df.signal()` resumes the workflow.

**Push (opt-in):** pg_durable calls `df.http()` → hits a FastAPI endpoint directly (requires DB network access to the app).

## Retry configuration

```python
from pyflows import RetryConfig

RetryConfig(
    max_retries=5,
    backoff="exponential",      # "exponential" or "linear"
    initial_delay_seconds=1.0,
    max_delay_seconds=60.0,
    jitter=True,
)
```

## Roadmap

- [x] M1 — Project scaffold: backend ABCs, Pydantic types, exception hierarchy
- [ ] M2 — Core SDK: `@step`, `@workflow`, `WorkflowContext`, step registry
- [ ] M3 — Compiler: Python workflow → pg_durable DSL
- [ ] M4 — Worker: pgmq poller, LISTEN/NOTIFY, async step executor
- [ ] M5 — FastAPI integration: management router, push endpoint
- [ ] M6 — Plugin system: hooks ABC, OpenTelemetry + logging built-ins
- [ ] M7 — Migrations + pg_cron scheduler
- [ ] M8 — AI SRE example + full documentation

## Development

```bash
# Install dependencies (including dev extras)
uv sync

# Run tests
uv run pytest

# Lint
uv run ruff check src/
```

## License

MIT
