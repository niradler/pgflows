# pyflows — project tracker

## What we're building

A Python + Postgres workflow engine SDK.

- Users bring: Postgres connection + FastAPI app
- We provide: durability, retries, scheduling, events, plugin hooks
- Primary use case: AI SRE automation

## Architecture decisions

### Engine stack

| Component | Default impl | Interface (swappable) |
|-----------|-------------|----------------------|
| Orchestration | pg_durable (microsoft/pg_durable) | `OrchestratorBackend` |
| Python step queue | pgmq | `QueueBackend` |
| Scheduling | pg_cron | `SchedulerBackend` |
| Push HTTP (optional) | df.http() / pg_net | `HttpExecutor` — plugin |

### Execution modes

- **Pull (default)**: pg_durable → pgmq.send() → LISTEN/NOTIFY wakes Python worker → fn runs → df.signal() resumes
- **Push (opt-in)**: pg_durable → df.http() → FastAPI endpoint (when DB can reach app)

### pg_durable SQL surface to expose

`df.start()`, `df.cancel()`, `df.if()`, `df.join()`, `df.loop()`, `df.http()`, `df.vars`, `df.grant_usage()`

Tables: `df.instances`, `df.nodes`. No Python client exists — we call these via psycopg.

### Type safety (Pydantic e2e)

- All SDK types: `BaseModel` (types.py)
- Steps declare typed Pydantic input/output models
- Serialization path: Pydantic → `.model_dump_json()` → pgmq jsonb → `Model.model_validate_json()` → worker
- Zero `dict[str, Any]` at the user-facing boundary
- Compiler (M3) knows schemas at decoration time — can validate before hitting DB

### API style

- Primary: async Python `await ctx.step(...)` — compiles to pg_durable DSL
- Typed step example:

  ```python
  @app.step(retry=RetryConfig(max_retries=3))
  async def check_service(ctx: StepContext, input: CheckInput) -> CheckResult: ...
  ```

- Escape hatch: raw pg_durable DSL via `ctx.dsl()` / `app.dsl.start(...)`

### Retries

- Step-level: `@app.step(max_retries=3, backoff="exponential")`
- Workflow-level override: `@app.workflow(..., step_defaults=RetryConfig(...))`
- Follows pg_durable semantics underneath

### Performance principles

- Everything async — `AsyncConnection` + `psycopg-pool` for all DB access
- Worker wake-up: LISTEN/NOTIFY (zero-poll idle), batch dequeue for throughput
- CPU-bound steps: `asyncio.run_in_executor` or separate worker process (M4 first-class option)

### Modularity + plugin system

- Core: `PgDurableBackend` + `PgmqBackend` — required
- Optional: `PgCronBackend` — scheduler plugin
- Optional features as plugins: OTel, logging, push-mode (df.http), multi-tenant RLS
- Hook points: `before_step`, `after_step`, `on_step_error`, `before_workflow`, `after_workflow`
- Register via `app.register_plugin(MyPlugin())`

### Package name

`pyflows` (may change — keep name isolated to pyproject.toml + `__init__.py`)

## Milestones

- [x] M1: Project scaffold — pyproject.toml, src layout, uv, backends ABCs + stubs
- [x] M2: Core SDK — WorkflowApp, WorkflowContext (replay), WorkflowWorker, WorkflowRegistry, @step/@workflow decorators, OTel telemetry, PgStateBackend, PgmqBackend (21 unit tests passing)
- [x] M3: SqlExporter — AST-based pg_durable DSL generation, dry-run, export_all (7 unit tests passing)
- [x] M4: E2E test suite — basic, retry, monitor/cancel (skip when Docker not running); run with `docker compose up -d` then `uv run pytest tests/e2e/`
- [ ] M5: FastAPI integration — push endpoint only (deferred; pull mode works without it)
- [ ] M6: Plugin system — hooks ABC, OTel + logging built-ins
- [ ] M7: Migrations + pg_cron scheduler
- [ ] M8: AI SRE example + README — shareable

## README

Written and reviewed. Key design decisions baked in:

- Alpha warning admonition — clearly marks the planned vs working API split
- "What works today" section shows stable imports (types, backends, exceptions)
- Planned API section shows the target decorator syntax — clearly labeled not yet implemented
- Postgres extension setup section (Docker + SQL)
- FastAPI optional install documented
- Two subagent reviews completed; review files saved to `.claude/docs/readme-review-content.md` and `.claude/docs/readme-review-clarity.md`

## M1 state (done)

```text
src/pyflows/
├── __init__.py          # public exports (all types, backends, exceptions)
├── py.typed
├── types.py             # WorkflowStatus, QueueMessage, ScheduledJob, RetryConfig, StepConfig — Pydantic BaseModel
├── exceptions.py        # PyflowsError hierarchy
└── backends/
    ├── __init__.py
    ├── base.py          # OrchestratorBackend, QueueBackend, SchedulerBackend ABCs
    ├── pg_durable.py    # PgDurableBackend stub
    ├── pgmq.py          # PgmqBackend stub
    └── pg_cron.py       # PgCronBackend stub
```

Deps: `pydantic`, `psycopg[binary]`, `tembo-pgmq-python` (all latest, no pins)

## Open issues / decisions

- Workflow versioning strategy (TBD when we hit M3)
- Observability dashboard (post-M8)
- Multi-tenant / RLS (pg_durable has this built-in via df.grant_usage(), surface in SDK)
- Push mode (df.http) implementation strategy — plugin or core? (leaning plugin, M5/M6)
