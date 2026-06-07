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
- [x] M6: Plugin system — PyflowsPlugin ABC, LoggingPlugin, lifecycle hooks (before/after/error) in worker + context, fire() helper swallows plugin errors (10 unit tests)
- [x] M7: Migrations + pg_cron scheduler — migration runner (run_migrations, schema_migrations table, versioned SQL), PgCronBackend implemented with asyncpg, pg_state migrated to asyncpg with JSONB codecs (16 unit tests)
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

## Evidence review — 2026-06-07

Scope: review current implementation against the tracker goal: Python + Postgres durable workflow SDK with typed decorators, replay checkpoints, pgmq worker execution, plugin lifecycle hooks, OTel, SQL export, and runnable examples.

### Critical issues fixed

1. Step plugin hooks were declared and wired into `WorkflowWorker`, but never executed by `WorkflowContext`.
   - Evidence: `plugins.py` defines `before_step`, `after_step`, and `on_step_error`; `worker.py` passed `plugins=self._plugins` into `WorkflowContext`; `context.py` accepted `plugins` but did not store or call them.
   - Impact: M6 plugin lifecycle was marked complete, but `LoggingPlugin` and custom plugins never saw step events during real workflow execution.
   - Fix: `WorkflowContext` now stores plugins and fires step hooks around each real execution attempt. Replay from cached results still skips hooks because no step runs.
   - Proof: focused offline tests passed: `uv run pytest tests/unit/test_context_replay.py tests/unit/test_registry.py tests/unit/test_worker.py tests/unit/test_plugins.py -q` → 30 passed.

2. Decorated step retry config was stored but ignored at runtime.
   - Evidence: `WorkflowRegistry.register_step()` captured `retry`, and examples/e2e use `@app.step(retry=...)`; `WorkflowContext.step()` only used explicit call retry or workflow defaults and had no registry access.
   - Impact: `@app.step(retry=RetryConfig(...))` did not control execution unless callers duplicated `retry=` in every `ctx.step()` call.
   - Fix: `WorkflowWorker` passes the registry into `WorkflowContext`; `WorkflowContext` resolves retry precedence as explicit `ctx.step(..., retry=...)`, then explicitly configured step decorator retry, then workflow defaults.
   - Proof: regression tests cover explicit step retry and ensure default registered retry does not override workflow defaults; focused offline tests passed.

### Non-critical findings to track

1. Some real Postgres/pgmq tests are located under `tests/unit`.
   - Evidence: `tests/unit/test_pg_state.py` and `tests/unit/test_pgmq.py` open real connections to `localhost:5433`.
   - Impact: useful coverage, but these are integration tests, not offline unit tests. This can make `uv run pytest tests/unit/` depend on Docker/Postgres state.
   - Recommendation: later move them under `tests/integration/` or `tests/e2e/`, or mark them clearly with skip behavior when DB is unavailable.

2. `RetryConfig.backoff` accepts any string.
   - Evidence: `types.py` has `backoff: str = "exponential"` while README review docs expect constrained values.
   - Impact: typoed values are accepted silently. Current runtime does not branch on `backoff`, so this is API polish until linear/exponential behavior is implemented.
   - Recommendation: change to an enum or `Literal["exponential", "linear"]` when implementing actual backoff modes.

3. `PgmqBackend.listen()` is an async generator while the ABC uses `def listen(...) -> AsyncIterator[None]`.
   - Evidence: `base.py` declares a regular method returning `AsyncIterator`; `pgmq.py` implements `async def listen(...)` with `# type: ignore[override]`.
   - Impact: not currently used by `WorkflowWorker`, which polls with `dequeue`, but the interface is inconsistent.
   - Recommendation: decide whether `listen` should be an async generator factory or an async method and make the ABC and backend match.
