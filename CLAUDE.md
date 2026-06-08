# pgflows — Repository Guidelines

## Core principles

### 1. Component abstraction — swap technologies without touching call sites

Every infrastructure concern lives behind an ABC in `backends/base.py`. The rest of the SDK only depends on the interface, never the concrete class.

- **Add a new backend** → implement the ABC, register it in `__init__.py`. Zero changes elsewhere.
- **Replace a backend** → swap the constructor argument in `WorkflowApp`. Zero changes to workflows or steps.
- Never import a concrete backend class (`PgmqBackend`, `PgStateBackend`, …) outside of `app.py` and tests.

### 2. Type safety — Pydantic end-to-end, no `dict[str, Any]` at boundaries

- All user-facing inputs and outputs are `pydantic.BaseModel` subclasses.
- Serialization path: `model.model_dump_json()` → DB → `Model.model_validate_json()`. Never raw dicts.
- Internal helpers may use `dict[str, Any]` for DB rows, but re-hydrate to a model before returning to the caller.
- No `Any` in public function signatures. Use `TypeVar` + `Generic` or `BaseModel` subtypes.

### 3. Great developer experience

- `WorkflowApp` is the single entry point. One import, one object.
- Decorators (`@app.workflow()`, `@app.step()`) are the primary API — no manual registry calls.
- Errors must be actionable: include the workflow/step name, the instance ID, and the cause.
- `await app.initialize()` applies schema migrations automatically — no manual `psql` required.
- `docker compose up -d` + `uv run pytest` must be the full getting-started story.

### 4. Easy to extend — plugin hooks, not monkey-patching

- Extend behavior via hook points (`before_step`, `after_step`, `on_step_error`, etc.), not subclassing.
- Register via `app.register_plugin(MyPlugin())`.
- Plugins receive typed context objects — they never reach into internals.
- Adding a plugin must not require changes to existing code.

### 5. Modularity — one responsibility per module

| Module | Owns |
| --- | --- |
| `app.py` | Wiring only — assembles backends, registry, worker |
| `context.py` | Step execution + replay logic |
| `worker.py` | Workflow queue polling + dispatch loop |
| `step_worker.py` | pgmq+NOTIFY step consumer — runs Python steps, signals results to pg_durable |
| `registry.py` | Decorator registration + type extraction |
| `backends/` | All I/O (DB, queue, scheduler) |
| `backends/base.py` | ABCs for all backends |
| `backends/pg_durable.py` | pg_durable extension backend |
| `telemetry.py` | OTel span management |
| `sql_exporter.py` | pg_durable DSL generation (selectable `http` / `pgmq` step bindings) |
| `dsl.py` | Python DSL builders for pg_durable operators (incl. `worker_step`, `enqueue`) |
| `graph.py` | Typed, extensible `GraphSpec` workflow schema (discriminated-union nodes) |
| `graph_compiler.py` | Compile a `GraphSpec` → pg_durable DSL + composition-limit guard |
| `backends/pg_cron.py` | Real pg_cron-backed recurring scheduler (`cron.schedule`) |
| `fastapi_integration.py` | FastAPI router for push-mode endpoints |
| `migrations.py` | Automatic schema migration on `initialize()` |
| `pg_durable_client.py` | High-level client for pg_durable operations |

Cross-module imports must flow downward (app → worker → context → backends). No circular deps.

### 6. Slim code — experience over line count

- **No over-abstraction.** Three similar lines beat a premature helper.
- **No defensive coding for impossible cases.** Trust the type system and internal invariants.
- **No feature flags or shims.** Change the code directly.
- **No comments explaining what the code does.** Only add a comment when the *why* is non-obvious.
- Prefer deleting code over adding it. If two paths do the same thing, pick one.

## Code style

- Python 3.13+, fully async (`async def` everywhere that touches I/O).
- `from __future__ import annotations` at the top of every file.
- Imports: stdlib → third-party → internal (ruff enforces this).
- Line length: 100. Enforced by `uv run ruff check src/`.
- No `type: ignore` without a comment explaining why it's safe.

## Testing

- Unit tests: mock backends with `AsyncMock` — no DB required, must run offline.
- E2E tests: real Postgres via `docker compose up -d`. Skip automatically when DB is down.
- Every public method has at least one unit test.
- Test file mirrors source: `src/pgflows/foo.py` → `tests/unit/test_foo.py`.

## Commands

```bash
make install                     # uv sync
make up                          # docker compose up -d --wait
make test-unit                   # unit tests (no DB)
make test-e2e                    # E2E tests (starts Postgres first)
make lint                        # ruff check
make fmt                         # ruff check --fix

# Run a single test
uv run pytest tests/unit/test_app.py::test_initialize -v

# E2E tests use Postgres at 127.0.0.1:5433 (set via PGFLOWS_TEST_DSN)
PGFLOWS_TEST_DSN=postgresql://pgflows:pgflows@127.0.0.1:5433/pgflows_test uv run pytest tests/e2e/ -v
```

Install the FastAPI optional dependency when working on push-mode features:

```bash
uv sync --extra fastapi
```

### Live push-mode e2e (pg_durable + pgmq + pg_cron, Postgres 18)

The default `docker compose` DB has only pgmq. The push-mode flows (`df.http`,
pgmq+NOTIFY steps, `GraphSpec` compilation) need a Postgres that also has the
`pg_durable` (`df`) extension; recurring scheduling (`app.schedule_workflow`) needs
`pg_cron`. The combined e2e image is **Postgres 18 with pg_durable + pgmq + pg_cron**.
Build it and point the tests at it:

```bash
docker build -t pgflows-e2e-dfpgmq:latest tests/e2e/docker
docker compose -f tests/e2e/docker/docker-compose.yml up -d --wait
uv run pytest tests/e2e/test_live_dfpgmq.py -v   # auto-skips if df is absent
```

The full two-container stack (DB + the example app server) lives in
`docker-compose.full.yml` (`Dockerfile.app` + `examples/server.py`).

Notes for push-mode internals (learned by running real workflows on `df`):
- `PgDurableClient.start()` **interpolates** the DSL expression into the SQL (so
  Postgres evaluates `~>`, `|=>`, `df.http()` operators); only `label`/`database`
  are bound params.
- `df` substitutes `$capture` with the captured node's first-column value (not the
  `{"rows":[…]}` envelope), so step output threads as `input_expr="$capture::jsonb"`.
- **Parallelism is pg_durable's job and it works**: `&` (join, wait ALL) and `|`
  (race) run branches concurrently and durably; captures made inside a branch are
  visible after the join. `~>` binds tighter than `&`/`|`, so the DSL builders fully
  parenthesize a parallel group (`d >> (a & b)` → `d ~> ((a) & (b))`); otherwise it
  mis-parses as `(d ~> a) & b` and the right branch misses `d`'s captures.
- **Thread data with result captures (`|=>`), not many `df.setvar`s.** With >1
  durable var set, `df` serializes the vars snapshot with non-deterministic key order
  and a JOIN replay then fails as "nondeterministic: schedule mismatch". Keep one
  config var (e.g. `input`) and pass everything else via captures.
- pgmq steps use a **poll-result table** (`pgflows.worker_step_results`) rather than
  `df.wait_for_signal`: a NOTIFY-woken worker can signal before `df` registers the
  waiter, and that signal is dropped. The poll table is race-free (the row persists);
  `wait_for_signal` remains the right primitive for genuinely external events.
- Prefer `app.worker_step(...)` over the bare `worker_step(...)` builder: it binds the
  configured `step_queue`/`step_notify_channel`. The bare builder hardcodes `pgflows_steps`,
  so a renamed queue silently enqueues where no worker listens and the instance hangs.
- A captured `df.wait_for_signal` is the whole `{signal_name, timed_out, data}` envelope —
  read the `df.signal` payload under `->'data'` (e.g. `$decision::jsonb->'data'->>'approved'`).
- **pg_durable composition limits (bundled build, verified live — don't "fix" in pgflows):**
  a join (`&`/`join3`) of trivial bare-SQL branches can hang (children `completed`, JOIN
  `running`) while `worker_step`-branch joins resolve reliably; a loop and a parallel node
  cannot share one instance (ContinueAsNew replay deadlock — split into separate `df.start`s);
  `|` (race) is reliable only as a terminal node and does not cancel the loser. Accumulated
  hung instances exhaust the worker connection pool (~10) and wedge the executor — cancel
  stale `running` instances (`df.list_instances('running')` → `df.cancel`) or restart the DB
  container before running the live suite.
- Run history lives in the DB and is wrapped on `app.pg_durable`: `instance_info`,
  `instance_nodes` (per-node trail; expands to structural THEN/JOIN/IF rows),
  `instance_executions` (timing/events), `metrics` (cluster-wide). `app.acquire()` yields a
  pooled connection for ad-hoc SQL around a run.
