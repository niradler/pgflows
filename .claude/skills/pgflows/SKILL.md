---
name: pgflows-sdk
description: "Use when working with the pgflows Python SDK — WorkflowApp wiring, @app.workflow()/@app.step() decorators, Python DSL builders (sql_node/sleep/http/loop/wait_for_signal), pull-worker vs push-mode FastAPI architecture, SqlExporter, PgDurableClient, plugin hooks, RetryConfig, OTel telemetry, or PgflowsConfig. Also use when deciding whether to run a workflow in pull (WorkflowWorker) or push (pg_durable → df.http() → FastAPI) mode."
---

# pgflows Python SDK

The pgflows SDK wraps Postgres-based durable workflows. Two execution modes exist side-by-side: a **pull worker** that polls pgmq, and **push mode** where pg_durable calls your FastAPI endpoints via `df.http()`. Choose one per workflow; they share the same Python step definitions.

## Quick Setup

```python
from pgflows import WorkflowApp, PgflowsConfig

app = WorkflowApp(PgflowsConfig(dsn="postgresql://user:pass@host/db"))
await app.initialize()   # runs migrations, opens pools, checks extensions
# ... register workflows and run worker or mount router ...
await app.close()        # always call on teardown
```

`await app.initialize()` must run before any other call. It applies pending DB migrations automatically — no manual `psql` required.

## Execution Modes — Pull vs Push

| Concern | Pull (Worker) | Push (pg_durable) |
|---------|--------------|-------------------|
| Orchestrator | Python `WorkflowWorker` polling pgmq | pg_durable in Postgres |
| Durability | Python-side checkpointing via pgmq + pg_state | pg_durable native replay |
| Step invocation | `await ctx.step(fn, input)` in Python | `df.http()` calls FastAPI endpoint |
| Best for | Python-heavy logic, complex branching | SQL-native workflows, cross-DB, signals |

**Pull mode** — define and run:

```python
@app.workflow(name="process_order")
async def process_order(ctx: WorkflowContext, inp: OrderInput) -> None:
    result = await ctx.step(validate_order, inp)
    await ctx.step(charge_card, ChargeInput(order_id=result.order_id))

@app.step()
async def validate_order(ctx: StepContext, inp: OrderInput) -> ValidationResult:
    ...

await app.run_worker()   # blocking; use asyncio.create_task for background
```

**Push mode** — wire pg_durable → FastAPI → steps:

```python
from pgflows import SqlExporter
from pgflows.fastapi_integration import create_pgflows_router

# FastAPI router handles df.http() calls from pg_durable
router = create_pgflows_router(app, prefix="/pgflows")
fastapi_app.include_router(router)

# Export Python workflow definition to pg_durable SQL
exporter = SqlExporter(app.registry, base_url="https://api.example.com/pgflows")
sql = exporter.export_workflow("process_order")
# → SELECT df.setvar / df.start(df.http('.../steps/validate_order') ~> ...)
```

## Workflow and Step Definitions

```python
from pgflows import RetryConfig

@app.workflow(
    name="my_workflow",          # optional; defaults to function name
    step_defaults=RetryConfig(max_retries=5, backoff="exponential"),
)
async def my_workflow(ctx: WorkflowContext, inp: MyInput) -> None:
    ...

@app.step(
    name="my_step",              # optional
    retry=RetryConfig(max_retries=2, initial_delay_seconds=0.5),
    timeout_seconds=30.0,
)
async def my_step(ctx: StepContext, inp: MyStepInput) -> MyStepOutput:
    return MyStepOutput(...)
```

**Step signature is always `(StepContext, InputModel) -> OutputModel | Any`.**

`WorkflowContext.step()` provides checkpoint replay: on worker restart, a completed step returns its cached result without re-executing, making the workflow idempotent.

## Python DSL Builders (dsl.py)

These mirror `pg-durable-sql` operators but with Python ergonomics. Import from `pgflows`:

```python
from pgflows import sql_node, sleep, http, loop, wait_for_signal, wait_for_schedule
from pgflows import if_node, if_rows, join3, break_, DslNode
```

| Python | pg_durable SQL | Description |
|--------|---------------|-------------|
| `a >> b` | `a ~> b` | Sequence |
| `a & b` | `a & b` | Parallel join (wait ALL) |
| `a \| b` | `a \| b` | Race (first wins) |
| `node.capture("x")` | `node \|=> 'x'` | Capture result as `$x` |
| `node.if_then(t, e)` | `node ?> t !> e` | Conditional |
| `sql_node("SELECT 1")` | `'SELECT 1'` | Wrap SQL (auto-escapes `'`) |
| `sleep(30)` | `df.sleep(30)` | Sleep |
| `wait_for_signal("ok")` | `df.wait_for_signal('ok')` | Wait for signal |
| `wait_for_schedule("0 * * * *")` | `df.wait_for_schedule('0 * * * *')` | Wait for cron |
| `loop(body)` | `@> (body)` | Infinite loop |
| `loop(body, cond)` | `df.loop(body, cond)` | While-loop |
| `join3(a, b, c)` | `df.join3(a, b, c)` | Three-way join |
| `if_node(cond, t, e)` | `cond ?> t !> e` | Conditional (standalone) |
| `if_rows("x", t, e)` | `df.if_rows('x', t, e)` | Branch on captured rows |
| `break_()` | `df.break()` | Exit loop |
| `worker_step("s")` | `pgmq.send ~> pg_notify ~> df.loop(poll) ~> read` | Run Python step via pgmq+NOTIFY |

```python
# Example: parallel fan-out then sequence
node = (sql_node("SELECT count(*) FROM users") & sql_node("SELECT count(*) FROM orders"))
node = node >> sql_node("INSERT INTO audit(msg) VALUES ('done')")

# Use with PgDurableClient
instance_id = await app.pg_durable.start(node, label="audit-run")
```

The `DslNode` renders to a SQL string via `str(node)` — pass it directly to `app.pg_durable.start()`.

## pgmq+NOTIFY step binding (`worker_step` + `StepWorker`)

A second push-mode binding alongside `df.http()`: pg_durable enqueues the step and a
Python `StepWorker` runs it — no inbound HTTP server needed.

```python
from pgflows import worker_step

# double_it then add_ten consuming its output, threaded via a capture
node = (
    worker_step("double_it", capture="d")
    >> worker_step("add_ten", input_expr="$d::jsonb", capture="r")
)
worker = asyncio.create_task(app.run_step_worker())   # drains queue, writes results
iid = await app.pg_durable.start(node, label="pipeline")
```

`worker_step` emits `pgmq.send → pg_notify → df.loop(poll) → SELECT result`. The worker
runs the registered step and INSERTs the output into `pgflows.worker_step_results`; the
graph polls that table (race-free) instead of `df.wait_for_signal`. Select the binding
for whole-workflow export with `app.exporter(mode="worker")` vs `mode="http"`.

## PgDurableClient

Access via `app.pg_durable` (raises `RuntimeError` if extension absent — check `app.pg_durable_available` first).

```python
if app.pg_durable_available:
    client = app.pg_durable        # PgDurableClient

    await client.setvar("api_url", "https://api.example.com")
    instance_id = await client.start(node, label="my-flow")
    status = await client.status(instance_id)      # 'pending'|'running'|'completed'|'failed'|'cancelled'
    result = await client.result(instance_id)      # parsed JSON
    await client.signal(instance_id, "approval", {"approved": True})
    await client.cancel(instance_id, "user request")
    instances = await client.list_instances(status="running", limit=50)
    graph = await client.explain(instance_id)      # or pass a DSL string
    await client.grant_usage("my_role", include_http=True)
```

## SqlExporter

Bridges Python workflow definitions to pg_durable SQL (introspects AST to find `ctx.step()` calls).

```python
exporter = SqlExporter(app.registry, base_url="https://api.example.com/pgflows")

# Export a single workflow → SQL with df.start(df.http(...) ~> df.http(...))
sql = exporter.export_workflow("process_order")

# Dry-run: inspect steps without executing
result = exporter.dry_run("process_order")   # DryRunResult(workflow_name, steps, sql)
for s in result.steps:
    print(s.step_name, s.http_url)

# Compose at runtime from explicit step list (no Python workflow fn required)
sql = exporter.compose("on_call_response", ["page_engineer", "create_ticket"])

# Export all registered workflows
sql_all = exporter.export_all()
```

Step names in `export_workflow` come from AST introspection — step functions must be called as `await ctx.step(fn_name, ...)` with a direct name reference.

## Push-Mode FastAPI Integration

```python
from pgflows.fastapi_integration import create_pgflows_router

router = create_pgflows_router(app, prefix="/pgflows")
fastapi_app.include_router(router)
```

Endpoints created:

| Method | Path | Purpose |
|--------|------|---------|
| POST | `/pgflows/steps/{step_name}` | pg_durable calls this via `df.http()` |
| POST | `/pgflows/workflows/{name}/start` | Start workflow by name |
| GET | `/pgflows/workflows/{id}` | Get status |
| DELETE | `/pgflows/workflows/{id}` | Cancel |
| POST | `/pgflows/workflows/{id}/signal` | Send signal (pg_durable only) |
| GET | `/pgflows/workflows` | List instances |

**Pass instance ID from pg_durable to the step endpoint using the header:**

```sql
df.http('{base_url}/steps/my_step', 'POST', '{...}',
        '{"X-DF-Instance-ID": "{sys_instance_id}"}'::jsonb)
```

The step handler reads `X-DF-Instance-ID` and includes it in `StepContext.instance_id` for telemetry correlation.

## Plugin System

```python
from pgflows import PgflowsPlugin, StepEvent, WorkflowEvent

class MetricsPlugin(PgflowsPlugin):
    async def before_step(self, event: StepEvent, input_model: BaseModel) -> None:
        ...  # event.instance_id, event.workflow_name, event.step_name, event.step_index, event.attempt

    async def after_step(self, event: StepEvent, result: Any) -> None:
        ...

    async def on_step_error(self, event: StepEvent, error: Exception) -> None:
        ...

    # Also available: before_workflow, after_workflow, on_workflow_error

app.register_plugin(MetricsPlugin())
app.register_plugin(LoggingPlugin())   # built-in; logs all lifecycle events
```

Plugin errors are swallowed — they never abort the step. Register before `initialize()`.

## Configuration Reference (PgflowsConfig)

| Field | Default | Description |
|-------|---------|-------------|
| `dsn` | required | PostgreSQL connection string |
| `workflow_queue` | `"pgflows_workflows"` | pgmq queue name for pull mode |
| `step_queue` | `"pgflows_steps"` | pgmq queue name for steps |
| `worker_concurrency` | `10` | Max concurrent workflows in pull mode |
| `step_visibility_timeout_seconds` | `300` | pgmq message re-delivery timeout |
| `otel_enabled` | `True` | Enable OpenTelemetry tracing |
| `otel_service_name` | `"pgflows"` | OTel service name |
| `db_ssl` | `True` | Require SSL for DB connections |

OTel is on by default and reads from standard `OTEL_*` env vars. Set `otel_enabled=False` for local dev without a collector.

## RetryConfig

```python
RetryConfig(
    max_retries=3,              # additional attempts after first failure
    backoff="exponential",      # or "linear"
    initial_delay_seconds=1.0,
    max_delay_seconds=60.0,
    jitter=True,                # randomizes delay by ±50%
)
```

Set on `@app.workflow(step_defaults=...)` or `@app.step(retry=...)`. Step-level retry overrides workflow defaults.

## Common Mistakes

1. **Forgetting `await app.initialize()`** before calling `app.start()`, `app.get_status()`, etc. — raises `RuntimeError`.

2. **Using `app.pg_durable` without checking `app.pg_durable_available`** — raises `RuntimeError` if the `df` extension is absent.

3. **Calling `app.pg_durable.setvar()` inside a push-mode step** — variables must be set before `df.start()` in SQL. From Python, call `await client.setvar()` before `await client.start()`.

4. **Expecting `app.start()` (pull mode) to block** — it enqueues and returns `instance_id` immediately. Poll `app.get_status()` or use `app.process_once()` in tests.

5. **DSL operators precedence** — `>>` (sequence) has lower precedence than `&` and `|`, but always wrap parallel branches explicitly: `(a & b) >> c`, not `a & b >> c`.

6. **`SqlExporter` step discovery requires direct function references** — `await ctx.step(validate_order, inp)` is detected; `await ctx.step(lookup_fn(name), inp)` is not.

7. **Forgetting `await app.close()`** — connection pools stay open; always close in teardown or use as async context.

## Push-mode gotchas (verified against a live pg_durable + pgmq)

8. **`PgDurableClient.start()`/`explain()` interpolate the DSL, never bind it.** The DSL
   operators are SQL-level and Postgres must evaluate them; a bound `$1` text param
   reaches `df` as inert text and fails. (The client already does this — don't "fix" it
   to a bound param.)

9. **Thread step data with captures, not multiple `setvar`s.** With >1 durable var set,
   pg_durable serializes the vars snapshot non-deterministically and a parallel join
   then fails replay (`nondeterministic: schedule mismatch`). Keep one config var (e.g.
   `base_url`) and pass data via `|=>` captures / `worker_step(input_expr="$cap::jsonb")`.

10. **`$capture` is the captured node's first-column value**, not the
    `{"rows":[…]}` envelope — so thread with `input_expr="$cap::jsonb"`, and read a
    step's final result from `result["rows"][0]["<col>"]`.

11. **Parallel join works; group it before sequencing.** `&`/`|` already fully
    parenthesize, so `d >> (a & b)` is correct. pg_durable runs the branches
    concurrently and durably — let it; don't reimplement fan-out in Python.

12. **`worker_step` polls a result table, not `df.wait_for_signal`** — a NOTIFY-woken
    worker can signal before the waiter is registered (lost signal → hang). Run a
    `StepWorker` (`app.run_step_worker()`); it writes results to
    `pgflows.worker_step_results` and the graph polls (race-free).

13. **Live push-mode tests need `df` + `pgmq` in one DB.** The default compose image has
    only pgmq; build `tests/e2e/docker` for both, or use `docker-compose.full.yml`.

## Related Skill

For the raw pg_durable SQL DSL (operators, functions, variable substitution, common SQL patterns) — see **`pg-durable-sql`**.
