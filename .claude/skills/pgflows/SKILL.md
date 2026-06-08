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

`PgflowsConfig.dsn` **normalizes the URL**: SQLAlchemy/Django-style driver suffixes
(`postgresql+psycopg://`, `postgresql+asyncpg://`) and the short `postgres://` scheme are
rewritten to the bare `postgresql://` asyncpg needs — pass the same DSN you give your ORM,
no manual `.replace()` dance.

## The model — pg_durable orchestrates, Python runs the steps

**The canonical architecture: pg_durable manages the workflow; your Python is the steps.**
pg_durable durably drives the graph (sequence, parallel, branch, loop) inside Postgres and
invokes each step via `df.http()` or **pgmq+NOTIFY** (`worker_step` + a `StepWorker`).
pgmq+NOTIFY is the *step transport* — **not** a workflow engine. Define a pg_durable
workflow as data (`GraphSpec` → `start_graph`) or as Python (`@app.workflow` → `exporter()`).

A separate **pull worker** (`@app.workflow` + `ctx.step` + `run_worker`) is the
self-contained Python orchestrator: it polls the pgmq *workflow* queue and checkpoints steps
to `pg_state`, needing no pg_durable. It suits simple/local runs. **Anti-pattern to avoid:**
building a large workflow as Python `ctx.step` calls on the pull worker and treating pgmq as
the engine — that reimplements orchestration pg_durable already provides. Use it for simple
cases; reach for pg_durable (`GraphSpec`/export) for real durable orchestration.

| Concern | Pull worker (standalone) | pg_durable (recommended) |
|---------|--------------|-------------------|
| Orchestrator | Python `WorkflowWorker` polling pgmq | **pg_durable in Postgres** |
| Durability | Python checkpointing via pgmq + pg_state | pg_durable native replay |
| Define as | `@app.workflow` Python fn | `GraphSpec` JSON, or `@app.workflow` exported to DSL |
| Steps run via | `await ctx.step(fn, input)` in-process | `df.http()` or pgmq+NOTIFY `worker_step`/`StepWorker` |
| Best for | simple/local, no pg_durable | durable orchestration, branching, parallelism, scheduling |

**Pull worker** — the simpler standalone path (Python orchestrates):

```python
@app.workflow(name="process_order")
async def process_order(ctx: WorkflowContext, inp: OrderInput) -> None:
    result = await ctx.step(validate_order, inp)
    await ctx.step(charge_card, ChargeInput(order_id=result.order_id))

@app.step()
async def validate_order(ctx: StepContext, inp: OrderInput) -> ValidationResult:
    ...

await app.run_worker()   # blocking; use asyncio.create_task for background
# Production: supervise the loop — transient DB drops are caught and the backends
# re-established with exponential backoff (up to max_backoff). Stops on close()/cancel.
await app.run_worker(reconnect=True, max_backoff=30.0)
```

**Starting and observing a pull-mode run** — the exact signatures:

```python
iid = await app.start(process_order, OrderInput(...))  # pass the FUNCTION, not its name → str
await app.process_once()                               # pump one poll batch (returns int handled)
status = await app.get_status(iid)                     # → WorkflowStatus (a record, not a string)
if status.state is WorkflowState.COMPLETED:            # WorkflowState is a str-enum, UPPERCASE
    out = MyOutput.model_validate(status.output)       # workflow return value lands in .output
```

- `app.start(workflow_fn: Callable, input_model: BaseModel) -> str` — pass the **function object**, not the registered name (a `str` raises `AttributeError`).
- `app.get_status(instance_id) -> WorkflowStatus` with fields `workflow_id, name, state, created_at, updated_at, error, output`. `WorkflowStatus` is the rich record; `WorkflowState` is the enum — easy to swap by mistake.
- `WorkflowState` members are **UPPERCASE**: `PENDING, RUNNING, SUSPENDED, COMPLETED, FAILED, CANCELLED`. (This differs from `PgDurableClient.status()`, which returns *lowercase* strings — see below.)
- **Return a `BaseModel` from the workflow fn** to populate `get_status().output`; a `-> None` workflow leaves `output` empty.

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

**Quoting with `sql_node()` — write single quotes naturally.** The builder doubles them
for the SQL-literal layer, so `sql_node("... WHERE status = 'pending'")` is correct. Do
**not** pre-double quotes the way the raw `pg-durable-sql` examples do (`''pending''`) — that
double-doubles and produces a `syntax error`. The hand-doubling rule applies only to raw SQL
strings you write *outside* the Python builders.

**`http()` is a Python builder, not raw SQL** — `http(url, method="POST", body: str|None=None,
headers: dict[str, str]|None=None, timeout_seconds=30)`. `headers` is a **dict**, not a JSON
string (a string double-encodes and the endpoint 422s; the builder now raises `TypeError` to
catch this). Step endpoints parse a JSON body, so include `Content-Type: application/json`:
`http(url, body="{...}", headers={"X-DF-Instance-ID": "{sys_instance_id}", "Content-Type": "application/json"})`.

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

The bare `worker_step(...)` builder hardcodes `queue="pgflows_steps"` regardless of
config — if you override `step_queue`/`step_notify_channel` in `PgflowsConfig` and forget
to pass the same `queue=`/`notify_channel=` to every `worker_step`, the graph enqueues to a
queue the `StepWorker` never drains and **the instance hangs silently, no error**. Prefer
**`app.worker_step("name", ...)`**, which binds this app's configured queue/channel for you
(override per-call via kwargs):

```python
node = app.worker_step("double_it", capture="d")   # uses config.step_queue + step_notify_channel
```

`worker_step`'s first step needs its input seeded: `input_expr` defaults to
`"'{input}'::jsonb"` (the `{input}` durable var), so `await client.setvar("input", json_str)`
before `start`, or pass a JSON literal: `input_expr="'{\"n\":4}'::jsonb"`.

## Data-driven workflows — GraphSpec → DSL compiler

Describe a workflow as a typed JSON document and let pgflows compile it to a pg_durable
DSL graph that Postgres runs — no Python workflow function required. **Requires pg_durable.**

```python
from pgflows import GraphSpec

spec = GraphSpec.model_validate({
    "input": {"n": 4},                       # seeds the single {input} durable var
    "root": {"type": "sequence", "nodes": [
        {"type": "step", "step": "double_it"},          # → worker_step, auto-threaded
        {"type": "branch",
         "condition": {"step": "is_big"},               # a step whose truthy output drives ?>
         "then":  {"type": "step", "step": "add_hundred"},
         "else":  {"type": "step", "step": "add_ten"}},
    ]},
})
worker = asyncio.create_task(app.run_step_worker())      # executes the dispatched steps
iid = await app.start_graph(spec, label="my-graph")      # compile + df.start (seeds input)
schema = app.graph_json_schema()                         # JSON Schema for UIs / validation
node = app.compile_graph(spec)                           # just the DslNode (offline, pure)
```

Node types (discriminated union on `type`): leaf — `step`, `sleep`, `wait_signal`,
`wait_schedule`; structural — `sequence`, `parallel` (`mode:"all"|"race"`), `branch`,
`loop`. Each `step` maps to a `worker_step`; a `sequence` auto-threads each node's output
into the next (`$cap::jsonb`); after a `parallel mode="all"` the next step's default input
is a `jsonb_build_object` of the branch captures (override any step's input with the optional
`input` SQL-expression field). Extend the schema = add one node class in `graph.py` + one
compile case in `graph_compiler.py`.

**The compiler enforces verified pg_durable limits** (raises `GraphCompileError`): a `loop`
and a `parallel` may not share an instance; `parallel mode="race"` must be terminal. There is
**no per-node `retry`** — in worker mode a failed step is `nack`ed and pgmq redelivers it (the
`StepWorker` does not honor `RetryConfig`; that's a pull-mode concept).

Note: `(worker_step & worker_step) >> next` (parallel-then-merge) is correct and runs in
isolation, but the bundled pg_durable build can leave the `JOIN` stuck `running` under heavy
executor load (children completed) — a known extension limit, recovered by a DB restart.

## Recurring schedules — pg_cron (not pg_durable loops)

For recurring cron, use **pg_cron**, not a pg_durable `@> (… ~> wait_for_schedule)` loop (a
loop pins a worker connection forever and can't share an instance with parallel nodes).
`app.schedule_workflow` registers a real `cron.schedule` job whose command creates a pending
instance + `pgmq.send` + `pg_notify` — a running pull worker (`run_worker`) then picks it up
each tick. **Requires the pg_cron extension** (`app.pg_cron_available`).

```python
await app.schedule_workflow("nightly_report", "0 2 * * *", report_workflow, ReportInput(...))
await app.schedule_workflow("fast", "10 seconds", ticker)   # pg_cron 1.5+ sub-minute intervals
jobs = await app.list_schedules()                            # list[ScheduledJob]
await app.unschedule_workflow("nightly_report")             # idempotent, by name
```

`enqueue(queue, payload, notify_channel=...)` is the reusable pgmq.send + pg_notify DSL
builder (the trigger primitive) if you want to fan out from inside a durable flow.

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

`client.status()` returns **lowercase** strings (`'completed'`), unlike pull-mode
`app.get_status().state` (UPPERCASE enum). `result()` is the final node's parsed JSON.

### Observing / auditing a run (execution history)

pg_durable records the full per-run history in the DB. These return typed Pydantic
models (not dicts):

```python
info  = await client.instance_info(iid)              # InstanceInfo | None
nodes = await client.instance_nodes(iid)             # list[InstanceNode] — per-node trail
execs = await client.instance_executions(iid)        # list[ExecutionRecord] — timing/events
m     = await client.metrics()                       # Metrics — cluster-wide counters
```

- `instance_nodes` is the durable node trail: each node's `node_type`, `query`,
  `result_name`, `status`, decoded `result`, `updated_at`. The graph **expands into more
  rows than you wrote** — structural `THEN`/`JOIN`/`IF` nodes each get a row.
- `instance_executions[*].status` is **Title-case** (`Completed`) — distinct again from
  `status()`'s lowercase. `duration_ms`/`event_count` are populated per execution.
- `metrics()` is cluster-wide, not per-instance.

### Ad-hoc SQL around a run

Creating/seeding/inspecting your own tables next to a durable graph is the archetypal
operational pattern. Use the pooled connection accessor — never reach into internals:

```python
async with app.acquire() as conn:
    await conn.execute("CREATE TABLE IF NOT EXISTS audit(...)")
    rows = await conn.fetch("SELECT * FROM audit WHERE run = $1", run_id)
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
| `step_queue` | `"pgflows_steps"` | pgmq queue for `worker_step` dispatch |
| `step_notify_channel` | `"pgflows_steps"` | `pg_notify` channel the `StepWorker` listens on |
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

11. **Parallel join works for `worker_step` branches; group it before sequencing.** `&`/`|`
    already fully parenthesize, so `d >> (a & b)` is correct, and a join of `worker_step`
    branches runs concurrently and durably (the reliable pattern). **But** on the bundled
    pg_durable build, a join (`&`/`join3`) of *trivial bare-SQL* branches
    (`sql_node("SELECT 1") & sql_node("SELECT 2")`) can hang — branches `completed`, JOIN
    `running` — and a loop must **not** share an instance with any parallel node (`loop ~> (a&b)`,
    `join3 ~> loop` both deadlock via ContinueAsNew replay). Keep parallel work to
    `worker_step` branches, and run loop-drains as a separate `df.start` instance from
    parallel checks. See gotcha 15.

12. **`worker_step` polls a result table, not `df.wait_for_signal`** — a NOTIFY-woken
    worker can signal before the waiter is registered (lost signal → hang). Run a
    `StepWorker` (`app.run_step_worker()`); it writes results to
    `pgflows.worker_step_results` and the graph polls (race-free).

13. **Live push-mode tests need `df` + `pgmq` in one DB.** The default compose image has
    only pgmq; build `tests/e2e/docker` for both, or use `docker-compose.full.yml`.

14. **A captured `wait_for_signal` is the full envelope, not your payload.** Capturing
    `wait_for_signal('approval') |=> 'decision'` makes `$decision` the whole
    `{"signal_name":…, "timed_out":…, "data":{…}}` object — the data you passed to
    `df.signal` lands under `->'data'`. Branch on
    `($decision::jsonb->'data'->>'approved')::boolean`, and check `->>'timed_out'` first.
    Reading `->>'approved'` at the top level is always NULL → silently takes the wrong
    branch.

15. **pg_durable composition limits (extension behavior, not pgflows) — verified live:**
    - **`&`/`join3` of bare-SQL branches may never complete** — branches go `completed`, the
      JOIN stays `running`. Joins of `worker_step` branches resolve reliably; prefer those.
    - **A loop cannot share an instance with a parallel node.** `loop ~> join3`, `join3 ~> loop`,
      and `loop ~> (a & b)` all deadlock (loop ContinueAsNew replay vs. parallel state). Split
      the loop-drain and the parallel checks into separate `df.start` instances.
    - **Race `|` is reliable only as a terminal node**, and does **not** cancel the loser —
      both branches' side-effects can fire. Don't put effects in a race branch you don't want,
      and don't sequence anything after a race (`race ~> next` hangs).
    - **The executor wedges under accumulated hung instances.** Each wedged instance holds a
      worker connection; once `max_duroxide_connections` (≈10) is exhausted, new instances get
      auto-cancelled (`execution_acquire_timeout`) around execution 3. Cancel stale `running`
      instances (`client.list_instances("running")` → `client.cancel`); a DB container restart
      clears worker state. Inspect a stall with `await client.instance_nodes(iid)`.
    - Confirmed working: loop/`break_`/`if_rows`/capture/`sleep`, and `wait_for_schedule`
      (`'* * * * *'` fires within ~1 min) — chain `wait_for_schedule` from non-parallel nodes.

## Related Skill

For the raw pg_durable SQL DSL (operators, functions, variable substitution, common SQL patterns) — see **`pg-durable-sql`**.
