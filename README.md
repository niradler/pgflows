# pgflows

[![PyPI](https://img.shields.io/pypi/v/pgflows)](https://pypi.org/project/pgflows/)
[![Python](https://img.shields.io/pypi/pyversions/pgflows)](https://pypi.org/project/pgflows/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> Durable workflow engine SDK for Python + Postgres

pgflows lets you write long-running, fault-tolerant workflows as plain async Python functions — backed entirely by your existing Postgres database. No extra infrastructure, no separate orchestration service, no new runtime to operate.

> [!WARNING]
> **Early development (alpha).** The core API is stabilizing but not yet 1.0. Expect breaking changes before the first stable release.

## How it works

**pg_durable orchestrates the workflow; your Python runs the steps.** A workflow is a
durable graph — sequence, parallel, branch, loop — that **pg_durable drives inside
Postgres**. It owns the orchestration: ordering, branching, parallelism, durability, and
crash replay. Your Python functions are the **steps**, which pg_durable invokes via either
an HTTP call (`df.http()`) or a **pgmq+NOTIFY** hand-off (`worker_step` → a `StepWorker`
runs the function and returns the result).

> **pgmq+NOTIFY is the step transport, not a workflow engine.** It is how a pg_durable
> workflow runs a Python step; the workflow itself is still managed by pg_durable.

You define a pg_durable workflow either **as data** (a [`GraphSpec`](#data-driven-workflows-graphspec)
JSON document compiled to DSL) or **as Python** (`@app.workflow`, exported to DSL).

```text
GraphSpec  or  @app.workflow  ──►  pg_durable graph  (orchestrator, runs in Postgres)
                                        │  invokes each step via
                                        ├──►  df.http()           → your FastAPI endpoint
                                        └──►  pgmq.send + NOTIFY   → StepWorker → your Python step
```

> A self-contained **pull worker** also exists (`@app.workflow` + `ctx.step` +
> `run_worker`): here *Python* orchestrates and each step is checkpointed to `pg_state`, so
> it needs no pg_durable and suits simple or local runs. It is **not** a general workflow
> engine — for durable orchestration, branching, and parallelism, let pg_durable drive
> (above). Don't build a large workflow as Python glued together by the pgmq pull worker;
> that reimplements what pg_durable already does.

## Quick start

```bash
docker compose up -d   # start Postgres with pgmq + pg_durable
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

> The quick start above uses the **pull worker** — the simplest path, ideal for a first run.
> For production durability, branching, and parallelism, define the workflow as a
> [`GraphSpec`](#data-driven-workflows-graphspec) (or export it to DSL) and let **pg_durable**
> orchestrate it; see [How it works](#how-it-works).

## Features

- **Checkpoint replay** — workflows survive crashes; completed steps are never re-executed
- **Typed end-to-end** — step inputs and outputs are Pydantic models; no `dict[str, Any]` at the boundary
- **Configurable retries** — per-step `RetryConfig` with exponential or linear backoff and jitter
- **Plugin hooks** — `before_workflow`, `after_workflow`, `on_workflow_error`, `before_step`, `after_step`, `on_step_error`
- **Automatic migrations** — `await app.initialize()` applies schema migrations; no manual SQL required
- **pg_durable orchestration** — workflows run as durable pg_durable graphs (sequence, parallel, branch, loop); your Python runs as steps, invoked via `df.http()` or a pgmq+NOTIFY `StepWorker`. A self-contained Python pull worker is also available for simple/local runs
- **Data-driven workflows** — compile a typed `GraphSpec` JSON document to a pg_durable graph (`app.start_graph`); no Python workflow function required
- **Cron scheduler (optional)** — recurring workflows via `app.schedule_workflow`, backed by the `pg_cron` extension; pgflows runs fine without it (only scheduling is unavailable)
- **Dead-letter queue** — failed workflows are archived to `pgmq.a_{queue}` instead of being re-queued indefinitely
- **Worker coordination** — atomic `pending→running` claim prevents duplicate processing when multiple workers race on the same instance
- **Swappable backends** — orchestrator, queue, and scheduler implement ABCs; swap without touching workflow code
- **Execution history** — typed access to pg_durable's per-run trail: `instance_info`, `instance_nodes`, `instance_executions`, `metrics`
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

## Data-driven workflows (GraphSpec)

Describe a workflow as a typed JSON document and let pgflows compile it to a pg_durable
graph that Postgres orchestrates — no Python workflow function required. This is the
recommended path for workflows defined by config, an API, or a UI. **Requires pg_durable;**
run a `StepWorker` to execute the Python steps the graph dispatches.

```python
from pgflows import GraphSpec

spec = GraphSpec.model_validate({
    "input": {"n": 4},
    "root": {"type": "sequence", "nodes": [
        {"type": "step", "step": "double_it"},
        {"type": "branch",
         "condition": {"step": "is_big"},
         "then": {"type": "step", "step": "celebrate"},
         "else": {"type": "step", "step": "retry_later"}},
    ]},
})

import asyncio
worker = asyncio.create_task(app.run_step_worker())   # executes the dispatched Python steps
instance_id = await app.start_graph(spec, label="order-flow")   # compile → df.start()
schema = app.graph_json_schema()                      # JSON Schema for UIs / validation
```

Node types (a discriminated union on `type`): `step`, `sleep`, `wait_signal`,
`wait_schedule`, `sequence`, `parallel` (`mode: "all"|"race"`), `branch`, `loop`. The
compiler enforces verified pg_durable limits (raises `GraphCompileError`): a `loop` and a
`parallel` can't share an instance, and `race` must be terminal. Extend the schema by adding
one node class in `graph.py` plus one compile case in `graph_compiler.py`.

## SQL export and runtime workflows

pgflows can export any registered Python workflow to a [pg_durable](https://github.com/microsoft/pg_durable) SQL DSL. Use this to:

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

## Push-mode step bindings (pg_durable orchestrates, Python runs the steps)

In push mode **pg_durable is the orchestrator** — it durably drives the graph
(`~>` sequence, `&` parallel join, `|` race, `?>`/`!>` branch, loops) and calls out
to your Python steps. There are two selectable bindings for that call-out:

| `SqlExporter(mode=...)` | How pg_durable invokes a step |
| --- | --- |
| `"http"` (default) | `df.http()` → your FastAPI step endpoint (`X-DF-Instance-ID` header + `{input}` body) |
| `"pgmq"` | `pgmq.send` + `pg_notify` → a `StepWorker` runs the step and writes the result to a poll table pg_durable reads |

```python
# Pull both bindings off the app (registry + queue config wired for you)
http_sql = app.exporter(base_url="https://api.example.com/pgflows", mode="http").export_workflow("process_order")
worker_sql = app.exporter(mode="worker").export_workflow("process_order")

# The pgmq binding needs a step worker draining the queue + signalling results back:
await app.run_step_worker()          # blocking; use asyncio.create_task for background
```

Compose graphs directly with the Python DSL builders. `app.worker_step()` is a native
`pgmq.send → pg_notify → poll-result → read` unit that composes with the operators; prefer
it over the bare `worker_step()` builder because it binds the app's configured
`step_queue`/`notify_channel` (the bare builder hardcodes `pgflows_steps`, so a renamed
queue silently hangs). Here it runs `double_it`, then `add_ten` consuming its output via a
result capture:

```python
node = (
    app.worker_step("double_it", capture="d")
    >> app.worker_step("add_ten", input_expr="$d::jsonb", capture="r")
)
instance_id = await app.pg_durable.start(node, label="pipeline")
```

Gotchas worth knowing (all about using pg_durable correctly):

- **Thread data with captures (`|=>` / `capture=`), not many `df.setvar`s.** With more
  than one durable var set, pg_durable serializes the vars snapshot with
  non-deterministic key order and a parallel-join replay then fails. Keep a single
  config var and pass step data through captures.
- The pgmq binding polls a result table instead of `df.wait_for_signal`, because a
  NOTIFY-woken worker can signal before pg_durable registers the waiter (that signal
  would be dropped). The poll table is race-free.
- A captured `wait_for_signal` is the full `{signal_name, timed_out, data}` envelope —
  read your payload under `->'data'` (e.g. `$decision::jsonb->'data'->>'approved'`).
- **pg_durable composition limits (bundled build):** join `worker_step` branches, not
  trivial bare-SQL ones (`SELECT 1 & SELECT 2` can hang); never put a loop and a parallel
  node in the same instance (deadlocks); `|` (race) is reliable only as a terminal node and
  does not cancel the loser. Cancel stale `running` instances if the executor wedges.

### Running push mode for real (pg_durable + pgmq)

The bundled compose DB ships only pgmq. To exercise push mode end to end you need a
Postgres with `pg_durable` too (and `pg_cron` for scheduling) — the combined e2e image is
**Postgres 18 with pg_durable + pgmq + pg_cron**. Build it and run the live e2e
(real `df.start` / `df.http` / `pgmq.send` / `cron.schedule`, no mocks):

```bash
docker build -t pgflows-e2e-dfpgmq:latest tests/e2e/docker
docker compose -f tests/e2e/docker/docker-compose.yml up -d --wait
uv run pytest tests/e2e/test_live_dfpgmq.py -v
```

`compose.yml` brings up the full two-container stack — that Postgres image plus the example app server (`examples/server.py`).

## Observability — execution history

pg_durable records the full per-run history in the database; `PgDurableClient` exposes it
as typed models (no raw `df.*` SQL needed):

```python
client = app.pg_durable

info  = await client.instance_info(iid)        # InstanceInfo: label, function, status, output
nodes = await client.instance_nodes(iid)        # list[InstanceNode]: per-node trail (type, result, status)
execs = await client.instance_executions(iid)   # list[ExecutionRecord]: status, event_count, duration_ms
m     = await client.metrics()                   # Metrics: cluster-wide counters

# Need your own tables around a durable run? Use the pooled connection accessor:
async with app.acquire() as conn:
    rows = await conn.fetch("SELECT * FROM my_audit WHERE run = $1", run_id)
```

`instance_nodes` expands the graph into structural `THEN`/`JOIN`/`IF` rows, so it returns
more rows than the nodes you wrote — handy for seeing exactly where a run is.

## Scheduling (optional — pg_cron)

Recurring schedules use the **`pg_cron`** extension — the right tool for recurring cron. (A
pg_durable `@> (… ~> wait_for_schedule)` loop is *not*: it pins a worker connection forever
and can't share an instance with parallel nodes.) `pg_cron` is **optional** — pgflows runs
fine without it; only scheduling is unavailable (`app.pg_cron_available` reports presence,
and `schedule_workflow` raises a clear error if it's missing).

`app.schedule_workflow` registers a `cron.schedule` job whose command creates a pending
instance + `pgmq.send` + `pg_notify` — a running worker then picks it up each tick.

```python
if app.pg_cron_available:
    await app.schedule_workflow("hourly_health_check", "0 * * * *", health_check, CheckInput(...))
    await app.schedule_workflow("fast", "10 seconds", ticker)   # pg_cron 1.5+ sub-minute
    jobs = await app.list_schedules()
    await app.unschedule_workflow("hourly_health_check")        # idempotent, by name
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

| Extension | Required? | Purpose |
| --------- | --------- | ------- |
| [`pg_durable` (`df`)](https://github.com/microsoft/pg_durable) | Recommended | Workflow orchestration — durable graph execution, branching, parallelism, `GraphSpec`/SQL export, execution history |
| [`pgmq`](https://github.com/tembo-io/pgmq) | Yes | Step queue + the pull worker — enqueue, dequeue, dead-letter, pgmq+NOTIFY step transport |
| [`pg_cron`](https://github.com/citusdata/pg_cron) | Optional | Recurring schedules (`app.schedule_workflow`); everything else works without it |

The bundled Postgres image ships both extensions pre-installed. To start it:

```bash
docker compose up -d
```

## Installation

```bash
pip install pgflows
# or
uv add pgflows
```

### Docker

Two images are published on every release:

| Image | Registries | Tag scheme |
| ----- | ---------- | ---------- |
| **App** (Python SDK runtime) | `ghcr.io/niradler/pgflows` · `niradler/pgflows` | `latest`, `0.1.1` |
| **Postgres** (pgmq + pg_durable pre-installed) | `niradler/pgflows-postgres` | `<pg>-<pgmq>-<pg_durable>` e.g. `17-1.5.1-0.2.2` |

```bash
# App image — GitHub Container Registry (preferred)
docker pull ghcr.io/niradler/pgflows:0.1.1

# App image — Docker Hub
docker pull niradler/pgflows:0.1.1

# Postgres image with pgmq 1.5.1 + pg_durable 0.2.2 on PG 17
docker pull niradler/pgflows-postgres:17-1.5.1-0.2.2
```

Extend the app image with your workflow code:

```dockerfile
FROM ghcr.io/niradler/pgflows:0.1.1
WORKDIR /app
COPY . .
CMD ["python", "worker.py"]
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

## License

MIT
