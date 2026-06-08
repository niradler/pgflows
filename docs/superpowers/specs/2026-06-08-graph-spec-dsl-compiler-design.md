# GraphSpec → pg_durable DSL compiler

**Date:** 2026-06-08
**Status:** Approved design — ready for implementation plan

## Problem

A real pgflows consumer (`agentops/pgflows_engine.py`) hand-rolls a ~170-line
data-driven workflow engine *on top of* pgflows: it takes a JSON list of steps
(`{id, type, next_steps, input}`), topologically sorts them, threads each step's
output into its successors' inputs, evaluates conditional branches, and dispatches
each node by `type` to a registered step. This is a second orchestrator layered on
an engine that already orchestrates.

pgflows is a **code-first** SDK (decorators). The consumer needs **data-first**:
a JSON document compiled into a runnable workflow. pgflows already does a narrow
version of this — `SqlExporter.compose(name, steps: list[str])` builds a *linear*
chain of registered steps into pg_durable DSL — but it cannot express branching,
parallelism, or per-node inputs.

## Goal

Let consumers describe a workflow as a typed JSON document and have pgflows
**compile it to a pg_durable DSL graph** that Postgres runs via `df.start()`.
pgflows stays the SDK/compiler; pg_durable remains the orchestrator. The consumer's
hand-rolled interpreter collapses to: register steps, build a `GraphSpec`, call
`start_graph`.

## Decisions (locked during brainstorming)

1. **Target — compile to DSL.** JSON → pg_durable DSL → `df.start()`. Postgres
   orchestrates; pgflows is a thin compiler. No pull-mode interpreter is shipped.
2. **pg_durable is always required.** `start_graph` hard-checks
   `pg_durable_available` and raises an actionable error if `df` is absent. There is
   no fallback execution path.
3. **Maximize representable capability, bounded only by real pg_durable limits.**
   Emit the richest correct DSL; reject *only* the shapes pg_durable genuinely
   cannot run (verified live — see §Composition-limit guard), each with a fix-it
   message.
4. **Spec shape — a strong, consistent, extensible typed schema** (a discriminated
   union of nested blocks), with a published JSON Schema. Not a loose adjacency
   list: nested blocks map 1:1 to DSL operators, so every valid spec is compilable
   and there is no series-parallel reduction guesswork.

## Architecture

One responsibility per module (per repo principle #5):

| Module | Owns |
| --- | --- |
| `graph.py` (new) | The typed `GraphSpec` + node union. **Schema only, no logic.** Emits JSON Schema via `GraphSpec.model_json_schema()`. |
| `graph_compiler.py` (new) | `compile_graph(spec, *, step_queue, notify_channel) -> DslNode` + the limit-guard validation pass. Reuses `dsl.py` builders. |
| `app.py` | Thin wiring: `compile_graph`, `start_graph`, `graph_json_schema`. |

Cross-module imports flow downward: `app → graph_compiler → dsl`, and
`graph_compiler → graph`. No new circular dependencies.

## The schema (`graph.py`)

A recursive discriminated union on `type`. Adding a capability = add one node class
plus one compile case — that is the "easy to extend" property.

```
GraphSpec = { version: int, input: dict | None, root: Node }

Node (discriminator="type"):
  leaf nodes:
    step          { type:"step", step:str, input?:str, capture?:str }
    sleep         { type:"sleep", seconds:int }
    wait_signal   { type:"wait_signal", signal:str, timeout?:int }
    wait_schedule { type:"wait_schedule", cron:str }
  structural nodes:
    sequence      { type:"sequence", nodes:[Node, ...] }
    parallel      { type:"parallel", branches:[Node, ...], mode:"all"|"race" }
    branch        { type:"branch", condition:Condition, then:Node, else_?:Node }
    loop          { type:"loop", body:Node, while_?:Condition }

Condition = { step:str, input?:str }   # a registered step whose truthy output drives ?>
```

- `GraphSpec.input` seeds the single `{input}` durable var. Exactly one durable var
  is ever set — multi-var serialization nondeterminism (a verified pg_durable replay
  hazard) is structurally prevented; all other data flows via captures.
- All field names are `BaseModel` fields with explicit types — no `dict[str, Any]`
  at the boundary except `GraphSpec.input` (raw user payload, re-hydrated downstream
  by the step's own input model). `else_`/`while_` use trailing underscores
  (reserved words); JSON keys are aliased to `else`/`while`.

## Node → DSL mapping (`graph_compiler.py`)

| Node | DSL emitted |
| --- | --- |
| `step` | `worker_step(step, input_expr=…, capture=…)` bound to the app's step queue/channel |
| `sleep` | `sleep(seconds)` |
| `wait_signal` | `wait_for_signal(signal, timeout)` |
| `wait_schedule` | `wait_for_schedule(cron)` |
| `sequence` | `a >> b >> c` |
| `parallel` (2 / 3 / N) | `(a & b)` / `join3(a,b,c)` / left-folded `&` for N>3 |
| `parallel mode="race"` | `(a \| b)` — terminal only |
| `branch` | `if_node(cond, then, else_)` (or `cond.if_then(then)` when no else) |
| `loop` | `loop(body)` infinite, or `loop(body, while_cond)` |

### Data threading

- Each `step`/branch leaf auto-captures its output under a generated, collision-safe
  name (step name + position, matching the existing `_step_sqls` scheme).
- Within a `sequence`, step N's input defaults to the previous node's capture:
  `input_expr = "$<prev_cap>::jsonb"`. The first node of the whole graph receives the
  `{input}` durable var (`"'{input}'::jsonb"`).
- After a `parallel mode="all"`, a single auto-thread is ambiguous, so the next
  node's default input is an object built from the branch captures:
  `jsonb_build_object('<capA>', $capA, '<capB>', $capB)` (branch captures are visible
  after the join — verified). The author overrides any default with the optional
  `input` field, a jsonb SQL expression that may reference `$<capture>` names — the
  escape hatch for fan-in and custom merges.

## Composition-limit guard

A validation pass walks the spec tree **before** compiling and raises
`GraphCompileError` for shapes pg_durable cannot run reliably. These limits are
extension behavior verified live (recorded in the pgflows skill), **not** something
to "fix" in pgflows:

1. **Loop ⊕ parallel exclusion.** A `loop` subtree may not contain a `parallel`
   node, and a `loop` and a `parallel` may not be siblings in the same `sequence`
   (they cannot share one pg_durable instance — ContinueAsNew replay vs. parallel
   state deadlocks). Error: *"loop and parallel cannot share a pg_durable instance —
   model them as separate graphs / start calls."*
2. **Race is terminal.** `parallel mode="race"` must be the last node of its
   enclosing sequence; nothing may be sequenced after it (`race ~> next` hangs).
   Error otherwise. (The guard also notes that race does not cancel the loser, so
   both branches' side effects may fire.)
3. **One durable var.** Enforced by the schema (`input` only); no guard needed, but
   documented as the reason the schema has no `vars` map.
4. **Leaves are always `worker_step`s** (no bare-SQL node type is exposed), so the
   "join of trivial bare-SQL branches hangs" failure mode cannot be expressed.

`parallel` with N>3 branches emits a left-folded `&` chain. Only pairwise `&` and
`join3` are battle-tested live; N>3 is emitted but logged/flagged as
needs-live-verification rather than silently trusted.

## API surface (`app.py`)

```python
schema = app.graph_json_schema()              # dict — JSON Schema for UIs / validators
node   = app.compile_graph(spec)              # GraphSpec -> DslNode (runs the limit guard)
iid    = await app.start_graph(spec, label="my-flow")  # compile + pg_durable.start
                                              # raises RuntimeError if pg_durable absent
```

Top-level exports: `from pgflows import GraphSpec, compile_graph, GraphCompileError`
(and the node classes for programmatic construction).

`start_graph` is the one-call path: assert initialized → assert
`pg_durable_available` → `compile_graph` → `pg_durable.start(node, label=label)`.

## Retry / durability semantics (honest scope)

In compile (worker) mode, step durability is **pgmq redelivery**: `StepWorker`
`nack`s a failed step and pgmq redelivers it after the visibility timeout. The
`StepWorker` does **not** honor `RetryConfig` — that is a pull-mode (`WorkflowContext.step`)
concept. Therefore the schema deliberately has **no per-node `retry` field** (it would
silently lie). Configurable backoff in worker mode (attempt count + delay in
`StepWorker._handle_message`) is a separate future enhancement, called out below.

## Testing

- **Unit (offline, no DB):**
  - Schema: discriminator validation, rejects unknown `type`, alias round-trip for
    `else`/`while`, JSON Schema generation is non-empty and stable.
  - Compiler: per-node DSL string-match (mirrors `test_dsl.py` / `test_sql_exporter.py`),
    sequence threading, parallel auto-merge input, 2/3/N branch fan-out.
  - Guard: rejects loop+parallel coexistence; rejects non-terminal race; error
    messages name the offending node and the fix.
- **E2E (live `df` + `pgmq`, auto-skips when `df` absent):** compile and run via
  `start_graph` a sequence, a `worker_step` parallel-join, and a branch; assert the
  instance completes and the result matches. Reuses the existing `tests/e2e/docker`
  combined image.

## Out of scope (v1)

- Per-node `RetryConfig` honored by `StepWorker` (separate enhancement).
- Arbitrary non-series-parallel DAGs / adjacency-list input (the structured schema
  sidesteps this by construction).
- Loops sharing an instance with parallel nodes (rejected by the guard; model as
  separate `start_graph` calls).
- Multiple durable vars (one `input` only).
- A pull-mode interpreter fallback (pg_durable is always required).

## Consumer impact

`agentops` deletes `_ordered_steps()` + the `execute_workflow` dispatch body
(~170 lines), keeping only its registered step functions and its security policy
(the eval sandbox stays — pgflows must not own that). Workflow definitions become
`GraphSpec` JSON, validated against `app.graph_json_schema()`, started with
`await app.start_graph(spec, label=...)`.
