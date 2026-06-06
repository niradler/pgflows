# pyflows — Repository Guidelines

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
| `worker.py` | Queue polling + dispatch loop |
| `registry.py` | Decorator registration + type extraction |
| `backends/` | All I/O (DB, queue, scheduler) |
| `telemetry.py` | OTel span management |
| `sql_exporter.py` | pg_durable DSL generation |

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
- Test file mirrors source: `src/pyflows/foo.py` → `tests/unit/test_foo.py`.

## Commands

```bash
uv sync                          # install deps
uv run pytest tests/unit/        # unit tests (no DB needed)
docker compose up -d             # start Postgres
uv run pytest tests/e2e/         # E2E tests
uv run ruff check src/ tests/    # lint
uv run ruff check --fix          # lint + autofix
```
