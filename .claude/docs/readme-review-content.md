# README Review — pyflows

Reviewed against: `src/pyflows/types.py`, `src/pyflows/backends/base.py`, `src/pyflows/exceptions.py`, `src/pyflows/__init__.py`, `pyproject.toml`
Date: 2026-06-07

---

## CRITICAL

### C1 — Quickstart `app` object is completely undefined

The Quickstart section uses `app.workflow()`, `app.step()`, `app.run()`, and `app.register_plugin()` throughout the README, but none of these exist in the codebase. There is no `PyflowsApp` (or equivalent) class anywhere — no `@app.workflow`, no `@app.step`, no `app.run()`. The entire decorator-based API is M2+ work that hasn't been implemented yet.

**Fix:** Replace the Quickstart example with code that only exercises what M1 actually provides (instantiating backends, calling `initialize()`), and add a prominent banner at the top of the Quickstart section like:

```
> **Status: M1 only.** The decorator API (`@app.workflow`, `@app.step`, `app.run()`) is not yet implemented. See the Roadmap.
```

### C2 — Plugin class `PyflowsPlugin` is not exported (or defined anywhere)

The Plugin system section shows `from pyflows import PyflowsPlugin` and a full `OpenTelemetryPlugin` subclass. `PyflowsPlugin` does not exist in `__init__.py`, `types.py`, `backends/base.py`, or anywhere else in the package. This import will raise `ImportError` if anyone tries it.

**Fix:** Either remove this section entirely until M6, or stub the ABC with a `# not yet implemented` comment and add it to `__all__`.

### C3 — Quickstart import of `PgDurableBackend` and `PgmqBackend` is importable but calling any method raises `NotImplementedError`

`from pyflows import PgDurableBackend, PgmqBackend, RetryConfig` will succeed (all three are exported), but every method body on `PgDurableBackend` and `PgmqBackend` is `raise NotImplementedError`. The Quickstart makes them look like working code.

**Fix:** Add a callout box in the Quickstart making clear these are scaffold stubs that will raise `NotImplementedError` until M2–M4 land. Alternatively, show a minimal "what you can do today" example limited to type/config construction only.

### C4 — `RetryConfig.backoff` is a plain `str`, not a `Literal`

The README documents `backoff="exponential"` or `"linear"` as if these are validated enum values. In `types.py` the field is typed `backoff: str = "exponential"` — any string is accepted with no validation. This creates a false impression of type safety.

**Fix:** Either change the type to `Literal["exponential", "linear"]` in `types.py` (the right fix), or note in the README that validation is not yet enforced.

---

## WARNING

### W1 — `pg_durable` library reference may be incorrect

The README links `pg_durable` to `https://github.com/microsoft/pg_durable`. A Microsoft-hosted Postgres extension for durable workflows is unusual and potentially incorrect. The actual dependency in `pyproject.toml` is only `pydantic`, `psycopg[binary]`, and `tembo-pgmq-python` — there is no `pg_durable` Python package listed as a dependency.

**Fix:** Verify the correct upstream project URL and whether there is a Python client library for it, or clarify that `pg_durable` is a Postgres extension (not a Python package) and document how it gets installed.

### W2 — `listen()` on `QueueBackend` is typed as returning `AsyncIterator[None]` but is not `async`

In `base.py`, `listen()` is declared as a plain `def` returning `AsyncIterator[None]`. This is inconsistent — callers expecting an `async def` will get a type error at runtime. All other backend methods are `async def`.

**Fix:** Make it `async def listen(...)` or, if it's intentionally a sync generator factory, document the calling convention clearly and update the README's architecture description accordingly.

### W3 — `pyproject.toml` description is placeholder text

`description = "Add your description here"` is still the uv scaffold default. This will appear verbatim on PyPI.

**Fix:** Set it to `"Durable workflow engine SDK for Python + Postgres"` (mirrors the README tagline).

### W4 — `pyproject.toml` `keywords` is empty

`keywords = []` means the package is unsearchable on PyPI.

**Fix:** Add relevant keywords, e.g. `["workflow", "durable", "postgres", "async", "pgmq"]`.

### W5 — Architecture table claims `PgCronBackend` is the scheduling default but it requires an optional Postgres extension

The architecture table lists `PgCronBackend` as the default scheduler, but `pg_cron` is listed as optional in the Requirements section. A "default" component should not depend on an optional extension.

**Fix:** Either change the wording in the architecture table to "optional / `PgCronBackend`" or clarify that scheduling is opt-in and the table shows available implementations, not defaults.

### W6 — Opening hero example (`remediate_incident`) uses `WorkflowContext` type annotation but that type does not exist

The top-of-README code snippet has `ctx: WorkflowContext`. `WorkflowContext` is not in `types.py`, not in `__init__.py`, and not exported from the package. It will cause a `NameError` if run.

**Fix:** Add a note that this snippet is illustrative (M2+), or replace the hero example with something that compiles today.

---

## INFO

### I1 — `StepConfig` is exported but never referenced in the README

`StepConfig` (which holds `name`, `retry`, `timeout_seconds`) is part of the public API (`__all__`) but goes unmentioned. It's likely the type that will power `@app.step()` once M2 ships.

**Fix:** Add a brief mention in the Retry configuration section explaining that step-level config is represented by `StepConfig`, so developers can discover it before the full decorator API is documented.

### I2 — Exception hierarchy is not documented

`PyflowsError`, `WorkflowNotFoundError`, `WorkflowAlreadyExistsError`, `StepExecutionError`, `BackendNotInitializedError`, `SchedulerJobNotFoundError` are all exported but not mentioned. Backend consumers need to know what to `except`.

**Fix:** Add a short "Error handling" section listing the exception hierarchy.

### I3 — Python 3.13+ requirement is unusually narrow and may deter early adopters

The README states "Python 3.13+" and `pyproject.toml` sets `requires-python = ">=3.13"`. The code uses `StrEnum` (3.11+) and `from __future__ import annotations` but nothing that strictly requires 3.13. This is a valid choice but worth being intentional about.

**Fix:** No change required if 3.13 is intentional — but consider documenting the rationale (e.g. "we target the latest stable release") so contributors don't try to back-port.

### I4 — No `pytest` configuration or test files are visible

The Development section says `uv run pytest`, but there are no test files in the project tree. Running `pytest` against a package with only `NotImplementedError` stubs will collect zero tests, which is misleading.

**Fix:** Either add a placeholder test file (e.g. `tests/test_types.py`) with a basic smoke test, or note in the Development section that tests are part of M2+.

### I5 — Roadmap M1 marked complete but `pg_durable` and `pgmq` backends are stubs

The roadmap marks `[x] M1 — Project scaffold: backends ABCs, types, exceptions`. The ABCs and types are complete. However, the concrete backends (`PgDurableBackend`, `PgmqBackend`, `PgCronBackend`) are all stub classes with every method raising `NotImplementedError`. If "scaffold" means "structure only, no implementation", M1 is fair — but this should be explicit in the roadmap item so users aren't misled.

**Fix:** Clarify the M1 description: `[x] M1 — Project scaffold: backend ABCs, types, exceptions, and stub concrete implementations`.
