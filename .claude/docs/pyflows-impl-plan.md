# pyflows — Full Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a complete, open-source Python + Postgres durable workflow engine SDK with great DevEx, full pg_durable SQL export, OTel-first observability, and passing E2E tests.

**Architecture:** Python-driven workflow execution with replay-based checkpointing (Temporal-style). Our `pyflows.*` Postgres tables store state; pgmq queues work items; pg_durable SQL DSL is exported for push-mode and for migration between environments (dev→prod). OTel traces every workflow and step from the start.

**Tech Stack:** Python 3.13, psycopg 3 + psycopg-pool, asyncpg (via tembo-pgmq-python), pydantic v2, opentelemetry-sdk, pg_durable extension, pgmq extension, FastAPI (optional mount), Docker Compose for E2E.

---

## File Map

```
src/pyflows/
├── __init__.py                   UPDATE — add new exports
├── types.py                      DONE
├── exceptions.py                 DONE
├── schema.sql                    CREATE — pyflows.* DDL
├── config.py                     CREATE — PyflowsConfig (DSN, queue names, OTel)
├── registry.py                   CREATE — step/workflow function registry + decorators
├── context.py                    CREATE — WorkflowContext, StepContext
├── worker.py                     CREATE — async worker (pgmq poll + execution loop)
├── app.py                        CREATE — WorkflowApp (main entry point)
├── telemetry.py                  CREATE — OTel tracer + span helpers
├── sql_exporter.py               CREATE — pg_durable DSL generator + dry-run
├── fastapi.py                    CREATE — FastAPI router (management + push endpoint)
├── backends/
│   ├── base.py                   DONE (ABCs)
│   ├── pg_state.py               CREATE — PgStateBackend (our own tables via psycopg)
│   ├── pgmq.py                   IMPLEMENT — PgmqBackend (asyncpg via tembo)
│   ├── pg_durable.py             UPDATE — wire df.* SQL calls via psycopg
│   └── pg_cron.py                UPDATE — implement PgCronBackend
└── plugins/
    ├── __init__.py               CREATE — Plugin ABC + registry
    └── otel.py                   CREATE — built-in OTel plugin

tests/
├── conftest.py                   CREATE — DB fixtures, pytest-asyncio setup
├── unit/
│   ├── test_registry.py          CREATE
│   ├── test_context_replay.py    CREATE
│   └── test_sql_exporter.py      CREATE
└── e2e/
    ├── conftest.py               CREATE — Docker/Postgres lifecycle
    ├── test_workflow_basic.py    CREATE — start, run, status
    ├── test_workflow_retry.py    CREATE — step retry on failure
    ├── test_workflow_cancel.py   CREATE — cancel running workflow
    ├── test_workflow_monitor.py  CREATE — monitoring queries
    └── test_sql_export.py        CREATE — pg_durable SQL export round-trip

docker-compose.yml               CREATE — Postgres + pgmq + pg_durable
pyproject.toml                   UPDATE — add asyncpg, OTel, fastapi, pytest-asyncio
```

---

## Task 1: Dependencies + Docker Compose

**Files:**
- Modify: `pyproject.toml`
- Create: `docker-compose.yml`
- Create: `.env.test`

- [ ] **Step 1: Update pyproject.toml with all missing deps**

```toml
[project]
name = "pyflows"
version = "0.1.0"
description = "Durable workflow engine SDK for Python + Postgres"
readme = "README.md"
license = { text = "MIT" }
authors = [{ name = "Nir Adler", email = "me@niradler.com" }]
keywords = ["workflow", "durable", "postgres", "async"]
classifiers = [
    "Development Status :: 3 - Alpha",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: MIT License",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.13",
    "Typing :: Typed",
]
requires-python = ">=3.13"
dependencies = [
    "pydantic",
    "psycopg[binary]",
    "psycopg-pool",
    "asyncpg",
    "tembo-pgmq-python",
    "opentelemetry-api",
    "opentelemetry-sdk",
    "opentelemetry-exporter-otlp-proto-grpc",
]

[project.optional-dependencies]
fastapi = ["fastapi", "uvicorn[standard]"]

[project.urls]
Homepage = "https://github.com/niradler/pyflows"
Repository = "https://github.com/niradler/pyflows"
Issues = "https://github.com/niradler/pyflows/issues"

[build-system]
requires = ["uv_build>=0.10.0,<0.11.0"]
build-backend = "uv_build"

[dependency-groups]
dev = [
    "pytest>=8.0",
    "pytest-asyncio",
    "ruff>=0.9",
    "fastapi",
    "uvicorn[standard]",
    "httpx",
]

[tool.ruff]
line-length = 100

[tool.ruff.lint]
select = ["E", "F", "I", "UP"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
```

- [ ] **Step 2: Create docker-compose.yml**

```yaml
services:
  postgres:
    image: quay.io/tembo/pg17-pgmq:latest
    environment:
      POSTGRES_USER: pyflows
      POSTGRES_PASSWORD: pyflows
      POSTGRES_DB: pyflows_test
    ports:
      - "5433:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U pyflows -d pyflows_test"]
      interval: 5s
      timeout: 5s
      retries: 10

  # pg_durable requires a separate image or installation — skip for now, use our state backend
```

Note: The `quay.io/tembo/pg17-pgmq` image has pgmq built in. pg_durable needs a separate setup (see Task 10).

- [ ] **Step 3: Create .env.test**

```
PYFLOWS_TEST_DSN=postgresql://pyflows:pyflows@localhost:5433/pyflows_test
PYFLOWS_TEST_QUEUE=pyflows_test_q
```

- [ ] **Step 4: Run `uv sync` and verify**

```bash
uv sync
uv run python -c "import asyncpg, opentelemetry, psycopg; print('OK')"
```

Expected: `OK`

- [ ] **Step 5: Start Postgres and verify connectivity**

```bash
docker compose up -d --wait
uv run python -c "
import asyncio, asyncpg
async def check():
    conn = await asyncpg.connect('postgresql://pyflows:pyflows@localhost:5433/pyflows_test')
    row = await conn.fetchrow('SELECT version()')
    print(row['version'][:40])
    await conn.close()
asyncio.run(check())
"
```

Expected: prints Postgres version string.

---

## Task 2: Config + Schema

**Files:**
- Create: `src/pyflows/config.py`
- Create: `src/pyflows/schema.sql`

- [ ] **Step 1: Write config.py**

```python
from __future__ import annotations

from pydantic import BaseModel, Field


class PyflowsConfig(BaseModel):
    dsn: str
    workflow_queue: str = "pyflows_workflows"
    step_queue: str = "pyflows_steps"
    worker_concurrency: int = 10
    step_visibility_timeout_seconds: int = 300
    otel_enabled: bool = True
    otel_service_name: str = "pyflows"
```

- [ ] **Step 2: Write schema.sql**

```sql
CREATE SCHEMA IF NOT EXISTS pyflows;

CREATE TABLE IF NOT EXISTS pyflows.workflow_definitions (
    name        TEXT PRIMARY KEY,
    version     INT  NOT NULL DEFAULT 1,
    config      JSONB NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pyflows.workflow_instances (
    instance_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_name TEXT NOT NULL REFERENCES pyflows.workflow_definitions(name),
    state         TEXT NOT NULL DEFAULT 'pending',
    input         JSONB NOT NULL,
    output        JSONB,
    error         TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_instances_state
    ON pyflows.workflow_instances(state);

CREATE INDEX IF NOT EXISTS idx_instances_workflow_name
    ON pyflows.workflow_instances(workflow_name);

CREATE TABLE IF NOT EXISTS pyflows.step_results (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    instance_id  UUID NOT NULL
        REFERENCES pyflows.workflow_instances(instance_id) ON DELETE CASCADE,
    step_name    TEXT NOT NULL,
    step_index   INT  NOT NULL,
    state        TEXT NOT NULL DEFAULT 'pending',
    input        JSONB NOT NULL,
    output       JSONB,
    error        TEXT,
    attempt      INT  NOT NULL DEFAULT 1,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    UNIQUE(instance_id, step_name, step_index)
);
```

- [ ] **Step 3: Write a test that applies the schema**

In `tests/conftest.py`:

```python
import asyncio
import os
import pytest
import asyncpg

TEST_DSN = os.getenv("PYFLOWS_TEST_DSN", "postgresql://pyflows:pyflows@localhost:5433/pyflows_test")

@pytest.fixture(scope="session")
def event_loop_policy():
    return asyncio.DefaultEventLoopPolicy()

@pytest.fixture(scope="session")
async def db_pool():
    pool = await asyncpg.create_pool(TEST_DSN, min_size=2, max_size=10)
    yield pool
    await pool.close()

@pytest.fixture(scope="session")
async def schema(db_pool):
    schema_path = __file__
    import pathlib
    sql = (pathlib.Path(__file__).parent.parent / "src" / "pyflows" / "schema.sql").read_text()
    async with db_pool.acquire() as conn:
        await conn.execute(sql)
    return db_pool
```

- [ ] **Step 4: Run test to verify schema applies**

```bash
uv run pytest tests/conftest.py -v --no-header -q
```

Expected: session fixtures collected with no errors.

---

## Task 3: PgStateBackend (State Store)

**Files:**
- Create: `src/pyflows/backends/pg_state.py`
- Create: `tests/unit/test_pg_state.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_pg_state.py
import pytest
from pyflows.backends.pg_state import PgStateBackend
from pyflows.types import WorkflowState

@pytest.mark.asyncio
async def test_register_and_get_definition(schema):
    backend = PgStateBackend(dsn=schema._dsn)
    await backend.initialize()
    try:
        await backend.register_workflow("test_wf", config={})
        defn = await backend.get_workflow_definition("test_wf")
        assert defn["name"] == "test_wf"
    finally:
        await backend.close()

@pytest.mark.asyncio
async def test_create_and_get_instance(schema):
    backend = PgStateBackend(dsn=schema._dsn)
    await backend.initialize()
    try:
        await backend.register_workflow("test_wf2", config={})
        instance_id = await backend.create_instance("test_wf2", {"key": "val"})
        assert instance_id is not None
        status = await backend.get_instance(instance_id)
        assert status.state == WorkflowState.PENDING
        assert status.workflow_id == instance_id
    finally:
        await backend.close()
```

- [ ] **Step 2: Run test to confirm it fails**

```bash
uv run pytest tests/unit/test_pg_state.py -v
```

Expected: `ImportError` — module not found.

- [ ] **Step 3: Implement PgStateBackend**

```python
# src/pyflows/backends/pg_state.py
from __future__ import annotations

import json
from typing import Any
from datetime import datetime, timezone

import psycopg
import psycopg_pool

from pyflows.backends.base import OrchestratorBackend
from pyflows.exceptions import BackendNotInitializedError, WorkflowNotFoundError
from pyflows.types import WorkflowState, WorkflowStatus


class PgStateBackend(OrchestratorBackend):
    """Own-table state backend — stores workflow/step state in pyflows.* schema."""

    def __init__(self, dsn: str, min_pool: int = 2, max_pool: int = 10) -> None:
        self._dsn = dsn
        self._min_pool = min_pool
        self._max_pool = max_pool
        self._pool: psycopg_pool.AsyncConnectionPool | None = None

    async def initialize(self) -> None:
        self._pool = psycopg_pool.AsyncConnectionPool(
            self._dsn,
            min_size=self._min_pool,
            max_size=self._max_pool,
            open=False,
        )
        await self._pool.open()

    async def register_workflow(self, name: str, config: dict[str, Any]) -> None:
        async with self._get_conn() as conn:
            await conn.execute(
                """
                INSERT INTO pyflows.workflow_definitions (name, config)
                VALUES (%s, %s)
                ON CONFLICT (name) DO UPDATE SET config = EXCLUDED.config, updated_at = NOW()
                """,
                (name, json.dumps(config)),
            )

    async def get_workflow_definition(self, name: str) -> dict[str, Any]:
        async with self._get_conn() as conn:
            row = await conn.fetchone(
                "SELECT name, version, config FROM pyflows.workflow_definitions WHERE name = %s",
                (name,),
            )
        if row is None:
            raise WorkflowNotFoundError(name)
        return {"name": row[0], "version": row[1], "config": row[2]}

    async def create_instance(
        self,
        workflow_name: str,
        input_data: dict[str, Any],
    ) -> str:
        async with self._get_conn() as conn:
            row = await conn.fetchone(
                """
                INSERT INTO pyflows.workflow_instances (workflow_name, input)
                VALUES (%s, %s)
                RETURNING instance_id::text
                """,
                (workflow_name, json.dumps(input_data)),
            )
        return row[0]

    async def get_instance(self, instance_id: str) -> WorkflowStatus:
        async with self._get_conn() as conn:
            row = await conn.fetchone(
                """
                SELECT instance_id, workflow_name, state, output, error, created_at, updated_at
                FROM pyflows.workflow_instances
                WHERE instance_id = %s::uuid
                """,
                (instance_id,),
            )
        if row is None:
            raise WorkflowNotFoundError(instance_id)
        return WorkflowStatus(
            workflow_id=str(row[0]),
            name=row[1],
            state=WorkflowState(row[2]),
            created_at=row[5],
            updated_at=row[6],
            output=row[3],
            error=row[4],
        )

    async def update_instance_state(
        self,
        instance_id: str,
        state: WorkflowState,
        output: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        async with self._get_conn() as conn:
            await conn.execute(
                """
                UPDATE pyflows.workflow_instances
                SET state = %s, output = %s, error = %s, updated_at = NOW()
                WHERE instance_id = %s::uuid
                """,
                (state.value, json.dumps(output) if output else None, error, instance_id),
            )

    async def get_step_result(
        self,
        instance_id: str,
        step_name: str,
        step_index: int,
    ) -> dict[str, Any] | None:
        async with self._get_conn() as conn:
            row = await conn.fetchone(
                """
                SELECT output FROM pyflows.step_results
                WHERE instance_id = %s::uuid AND step_name = %s AND step_index = %s
                AND state = 'completed'
                """,
                (instance_id, step_name, step_index),
            )
        return row[0] if row else None

    async def save_step_result(
        self,
        instance_id: str,
        step_name: str,
        step_index: int,
        input_data: dict[str, Any],
        output: dict[str, Any],
    ) -> None:
        async with self._get_conn() as conn:
            await conn.execute(
                """
                INSERT INTO pyflows.step_results
                    (instance_id, step_name, step_index, state, input, output, completed_at)
                VALUES (%s::uuid, %s, %s, 'completed', %s, %s, NOW())
                ON CONFLICT (instance_id, step_name, step_index)
                DO UPDATE SET state = 'completed', output = EXCLUDED.output, completed_at = NOW()
                """,
                (instance_id, step_name, step_index, json.dumps(input_data), json.dumps(output)),
            )

    async def save_step_error(
        self,
        instance_id: str,
        step_name: str,
        step_index: int,
        input_data: dict[str, Any],
        error: str,
        attempt: int,
    ) -> None:
        async with self._get_conn() as conn:
            await conn.execute(
                """
                INSERT INTO pyflows.step_results
                    (instance_id, step_name, step_index, state, input, error, attempt)
                VALUES (%s::uuid, %s, %s, 'failed', %s, %s, %s)
                ON CONFLICT (instance_id, step_name, step_index)
                DO UPDATE SET state = 'failed', error = EXCLUDED.error, attempt = EXCLUDED.attempt
                """,
                (instance_id, step_name, step_index, json.dumps(input_data), error, attempt),
            )

    async def list_instances(
        self,
        workflow_name: str | None = None,
        state: WorkflowState | None = None,
        limit: int = 100,
    ) -> list[WorkflowStatus]:
        filters: list[str] = []
        params: list[Any] = []
        if workflow_name:
            filters.append("workflow_name = %s")
            params.append(workflow_name)
        if state:
            filters.append("state = %s")
            params.append(state.value)
        where = ("WHERE " + " AND ".join(filters)) if filters else ""
        params.append(limit)
        async with self._get_conn() as conn:
            rows = await conn.fetchall(
                f"""
                SELECT instance_id, workflow_name, state, output, error, created_at, updated_at
                FROM pyflows.workflow_instances
                {where}
                ORDER BY created_at DESC
                LIMIT %s
                """,
                params,
            )
        return [
            WorkflowStatus(
                workflow_id=str(r[0]),
                name=r[1],
                state=WorkflowState(r[2]),
                created_at=r[5],
                updated_at=r[6],
                output=r[3],
                error=r[4],
            )
            for r in rows
        ]

    # --- OrchestratorBackend ABC required methods ---

    async def start_workflow(self, workflow_id: str, name: str, payload: dict[str, Any]) -> str:
        return await self.create_instance(name, payload)

    async def signal_workflow(self, workflow_id: str, signal: str, data: dict[str, Any] | None = None) -> None:
        pass  # signals not used in state-backend mode

    async def get_workflow_status(self, workflow_id: str) -> WorkflowStatus:
        return await self.get_instance(workflow_id)

    async def cancel_workflow(self, workflow_id: str) -> None:
        await self.update_instance_state(workflow_id, WorkflowState.CANCELLED)

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    def _get_conn(self):
        if self._pool is None:
            raise BackendNotInitializedError("PgStateBackend")
        return self._pool.connection()
```

- [ ] **Step 4: Add `fetchone` / `fetchall` helpers** 

Note: psycopg uses `.fetchone()` on cursors, not on connections. Wrap properly:

```python
# In pg_state.py, replace _get_conn() usage with a helper:

    async def _execute(self, query: str, params: tuple = ()) -> None:
        async with self._pool.connection() as conn:
            await conn.execute(query, params)

    async def _fetchone(self, query: str, params: tuple = ()) -> tuple | None:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, params)
                return await cur.fetchone()

    async def _fetchall(self, query: str, params: tuple = ()) -> list[tuple]:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, params)
                return await cur.fetchall()
```

Replace all `_get_conn()` patterns with `_execute` / `_fetchone` / `_fetchall` in the methods above.

- [ ] **Step 5: Run tests**

```bash
PYFLOWS_TEST_DSN=postgresql://pyflows:pyflows@localhost:5433/pyflows_test \
  uv run pytest tests/unit/test_pg_state.py -v
```

Expected: 2 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/pyflows/backends/pg_state.py src/pyflows/config.py src/pyflows/schema.sql tests/conftest.py tests/unit/test_pg_state.py pyproject.toml docker-compose.yml .env.test
git commit -m "feat: task 1-3: config, schema, PgStateBackend"
```

---

## Task 4: PgmqBackend (Queue)

**Files:**
- Implement: `src/pyflows/backends/pgmq.py`
- Create: `tests/unit/test_pgmq.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_pgmq.py
import pytest
from pyflows.backends.pgmq import PgmqBackend
from pyflows.types import QueueMessage

@pytest.mark.asyncio
async def test_send_and_receive(schema):
    import os
    host, port, db = "localhost", "5433", "pyflows_test"
    backend = PgmqBackend(host=host, port=port, database=db,
                          username="pyflows", password="pyflows")
    await backend.initialize()
    try:
        queue = "test_queue_unit"
        msg_id = await backend.enqueue(queue, {"action": "test", "data": 42})
        assert isinstance(msg_id, str)
        msgs = await backend.dequeue(queue, batch_size=1)
        assert len(msgs) == 1
        assert msgs[0].payload["action"] == "test"
        await backend.ack(queue, msgs[0].message_id)
    finally:
        await backend.close()
```

- [ ] **Step 2: Run test to confirm it fails**

```bash
PYFLOWS_TEST_DSN=postgresql://pyflows:pyflows@localhost:5433/pyflows_test \
  uv run pytest tests/unit/test_pgmq.py -v
```

Expected: `NotImplementedError`.

- [ ] **Step 3: Implement PgmqBackend**

```python
# src/pyflows/backends/pgmq.py
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Coroutine
from typing import Any

from tembo_pgmq_python.async_queue import PGMQueue

from pyflows.backends.base import QueueBackend
from pyflows.exceptions import BackendNotInitializedError
from pyflows.types import QueueMessage


class PgmqBackend(QueueBackend):
    """pgmq-backed step queue using tembo async client."""

    def __init__(
        self,
        host: str = "localhost",
        port: str = "5432",
        database: str = "postgres",
        username: str = "postgres",
        password: str = "postgres",
        visibility_timeout_seconds: int = 30,
        pool_size: int = 10,
    ) -> None:
        self._host = host
        self._port = port
        self._database = database
        self._username = username
        self._password = password
        self._vt = visibility_timeout_seconds
        self._pool_size = pool_size
        self._client: PGMQueue | None = None
        self._known_queues: set[str] = set()

    async def initialize(self) -> None:
        self._client = PGMQueue(
            host=self._host,
            port=self._port,
            database=self._database,
            username=self._username,
            password=self._password,
            vt=self._vt,
            pool_size=self._pool_size,
        )
        await self._client.init()

    async def _ensure_queue(self, queue: str) -> None:
        if queue not in self._known_queues:
            await self._client.create_queue(queue)
            self._known_queues.add(queue)

    async def enqueue(self, queue: str, message: dict[str, Any], delay_seconds: int = 0) -> str:
        self._assert_initialized()
        await self._ensure_queue(queue)
        msg_id = await self._client.send(queue, message, delay=delay_seconds)
        return str(msg_id)

    async def dequeue(self, queue: str, batch_size: int = 1) -> list[QueueMessage]:
        self._assert_initialized()
        await self._ensure_queue(queue)
        msgs = await self._client.read_batch(queue, vt=self._vt, batch_size=batch_size)
        if not msgs:
            return []
        return [
            QueueMessage(
                message_id=str(m.msg_id),
                queue=queue,
                payload=m.message,
                enqueued_at=m.enqueued_at,
                read_count=m.read_ct,
            )
            for m in msgs
        ]

    async def ack(self, queue: str, message_id: str) -> None:
        self._assert_initialized()
        await self._client.delete(queue, int(message_id))

    async def nack(self, queue: str, message_id: str) -> None:
        self._assert_initialized()
        await self._client.set_vt(queue, int(message_id), 0)

    async def listen(
        self,
        queue: str,
        callback: Callable[[QueueMessage], Coroutine[Any, Any, None]],
    ) -> AsyncIterator[None]:
        self._assert_initialized()
        await self._ensure_queue(queue)
        while True:
            msgs = await self._client.read_batch(
                queue, vt=self._vt, batch_size=10
            )
            if msgs:
                for m in msgs:
                    qm = QueueMessage(
                        message_id=str(m.msg_id),
                        queue=queue,
                        payload=m.message,
                        enqueued_at=m.enqueued_at,
                        read_count=m.read_ct,
                    )
                    await callback(qm)
            else:
                await asyncio.sleep(0.1)
            yield

    async def close(self) -> None:
        if self._client is not None:
            await self._client.pool.close()
            self._client = None

    def _assert_initialized(self) -> None:
        if self._client is None:
            raise BackendNotInitializedError("PgmqBackend")
```

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/unit/test_pgmq.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/pyflows/backends/pgmq.py tests/unit/test_pgmq.py
git commit -m "feat: implement PgmqBackend"
```

---

## Task 5: OTel Telemetry

**Files:**
- Create: `src/pyflows/telemetry.py`
- Create: `tests/unit/test_telemetry.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_telemetry.py
from pyflows.telemetry import PyflowsTelemetry
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

def test_create_workflow_span():
    exporter = InMemorySpanExporter()
    telemetry = PyflowsTelemetry.with_in_memory_exporter(exporter)
    with telemetry.workflow_span("my_workflow", "inst-001") as span:
        span.set_attribute("pyflows.test", True)
    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "pyflows.workflow.my_workflow"
    assert spans[0].attributes.get("pyflows.workflow.id") == "inst-001"

def test_create_step_span():
    exporter = InMemorySpanExporter()
    telemetry = PyflowsTelemetry.with_in_memory_exporter(exporter)
    with telemetry.step_span("inst-001", "check_service", 0) as span:
        pass
    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "pyflows.step.check_service"
```

- [ ] **Step 2: Run test to confirm failure**

```bash
uv run pytest tests/unit/test_telemetry.py -v
```

Expected: `ImportError`.

- [ ] **Step 3: Implement telemetry.py**

```python
# src/pyflows/telemetry.py
from __future__ import annotations

from contextlib import contextmanager
from typing import Generator

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import Span, StatusCode


class PyflowsTelemetry:
    TRACER_NAME = "pyflows"

    def __init__(self, tracer: trace.Tracer) -> None:
        self._tracer = tracer

    @classmethod
    def with_provider(cls, provider: TracerProvider) -> PyflowsTelemetry:
        return cls(provider.get_tracer(cls.TRACER_NAME))

    @classmethod
    def with_in_memory_exporter(cls, exporter: InMemorySpanExporter) -> PyflowsTelemetry:
        provider = TracerProvider()
        provider.add_span_processor(BatchSpanProcessor(exporter))
        return cls(provider.get_tracer(cls.TRACER_NAME))

    @classmethod
    def from_env(cls, service_name: str = "pyflows") -> PyflowsTelemetry:
        provider = TracerProvider()
        return cls(provider.get_tracer(cls.TRACER_NAME))

    @classmethod
    def noop(cls) -> PyflowsTelemetry:
        return cls(trace.get_tracer(cls.TRACER_NAME))

    @contextmanager
    def workflow_span(self, workflow_name: str, instance_id: str) -> Generator[Span, None, None]:
        with self._tracer.start_as_current_span(
            f"pyflows.workflow.{workflow_name}"
        ) as span:
            span.set_attribute("pyflows.workflow.name", workflow_name)
            span.set_attribute("pyflows.workflow.id", instance_id)
            try:
                yield span
            except Exception as exc:
                span.set_status(StatusCode.ERROR, str(exc))
                raise

    @contextmanager
    def step_span(
        self, instance_id: str, step_name: str, step_index: int
    ) -> Generator[Span, None, None]:
        with self._tracer.start_as_current_span(
            f"pyflows.step.{step_name}"
        ) as span:
            span.set_attribute("pyflows.workflow.id", instance_id)
            span.set_attribute("pyflows.step.name", step_name)
            span.set_attribute("pyflows.step.index", step_index)
            try:
                yield span
            except Exception as exc:
                span.set_status(StatusCode.ERROR, str(exc))
                raise
```

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/unit/test_telemetry.py -v
```

Expected: 2 PASS.

- [ ] **Step 5: Commit**

```bash
git add src/pyflows/telemetry.py tests/unit/test_telemetry.py
git commit -m "feat: OTel telemetry — workflow and step spans"
```

---

## Task 6: Registry + Decorators

**Files:**
- Create: `src/pyflows/registry.py`
- Create: `tests/unit/test_registry.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_registry.py
import pytest
from pydantic import BaseModel
from pyflows.registry import WorkflowRegistry, StepDefinition, WorkflowDefinition

class MyInput(BaseModel):
    name: str

class MyOutput(BaseModel):
    message: str

async def my_step(ctx, input: MyInput) -> MyOutput:
    return MyOutput(message=f"hello {input.name}")

async def my_workflow(ctx, input: MyInput) -> MyOutput:
    return MyOutput(message="done")

def test_register_step():
    reg = WorkflowRegistry()
    defn = reg.register_step(my_step, name="my_step")
    assert defn.name == "my_step"
    assert defn.input_type is MyInput
    assert defn.output_type is MyOutput

def test_register_workflow():
    reg = WorkflowRegistry()
    defn = reg.register_workflow(my_workflow, name="my_workflow")
    assert defn.name == "my_workflow"
    assert reg.get_workflow("my_workflow") is defn

def test_get_step_missing():
    reg = WorkflowRegistry()
    with pytest.raises(KeyError):
        reg.get_step("nonexistent")
```

- [ ] **Step 2: Run test to confirm failure**

```bash
uv run pytest tests/unit/test_registry.py -v
```

Expected: `ImportError`.

- [ ] **Step 3: Implement registry.py**

```python
# src/pyflows/registry.py
from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, get_type_hints

from pyflows.types import RetryConfig


@dataclass
class StepDefinition:
    name: str
    fn: Callable
    input_type: type
    output_type: type
    retry: RetryConfig = field(default_factory=RetryConfig)
    timeout_seconds: float | None = None


@dataclass
class WorkflowDefinition:
    name: str
    fn: Callable
    input_type: type
    output_type: type
    step_defaults: RetryConfig = field(default_factory=RetryConfig)


class WorkflowRegistry:
    def __init__(self) -> None:
        self._workflows: dict[str, WorkflowDefinition] = {}
        self._steps: dict[str, StepDefinition] = {}

    def register_step(
        self,
        fn: Callable,
        name: str | None = None,
        retry: RetryConfig | None = None,
        timeout_seconds: float | None = None,
    ) -> StepDefinition:
        step_name = name or fn.__name__
        hints = get_type_hints(fn)
        params = list(inspect.signature(fn).parameters.values())
        input_type = hints.get(params[1].name) if len(params) > 1 else dict
        output_type = hints.get("return", dict)
        defn = StepDefinition(
            name=step_name,
            fn=fn,
            input_type=input_type,
            output_type=output_type,
            retry=retry or RetryConfig(),
            timeout_seconds=timeout_seconds,
        )
        self._steps[step_name] = defn
        return defn

    def register_workflow(
        self,
        fn: Callable,
        name: str | None = None,
        step_defaults: RetryConfig | None = None,
    ) -> WorkflowDefinition:
        wf_name = name or fn.__name__
        hints = get_type_hints(fn)
        params = list(inspect.signature(fn).parameters.values())
        input_type = hints.get(params[1].name) if len(params) > 1 else dict
        output_type = hints.get("return", dict)
        defn = WorkflowDefinition(
            name=wf_name,
            fn=fn,
            input_type=input_type,
            output_type=output_type,
            step_defaults=step_defaults or RetryConfig(),
        )
        self._workflows[wf_name] = defn
        return defn

    def get_step(self, name: str) -> StepDefinition:
        if name not in self._steps:
            raise KeyError(f"Step '{name}' not registered")
        return self._steps[name]

    def get_workflow(self, name: str) -> WorkflowDefinition:
        if name not in self._workflows:
            raise KeyError(f"Workflow '{name}' not registered")
        return self._workflows[name]

    def list_workflows(self) -> list[str]:
        return list(self._workflows.keys())

    def list_steps(self) -> list[str]:
        return list(self._steps.keys())
```

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/unit/test_registry.py -v
```

Expected: 3 PASS.

- [ ] **Step 5: Commit**

```bash
git add src/pyflows/registry.py tests/unit/test_registry.py
git commit -m "feat: WorkflowRegistry with step and workflow definitions"
```

---

## Task 7: WorkflowContext (Replay-Based Execution)

**Files:**
- Create: `src/pyflows/context.py`
- Create: `tests/unit/test_context_replay.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_context_replay.py
import pytest
from unittest.mock import AsyncMock, MagicMock
from pydantic import BaseModel
from pyflows.context import WorkflowContext
from pyflows.telemetry import PyflowsTelemetry
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

class NumberInput(BaseModel):
    value: int

class NumberOutput(BaseModel):
    result: int

async def double_step(ctx, input: NumberInput) -> NumberOutput:
    return NumberOutput(result=input.value * 2)

@pytest.mark.asyncio
async def test_step_executes_and_caches():
    state = AsyncMock()
    state.get_step_result.return_value = None
    telemetry = PyflowsTelemetry.noop()
    ctx = WorkflowContext(
        instance_id="test-001",
        workflow_name="test_wf",
        state_backend=state,
        telemetry=telemetry,
    )
    result = await ctx.step(double_step, NumberInput(value=5))
    assert result.result == 10
    state.save_step_result.assert_called_once()

@pytest.mark.asyncio
async def test_step_replays_from_cache():
    state = AsyncMock()
    state.get_step_result.return_value = {"result": 99}
    telemetry = PyflowsTelemetry.noop()
    ctx = WorkflowContext(
        instance_id="test-002",
        workflow_name="test_wf",
        state_backend=state,
        telemetry=telemetry,
    )
    result = await ctx.step(double_step, NumberInput(value=5))
    assert result.result == 99  # cached result used
    state.save_step_result.assert_not_called()
```

- [ ] **Step 2: Run test to confirm failure**

```bash
uv run pytest tests/unit/test_context_replay.py -v
```

Expected: `ImportError`.

- [ ] **Step 3: Implement context.py**

```python
# src/pyflows/context.py
from __future__ import annotations

import asyncio
import traceback
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TypeVar

from pydantic import BaseModel

from pyflows.exceptions import StepExecutionError
from pyflows.telemetry import PyflowsTelemetry
from pyflows.types import RetryConfig

if TYPE_CHECKING:
    from pyflows.backends.pg_state import PgStateBackend

T = TypeVar("T", bound=BaseModel)


class WorkflowContext:
    """Passed to workflow functions. Drives step execution with checkpoint replay."""

    def __init__(
        self,
        instance_id: str,
        workflow_name: str,
        state_backend: PgStateBackend,
        telemetry: PyflowsTelemetry,
        step_defaults: RetryConfig | None = None,
    ) -> None:
        self.instance_id = instance_id
        self.workflow_name = workflow_name
        self._state = state_backend
        self._telemetry = telemetry
        self._step_defaults = step_defaults or RetryConfig()
        self._step_counter: dict[str, int] = {}

    async def step(
        self,
        fn: Callable,
        input_model: BaseModel,
        *,
        name: str | None = None,
        retry: RetryConfig | None = None,
    ) -> Any:
        step_name = name or fn.__name__
        step_index = self._step_counter.get(step_name, 0)
        self._step_counter[step_name] = step_index + 1

        cached = await self._state.get_step_result(self.instance_id, step_name, step_index)
        if cached is not None:
            hints = fn.__annotations__
            return_type = hints.get("return")
            if return_type and issubclass(return_type, BaseModel):
                return return_type.model_validate(cached)
            return cached

        retry_cfg = retry or self._step_defaults
        last_error: Exception | None = None

        with self._telemetry.step_span(self.instance_id, step_name, step_index):
            for attempt in range(1, retry_cfg.max_retries + 2):
                try:
                    result = await fn(StepContext(self.instance_id, step_name), input_model)
                    output = result.model_dump() if isinstance(result, BaseModel) else result
                    await self._state.save_step_result(
                        self.instance_id, step_name, step_index,
                        input_model.model_dump(), output,
                    )
                    return result
                except Exception as exc:
                    last_error = exc
                    await self._state.save_step_error(
                        self.instance_id, step_name, step_index,
                        input_model.model_dump(), traceback.format_exc(), attempt,
                    )
                    if attempt <= retry_cfg.max_retries:
                        delay = min(
                            retry_cfg.initial_delay_seconds * (2 ** (attempt - 1)),
                            retry_cfg.max_delay_seconds,
                        )
                        await asyncio.sleep(delay)

        raise StepExecutionError(step_name, last_error)


class StepContext:
    """Passed to step functions — provides workflow context without step primitives."""

    def __init__(self, workflow_id: str, step_name: str) -> None:
        self.workflow_id = workflow_id
        self.step_name = step_name
```

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/unit/test_context_replay.py -v
```

Expected: 2 PASS.

- [ ] **Step 5: Commit**

```bash
git add src/pyflows/context.py tests/unit/test_context_replay.py
git commit -m "feat: WorkflowContext with replay-based step execution"
```

---

## Task 8: Worker

**Files:**
- Create: `src/pyflows/worker.py`
- Create: `tests/unit/test_worker.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_worker.py
import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock
from pyflows.worker import WorkflowWorker
from pyflows.types import WorkflowState, QueueMessage
from pyflows.registry import WorkflowRegistry, WorkflowDefinition
from pyflows.telemetry import PyflowsTelemetry
from pydantic import BaseModel
from datetime import datetime, timezone

class WInput(BaseModel):
    x: int

class WOutput(BaseModel):
    y: int

async def simple_workflow(ctx, input: WInput) -> WOutput:
    return WOutput(y=input.x + 1)

@pytest.mark.asyncio
async def test_worker_processes_task():
    registry = WorkflowRegistry()
    registry.register_workflow(simple_workflow, name="simple_workflow")

    state = AsyncMock()
    state.get_step_result.return_value = None
    state.save_step_result = AsyncMock()
    state.update_instance_state = AsyncMock()

    queue = AsyncMock()
    queue.dequeue.return_value = [
        QueueMessage(
            message_id="1",
            queue="pyflows_workflows",
            payload={
                "workflow_name": "simple_workflow",
                "instance_id": "inst-001",
                "input": {"x": 5},
            },
            enqueued_at=datetime.now(timezone.utc),
        )
    ]
    queue.ack = AsyncMock()

    worker = WorkflowWorker(
        registry=registry,
        state_backend=state,
        queue_backend=queue,
        telemetry=PyflowsTelemetry.noop(),
        queue_name="pyflows_workflows",
    )
    await worker.process_batch()
    state.update_instance_state.assert_called()
    call_args = state.update_instance_state.call_args_list[-1]
    assert call_args[0][1] == WorkflowState.COMPLETED
```

- [ ] **Step 2: Run to confirm failure**

```bash
uv run pytest tests/unit/test_worker.py -v
```

- [ ] **Step 3: Implement worker.py**

```python
# src/pyflows/worker.py
from __future__ import annotations

import asyncio
import traceback
from typing import TYPE_CHECKING

from pyflows.context import WorkflowContext
from pyflows.telemetry import PyflowsTelemetry
from pyflows.types import WorkflowState

if TYPE_CHECKING:
    from pyflows.backends.base import QueueBackend
    from pyflows.backends.pg_state import PgStateBackend
    from pyflows.registry import WorkflowRegistry


class WorkflowWorker:
    """Pulls workflow tasks from pgmq and executes them with checkpoint replay."""

    def __init__(
        self,
        registry: WorkflowRegistry,
        state_backend: PgStateBackend,
        queue_backend: QueueBackend,
        telemetry: PyflowsTelemetry,
        queue_name: str = "pyflows_workflows",
        batch_size: int = 5,
    ) -> None:
        self._registry = registry
        self._state = state_backend
        self._queue = queue_backend
        self._telemetry = telemetry
        self._queue_name = queue_name
        self._batch_size = batch_size
        self._running = False

    async def process_batch(self) -> int:
        """Dequeue and execute up to batch_size workflow tasks. Returns count processed."""
        msgs = await self._queue.dequeue(self._queue_name, batch_size=self._batch_size)
        if not msgs:
            return 0
        tasks = [self._handle_message(m) for m in msgs]
        await asyncio.gather(*tasks, return_exceptions=True)
        return len(msgs)

    async def run(self) -> None:
        """Run the worker loop indefinitely (graceful stop via shutdown())."""
        self._running = True
        while self._running:
            processed = await self.process_batch()
            if processed == 0:
                await asyncio.sleep(0.1)

    def shutdown(self) -> None:
        self._running = False

    async def _handle_message(self, msg) -> None:
        payload = msg.payload
        workflow_name = payload["workflow_name"]
        instance_id = payload["instance_id"]
        raw_input = payload["input"]

        try:
            defn = self._registry.get_workflow(workflow_name)
        except KeyError:
            await self._queue.ack(self._queue_name, msg.message_id)
            return

        input_model = defn.input_type.model_validate(raw_input)

        with self._telemetry.workflow_span(workflow_name, instance_id):
            await self._state.update_instance_state(instance_id, WorkflowState.RUNNING)
            try:
                ctx = WorkflowContext(
                    instance_id=instance_id,
                    workflow_name=workflow_name,
                    state_backend=self._state,
                    telemetry=self._telemetry,
                    step_defaults=defn.step_defaults,
                )
                result = await defn.fn(ctx, input_model)
                output = result.model_dump() if hasattr(result, "model_dump") else result
                await self._state.update_instance_state(
                    instance_id, WorkflowState.COMPLETED, output=output
                )
                await self._queue.ack(self._queue_name, msg.message_id)
            except Exception:
                error = traceback.format_exc()
                await self._state.update_instance_state(
                    instance_id, WorkflowState.FAILED, error=error
                )
                await self._queue.nack(self._queue_name, msg.message_id)
```

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/unit/test_worker.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/pyflows/worker.py tests/unit/test_worker.py
git commit -m "feat: WorkflowWorker — pgmq polling and workflow execution"
```

---

## Task 9: WorkflowApp (Main Entry Point)

**Files:**
- Create: `src/pyflows/app.py`
- Create: `tests/unit/test_app.py`
- Update: `src/pyflows/__init__.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_app.py
import pytest
from unittest.mock import AsyncMock, patch
from pydantic import BaseModel
from pyflows.app import WorkflowApp
from pyflows.config import PyflowsConfig
from pyflows.types import WorkflowState

class GreetInput(BaseModel):
    name: str

class GreetOutput(BaseModel):
    message: str

@pytest.mark.asyncio
async def test_app_registers_workflow():
    config = PyflowsConfig(dsn="postgresql://x:x@localhost/x")
    app = WorkflowApp(config=config)

    @app.workflow()
    async def greet(ctx, input: GreetInput) -> GreetOutput:
        return GreetOutput(message=f"hi {input.name}")

    assert "greet" in app.registry.list_workflows()

@pytest.mark.asyncio
async def test_app_registers_step():
    config = PyflowsConfig(dsn="postgresql://x:x@localhost/x")
    app = WorkflowApp(config=config)

    @app.step()
    async def process(ctx, input: GreetInput) -> GreetOutput:
        return GreetOutput(message="done")

    assert "process" in app.registry.list_steps()
```

- [ ] **Step 2: Run to confirm failure**

```bash
uv run pytest tests/unit/test_app.py -v
```

- [ ] **Step 3: Implement app.py**

```python
# src/pyflows/app.py
from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from pyflows.backends.pg_state import PgStateBackend
from pyflows.backends.pgmq import PgmqBackend
from pyflows.config import PyflowsConfig
from pyflows.registry import WorkflowRegistry
from pyflows.telemetry import PyflowsTelemetry
from pyflows.types import RetryConfig, WorkflowState, WorkflowStatus
from pyflows.worker import WorkflowWorker


class WorkflowApp:
    """Main entry point for the pyflows SDK."""

    def __init__(self, config: PyflowsConfig) -> None:
        self.config = config
        self.registry = WorkflowRegistry()
        self._telemetry: PyflowsTelemetry | None = None
        self._state: PgStateBackend | None = None
        self._queue: PgmqBackend | None = None
        self._worker: WorkflowWorker | None = None
        self._initialized = False

    async def initialize(self) -> None:
        """Set up DB tables, connection pools, and register workflows in the DB."""
        import pathlib, asyncpg

        # Apply schema
        schema_sql = (
            pathlib.Path(__file__).parent / "schema.sql"
        ).read_text()
        conn = await asyncpg.connect(self.config.dsn)
        try:
            await conn.execute(schema_sql)
        finally:
            await conn.close()

        # Init state backend
        self._state = PgStateBackend(dsn=self.config.dsn)
        await self._state.initialize()

        # Register workflow definitions in DB
        for name in self.registry.list_workflows():
            defn = self.registry.get_workflow(name)
            await self._state.register_workflow(name, config={})

        # Init queue backend (parse DSN for asyncpg)
        import urllib.parse
        parsed = urllib.parse.urlparse(self.config.dsn)
        self._queue = PgmqBackend(
            host=parsed.hostname or "localhost",
            port=str(parsed.port or 5432),
            database=(parsed.path or "/postgres").lstrip("/"),
            username=parsed.username or "postgres",
            password=parsed.password or "postgres",
        )
        await self._queue.initialize()

        # Ensure workflow queue exists
        await self._queue._ensure_queue(self.config.workflow_queue)

        # Telemetry
        self._telemetry = (
            PyflowsTelemetry.from_env(self.config.otel_service_name)
            if self.config.otel_enabled
            else PyflowsTelemetry.noop()
        )

        # Worker
        self._worker = WorkflowWorker(
            registry=self.registry,
            state_backend=self._state,
            queue_backend=self._queue,
            telemetry=self._telemetry,
            queue_name=self.config.workflow_queue,
        )

        self._initialized = True

    async def start(self, workflow_fn: Callable, input_model: Any) -> str:
        """Enqueue a workflow run. Returns instance_id."""
        self._assert_initialized()
        defn = self.registry.get_workflow(workflow_fn.__name__)
        instance_id = await self._state.create_instance(
            defn.name, input_model.model_dump()
        )
        await self._queue.enqueue(
            self.config.workflow_queue,
            {
                "workflow_name": defn.name,
                "instance_id": instance_id,
                "input": input_model.model_dump(),
            },
        )
        return instance_id

    async def get_status(self, instance_id: str) -> WorkflowStatus:
        self._assert_initialized()
        return await self._state.get_instance(instance_id)

    async def list_workflows(
        self,
        workflow_name: str | None = None,
        state: WorkflowState | None = None,
        limit: int = 100,
    ) -> list[WorkflowStatus]:
        self._assert_initialized()
        return await self._state.list_instances(workflow_name, state, limit)

    async def cancel(self, instance_id: str) -> None:
        self._assert_initialized()
        await self._state.cancel_workflow(instance_id)

    async def run_worker(self) -> None:
        """Run the worker loop (blocking). Use asyncio.create_task for background."""
        self._assert_initialized()
        await self._worker.run()

    async def process_once(self) -> int:
        """Process one batch of pending workflows. Useful for tests."""
        self._assert_initialized()
        return await self._worker.process_batch()

    async def close(self) -> None:
        if self._worker:
            self._worker.shutdown()
        if self._state:
            await self._state.close()
        if self._queue:
            await self._queue.close()
        self._initialized = False

    def workflow(
        self,
        name: str | None = None,
        step_defaults: RetryConfig | None = None,
    ) -> Callable:
        def decorator(fn: Callable) -> Callable:
            self.registry.register_workflow(fn, name=name, step_defaults=step_defaults)
            return fn
        return decorator

    def step(
        self,
        name: str | None = None,
        retry: RetryConfig | None = None,
        timeout_seconds: float | None = None,
    ) -> Callable:
        def decorator(fn: Callable) -> Callable:
            self.registry.register_step(fn, name=name, retry=retry, timeout_seconds=timeout_seconds)
            return fn
        return decorator

    def _assert_initialized(self) -> None:
        if not self._initialized:
            raise RuntimeError("WorkflowApp not initialized — call await app.initialize() first")
```

- [ ] **Step 4: Update `__init__.py`**

```python
"""pyflows — durable workflow engine SDK for Python + Postgres."""

from pyflows.app import WorkflowApp
from pyflows.backends import OrchestratorBackend, QueueBackend, SchedulerBackend
from pyflows.backends.pg_cron import PgCronBackend
from pyflows.backends.pg_durable import PgDurableBackend
from pyflows.backends.pg_state import PgStateBackend
from pyflows.backends.pgmq import PgmqBackend
from pyflows.config import PyflowsConfig
from pyflows.context import StepContext, WorkflowContext
from pyflows.exceptions import (
    BackendNotInitializedError,
    PyflowsError,
    SchedulerJobNotFoundError,
    StepExecutionError,
    WorkflowAlreadyExistsError,
    WorkflowNotFoundError,
)
from pyflows.registry import WorkflowRegistry
from pyflows.telemetry import PyflowsTelemetry
from pyflows.types import (
    QueueMessage,
    RetryConfig,
    ScheduledJob,
    StepConfig,
    WorkflowState,
    WorkflowStatus,
)

__all__ = [
    "WorkflowApp",
    "WorkflowContext",
    "StepContext",
    "WorkflowRegistry",
    "PyflowsConfig",
    "PyflowsTelemetry",
    "OrchestratorBackend",
    "QueueBackend",
    "SchedulerBackend",
    "PgDurableBackend",
    "PgmqBackend",
    "PgCronBackend",
    "PgStateBackend",
    "WorkflowState",
    "WorkflowStatus",
    "QueueMessage",
    "ScheduledJob",
    "RetryConfig",
    "StepConfig",
    "PyflowsError",
    "WorkflowNotFoundError",
    "WorkflowAlreadyExistsError",
    "StepExecutionError",
    "BackendNotInitializedError",
    "SchedulerJobNotFoundError",
]
```

- [ ] **Step 5: Run unit tests**

```bash
uv run pytest tests/unit/ -v
```

Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add src/pyflows/app.py src/pyflows/__init__.py tests/unit/test_app.py
git commit -m "feat: WorkflowApp — main entry point wiring all components"
```

---

## Task 10: SQL Exporter (pg_durable DSL)

**Files:**
- Create: `src/pyflows/sql_exporter.py`
- Create: `tests/unit/test_sql_exporter.py`

The exporter generates pg_durable SQL DSL using `df.http()` (push mode). This lets users export workflow definitions as SQL, import them into another database, and run workflows via `df.start()` pointing at their FastAPI app.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_sql_exporter.py
from pydantic import BaseModel
from pyflows.registry import WorkflowRegistry
from pyflows.sql_exporter import SqlExporter, DryRunResult

class CheckInput(BaseModel):
    url: str

class CheckOutput(BaseModel):
    healthy: bool

class NotifyInput(BaseModel):
    message: str

class NotifyOutput(BaseModel):
    sent: bool

async def check_service(ctx, input: CheckInput) -> CheckOutput:
    return CheckOutput(healthy=True)

async def notify(ctx, input: NotifyInput) -> NotifyOutput:
    return NotifyOutput(sent=True)

async def check_workflow(ctx, input: CheckInput) -> CheckOutput:
    result = await ctx.step(check_service, input)
    await ctx.step(notify, NotifyInput(message="done"))
    return result

def test_export_push_mode_sql():
    registry = WorkflowRegistry()
    registry.register_step(check_service)
    registry.register_step(notify)
    registry.register_workflow(check_workflow)

    exporter = SqlExporter(
        registry=registry,
        base_url="http://localhost:8000",
    )
    sql = exporter.export_workflow("check_workflow")
    assert "df.start(" in sql
    assert "df.http(" in sql
    assert "check_service" in sql
    assert "notify" in sql

def test_dry_run_returns_steps():
    registry = WorkflowRegistry()
    registry.register_step(check_service)
    registry.register_step(notify)
    registry.register_workflow(check_workflow)

    exporter = SqlExporter(registry=registry, base_url="http://localhost:8000")
    result: DryRunResult = exporter.dry_run("check_workflow", {"url": "http://example.com"})
    assert result.workflow_name == "check_workflow"
    assert len(result.steps) >= 1
    assert result.sql is not None
```

- [ ] **Step 2: Implement sql_exporter.py**

```python
# src/pyflows/sql_exporter.py
from __future__ import annotations

import json
import textwrap
from dataclasses import dataclass, field
from typing import Any

from pyflows.registry import WorkflowRegistry


@dataclass
class StepSql:
    step_name: str
    http_url: str
    sql_fragment: str


@dataclass
class DryRunResult:
    workflow_name: str
    steps: list[StepSql]
    sql: str
    input_schema: dict[str, Any]


class SqlExporter:
    """Generate pg_durable SQL DSL from registered workflow definitions.

    Push-mode export: each step becomes a df.http() call to the FastAPI
    step endpoint. Import the SQL into any database that has the pg_durable
    extension to transfer workflows from dev → prod.
    """

    def __init__(self, registry: WorkflowRegistry, base_url: str) -> None:
        self._registry = registry
        self._base_url = base_url.rstrip("/")

    def export_workflow(self, workflow_name: str) -> str:
        """Return pg_durable SQL that starts this workflow in push mode."""
        defn = self._registry.get_workflow(workflow_name)
        steps = self._collect_steps(defn.fn)
        dsl = self._build_dsl(steps, workflow_name)
        return textwrap.dedent(f"""\
            -- pyflows export: {workflow_name}
            -- Import into a database with pg_durable and run to start workflow.
            --
            -- Usage:
            --   \\i {workflow_name}.sql
            --   SELECT df.start_instance_id FROM df.start(...) -- see below

            SELECT df.setvar('base_url', '{self._base_url}');

            SELECT df.start(
                {dsl},
                '{workflow_name}'
            );
        """)

    def dry_run(self, workflow_name: str, input_data: dict[str, Any]) -> DryRunResult:
        """Trace the workflow structure without executing. Returns steps + SQL."""
        defn = self._registry.get_workflow(workflow_name)
        steps = self._collect_steps(defn.fn)
        dsl = self._build_dsl(steps, workflow_name)
        sql = textwrap.dedent(f"""\
            SELECT df.setvar('base_url', '{self._base_url}');
            SELECT df.start({dsl}, '{workflow_name}');
        """)
        return DryRunResult(
            workflow_name=workflow_name,
            steps=steps,
            sql=sql,
            input_schema=input_data,
        )

    def export_all(self) -> str:
        """Export all registered workflows to a single SQL file."""
        parts: list[str] = [
            "-- pyflows bulk export",
            f"-- base_url: {self._base_url}",
            "",
        ]
        for name in self._registry.list_workflows():
            parts.append(self.export_workflow(name))
            parts.append("")
        return "\n".join(parts)

    def _collect_steps(self, workflow_fn) -> list[StepSql]:
        """Introspect workflow function to collect step calls in order."""
        import ast
        import inspect
        import textwrap

        source = inspect.getsource(workflow_fn)
        source = textwrap.dedent(source)
        tree = ast.parse(source)

        steps: list[StepSql] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Await):
                continue
            call = node.value
            if not isinstance(call, ast.Call):
                continue
            if not (isinstance(call.func, ast.Attribute) and call.func.attr == "step"):
                continue
            if len(call.args) < 2:
                continue
            fn_name_node = call.args[0]
            step_name = fn_name_node.id if isinstance(fn_name_node, ast.Name) else "unknown"
            url = f"{{{{base_url}}}}/steps/{step_name}"
            fragment = f"df.http('{url}', 'POST', '{{\"step\": \"{step_name}\"}}')"
            steps.append(StepSql(
                step_name=step_name,
                http_url=f"{self._base_url}/steps/{step_name}",
                sql_fragment=fragment,
            ))
        return steps

    def _build_dsl(self, steps: list[StepSql], label: str) -> str:
        if not steps:
            return f"'SELECT ''workflow {label} has no steps'''"
        return "\n    ~> ".join(s.sql_fragment for s in steps)
```

- [ ] **Step 3: Run tests**

```bash
uv run pytest tests/unit/test_sql_exporter.py -v
```

Expected: 2 PASS.

- [ ] **Step 4: Commit**

```bash
git add src/pyflows/sql_exporter.py tests/unit/test_sql_exporter.py
git commit -m "feat: SQL exporter — pg_durable DSL generation and dry-run"
```

---

## Task 11: FastAPI Integration

**Files:**
- Create: `src/pyflows/fastapi.py`

- [ ] **Step 1: Implement fastapi.py**

```python
# src/pyflows/fastapi.py
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

if TYPE_CHECKING:
    from pyflows.app import WorkflowApp


class StartWorkflowRequest(BaseModel):
    workflow_name: str
    input: dict[str, Any]


class WorkflowResponse(BaseModel):
    instance_id: str


def create_router(app: WorkflowApp):
    """Create a FastAPI router that mounts pyflows management endpoints.

    Usage:
        from fastapi import FastAPI
        from pyflows.fastapi import create_router
        
        fastapi_app = FastAPI()
        fastapi_app.include_router(create_router(pyflows_app), prefix="/pyflows")
    """
    try:
        from fastapi import APIRouter, HTTPException
    except ImportError as e:
        raise ImportError(
            "FastAPI is required for pyflows.fastapi. Install with: pip install pyflows[fastapi]"
        ) from e

    router = APIRouter(tags=["pyflows"])

    @router.post("/workflows", response_model=WorkflowResponse)
    async def start_workflow(req: StartWorkflowRequest):
        try:
            defn = app.registry.get_workflow(req.workflow_name)
        except KeyError:
            raise HTTPException(404, f"Workflow '{req.workflow_name}' not registered")
        input_model = defn.input_type.model_validate(req.input)
        instance_id = await app.start(defn.fn, input_model)
        return WorkflowResponse(instance_id=instance_id)

    @router.get("/workflows/{instance_id}")
    async def get_workflow_status(instance_id: str):
        try:
            status = await app.get_status(instance_id)
            return status.model_dump()
        except Exception as e:
            raise HTTPException(404, str(e))

    @router.get("/workflows")
    async def list_workflow_instances(workflow_name: str | None = None, limit: int = 100):
        instances = await app.list_workflows(workflow_name=workflow_name, limit=limit)
        return [i.model_dump() for i in instances]

    @router.delete("/workflows/{instance_id}")
    async def cancel_workflow(instance_id: str):
        await app.cancel(instance_id)
        return {"cancelled": instance_id}

    @router.get("/registry/workflows")
    async def list_registered_workflows():
        return {"workflows": app.registry.list_workflows()}

    @router.get("/registry/steps")
    async def list_registered_steps():
        return {"steps": app.registry.list_steps()}

    @router.post("/steps/{step_name}")
    async def execute_step_push(step_name: str, payload: dict[str, Any]):
        """Push-mode step execution endpoint (called by pg_durable via df.http())."""
        try:
            defn = app.registry.get_step(step_name)
        except KeyError:
            raise HTTPException(404, f"Step '{step_name}' not registered")
        from pyflows.context import StepContext
        input_model = defn.input_type.model_validate(payload.get("input", payload))
        ctx = StepContext(
            workflow_id=payload.get("instance_id", "push-mode"),
            step_name=step_name,
        )
        result = await defn.fn(ctx, input_model)
        return result.model_dump() if hasattr(result, "model_dump") else result

    return router
```

- [ ] **Step 2: Smoke test the router**

```bash
uv run python -c "
from pyflows.fastapi import create_router
from pyflows.app import WorkflowApp
from pyflows.config import PyflowsConfig
app = WorkflowApp(config=PyflowsConfig(dsn='postgresql://x:x@localhost/x'))
router = create_router(app)
print('Routes:', [r.path for r in router.routes])
"
```

Expected: prints list of routes.

- [ ] **Step 3: Commit**

```bash
git add src/pyflows/fastapi.py
git commit -m "feat: FastAPI router — management API + push-mode step endpoint"
```

---

## Task 12: E2E Tests

**Files:**
- Create: `tests/e2e/conftest.py`
- Create: `tests/e2e/test_workflow_basic.py`
- Create: `tests/e2e/test_workflow_retry.py`
- Create: `tests/e2e/test_workflow_monitor.py`
- Create: `tests/e2e/test_sql_export.py`

These tests start a real Docker Postgres with pgmq, initialize the app, and verify end-to-end behavior.

- [ ] **Step 1: Create e2e conftest**

```python
# tests/e2e/conftest.py
import asyncio
import os
import pytest
from pyflows.app import WorkflowApp
from pyflows.config import PyflowsConfig

TEST_DSN = os.getenv(
    "PYFLOWS_TEST_DSN",
    "postgresql://pyflows:pyflows@localhost:5433/pyflows_test",
)

@pytest.fixture(scope="session")
def event_loop_policy():
    return asyncio.DefaultEventLoopPolicy()

@pytest.fixture
def pyflows_config():
    return PyflowsConfig(
        dsn=TEST_DSN,
        workflow_queue="pyflows_e2e_q",
        otel_enabled=False,
    )
```

- [ ] **Step 2: Write basic E2E tests**

```python
# tests/e2e/test_workflow_basic.py
import asyncio
import pytest
from pydantic import BaseModel
from pyflows.app import WorkflowApp
from pyflows.types import WorkflowState

class GreetInput(BaseModel):
    name: str

class GreetOutput(BaseModel):
    message: str

@pytest.mark.asyncio
async def test_workflow_completes(pyflows_config):
    app = WorkflowApp(config=pyflows_config)

    @app.workflow()
    async def greet_workflow(ctx, input: GreetInput) -> GreetOutput:
        return GreetOutput(message=f"Hello, {input.name}!")

    await app.initialize()
    try:
        instance_id = await app.start(greet_workflow, GreetInput(name="World"))
        assert instance_id is not None

        # Process the queued workflow task
        processed = await app.process_once()
        assert processed >= 1

        status = await app.get_status(instance_id)
        assert status.state == WorkflowState.COMPLETED
        assert status.output["message"] == "Hello, World!"
    finally:
        await app.close()

@pytest.mark.asyncio
async def test_workflow_with_step(pyflows_config):
    app = WorkflowApp(config=pyflows_config)

    @app.step()
    async def double_value(ctx, input: GreetInput) -> GreetOutput:
        return GreetOutput(message=f"doubled: {input.name}{input.name}")

    @app.workflow()
    async def step_workflow(ctx, input: GreetInput) -> GreetOutput:
        return await ctx.step(double_value, input)

    await app.initialize()
    try:
        instance_id = await app.start(step_workflow, GreetInput(name="hi"))
        await app.process_once()
        status = await app.get_status(instance_id)
        assert status.state == WorkflowState.COMPLETED
        assert "hihi" in status.output["message"]
    finally:
        await app.close()

@pytest.mark.asyncio
async def test_list_workflows(pyflows_config):
    app = WorkflowApp(config=pyflows_config)

    @app.workflow()
    async def list_test_wf(ctx, input: GreetInput) -> GreetOutput:
        return GreetOutput(message="done")

    await app.initialize()
    try:
        await app.start(list_test_wf, GreetInput(name="test"))
        await app.process_once()
        instances = await app.list_workflows(workflow_name="list_test_wf")
        assert len(instances) >= 1
        assert instances[0].state == WorkflowState.COMPLETED
    finally:
        await app.close()
```

- [ ] **Step 3: Write retry E2E tests**

```python
# tests/e2e/test_workflow_retry.py
import pytest
from pydantic import BaseModel
from pyflows.app import WorkflowApp
from pyflows.types import RetryConfig, WorkflowState

class NumInput(BaseModel):
    value: int

class NumOutput(BaseModel):
    result: int

attempt_counter = {"n": 0}

@pytest.mark.asyncio
async def test_step_retries_on_failure(pyflows_config):
    attempt_counter["n"] = 0
    app = WorkflowApp(config=pyflows_config)

    @app.step(retry=RetryConfig(max_retries=2, initial_delay_seconds=0.01))
    async def flaky_step(ctx, input: NumInput) -> NumOutput:
        attempt_counter["n"] += 1
        if attempt_counter["n"] < 3:
            raise ValueError("transient error")
        return NumOutput(result=input.value * 2)

    @app.workflow()
    async def retry_workflow(ctx, input: NumInput) -> NumOutput:
        return await ctx.step(flaky_step, input)

    await app.initialize()
    try:
        instance_id = await app.start(retry_workflow, NumInput(value=5))
        await app.process_once()
        status = await app.get_status(instance_id)
        assert status.state == WorkflowState.COMPLETED
        assert status.output["result"] == 10
        assert attempt_counter["n"] == 3
    finally:
        await app.close()

@pytest.mark.asyncio
async def test_step_fails_after_max_retries(pyflows_config):
    app = WorkflowApp(config=pyflows_config)

    @app.step(retry=RetryConfig(max_retries=1, initial_delay_seconds=0.01))
    async def always_fails(ctx, input: NumInput) -> NumOutput:
        raise ValueError("always fails")

    @app.workflow()
    async def fail_workflow(ctx, input: NumInput) -> NumOutput:
        return await ctx.step(always_fails, input)

    await app.initialize()
    try:
        instance_id = await app.start(fail_workflow, NumInput(value=1))
        await app.process_once()
        status = await app.get_status(instance_id)
        assert status.state == WorkflowState.FAILED
    finally:
        await app.close()
```

- [ ] **Step 4: Write monitoring E2E tests**

```python
# tests/e2e/test_workflow_monitor.py
import pytest
from pydantic import BaseModel
from pyflows.app import WorkflowApp
from pyflows.types import WorkflowState

class SimpleInput(BaseModel):
    x: int

class SimpleOutput(BaseModel):
    y: int

@pytest.mark.asyncio
async def test_monitor_running_then_completed(pyflows_config):
    app = WorkflowApp(config=pyflows_config)

    @app.workflow()
    async def monitor_wf(ctx, input: SimpleInput) -> SimpleOutput:
        return SimpleOutput(y=input.x + 1)

    await app.initialize()
    try:
        instance_id = await app.start(monitor_wf, SimpleInput(x=10))

        # Before processing: pending or running
        status = await app.get_status(instance_id)
        assert status.state in {WorkflowState.PENDING, WorkflowState.RUNNING}

        await app.process_once()

        status = await app.get_status(instance_id)
        assert status.state == WorkflowState.COMPLETED
        assert status.output["y"] == 11
    finally:
        await app.close()

@pytest.mark.asyncio
async def test_cancel_workflow(pyflows_config):
    import asyncio
    app = WorkflowApp(config=pyflows_config)

    @app.workflow()
    async def long_wf(ctx, input: SimpleInput) -> SimpleOutput:
        await asyncio.sleep(60)
        return SimpleOutput(y=input.x)

    await app.initialize()
    try:
        instance_id = await app.start(long_wf, SimpleInput(x=5))
        await app.cancel(instance_id)
        status = await app.get_status(instance_id)
        assert status.state == WorkflowState.CANCELLED
    finally:
        await app.close()
```

- [ ] **Step 5: Write SQL export E2E tests**

```python
# tests/e2e/test_sql_export.py
import pytest
from pydantic import BaseModel
from pyflows.app import WorkflowApp
from pyflows.sql_exporter import SqlExporter

class ExportInput(BaseModel):
    url: str

class ExportOutput(BaseModel):
    ok: bool

@pytest.mark.asyncio
async def test_export_workflow_sql(pyflows_config):
    app = WorkflowApp(config=pyflows_config)

    @app.step()
    async def ping(ctx, input: ExportInput) -> ExportOutput:
        return ExportOutput(ok=True)

    @app.workflow()
    async def export_test_wf(ctx, input: ExportInput) -> ExportOutput:
        return await ctx.step(ping, input)

    exporter = SqlExporter(registry=app.registry, base_url="http://localhost:8000")
    sql = exporter.export_workflow("export_test_wf")
    assert "df.start(" in sql
    assert "df.http(" in sql
    assert "ping" in sql

    dry = exporter.dry_run("export_test_wf", {"url": "http://example.com"})
    assert dry.workflow_name == "export_test_wf"
    assert len(dry.steps) == 1
    assert dry.steps[0].step_name == "ping"
```

- [ ] **Step 6: Run all E2E tests**

```bash
PYFLOWS_TEST_DSN=postgresql://pyflows:pyflows@localhost:5433/pyflows_test \
  uv run pytest tests/e2e/ -v --timeout=30
```

Expected: all PASS.

- [ ] **Step 7: Run full test suite**

```bash
PYFLOWS_TEST_DSN=postgresql://pyflows:pyflows@localhost:5433/pyflows_test \
  uv run pytest tests/ -v
```

Expected: all PASS.

- [ ] **Step 8: Lint**

```bash
uv run ruff check src/ tests/ --fix
```

Expected: no errors.

- [ ] **Step 9: Commit all E2E tests**

```bash
git add tests/e2e/ tests/conftest.py
git commit -m "feat: E2E test suite — basic, retry, monitor, cancel, SQL export"
```

---

## Task 13: Update Tracking File

- [ ] **Step 1: Update `.claude/docs/pyflows.md`**

Mark M2–M5 as complete. Update M1 state. Add next steps (M6 pg_durable backend, M7 pg_cron, M8 AI SRE example).

- [ ] **Step 2: Final lint + test run**

```bash
uv run ruff check src/ tests/ && \
  PYFLOWS_TEST_DSN=postgresql://pyflows:pyflows@localhost:5433/pyflows_test \
  uv run pytest tests/ -v
```

Expected: clean lint + all tests PASS.

---

## Self-Review

**Spec coverage:**
- ✅ pg_durable feature exposure → SQL exporter with full DSL using pg_durable skill patterns
- ✅ Expose with great flexible DevEx → `@app.workflow()`, `@app.step()`, `ctx.step()` 
- ✅ Run Python code → worker executes Python step functions
- ✅ SQL export for dev→prod → Task 10 `SqlExporter.export_all()` 
- ✅ Dry run → `SqlExporter.dry_run()` returns SQL + step list
- ✅ E2E tests → Task 12 covers basic, retry, cancel, monitor, SQL export
- ✅ OTel as first citizen → Task 5 + wired into WorkflowContext + Worker
- ✅ Modular + plugin-ready → registry, backends are all swappable
- ✅ Pydantic e2e → all inputs/outputs are `BaseModel`
- ✅ Async everywhere → all backends, context, worker are fully async

**Type consistency check:**
- `WorkflowContext.step(fn, input_model)` — used consistently in tests ✅
- `PgStateBackend.update_instance_state(instance_id, WorkflowState, output, error)` — consistent ✅
- `PgmqBackend.enqueue(queue, message, delay_seconds)` → `str` return — consistent ✅
- `WorkflowApp.process_once()` → `int` — consistent ✅

**Placeholder scan:**
- No TBD or TODO placeholders in task steps ✅

**Scope:** This produces a complete, working SDK with E2E tests. M6 (pg_cron), M8 (AI SRE example) are deferred follow-on.
