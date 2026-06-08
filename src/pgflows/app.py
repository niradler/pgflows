from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from typing import TYPE_CHECKING

import asyncpg
from pydantic import BaseModel

from pgflows.backends.pg_cron import PgCronBackend
from pgflows.backends.pg_state import PgStateBackend
from pgflows.backends.pgmq import PgmqBackend
from pgflows.config import PgflowsConfig
from pgflows.dsl import DslNode, worker_step
from pgflows.graph import GraphSpec
from pgflows.graph_compiler import compile_graph as _compile_graph
from pgflows.logger import get_logger
from pgflows.migrations import run_migrations
from pgflows.plugins import PgflowsPlugin
from pgflows.registry import WorkflowRegistry
from pgflows.sql_exporter import SqlExporter
from pgflows.step_worker import StepWorker
from pgflows.telemetry import PgflowsTelemetry
from pgflows.types import QueueMetrics, RetryConfig, WorkflowState, WorkflowStatus
from pgflows.worker import WorkflowWorker

if TYPE_CHECKING:
    # Lazy import — PgDurableClient is only available when pg_durable is installed.
    from pgflows.pg_durable_client import PgDurableClient

_log = get_logger("app")


class WorkflowApp:
    """Main entry point for the pgflows SDK.

    Mental model — **pg_durable orchestrates the workflow; your Python runs the steps.**
    A workflow is a durable graph that pg_durable drives inside Postgres (sequencing,
    branching, parallelism, durability, replay). Your ``@app.step`` functions are the steps,
    which pg_durable invokes via ``df.http()`` or the pgmq+NOTIFY binding (``worker_step`` +
    a ``StepWorker``). pgmq+NOTIFY is the *step transport*, not a workflow engine.

    Define a pg_durable workflow as data (``GraphSpec`` → ``start_graph``) or as Python
    (``@app.workflow`` → ``exporter()`` to DSL).

    A self-contained **pull worker** (``@app.workflow`` + ``ctx.step`` + ``run_worker``) is
    also provided: Python orchestrates and ``pg_state`` checkpoints each step, so it needs no
    pg_durable and suits simple/local runs. It is not a general workflow engine — for durable
    orchestration prefer pg_durable; don't build large workflows as Python on the pull worker.
    """

    def __init__(self, config: PgflowsConfig) -> None:
        self.config = config
        self.registry = WorkflowRegistry()
        self._plugins: list[PgflowsPlugin] = []
        self._telemetry: PgflowsTelemetry | None = None
        self._state: PgStateBackend | None = None
        self._queue: PgmqBackend | None = None
        self._worker: WorkflowWorker | None = None
        self._step_worker: StepWorker | None = None
        self._initialized = False
        self._supervising = False
        self._pg_durable_available: bool = False
        self._pg_durable_client: PgDurableClient | None = None
        self._pg_cron_available: bool = False
        self._scheduler: PgCronBackend | None = None

    async def initialize(self) -> None:
        """Apply pending DB migrations, open connection pools, register workflows."""
        await run_migrations(self.config.dsn, ssl=self.config.db_ssl)

        self._state = PgStateBackend(dsn=self.config.dsn, ssl=self.config.db_ssl)
        await self._state.initialize()

        for name in self.registry.list_workflows():
            await self._state.register_workflow(name, config={})

        self._queue = PgmqBackend(
            pool=self._state._pool,  # type: ignore[arg-type]  # non-None post-initialize
            visibility_timeout_seconds=self.config.step_visibility_timeout_seconds,
            per_queue_vt={
                self.config.workflow_queue: self.config.workflow_visibility_timeout_seconds
            },
        )
        await self._queue.initialize()
        await self._queue._ensure_queue(self.config.workflow_queue)
        await self._queue._ensure_queue(self.config.step_queue)

        if self._telemetry is None:
            self._telemetry = (
                PgflowsTelemetry.from_env(self.config.otel_service_name)
                if self.config.otel_enabled
                else PgflowsTelemetry.noop()
            )

        # The extension is named "pg_durable" (extname); it creates the "df" schema.
        self._pg_durable_available = await self._state.check_extension("pg_durable")

        if self._pg_durable_available:
            from pgflows.pg_durable_client import PgDurableClient

            self._pg_durable_client = PgDurableClient(self._state._pool)  # type: ignore[arg-type]  # _pool is non-None post-initialize

        self._pg_cron_available = await self._state.check_extension("pg_cron")
        if self._pg_cron_available:
            self._scheduler = PgCronBackend(pool=self._state._pool)
            await self._scheduler.initialize()
        else:
            # pg_cron is optional — everything except schedule_workflow() works without it.
            _log.info("pg_cron extension not installed — recurring scheduling disabled")

        self._worker = WorkflowWorker(
            registry=self.registry,
            state_backend=self._state,
            queue_backend=self._queue,
            telemetry=self._telemetry,
            queue_name=self.config.workflow_queue,
            plugins=self._plugins,
        )
        self._step_worker = StepWorker(
            registry=self.registry,
            queue_backend=self._queue,
            pool=self._state._pool,
            telemetry=self._telemetry,
            step_queue=self.config.step_queue,
            notify_channel=self.config.step_notify_channel,
            plugins=self._plugins,
        )
        self._initialized = True

    @property
    def pg_durable_available(self) -> bool:
        """True if the pg_durable (df) extension is installed in the connected database."""
        return self._pg_durable_available

    @property
    def telemetry(self) -> PgflowsTelemetry:
        """Active telemetry instance, or a no-op if not yet initialized."""
        return self._telemetry or PgflowsTelemetry.noop()

    @property
    def pg_durable(self) -> PgDurableClient:
        """Access the pg_durable runtime client.

        Raises RuntimeError if pg_durable is not installed.
        Check app.pg_durable_available first.
        """
        if self._pg_durable_client is None:
            raise RuntimeError(
                "pg_durable (df) extension not installed. "
                "Check app.pg_durable_available first."
            )
        return self._pg_durable_client

    async def list_instances(
        self,
        workflow_name: str | None = None,
        state: WorkflowState | None = None,
        limit: int = 100,
    ) -> list[WorkflowStatus]:
        """List workflow instances — alias for list_workflows() for pg_durable contexts."""
        return await self.list_workflows(workflow_name=workflow_name, state=state, limit=limit)

    async def start(self, workflow_fn: Callable, input_model: BaseModel) -> str:
        """Enqueue a workflow run. Returns instance_id."""
        self._assert_initialized()
        defn = self._resolve_defn(workflow_fn)
        input_dict = input_model.model_dump()
        instance_id = await self._state.create_instance(defn.name, input_dict)  # type: ignore[union-attr]
        await self._queue.enqueue(  # type: ignore[union-attr]
            self.config.workflow_queue,
            {"workflow_name": defn.name, "instance_id": instance_id, "input": input_dict},
        )
        return instance_id

    async def get_status(self, instance_id: str) -> WorkflowStatus:
        self._assert_initialized()
        return await self._state.get_instance(instance_id)  # type: ignore[union-attr]

    async def list_workflows(
        self,
        workflow_name: str | None = None,
        state: WorkflowState | None = None,
        limit: int = 100,
    ) -> list[WorkflowStatus]:
        self._assert_initialized()
        return await self._state.list_instances(workflow_name, state, limit)  # type: ignore[union-attr]

    async def cancel(self, instance_id: str) -> None:
        self._assert_initialized()
        await self._state.cancel_workflow(instance_id)  # type: ignore[union-attr]

    async def start_batch(self, workflow_fn: Callable, inputs: list[BaseModel]) -> list[str]:
        """Enqueue multiple workflow runs. Returns list of instance_ids."""
        self._assert_initialized()
        defn = self._resolve_defn(workflow_fn)

        async def _create(input_model: BaseModel) -> tuple[str, dict]:
            input_dict = input_model.model_dump()
            instance_id = await self._state.create_instance(defn.name, input_dict)  # type: ignore[union-attr]
            msg = {"workflow_name": defn.name, "instance_id": instance_id, "input": input_dict}
            return instance_id, msg

        results = await asyncio.gather(*(_create(m) for m in inputs))
        await self._queue.send_batch(  # type: ignore[union-attr]
            self.config.workflow_queue, [r[1] for r in results]
        )
        return [r[0] for r in results]

    async def queue_metrics(self, queue: str | None = None) -> list[QueueMetrics]:
        """Return pgmq metrics for one queue (by name) or all queues (queue=None)."""
        self._assert_initialized()
        return await self._queue.metrics(queue)  # type: ignore[union-attr]

    async def list_queues(self) -> list[str]:
        """Return names of all pgmq queues in this database."""
        self._assert_initialized()
        return await self._queue.list_queues()  # type: ignore[union-attr]

    async def purge_queue(self, queue: str) -> int:
        """Delete all messages in a queue. Returns count of deleted messages."""
        self._assert_initialized()
        return await self._queue.purge_queue(queue)  # type: ignore[union-attr]

    async def drop_queue(self, queue: str) -> None:
        """Drop a queue and all its messages permanently."""
        self._assert_initialized()
        await self._queue.drop_queue(queue)  # type: ignore[union-attr]

    async def run_worker(self, *, reconnect: bool = False, max_backoff: float = 30.0) -> None:
        """Run the **pull worker** loop (blocking). Use asyncio.create_task for background.

        This is the self-contained Python orchestrator: it polls the pgmq *workflow* queue,
        runs each ``@app.workflow`` function, and checkpoints steps to ``pg_state``. It does
        not use pg_durable. For durable pg_durable-orchestrated workflows you run a
        ``StepWorker`` (``run_step_worker``) instead — this loop is for the pull mode only.

        With ``reconnect=True`` the loop is supervised: a transient failure (e.g. a
        dropped DB connection) is caught and the backends are re-established with
        exponential backoff up to ``max_backoff`` seconds, instead of propagating and
        killing the worker. Stops cleanly on ``await app.close()`` or task cancellation.
        """
        self._assert_initialized()
        if not reconnect:
            await self._worker.run()  # type: ignore[union-attr]
            return

        self._supervising = True
        backoff = 1.0
        while self._supervising:
            try:
                await self._worker.run()  # type: ignore[union-attr]
                return  # clean stop via close()/shutdown()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _log.warning("worker loop stopped (%s); reconnecting in %.0fs", exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
                try:
                    await self._reconnect()
                    backoff = 1.0
                except asyncio.CancelledError:
                    raise
                except Exception as reinit_exc:
                    _log.warning("worker reconnect failed (%s); will retry", reinit_exc)

    async def process_once(self) -> int:
        """Process one batch of pending workflows. Useful for tests."""
        self._assert_initialized()
        return await self._worker.process_batch()  # type: ignore[union-attr]

    def exporter(
        self,
        base_url: str | None = None,
        *,
        mode: str = "http",
    ) -> SqlExporter:
        """Build a SqlExporter wired to this app's registry and queue config.

        ``mode='http'`` emits df.http() push-mode SQL (requires base_url).
        ``mode='worker'`` emits native enqueue + pg_notify + poll-result SQL,
        picked up by the step worker (``run_step_worker``).
        """
        return SqlExporter(
            self.registry,
            base_url,
            mode=mode,  # type: ignore[arg-type]
            step_queue=self.config.step_queue,
            notify_channel=self.config.step_notify_channel,
        )

    def worker_step(self, step_name: str, **kwargs: object) -> DslNode:
        """Build a ``worker_step`` DSL node bound to THIS app's configured queue/channel.

        The bare ``dsl.worker_step()`` builder defaults to the literal ``'pgflows_steps'``
        queue regardless of config. If you override ``step_queue``/``step_notify_channel``
        in ``PgflowsConfig`` and forget to pass them through, the DSL enqueues onto a queue
        the StepWorker never drains and the instance hangs with no error. This helper
        injects the configured names so the graph and the worker always agree; override
        any of them per-call via kwargs.
        """
        kwargs.setdefault("queue", self.config.step_queue)
        kwargs.setdefault("notify_channel", self.config.step_notify_channel)
        return worker_step(step_name, **kwargs)  # type: ignore[arg-type]

    def graph_json_schema(self) -> dict:
        """JSON Schema for the GraphSpec workflow document — hand to UIs and validators."""
        return GraphSpec.model_json_schema(by_alias=True)

    def compile_graph(self, spec: GraphSpec) -> DslNode:
        """Compile a data-driven GraphSpec into a pg_durable DSL node.

        Pure — usable offline. Validates the spec against pg_durable composition limits
        (raises GraphCompileError) and emits DSL bound to this app's step queue/channel.
        """
        return _compile_graph(
            spec,
            step_queue=self.config.step_queue,
            notify_channel=self.config.step_notify_channel,
        )

    async def start_graph(self, spec: GraphSpec, *, label: str) -> str:
        """Compile a GraphSpec and start it as a pg_durable run. Returns the instance ID.

        Requires the pg_durable (df) extension — GraphSpec workflows are compiled to DSL
        and orchestrated by Postgres. Run a StepWorker (``run_step_worker``) to execute the
        steps the graph dispatches.

        ``spec.input`` (if set) seeds the single ``{input}`` durable var the first node
        reads. Because that var is database-scoped, ``start_graph`` is single-flight per
        app: don't fire concurrent runs with different inputs against one app instance.
        """
        self._assert_initialized()
        if not self._pg_durable_available:
            raise RuntimeError(
                "start_graph requires the pg_durable (df) extension, which is not installed "
                "in the connected database. GraphSpec workflows compile to pg_durable DSL "
                "and run via df.start()."
            )
        node = self.compile_graph(spec)
        if spec.input is not None:
            await self.pg_durable.setvar("input", json.dumps(spec.input))
        return await self.pg_durable.start(node, label=label)

    @property
    def pg_cron_available(self) -> bool:
        """True if the pg_cron extension is installed — required for schedule_workflow()."""
        return self._pg_cron_available

    async def schedule_workflow(
        self,
        job_name: str,
        cron: str,
        workflow_fn: Callable,
        input_model: BaseModel | None = None,
    ) -> str:
        """Recurringly start a workflow on a cron schedule via pg_cron. Returns the job id.

        Registers a pg_cron job whose command creates a pending instance and enqueues it
        onto the workflow queue (+ a NOTIFY), the durable-safe recurring trigger — a
        running pull worker (``run_worker``) then picks it up each tick. Re-scheduling the
        same ``job_name`` replaces the existing job. Requires the pg_cron extension.
        """
        self._assert_initialized()
        if self._scheduler is None:
            raise RuntimeError(
                "schedule_workflow requires the pg_cron extension, which is not installed. "
                "Add pg_cron to shared_preload_libraries and CREATE EXTENSION pg_cron."
            )
        defn = self._resolve_defn(workflow_fn)
        input_json = input_model.model_dump_json() if input_model is not None else "{}"
        command = self._start_workflow_sql(defn.name, input_json)
        return await self._scheduler.schedule(job_name, cron, command)

    async def unschedule_workflow(self, job_name: str) -> None:
        """Remove a scheduled workflow by job name (idempotent)."""
        self._assert_initialized()
        if self._scheduler is None:
            raise RuntimeError("schedule_workflow requires the pg_cron extension")
        await self._scheduler.unschedule_by_name(job_name)

    async def list_schedules(self) -> list:
        """List all pg_cron jobs in the database."""
        self._assert_initialized()
        if self._scheduler is None:
            raise RuntimeError("schedule_workflow requires the pg_cron extension")
        return await self._scheduler.list_jobs()

    def _start_workflow_sql(self, workflow_name: str, input_json: str) -> str:
        """SQL (run by pg_cron) that starts a workflow run: create instance + enqueue + notify.

        Mirrors ``start()`` entirely in-database so no Python callback is needed on each tick.
        """
        wf = workflow_name.replace("'", "''")
        payload = input_json.replace("'", "''")
        queue = self.config.workflow_queue
        return (
            "WITH inst AS ("
            "INSERT INTO pgflows.workflow_instances (workflow_name, input) "
            f"VALUES ('{wf}', '{payload}'::jsonb) RETURNING instance_id, input) "
            f"SELECT pgmq.send('{queue}', jsonb_build_object("
            f"'workflow_name', '{wf}', "
            "'instance_id', inst.instance_id::text, "
            "'input', inst.input)), "
            f"pg_notify('{queue}', inst.instance_id::text) FROM inst"
        )

    def acquire(self) -> asyncpg.pool.PoolAcquireContext:
        """Acquire a pooled connection for ad-hoc SQL around a durable run.

        The archetypal operational pattern is to create/seed/inspect your own tables
        next to a durable graph. Use as ``async with app.acquire() as conn: ...`` instead
        of reaching into private backend internals.
        """
        self._assert_initialized()
        return self._state._pool.acquire()  # type: ignore[union-attr]

    async def run_step_worker(self) -> None:
        """Run the queue+NOTIFY step worker loop (blocking).

        Picks up steps dispatched by pg_durable (see SqlExporter ``mode='worker'`` /
        ``worker_step``), runs the Python step, and writes the result to the poll
        table pg_durable reads. Use asyncio.create_task to run in the background.
        """
        self._assert_initialized()
        await self._step_worker.run()  # type: ignore[union-attr]

    async def process_step_once(self) -> int:
        """Drain one batch of pgmq-dispatched steps. Useful for tests."""
        self._assert_initialized()
        return await self._step_worker.process_batch()  # type: ignore[union-attr]

    async def close(self) -> None:
        self._supervising = False
        await self._teardown()

    async def _teardown(self) -> None:
        if self._worker:
            self._worker.shutdown()
        if self._step_worker:
            self._step_worker.shutdown()
        if self._scheduler:
            await self._scheduler.close()
        if self._state:
            await self._state.close()
        if self._queue:
            await self._queue.close()
        self._initialized = False

    async def _reconnect(self) -> None:
        """Tear down pools and re-run initialize() — leaves the supervisor flag intact."""
        await self._teardown()
        await self.initialize()

    def workflow(
        self,
        name: str | None = None,
        step_defaults: RetryConfig | None = None,
    ) -> Callable:
        """Register a Python workflow function.

        Runnable two ways: by the pull worker (``run_worker`` — Python orchestrates), or
        exported to a pg_durable graph via ``exporter()`` (pg_durable orchestrates). For
        durable orchestration, branching, and parallelism prefer pg_durable — either export
        this function or define the flow as a ``GraphSpec`` (``start_graph``); the pull worker
        is the simpler local option, not a general workflow engine.
        """

        def decorator(fn: Callable) -> Callable:
            self.registry.register_workflow(fn, name=name, step_defaults=step_defaults)
            return fn

        return decorator

    def register_plugin(self, plugin: PgflowsPlugin) -> None:
        """Register a plugin to receive workflow and step lifecycle hooks."""
        self._plugins.append(plugin)

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

    def _resolve_defn(self, workflow_fn: Callable):  # type: ignore[return]
        return self.registry.get_workflow(
            getattr(workflow_fn, "_pgflows_name", workflow_fn.__name__)
        )

    def _assert_initialized(self) -> None:
        if not self._initialized:
            raise RuntimeError("WorkflowApp not initialized — call await app.initialize() first")
