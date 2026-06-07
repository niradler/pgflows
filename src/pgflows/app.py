from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

import asyncpg
from pydantic import BaseModel

from pgflows.backends.pg_state import PgStateBackend
from pgflows.backends.pgmq import PgmqBackend
from pgflows.config import PgflowsConfig
from pgflows.dsl import DslNode, worker_step
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


class WorkflowApp:
    """Main entry point for the pgflows SDK."""

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
        self._pg_durable_available: bool = False
        self._pg_durable_client: PgDurableClient | None = None

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
        defn = self.registry.get_workflow(
            getattr(workflow_fn, "_pgflows_name", workflow_fn.__name__)
        )
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
        """Enqueue multiple workflow runs atomically. Returns list of instance_ids."""
        self._assert_initialized()
        defn = self.registry.get_workflow(
            getattr(workflow_fn, "_pgflows_name", workflow_fn.__name__)
        )
        instance_ids: list[str] = []
        messages: list[dict] = []
        for input_model in inputs:
            input_dict = input_model.model_dump()
            instance_id = await self._state.create_instance(defn.name, input_dict)  # type: ignore[union-attr]
            instance_ids.append(instance_id)
            messages.append(
                {"workflow_name": defn.name, "instance_id": instance_id, "input": input_dict}
            )
        await self._queue.send_batch(self.config.workflow_queue, messages)  # type: ignore[union-attr]
        return instance_ids

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

    async def run_worker(self) -> None:
        """Run the worker loop (blocking). Use asyncio.create_task for background."""
        self._assert_initialized()
        await self._worker.run()  # type: ignore[union-attr]

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
        if self._worker:
            self._worker.shutdown()
        if self._step_worker:
            self._step_worker.shutdown()
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

    def _assert_initialized(self) -> None:
        if not self._initialized:
            raise RuntimeError("WorkflowApp not initialized — call await app.initialize() first")
