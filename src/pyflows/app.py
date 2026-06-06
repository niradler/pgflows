from __future__ import annotations

import pathlib
import urllib.parse
from collections.abc import Callable
from typing import Any

from pyflows.backends.pg_state import PgStateBackend
from pyflows.backends.pgmq import PgmqBackend
from pyflows.config import PyflowsConfig
from pyflows.plugins import PyflowsPlugin
from pyflows.registry import WorkflowRegistry
from pyflows.telemetry import PyflowsTelemetry
from pyflows.types import RetryConfig, WorkflowState, WorkflowStatus
from pyflows.worker import WorkflowWorker

_SCHEMA_SQL = (pathlib.Path(__file__).parent / "schema.sql").read_text()


class WorkflowApp:
    """Main entry point for the pyflows SDK."""

    def __init__(self, config: PyflowsConfig) -> None:
        self.config = config
        self.registry = WorkflowRegistry()
        self._plugins: list[PyflowsPlugin] = []
        self._telemetry: PyflowsTelemetry | None = None
        self._state: PgStateBackend | None = None
        self._queue: PgmqBackend | None = None
        self._worker: WorkflowWorker | None = None
        self._initialized = False

    async def initialize(self) -> None:
        """Apply DB schema, open connection pools, register workflows."""
        import asyncpg

        conn = await asyncpg.connect(self.config.dsn)
        try:
            await conn.execute(_SCHEMA_SQL)
        finally:
            await conn.close()

        self._state = PgStateBackend(dsn=self.config.dsn)
        await self._state.initialize()

        for name in self.registry.list_workflows():
            await self._state.register_workflow(name, config={})

        parsed = urllib.parse.urlparse(self.config.dsn)
        self._queue = PgmqBackend(
            host=parsed.hostname or "localhost",
            port=str(parsed.port or 5432),
            database=(parsed.path or "/postgres").lstrip("/"),
            username=parsed.username or "postgres",
            password=parsed.password or "postgres",
        )
        await self._queue.initialize()
        await self._queue._ensure_queue(self.config.workflow_queue)

        self._telemetry = (
            PyflowsTelemetry.from_env(self.config.otel_service_name)
            if self.config.otel_enabled
            else PyflowsTelemetry.noop()
        )

        self._worker = WorkflowWorker(
            registry=self.registry,
            state_backend=self._state,
            queue_backend=self._queue,
            telemetry=self._telemetry,
            queue_name=self.config.workflow_queue,
            plugins=self._plugins,
        )
        self._initialized = True

    async def start(self, workflow_fn: Callable, input_model: Any) -> str:
        """Enqueue a workflow run. Returns instance_id."""
        self._assert_initialized()
        defn = self.registry.get_workflow(workflow_fn.__name__)
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

    async def run_worker(self) -> None:
        """Run the worker loop (blocking). Use asyncio.create_task for background."""
        self._assert_initialized()
        await self._worker.run()  # type: ignore[union-attr]

    async def process_once(self) -> int:
        """Process one batch of pending workflows. Useful for tests."""
        self._assert_initialized()
        return await self._worker.process_batch()  # type: ignore[union-attr]

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

    def register_plugin(self, plugin: PyflowsPlugin) -> None:
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
