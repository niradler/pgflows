from __future__ import annotations

import asyncio
import traceback
from typing import TYPE_CHECKING

from pydantic import BaseModel

from pyflows.context import WorkflowContext
from pyflows.plugins import PyflowsPlugin, WorkflowEvent, fire
from pyflows.types import WorkflowState

if TYPE_CHECKING:
    from pyflows.backends.base import QueueBackend
    from pyflows.backends.pg_state import PgStateBackend
    from pyflows.registry import WorkflowRegistry
    from pyflows.telemetry import PyflowsTelemetry
    from pyflows.types import QueueMessage


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
        plugins: list[PyflowsPlugin] | None = None,
    ) -> None:
        self._registry = registry
        self._state = state_backend
        self._queue = queue_backend
        self._telemetry = telemetry
        self._queue_name = queue_name
        self._batch_size = batch_size
        self._plugins = plugins or []
        self._running = False

    async def process_batch(self) -> int:
        """Dequeue and execute up to batch_size workflow tasks. Returns count processed."""
        msgs = await self._queue.dequeue(self._queue_name, batch_size=self._batch_size)
        if not msgs:
            return 0
        await asyncio.gather(*[self._handle_message(m) for m in msgs], return_exceptions=True)
        return len(msgs)

    async def run(self) -> None:
        """Run the worker loop indefinitely (stop via shutdown())."""
        self._running = True
        while self._running:
            processed = await self.process_batch()
            if processed == 0:
                await asyncio.sleep(0.1)

    def shutdown(self) -> None:
        self._running = False

    async def _handle_message(self, msg: QueueMessage) -> None:
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
        wf_event = WorkflowEvent(instance_id=instance_id, workflow_name=workflow_name)

        with self._telemetry.workflow_span(workflow_name, instance_id):
            await self._state.update_instance_state(instance_id, WorkflowState.RUNNING)
            await fire(self._plugins, "before_workflow", wf_event)
            try:
                ctx = WorkflowContext(
                    instance_id=instance_id,
                    workflow_name=workflow_name,
                    state_backend=self._state,
                    telemetry=self._telemetry,
                    step_defaults=defn.step_defaults,
                    plugins=self._plugins,
                )
                result = await defn.fn(ctx, input_model)
                output = result.model_dump() if isinstance(result, BaseModel) else result
                await self._state.update_instance_state(
                    instance_id, WorkflowState.COMPLETED, output=output
                )
                await fire(self._plugins, "after_workflow", wf_event, result)
                await self._queue.ack(self._queue_name, msg.message_id)
            except Exception as exc:
                error = traceback.format_exc()
                await self._state.update_instance_state(
                    instance_id, WorkflowState.FAILED, error=error
                )
                await fire(self._plugins, "on_workflow_error", wf_event, exc)
                await self._queue.nack(self._queue_name, msg.message_id)
