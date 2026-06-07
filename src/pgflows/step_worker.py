from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from pgflows.context import StepContext
from pgflows.logger import get_logger
from pgflows.plugins import PgflowsPlugin, StepEvent, fire

_log = get_logger("step_worker")

if TYPE_CHECKING:
    import asyncpg

    from pgflows.backends.base import QueueBackend
    from pgflows.pg_durable_client import PgDurableClient
    from pgflows.registry import WorkflowRegistry
    from pgflows.telemetry import PgflowsTelemetry
    from pgflows.types import QueueMessage


class StepWorker:
    """Runs pgmq-dispatched steps and signals their results back to pg_durable.

    The push-mode counterpart to the HTTP step endpoint. pg_durable enqueues a
    ``{step, instance_id, signal, input}`` message via ``pgmq.send`` and suspends on
    ``df.wait_for_signal``. This worker drains the step queue, runs the registered
    Python step, then ``df.signal()``s the output back to resume the durable function.

    Use ``run()`` for the LISTEN/NOTIFY loop (low latency, with a poll fallback so no
    message is missed), or ``process_batch()`` to drain once (tests, manual control).
    """

    def __init__(
        self,
        registry: WorkflowRegistry,
        queue_backend: QueueBackend,
        pg_durable: PgDurableClient | None,
        telemetry: PgflowsTelemetry,
        step_queue: str = "pgflows_steps",
        notify_channel: str | None = None,
        batch_size: int = 10,
        poll_interval_seconds: float = 1.0,
        plugins: list[PgflowsPlugin] | None = None,
    ) -> None:
        self._registry = registry
        self._queue = queue_backend
        self._pg_durable = pg_durable
        self._telemetry = telemetry
        self._step_queue = step_queue
        self._notify_channel = notify_channel or step_queue
        self._batch_size = batch_size
        self._poll_interval = poll_interval_seconds
        self._plugins = plugins or []
        self._running = False

    async def process_batch(self) -> int:
        """Drain up to batch_size step messages. Returns count processed."""
        msgs = await self._queue.dequeue(self._step_queue, batch_size=self._batch_size)
        if not msgs:
            return 0
        results = await asyncio.gather(
            *[self._handle_message(m) for m in msgs], return_exceptions=True
        )
        for r in results:
            if isinstance(r, BaseException):
                _log.error("unhandled error in step _handle_message", exc_info=r)
        return len(msgs)

    async def run(self, pool: asyncpg.Pool) -> None:
        """LISTEN on the notify channel and drain the queue on each wake-up.

        A poll fallback (``poll_interval_seconds``) guarantees delivery even if a
        NOTIFY is missed (e.g. the message was enqueued without a doorbell ring).
        """
        self._running = True
        woke = asyncio.Event()

        def _on_notify(*_: Any) -> None:
            woke.set()

        conn = await pool.acquire()
        try:
            await conn.add_listener(self._notify_channel, _on_notify)
            while self._running:
                # Drain everything currently queued before sleeping again.
                while await self.process_batch():
                    pass
                try:
                    await asyncio.wait_for(woke.wait(), timeout=self._poll_interval)
                except TimeoutError:
                    pass
                woke.clear()
        finally:
            await conn.remove_listener(self._notify_channel, _on_notify)
            await pool.release(conn)

    def shutdown(self) -> None:
        self._running = False

    async def _handle_message(self, msg: QueueMessage) -> None:
        payload = msg.payload
        step_name = payload.get("step")
        instance_id = payload.get("instance_id", "pgmq-step")
        signal_name = payload.get("signal")
        raw_input = payload.get("input") or {}

        if not step_name or not signal_name:
            _log.error("malformed step message, archiving: %s", payload)
            await self._queue.archive(self._step_queue, msg.message_id)
            return

        try:
            step_defn = self._registry.get_step(step_name)
        except KeyError:
            _log.error("step '%s' not registered, archiving message", step_name)
            await self._queue.archive(self._step_queue, msg.message_id)
            return

        try:
            input_obj = step_defn.input_type.model_validate(raw_input)
        except Exception:
            _log.exception("invalid input for step '%s', archiving message", step_name)
            await self._signal(instance_id, signal_name, {"__error__": "invalid input"})
            await self._queue.archive(self._step_queue, msg.message_id)
            return

        event = StepEvent(
            instance_id=instance_id,
            workflow_name="",
            step_name=step_name,
            step_index=0,
            attempt=1,
        )
        await fire(self._plugins, "before_step", event, input_obj)
        ctx = StepContext(instance_id=instance_id, step_name=step_name)
        try:
            with self._telemetry.step_span(instance_id, step_name, 0):
                result = await step_defn.fn(ctx, input_obj)
        except Exception as exc:
            _log.warning("step '%s' failed: %s; nacking for redelivery", step_name, exc)
            await fire(self._plugins, "on_step_error", event, exc)
            await self._queue.nack(self._step_queue, msg.message_id)
            return

        output = result.model_dump() if isinstance(result, BaseModel) else result
        await self._signal(instance_id, signal_name, output)
        await fire(self._plugins, "after_step", event, result)
        await self._queue.ack(self._step_queue, msg.message_id)

    async def _signal(self, instance_id: str, signal_name: str, data: Any) -> None:
        if self._pg_durable is None:
            _log.error(
                "cannot signal instance %s/%s: pg_durable not available",
                instance_id,
                signal_name,
            )
            return
        await self._pg_durable.signal(instance_id, signal_name, data)


__all__ = ["StepWorker"]
