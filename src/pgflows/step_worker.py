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
    from pgflows.registry import WorkflowRegistry
    from pgflows.telemetry import PgflowsTelemetry
    from pgflows.types import QueueMessage


class StepWorker:
    """Runs pgmq-dispatched steps and writes their results to the poll table.

    The push-mode counterpart to the HTTP step endpoint. pg_durable enqueues a
    ``{step, instance_id, result_key, input}`` message via ``pgmq.send``, rings
    ``pg_notify``, then polls ``results_table`` for the row. This worker drains the
    step queue, runs the registered Python step, and INSERTs the output keyed by
    ``result_key`` — a durable hand-off that can't lose a result to a race the way a
    fire-and-forget ``df.signal`` can.

    Use ``run()`` for the LISTEN/NOTIFY loop (low latency, with a poll fallback so no
    message is missed), or ``process_batch()`` to drain once (tests, manual control).
    """

    def __init__(
        self,
        registry: WorkflowRegistry,
        queue_backend: QueueBackend,
        pool: asyncpg.Pool | None,
        telemetry: PgflowsTelemetry,
        step_queue: str = "pgflows_steps",
        notify_channel: str | None = None,
        results_table: str = "pgflows.pgmq_step_results",
        batch_size: int = 10,
        poll_interval_seconds: float = 1.0,
        plugins: list[PgflowsPlugin] | None = None,
    ) -> None:
        self._registry = registry
        self._queue = queue_backend
        self._pool = pool
        self._telemetry = telemetry
        self._step_queue = step_queue
        self._notify_channel = notify_channel or step_queue
        self._results_table = results_table
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

    async def run(self) -> None:
        """LISTEN on the notify channel and drain the queue on each wake-up.

        A poll fallback (``poll_interval_seconds``) guarantees delivery even if a
        NOTIFY is missed (e.g. the message was enqueued without a doorbell ring).
        """
        if self._pool is None:
            raise RuntimeError("StepWorker.run() requires a connection pool")
        self._running = True
        woke = asyncio.Event()

        def _on_notify(*_: Any) -> None:
            woke.set()

        conn = await self._pool.acquire()
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
            await self._pool.release(conn)

    def shutdown(self) -> None:
        self._running = False

    async def _handle_message(self, msg: QueueMessage) -> None:
        payload = msg.payload
        step_name = payload.get("step")
        instance_id = payload.get("instance_id", "pgmq-step")
        result_key = payload.get("result_key")
        raw_input = payload.get("input") or {}

        if not step_name or not result_key:
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
            await self._write_result(result_key, {"__error__": "invalid input"})
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
        await self._write_result(result_key, output)
        await fire(self._plugins, "after_step", event, result)
        await self._queue.ack(self._step_queue, msg.message_id)

    async def _write_result(self, result_key: str, output: Any) -> None:
        if self._pool is None:
            _log.error("cannot write result for %s: no pool", result_key)
            return
        # Pass output as-is: the pool registers a jsonb codec (json.dumps), so
        # pre-serializing here would double-encode it into a JSON string.
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"INSERT INTO {self._results_table} (key, result) VALUES ($1, $2) "
                "ON CONFLICT (key) DO NOTHING",
                result_key,
                output,
            )


__all__ = ["StepWorker"]
