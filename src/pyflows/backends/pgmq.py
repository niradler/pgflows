from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Coroutine
from typing import Any  # noqa: F401 — Callable/Coroutine used in listen signature

from tembo_pgmq_python.async_queue import PGMQueue

from pyflows.backends.base import QueueBackend
from pyflows.exceptions import BackendNotInitializedError
from pyflows.types import QueueMessage


class PgmqBackend(QueueBackend):
    """pgmq-backed step queue using the tembo async client (asyncpg)."""

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
        self._assert_initialized()
        if queue not in self._known_queues:
            try:
                await self._client.create_queue(queue)  # type: ignore[union-attr]
            except Exception:
                pass  # queue already exists — safe to ignore
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
        # Reset visibility timeout to 0 so message is immediately re-readable
        await self._client.set_vt(queue, int(message_id), 0)

    async def archive(self, queue: str, message_id: str) -> None:
        self._assert_initialized()
        await self._client.archive(queue, int(message_id))

    async def listen(
        self,
        queue: str,
        callback: Callable[[QueueMessage], Coroutine[Any, Any, None]],
    ) -> AsyncIterator[None]:
        """Poll pgmq continuously, calling callback for each message."""
        self._assert_initialized()
        await self._ensure_queue(queue)
        while True:
            msgs = await self._client.read_batch(queue, vt=self._vt, batch_size=10)
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
