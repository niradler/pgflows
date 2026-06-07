from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable, Coroutine
from typing import Any

import asyncpg

from pgflows.backends.base import QueueBackend
from pgflows.types import QueueMessage


class PgmqBackend(QueueBackend):
    """pgmq queue backend using the shared asyncpg pool.

    Calls pgmq extension functions directly — no second connection pool is opened.
    Pass the same asyncpg.Pool as PgStateBackend to eliminate the duplicate connections.
    """

    def __init__(self, pool: asyncpg.Pool, visibility_timeout_seconds: int = 30) -> None:
        self._pool = pool
        self._vt = visibility_timeout_seconds
        self._known_queues: set[str] = set()

    async def initialize(self) -> None:
        pass

    async def _ensure_queue(self, queue: str) -> None:
        if queue not in self._known_queues:
            async with self._pool.acquire() as conn:
                try:
                    await conn.execute("SELECT pgmq.create($1::text)", queue)
                except Exception:
                    pass
            self._known_queues.add(queue)

    async def enqueue(self, queue: str, message: dict[str, Any], delay_seconds: int = 0) -> str:
        await self._ensure_queue(queue)
        async with self._pool.acquire() as conn:
            msg_id = await conn.fetchval(
                "SELECT pgmq.send($1::text, $2::jsonb, $3::int)",
                queue,
                message,
                delay_seconds,
            )
        return str(msg_id)

    async def dequeue(self, queue: str, batch_size: int = 1) -> list[QueueMessage]:
        await self._ensure_queue(queue)
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM pgmq.read($1::text, $2::int, $3::int)",
                queue,
                self._vt,
                batch_size,
            )
        if not rows:
            return []
        return [
            QueueMessage(
                message_id=str(r["msg_id"]),
                queue=queue,
                payload=(
                    r["message"] if isinstance(r["message"], dict) else json.loads(r["message"])
                ),
                enqueued_at=r["enqueued_at"],
                read_count=r["read_ct"],
            )
            for r in rows
        ]

    async def ack(self, queue: str, message_id: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                "SELECT pgmq.delete($1::text, $2::bigint)", queue, int(message_id)
            )

    async def nack(self, queue: str, message_id: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.fetchval(
                "SELECT (pgmq.set_vt($1::text, $2::bigint, 0)).msg_id",
                queue,
                int(message_id),
            )

    async def archive(self, queue: str, message_id: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                "SELECT pgmq.archive($1::text, $2::bigint)", queue, int(message_id)
            )

    async def listen(
        self,
        queue: str,
        callback: Callable[[QueueMessage], Coroutine[Any, Any, None]],
    ) -> AsyncIterator[None]:
        await self._ensure_queue(queue)
        while True:
            msgs = await self.dequeue(queue, batch_size=10)
            if msgs:
                for m in msgs:
                    await callback(m)
            else:
                await asyncio.sleep(0.1)
            yield

    async def close(self) -> None:
        pass
