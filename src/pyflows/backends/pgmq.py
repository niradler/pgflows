from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Coroutine
from typing import TYPE_CHECKING, Any

from pyflows.backends.base import QueueBackend
from pyflows.exceptions import BackendNotInitializedError
from pyflows.types import QueueMessage

if TYPE_CHECKING:
    pass


class PgmqBackend(QueueBackend):
    """pgmq-backed step queue.

    Uses the pgmq Postgres extension (via tembo-pgmq-python) to enqueue
    step execution requests and receive results via LISTEN/NOTIFY.
    """

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._client: Any = None  # tembo_pgmq.PGMQueue, set after initialize()

    async def initialize(self) -> None:
        # TODO(M4): open tembo_pgmq.PGMQueue, verify pgmq extension installed
        raise NotImplementedError

    async def enqueue(
        self,
        queue: str,
        message: dict[str, Any],
        delay_seconds: int = 0,
    ) -> str:
        self._assert_initialized()
        # TODO(M4): await self._client.send(queue, message, delay=delay_seconds)
        raise NotImplementedError

    async def dequeue(self, queue: str, batch_size: int = 1) -> list[QueueMessage]:
        self._assert_initialized()
        # TODO(M4): await self._client.read_batch(queue, batch_size)
        raise NotImplementedError

    async def ack(self, queue: str, message_id: str) -> None:
        self._assert_initialized()
        # TODO(M4): await self._client.delete(queue, int(message_id))
        raise NotImplementedError

    async def nack(self, queue: str, message_id: str) -> None:
        self._assert_initialized()
        # TODO(M4): set VT to 0 so message is immediately re-readable
        raise NotImplementedError

    def listen(
        self,
        queue: str,
        callback: Callable[[QueueMessage], Coroutine[Any, Any, None]],
    ) -> AsyncIterator[None]:
        self._assert_initialized()
        # TODO(M4): LISTEN on pgmq channel; fall back to polling when NOTIFY absent
        raise NotImplementedError

    async def close(self) -> None:
        if self._client is not None:
            # TODO(M4): close the pgmq connection pool
            self._client = None

    def _assert_initialized(self) -> None:
        if self._client is None:
            raise BackendNotInitializedError("PgmqBackend")
