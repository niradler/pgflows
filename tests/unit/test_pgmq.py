from __future__ import annotations

import json
import os

import asyncpg
import pytest

from pgflows.backends.pgmq import PgmqBackend
from pgflows.types import QueueMessage, QueueMetrics

TEST_DSN = os.getenv(
    "PGFLOWS_TEST_DSN",
    "postgresql://pgflows:pgflows@127.0.0.1:5433/pgflows_test",
)


async def _init_conn(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )
    await conn.set_type_codec(
        "json",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )


@pytest.fixture
async def backend(require_db):
    pool = await asyncpg.create_pool(
        TEST_DSN,
        ssl=False,
        init=_init_conn,
        min_size=2,
        max_size=5,
    )
    b = PgmqBackend(pool=pool, visibility_timeout_seconds=5)
    yield b
    await pool.close()


async def test_enqueue_returns_string_id(backend):
    msg_id = await backend.enqueue("test_unit_q", {"action": "test", "data": 42})
    assert isinstance(msg_id, str)
    assert msg_id.isdigit()


async def test_dequeue_gets_message(backend):
    await backend.enqueue("test_unit_q2", {"value": 99})
    msgs = await backend.dequeue("test_unit_q2", batch_size=1)
    assert len(msgs) == 1
    assert isinstance(msgs[0], QueueMessage)
    assert msgs[0].payload["value"] == 99
    assert msgs[0].queue == "test_unit_q2"
    await backend.ack("test_unit_q2", msgs[0].message_id)


async def test_ack_removes_message(backend):
    await backend._ensure_queue("test_unit_q3_ack")
    async with backend._pool.acquire() as conn:
        await conn.execute("SELECT pgmq.purge_queue($1::text)", "test_unit_q3_ack")
    await backend.enqueue("test_unit_q3_ack", {"task": "ack_test"})
    msgs = await backend.dequeue("test_unit_q3_ack", batch_size=1)
    assert len(msgs) == 1
    msg_id = msgs[0].message_id
    await backend.ack("test_unit_q3_ack", msg_id)
    remaining = await backend.dequeue("test_unit_q3_ack", batch_size=10)
    assert len(remaining) == 0


async def test_dequeue_empty_queue_returns_empty(backend):
    msgs = await backend.dequeue("test_unit_empty_q", batch_size=5)
    assert msgs == []


async def test_nack_makes_message_readable_again(backend):
    await backend.enqueue("test_unit_nack_q", {"x": 1})
    msgs = await backend.dequeue("test_unit_nack_q", batch_size=1)
    assert len(msgs) == 1
    msg_id = msgs[0].message_id
    await backend.nack("test_unit_nack_q", msg_id)
    re_read = await backend.dequeue("test_unit_nack_q", batch_size=1)
    assert len(re_read) == 1
    await backend.ack("test_unit_nack_q", re_read[0].message_id)


async def test_per_queue_vt_overrides_default(backend):
    b = PgmqBackend(
        pool=backend._pool,
        visibility_timeout_seconds=5,
        per_queue_vt={"custom_vt_q": 999},
    )
    assert b._vt_for("custom_vt_q") == 999
    assert b._vt_for("other_q") == 5


async def test_send_batch_enqueues_multiple(backend):
    await backend._ensure_queue("test_unit_batch_q")
    async with backend._pool.acquire() as conn:
        await conn.execute("SELECT pgmq.purge_queue($1::text)", "test_unit_batch_q")
    ids = await backend.send_batch("test_unit_batch_q", [{"i": 0}, {"i": 1}, {"i": 2}])
    assert len(ids) == 3
    assert all(i.isdigit() for i in ids)
    msgs = await backend.dequeue("test_unit_batch_q", batch_size=10)
    assert len(msgs) == 3
    payloads = {m.payload["i"] for m in msgs}
    assert payloads == {0, 1, 2}


async def test_metrics_returns_queue_metrics(backend):
    await backend._ensure_queue("test_unit_metrics_q")
    await backend.enqueue("test_unit_metrics_q", {"data": 1})
    result = await backend.metrics("test_unit_metrics_q")
    assert len(result) == 1
    assert isinstance(result[0], QueueMetrics)
    assert result[0].queue_name == "test_unit_metrics_q"
    assert result[0].queue_length >= 1


async def test_metrics_all_returns_list(backend):
    await backend._ensure_queue("test_unit_metrics_all_q")
    result = await backend.metrics()
    assert isinstance(result, list)
    assert all(isinstance(m, QueueMetrics) for m in result)
    names = [m.queue_name for m in result]
    assert "test_unit_metrics_all_q" in names


async def test_list_queues_includes_created_queue(backend):
    await backend._ensure_queue("test_unit_listq_q")
    queues = await backend.list_queues()
    assert "test_unit_listq_q" in queues


async def test_purge_queue_removes_all_messages(backend):
    await backend._ensure_queue("test_unit_purge_q")
    await backend.enqueue("test_unit_purge_q", {"x": 1})
    await backend.enqueue("test_unit_purge_q", {"x": 2})
    count = await backend.purge_queue("test_unit_purge_q")
    assert count >= 2
    msgs = await backend.dequeue("test_unit_purge_q", batch_size=10)
    assert len(msgs) == 0


async def test_drop_queue_removes_queue(backend):
    await backend._ensure_queue("test_unit_drop_q")
    await backend.drop_queue("test_unit_drop_q")
    queues = await backend.list_queues()
    assert "test_unit_drop_q" not in queues
    assert "test_unit_drop_q" not in backend._known_queues
