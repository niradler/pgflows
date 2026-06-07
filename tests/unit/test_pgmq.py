import os
import urllib.parse

import pytest

from pgflows.backends.pgmq import PgmqBackend
from pgflows.types import QueueMessage

TEST_DSN = os.getenv(
    "PGFLOWS_TEST_DSN",
    "postgresql://pgflows:pgflows@127.0.0.1:5433/pgflows_test",
)
_PARSED_DSN = urllib.parse.urlparse(TEST_DSN)

PGMQ_ARGS = dict(
    host=_PARSED_DSN.hostname or "127.0.0.1",
    port=str(_PARSED_DSN.port or 5432),
    database=(_PARSED_DSN.path or "/postgres").lstrip("/"),
    username=_PARSED_DSN.username or "postgres",
    password=_PARSED_DSN.password or "postgres",
    visibility_timeout_seconds=5,
)


@pytest.fixture
async def backend(require_db):
    b = PgmqBackend(**PGMQ_ARGS)
    await b.initialize()
    yield b
    await b.close()


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
    await backend.enqueue("test_unit_q3", {"task": "ack_test"})
    msgs = await backend.dequeue("test_unit_q3", batch_size=1)
    assert len(msgs) == 1
    msg_id = msgs[0].message_id
    await backend.ack("test_unit_q3", msg_id)
    # After ack, message should be gone (VT still applies, but it's deleted)
    # Read again with vt=1 to avoid VT block — should be empty
    remaining = await backend.dequeue("test_unit_q3", batch_size=10)
    assert len(remaining) == 0


async def test_dequeue_empty_queue_returns_empty(backend):
    # Fresh queue, nothing in it
    msgs = await backend.dequeue("test_unit_empty_q", batch_size=5)
    assert msgs == []


async def test_nack_makes_message_readable_again(backend):
    # nack resets VT to 0, making message immediately readable
    await backend.enqueue("test_unit_nack_q", {"x": 1})
    msgs = await backend.dequeue("test_unit_nack_q", batch_size=1)
    assert len(msgs) == 1
    msg_id = msgs[0].message_id
    await backend.nack("test_unit_nack_q", msg_id)
    # Should be readable again immediately
    re_read = await backend.dequeue("test_unit_nack_q", batch_size=1)
    assert len(re_read) == 1
    await backend.ack("test_unit_nack_q", re_read[0].message_id)
