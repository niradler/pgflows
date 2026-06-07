from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from pgflows.dsl import sleep
from pgflows.pg_durable_client import PgDurableClient


def _make_pool(fetchrow_return=None, fetch_return=None):
    """Build a minimal asyncpg.Pool mock."""
    conn = AsyncMock()
    conn.execute = AsyncMock(return_value=None)
    conn.fetchrow = AsyncMock(return_value=fetchrow_return)
    conn.fetch = AsyncMock(return_value=fetch_return or [])

    pool = MagicMock()
    pool.acquire = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    return pool, conn


# ---------------------------------------------------------------------------
# setvar
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_setvar_calls_correct_sql():
    pool, conn = _make_pool()
    client = PgDurableClient(pool)
    await client.setvar("my_key", "my_value")
    conn.execute.assert_awaited_once_with("SELECT df.setvar($1, $2)", "my_key", "my_value")


# ---------------------------------------------------------------------------
# start
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_returns_instance_id():
    row = MagicMock()
    row.__getitem__ = lambda self, i: "abc12345"
    pool, conn = _make_pool(fetchrow_return=row)
    client = PgDurableClient(pool)
    node = sleep(5)
    result = await client.start(node)
    assert result == "abc12345"


@pytest.mark.asyncio
async def test_start_interpolates_dsl_expression():
    # The DSL must be interpolated (Postgres evaluates the operators), NOT bound.
    row = MagicMock()
    row.__getitem__ = lambda self, i: "id"
    pool, conn = _make_pool(fetchrow_return=row)
    client = PgDurableClient(pool)
    await client.start(sleep(5))
    args = conn.fetchrow.call_args[0]
    assert "df.sleep(5)" in args[0]   # interpolated into the statement
    assert len(args) == 1             # no bound params


@pytest.mark.asyncio
async def test_start_plain_str_wrapped_as_sql_node():
    row = MagicMock()
    row.__getitem__ = lambda self, i: "id"
    pool, conn = _make_pool(fetchrow_return=row)
    client = PgDurableClient(pool)
    await client.start("SELECT 1 AS x")
    args = conn.fetchrow.call_args[0]
    assert "df.start('SELECT 1 AS x')" in args[0]   # quoted as a single SQL node


@pytest.mark.asyncio
async def test_start_with_label_builds_query():
    row = MagicMock()
    row.__getitem__ = lambda self, i: "xyz99999"
    pool, conn = _make_pool(fetchrow_return=row)
    client = PgDurableClient(pool)
    await client.start(sleep(5), label="my-label")
    args = conn.fetchrow.call_args[0]
    assert "df.sleep(5)" in args[0]   # dsl interpolated
    assert "$1" in args[0]            # label is the only bound parameter
    assert args[1] == "my-label"


@pytest.mark.asyncio
async def test_start_with_label_and_database():
    row = MagicMock()
    row.__getitem__ = lambda self, i: "id"
    pool, conn = _make_pool(fetchrow_return=row)
    client = PgDurableClient(pool)
    await client.start(sleep(1), label="lbl", database="mydb")
    args = conn.fetchrow.call_args[0]
    assert "$1" in args[0] and "$2" in args[0]
    assert args[1] == "lbl"
    assert args[2] == "mydb"


@pytest.mark.asyncio
async def test_start_with_database_only():
    row = MagicMock()
    row.__getitem__ = lambda self, i: "id"
    pool, conn = _make_pool(fetchrow_return=row)
    client = PgDurableClient(pool)
    await client.start(sleep(1), database="mydb")
    args = conn.fetchrow.call_args[0]
    assert "NULL" in args[0]        # literal NULL for missing label
    assert "$1" in args[0]
    assert args[1] == "mydb"


# ---------------------------------------------------------------------------
# cancel
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancel_calls_correct_sql():
    pool, conn = _make_pool()
    client = PgDurableClient(pool)
    await client.cancel("inst-1", reason="timeout")
    conn.execute.assert_awaited_once_with(
        "SELECT df.cancel($1, $2)", "inst-1", "timeout"
    )


@pytest.mark.asyncio
async def test_cancel_default_reason():
    pool, conn = _make_pool()
    client = PgDurableClient(pool)
    await client.cancel("inst-1")
    conn.execute.assert_awaited_once_with(
        "SELECT df.cancel($1, $2)", "inst-1", "Cancelled by user"
    )


# ---------------------------------------------------------------------------
# signal
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_signal_with_data():
    pool, conn = _make_pool()
    client = PgDurableClient(pool)
    await client.signal("inst-1", "approved", data={"user": "alice"})
    conn.execute.assert_awaited_once_with(
        "SELECT df.signal($1, $2, $3)",
        "inst-1",
        "approved",
        json.dumps({"user": "alice"}),
    )


@pytest.mark.asyncio
async def test_signal_no_data_sends_empty_object():
    pool, conn = _make_pool()
    client = PgDurableClient(pool)
    await client.signal("inst-1", "ping")
    conn.execute.assert_awaited_once_with(
        "SELECT df.signal($1, $2, $3)", "inst-1", "ping", "{}"
    )


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_status_returns_string():
    row = MagicMock()
    row.__getitem__ = lambda self, i: "completed"
    pool, conn = _make_pool(fetchrow_return=row)
    client = PgDurableClient(pool)
    result = await client.status("inst-1")
    assert result == "completed"
    conn.fetchrow.assert_awaited_once_with("SELECT df.status($1)", "inst-1")


# ---------------------------------------------------------------------------
# result
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_result_parses_json():
    row = MagicMock()
    row.__getitem__ = lambda self, i: '{"answer": 42}'
    row.__bool__ = lambda self: True
    pool, conn = _make_pool(fetchrow_return=row)
    client = PgDurableClient(pool)
    result = await client.result("inst-1")
    assert result == {"answer": 42}


@pytest.mark.asyncio
async def test_result_none_when_no_row():
    pool, conn = _make_pool(fetchrow_return=None)
    client = PgDurableClient(pool)
    result = await client.result("inst-1")
    assert result is None


# ---------------------------------------------------------------------------
# list_instances
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_instances_calls_fetch():
    pool, conn = _make_pool(fetch_return=[])
    client = PgDurableClient(pool)
    result = await client.list_instances(status="running", limit=50)
    assert result == []
    conn.fetch.assert_awaited_once_with(
        "SELECT * FROM df.list_instances($1, $2)", "running", 50
    )


@pytest.mark.asyncio
async def test_list_instances_maps_to_dicts():
    fake_row = {"id": "abc", "status": "completed"}

    class FakeRecord(dict):
        pass

    record = FakeRecord(fake_row)
    pool, conn = _make_pool(fetch_return=[record])
    client = PgDurableClient(pool)
    result = await client.list_instances()
    assert result == [{"id": "abc", "status": "completed"}]


# ---------------------------------------------------------------------------
# getvar / unsetvar / clearvars
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_getvar_returns_value():
    row = MagicMock()
    row.__getitem__ = lambda self, i: "hello"
    pool, conn = _make_pool(fetchrow_return=row)
    client = PgDurableClient(pool)
    result = await client.getvar("my_key")
    assert result == "hello"
    conn.fetchrow.assert_awaited_once_with("SELECT df.getvar($1)", "my_key")


@pytest.mark.asyncio
async def test_getvar_returns_none_when_no_row():
    pool, conn = _make_pool(fetchrow_return=None)
    client = PgDurableClient(pool)
    result = await client.getvar("missing")
    assert result is None


@pytest.mark.asyncio
async def test_unsetvar_calls_correct_sql():
    pool, conn = _make_pool()
    client = PgDurableClient(pool)
    await client.unsetvar("my_key")
    conn.execute.assert_awaited_once_with("SELECT df.unsetvar($1)", "my_key")


@pytest.mark.asyncio
async def test_clearvars_calls_correct_sql():
    pool, conn = _make_pool()
    client = PgDurableClient(pool)
    await client.clearvars()
    conn.execute.assert_awaited_once_with("SELECT df.clearvars()")


# ---------------------------------------------------------------------------
# grant_usage / revoke_usage
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_grant_usage_defaults():
    pool, conn = _make_pool()
    client = PgDurableClient(pool)
    await client.grant_usage("analyst")
    conn.execute.assert_awaited_once_with(
        "SELECT df.grant_usage($1, $2, $3)", "analyst", False, False
    )


@pytest.mark.asyncio
async def test_grant_usage_with_http_and_grant():
    pool, conn = _make_pool()
    client = PgDurableClient(pool)
    await client.grant_usage("analyst", include_http=True, with_grant=True)
    conn.execute.assert_awaited_once_with(
        "SELECT df.grant_usage($1, $2, $3)", "analyst", True, True
    )


@pytest.mark.asyncio
async def test_revoke_usage_calls_correct_sql():
    pool, conn = _make_pool()
    client = PgDurableClient(pool)
    await client.revoke_usage("analyst")
    conn.execute.assert_awaited_once_with("SELECT df.revoke_usage($1)", "analyst")
