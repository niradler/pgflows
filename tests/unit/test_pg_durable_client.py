from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from pgflows.dsl import sleep
from pgflows.pg_durable_client import (
    ExecutionRecord,
    InstanceInfo,
    InstanceNode,
    Metrics,
    PgDurableClient,
)


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
# execution-history surface: instance_info / instance_nodes /
# instance_executions / metrics
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_instance_info_parses_output_json():
    row = {
        "instance_id": "abc12345",
        "label": "my-run",
        "function_name": "graph",
        "function_version": "1.0.0",
        "current_execution_id": 1,
        "status": "completed",
        "output": '{"rows": [{"result": {"rows": 3}}]}',
    }
    pool, conn = _make_pool(fetchrow_return=row)
    client = PgDurableClient(pool)
    info = await client.instance_info("abc12345")
    assert isinstance(info, InstanceInfo)
    assert info.instance_id == "abc12345"
    assert info.status == "completed"
    assert info.output == {"rows": [{"result": {"rows": 3}}]}
    conn.fetchrow.assert_awaited_once_with("SELECT * FROM df.instance_info($1)", "abc12345")


@pytest.mark.asyncio
async def test_instance_info_none_when_missing():
    pool, conn = _make_pool(fetchrow_return=None)
    client = PgDurableClient(pool)
    assert await client.instance_info("nope") is None


@pytest.mark.asyncio
async def test_instance_nodes_maps_rows_and_decodes_result():
    rows = [
        {
            "execution_id": 1,
            "node_id": "n1",
            "node_type": "SQL",
            "query": "SELECT 1",
            "result_name": None,
            "left_node": None,
            "right_node": None,
            "status": "completed",
            "result": '{"rows": [{"x": 1}]}',
            "updated_at": None,
        },
        {
            "execution_id": 1,
            "node_id": "n2",
            "node_type": "THEN",
            "query": None,
            "result_name": "amount",
            "left_node": None,
            "right_node": None,
            "status": "completed",
            "result": '{"value": 42}',
            "updated_at": None,
        },
    ]
    pool, conn = _make_pool(fetch_return=rows)
    client = PgDurableClient(pool)
    nodes = await client.instance_nodes("inst-1", last_n_executions=3)
    assert all(isinstance(n, InstanceNode) for n in nodes)
    assert [n.node_type for n in nodes] == ["SQL", "THEN"]
    assert nodes[0].result == {"rows": [{"x": 1}]}
    assert nodes[1].result_name == "amount"
    conn.fetch.assert_awaited_once_with(
        "SELECT * FROM df.instance_nodes($1, $2)", "inst-1", 3
    )


@pytest.mark.asyncio
async def test_instance_executions_maps_rows():
    rows = [
        {
            "execution_id": 1,
            "status": "Completed",
            "event_count": 46,
            "duration_ms": 2483,
            "output": '{"row_count": 1}',
        }
    ]
    pool, conn = _make_pool(fetch_return=rows)
    client = PgDurableClient(pool)
    execs = await client.instance_executions("inst-1")
    assert isinstance(execs[0], ExecutionRecord)
    assert execs[0].duration_ms == 2483
    assert execs[0].event_count == 46
    assert execs[0].output == {"row_count": 1}
    conn.fetch.assert_awaited_once_with(
        "SELECT * FROM df.instance_executions($1, $2)", "inst-1", 5
    )


@pytest.mark.asyncio
async def test_metrics_maps_to_model():
    row = {
        "total_instances": 126,
        "running_instances": 1,
        "completed_instances": 110,
        "failed_instances": 15,
        "total_executions": 265,
        "total_events": 12103,
    }
    pool, conn = _make_pool(fetchrow_return=row)
    client = PgDurableClient(pool)
    m = await client.metrics()
    assert isinstance(m, Metrics)
    assert m.total_instances == 126 and m.completed_instances == 110
    conn.fetchrow.assert_awaited_once_with("SELECT * FROM df.metrics()")


@pytest.mark.asyncio
async def test_metrics_empty_when_no_row():
    pool, conn = _make_pool(fetchrow_return=None)
    client = PgDurableClient(pool)
    m = await client.metrics()
    assert isinstance(m, Metrics)
    assert m.total_instances == 0


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
