import pytest

from pgflows.config import PgflowsConfig


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("postgresql://u:p@h:5432/db", "postgresql://u:p@h:5432/db"),
        ("postgresql+psycopg://u:p@h/db", "postgresql://u:p@h/db"),
        ("postgresql+asyncpg://u:p@h/db", "postgresql://u:p@h/db"),
        ("postgres://u:p@h/db", "postgresql://u:p@h/db"),
        ("postgres+psycopg2://u:p@h/db", "postgresql://u:p@h/db"),
        ("POSTGRESQL+PSYCOPG://u:p@h/db", "postgresql://u:p@h/db"),
    ],
)
def test_dsn_driver_suffix_normalized(raw: str, expected: str):
    assert PgflowsConfig(dsn=raw).dsn == expected


def test_dsn_query_string_preserved():
    cfg = PgflowsConfig(dsn="postgresql+asyncpg://u:p@h/db?sslmode=require")
    assert cfg.dsn == "postgresql://u:p@h/db?sslmode=require"
