"""Live test for native PostgreSQL mixed-depth array columns (#440).

Uses PostgreSQL 19 Beta 3 and compares the native ``voyager-drivers`` client
with the psycopg bridge. The schema is unique, ``zz``-prefixed and dropped
afterwards.
"""

from __future__ import annotations

import datetime
import uuid
from decimal import Decimal
from typing import Any

import pytest
from conftest import get_test_host, is_port_open
from voyager_ogm import Session
from voyager_ogm.bridge import PostgresBridge

psycopg: Any = None
try:
    import psycopg
except ImportError:
    pass

PG19_PORT = 5456
PG_USER = "postgres"
PG_PASSWORD = "voyagerpass123"
PG_DATABASE = "voyager_graph"


def _uri(port: int) -> str:
    return f"postgresql://{PG_USER}:{PG_PASSWORD}@{get_test_host()}:{port}/{PG_DATABASE}"


def _connect(port: int) -> Any:
    if psycopg is None:
        pytest.skip("psycopg is not installed in this environment")
    host = get_test_host()
    if not is_port_open(host, port):
        pytest.skip(f"PostgreSQL on {host}:{port} is not available")
    return psycopg.connect(
        host=host,
        port=port,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DATABASE,
        connect_timeout=5,
        autocommit=True,
    )


def _normalize(value: Any) -> Any:
    """Normalize bridge-only Python types so native and bridge rows compare equal."""
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime.datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, (datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, list):
        return [_normalize(item) for item in value]
    if isinstance(value, dict):
        return {key: _normalize(item) for key, item in value.items()}
    return value


@pytest.mark.live
def test_pg19_native_mixed_depth_arrays_match_bridge() -> None:
    """Rows with different array depths decode per-row instead of raising a protocol error."""
    conn = _connect(PG19_PORT)
    native_session = Session(_uri(PG19_PORT), dialect="sql_pgq", backend="native", pool_size=1)
    schema = f"zz_native_mixed_arrays_{uuid.uuid4().hex[:12]}"
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"')
            cur.execute(f'SET search_path TO "{schema}", public')
            cur.execute("CREATE TABLE mixed_arrays (id int, ints int[], dates date[])")
            cur.execute(
                """
                INSERT INTO mixed_arrays VALUES
                (1, '{1,2}', '{2026-01-15}'),
                (2, '{{3,NULL},{5,6}}', '{{2026-01-15,NULL},{2026-02-20,2026-03-01}}'),
                (3, '{}', '{}'),
                (4, NULL, NULL)
                """
            )
        native_session.execute(f'SET search_path TO "{schema}", public')
        statement = "SELECT ints, dates FROM mixed_arrays ORDER BY id"
        bridge_session = Session(bridge=PostgresBridge(conn), dialect="sql_pgq")
        bridge_rows = [_normalize(row) for row in bridge_session.execute(statement).all()]
        native_rows = [_normalize(row) for row in native_session.execute(statement).all()]
        assert native_rows == bridge_rows
        assert native_rows == [
            {"ints": [1, 2], "dates": ["2026-01-15"]},
            {
                "ints": [[3, None], [5, 6]],
                "dates": [["2026-01-15", None], ["2026-02-20", "2026-03-01"]],
            },
            {"ints": [], "dates": []},
            {"ints": None, "dates": None},
        ]

        import pyarrow as pa

        table = native_session.execute_to_arrow(statement)
        for name in ("ints", "dates"):
            field = table.schema.field(name)
            assert field.type == pa.string()
            assert field.metadata[b"voyager.encoding"] == b"json"
        assert table.column("ints").to_pylist() == [
            "[1,2]",
            "[[3,null],[5,6]]",
            "[]",
            None,
        ]

        frame = native_session.execute_to_polars(statement)
        assert frame["ints"].to_list() == [
            "[1,2]",
            "[[3,null],[5,6]]",
            "[]",
            None,
        ]
    finally:
        native_session.close()
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()
