"""Live test for numeric/uuid elements in mixed-depth array columns (#440).

Runs against PostgreSQL 19 Beta 3 and compares the native ``voyager-drivers``
client with the psycopg bridge. The schema is unique, ``zz``-prefixed and
dropped afterwards.
"""

from __future__ import annotations

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


@pytest.mark.live
def test_pg19_mixed_depth_numeric_and_uuid_elements_match_bridge() -> None:
    """Mixed-depth numeric/uuid arrays keep Decimal/UUID elements like psycopg."""
    conn = _connect(PG19_PORT)
    native_session = Session(_uri(PG19_PORT), dialect="sql_pgq", backend="native", pool_size=1)
    schema = f"zz_native_mixed_typed_{uuid.uuid4().hex[:12]}"
    first = "12345678-1234-5678-1234-567812345678"
    second = "87654321-4321-8765-4321-876543218765"
    third = "00000000-0000-0000-0000-000000000001"
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"')
            cur.execute(f'SET search_path TO "{schema}", public')
            cur.execute("CREATE TABLE mixed_typed (id int, nums numeric[], ids uuid[])")
            cur.execute(
                f"""
                INSERT INTO mixed_typed VALUES
                (1, '{{1.5,2}}', '{{{first}}}'),
                (2, '{{{{3.25}},{{4}}}}', '{{{{{second}}},{{{third}}}}}'),
                (3, '{{}}', '{{}}'),
                (4, NULL, NULL)
                """
            )
        native_session.execute(f'SET search_path TO "{schema}", public')
        statement = "SELECT nums, ids FROM mixed_typed ORDER BY id"
        bridge_session = Session(bridge=PostgresBridge(conn), dialect="sql_pgq")
        bridge_rows = bridge_session.execute(statement).all()
        native_rows = native_session.execute(statement).all()
        assert native_rows == bridge_rows
        assert native_rows == [
            {
                "nums": [Decimal("1.5"), Decimal("2")],
                "ids": [uuid.UUID(first)],
            },
            {
                "nums": [[Decimal("3.25")], [Decimal("4")]],
                "ids": [[uuid.UUID(second)], [uuid.UUID(third)]],
            },
            {"nums": [], "ids": []},
            {"nums": None, "ids": None},
        ]

        import pyarrow as pa

        table = native_session.execute_to_arrow(statement)
        for name in ("nums", "ids"):
            field = table.schema.field(name)
            assert field.type == pa.string()
            assert field.metadata[b"voyager.encoding"] == b"json"
        assert table.column("nums").to_pylist() == [
            "[1.5,2]",
            "[[3.25],[4]]",
            "[]",
            None,
        ]
        assert table.column("ids").to_pylist() == [
            f'["{first}"]',
            f'[["{second}"],["{third}"]]',
            "[]",
            None,
        ]

        frame = native_session.execute_to_polars(statement)
        assert frame["nums"].to_list() == table.column("nums").to_pylist()
        assert frame["ids"].to_list() == table.column("ids").to_pylist()
    finally:
        native_session.close()
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()
