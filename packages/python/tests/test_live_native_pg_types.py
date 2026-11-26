"""Live tests for native PostgreSQL array decoding and typed parameters (#389, #390).

Covers PostgreSQL 19 Beta 3 (SQL/PGQ) and Apache AGE, comparing the native
``voyager-drivers`` client with the psycopg bridge. All schemas, tables and
graphs are unique and prefixed with ``zz`` and are dropped afterwards.
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
AGE_PORT = 5455
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
def test_pg19_native_array_columns_match_bridge() -> None:
    """Array columns return Python lists on native and match the psycopg bridge."""
    conn = _connect(PG19_PORT)
    schema = f"zz_native_arrays_{uuid.uuid4().hex[:12]}"
    table = "array_values"
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"')
            cur.execute(f'SET search_path TO "{schema}", public')
            cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    id bigint PRIMARY KEY,
                    bigints bigint[],
                    integers integer[],
                    reals double precision[],
                    numerics numeric[],
                    texts text[],
                    varchars varchar[],
                    booleans boolean[],
                    dates date[],
                    timestamps timestamp[],
                    nullable bigint[]
                )
                """
            )
            cur.execute(
                f"""
                INSERT INTO "{table}" VALUES
                (1, ARRAY[1,2,3], ARRAY[4,5], ARRAY[1.5,2.5],
                 ARRAY[1.25,2.5], ARRAY['a','b,c','NULL','quote"d','back\\slash'],
                 ARRAY['v1','v2'], ARRAY[true,false],
                 ARRAY[DATE '2026-01-15', DATE '2026-02-20'],
                 ARRAY[TIMESTAMP '2026-01-15 10:11:12'], NULL),
                (2, ARRAY[NULL,9], ARRAY[]::integer[], ARRAY[]::double precision[],
                 ARRAY[]::numeric[], ARRAY[]::text[], ARRAY[]::varchar[],
                 ARRAY[]::boolean[], ARRAY[]::date[], ARRAY[]::timestamp[], NULL)
                """
            )

        bridge_session = Session(bridge=PostgresBridge(conn), dialect="sql_pgq")
        native_session = Session(_uri(PG19_PORT), dialect="sql_pgq", backend="native", pool_size=1)
        try:
            native_session.execute(f'SET search_path TO "{schema}", public')
            statement = f'SELECT * FROM "{table}" ORDER BY id'
            bridge_rows = [_normalize(row) for row in bridge_session.execute(statement).all()]
            native_rows = [_normalize(row) for row in native_session.execute(statement).all()]
            assert native_rows == bridge_rows
            assert native_rows[0]["bigints"] == [1, 2, 3]
            assert native_rows[0]["integers"] == [4, 5]
            assert native_rows[0]["reals"] == [1.5, 2.5]
            assert native_rows[0]["numerics"] == [1.25, 2.5]
            assert native_rows[0]["texts"] == [
                "a",
                "b,c",
                "NULL",
                'quote"d',
                "back\\slash",
            ]
            assert native_rows[0]["booleans"] == [True, False]
            assert native_rows[0]["dates"] == ["2026-01-15", "2026-02-20"]
            assert native_rows[0]["timestamps"] == ["2026-01-15 10:11:12"]
            assert native_rows[0]["nullable"] is None
            assert native_rows[1]["bigints"] == [None, 9]
            assert native_rows[1]["integers"] == []
            assert native_rows[1]["texts"] == []

            table_result = native_session.execute_to_arrow(statement)
            import pyarrow as pa

            assert pa.types.is_list(table_result.schema.field("bigints").type)
            assert table_result.schema.field("bigints").type.value_type == pa.int64()
            assert table_result.schema.field("texts").type.value_type == pa.string()

            frame = native_session.execute_to_polars(f'SELECT bigints FROM "{table}" ORDER BY id')
            import polars as pl

            assert frame.schema["bigints"] == pl.List(pl.Int64)
            assert frame.to_dicts() == [{"bigints": [1, 2, 3]}, {"bigints": [None, 9]}]
        finally:
            native_session.close()
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


@pytest.mark.live
def test_pg19_native_multidimensional_array_is_rejected_clearly() -> None:
    """Multi-dimensional arrays are rejected with an actionable error instead of mis-decoded."""
    conn = _connect(PG19_PORT)
    native_session = Session(_uri(PG19_PORT), dialect="sql_pgq", backend="native", pool_size=1)
    try:
        with pytest.raises(ValueError, match="Multi-dimensional"):
            native_session.execute("SELECT ARRAY[[1,2],[3,4]] AS matrix, ARRAY[5,6] AS flat").all()
    finally:
        native_session.close()
        conn.close()


@pytest.mark.live
def test_pg19_native_typed_parameters_and_bare_boolean_predicate() -> None:
    """Typed parameters work with int4 columns, LIMIT/SKIP, lists and a bare GRAPH_TABLE predicate."""
    conn = _connect(PG19_PORT)
    schema = f"zz_native_params_{uuid.uuid4().hex[:12]}"
    graph = f"zz_native_params_graph_{uuid.uuid4().hex[:8]}"
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"')
            cur.execute(f'SET search_path TO "{schema}", public')
            cur.execute(
                """
                CREATE TABLE parameter_person (
                    __id bigint PRIMARY KEY,
                    name varchar,
                    age integer,
                    score double precision,
                    active boolean
                )
                """
            )
            cur.execute(
                """
                INSERT INTO parameter_person VALUES
                (1, 'Alice', 30, 95.5, true),
                (2, 'Bob', 20, 80.0, false)
                """
            )
            cur.execute(
                f'''CREATE PROPERTY GRAPH "{graph}"
                    VERTEX TABLES (parameter_person KEY (__id) LABEL "Person")'''
            )

        bridge_session = Session(bridge=PostgresBridge(conn), dialect="sql_pgq")
        native_session = Session(_uri(PG19_PORT), dialect="sql_pgq", backend="native", pool_size=1)
        try:
            native_session.execute(f'SET search_path TO "{schema}", public')
            graph_table = f'''GRAPH_TABLE ("{graph}" MATCH (n IS "Person")'''
            # A bare typed boolean is what previously failed with SQLSTATE 42P18.
            false_rows = native_session.execute(
                f"SELECT * FROM {graph_table} WHERE $p0 COLUMNS (n.age))", {"p0": False}
            ).all()
            true_rows = native_session.execute(
                f"SELECT * FROM {graph_table} WHERE $p0 COLUMNS (n.age))", {"p0": True}
            ).all()
            assert false_rows == []
            assert sorted(row["age"] for row in true_rows) == [20, 30]
            bridge_false = bridge_session.execute(
                f"SELECT * FROM {graph_table} WHERE $p0 COLUMNS (n.age))", {"p0": False}
            ).all()
            bridge_true = bridge_session.execute(
                f"SELECT * FROM {graph_table} WHERE $p0 COLUMNS (n.age))", {"p0": True}
            ).all()
            assert false_rows == bridge_false
            assert true_rows == bridge_true

            # int8 parameters compared with int4 columns, and LIMIT/SKIP.
            assert native_session.execute(
                "SELECT __id FROM parameter_person WHERE age = $age", {"age": 30}
            ).all() == [{"__id": 1}]
            assert native_session.execute(
                "SELECT __id FROM parameter_person ORDER BY __id LIMIT $n OFFSET $o",
                {"n": 1, "o": 1},
            ).all() == [{"__id": 2}]

            # text against varchar and a typed list against an int4 column.
            assert native_session.execute(
                "SELECT __id FROM parameter_person WHERE name = $name", {"name": "Bob"}
            ).all() == [{"__id": 2}]
            assert native_session.execute(
                "SELECT __id FROM parameter_person WHERE age = ANY($ages) ORDER BY __id",
                {"ages": [20, 99]},
            ).all() == [{"__id": 2}]
            assert (
                native_session.execute(
                    "SELECT __id FROM parameter_person WHERE age = ANY($ages)", {"ages": []}
                ).all()
                == []
            )
            assert native_session.execute(
                "SELECT __id FROM parameter_person WHERE score > $score", {"score": 90.0}
            ).all() == [{"__id": 1}]
            assert native_session.execute(
                "SELECT __id FROM parameter_person WHERE active = $active", {"active": True}
            ).all() == [{"__id": 1}]
            assert (
                native_session.execute(
                    "SELECT __id FROM parameter_person WHERE name IS NOT DISTINCT FROM $name",
                    {"name": None},
                ).all()
                == []
            )

            # Genuine strings stay untyped so the server resolves them from column
            # context, while typed date/timestamptz/uuid values bind with their PG OIDs.
            native_session.execute(
                "CREATE TABLE typed_values (d date, ts timestamptz, u uuid, j jsonb)"
            )
            ident = uuid.uuid4()
            native_session.execute(
                "INSERT INTO typed_values VALUES "
                f"('2024-01-02', '2024-01-02T03:04:05Z', '{ident}', '{{\"k\": 1}}')"
            )
            for column, value in (
                ("d", datetime.date(2024, 1, 2)),
                ("d", "2024-01-02"),
                ("u", str(ident)),
                ("j", {"k": 1}),
            ):
                assert native_session.execute(
                    f"SELECT count(*) AS c FROM typed_values WHERE {column} = $v", {"v": value}
                ).all() == [{"c": 1}], column
            assert native_session.execute(
                "SELECT count(*) AS c FROM typed_values WHERE ts >= $v",
                {"v": datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)},
            ).all() == [{"c": 1}]
        finally:
            native_session.close()
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


@pytest.mark.live
def test_age_native_arrays_and_typed_parameters_match_bridge() -> None:
    """Apache AGE array columns decode natively and cypher() parameter maps keep working."""
    conn = _connect(AGE_PORT)
    graph = f"zz_native_age_{uuid.uuid4().hex[:12]}"
    table = f"zz_native_age_arrays_{uuid.uuid4().hex[:8]}"
    try:
        with conn.cursor() as cur:
            cur.execute("LOAD 'age'; SET search_path = ag_catalog, \"$user\", public;")
            cur.execute(f"SELECT create_graph('{graph}')")
            cur.execute(
                f"""
                SELECT * FROM cypher('{graph}', $$
                    CREATE (:Person {{name: 'Alice', age: 30}}),
                           (:Person {{name: 'Bob', age: 20}})
                $$) AS (v agtype)
                """
            )
            cur.execute(f'CREATE TABLE "{table}" (id bigint PRIMARY KEY, values bigint[])')
            cur.execute(f'INSERT INTO "{table}" VALUES (1, ARRAY[1,2,3]), (2, ARRAY[]::bigint[])')

        bridge_session = Session(bridge=PostgresBridge(conn, graph_name=graph), dialect="age")
        native_session = Session(
            _uri(AGE_PORT),
            dialect="age",
            graph_name=graph,
            backend="native",
            pool_size=1,
        )
        try:
            statement = f'SELECT * FROM "{table}" ORDER BY id'
            assert native_session.execute(statement).all() == [
                {"id": 1, "values": [1, 2, 3]},
                {"id": 2, "values": []},
            ]
            assert (
                native_session.execute(statement).all() == bridge_session.execute(statement).all()
            )

            cypher_bool = (
                f"SELECT * FROM cypher('{graph}', $$ "
                "MATCH (n:Person) WHERE $p0 RETURN n.name AS name $$, %s"
                ") AS (name agtype)"
            )
            assert native_session.execute(cypher_bool, {"p0": False}).all() == []
            assert native_session.execute(cypher_bool, {"p0": True}).all() == [
                {"name": "Alice"},
                {"name": "Bob"},
            ]
            assert native_session.execute(cypher_bool, {"p0": False}).all() == (
                bridge_session.execute(cypher_bool, {"p0": False}).all()
            )

            cypher_int = (
                f"SELECT * FROM cypher('{graph}', $$ "
                "MATCH (n:Person) WHERE n.age = $p0 RETURN n.name AS name $$, %s"
                ") AS (name agtype)"
            )
            assert native_session.execute(cypher_int, {"p0": 30}).all() == [{"name": "Alice"}]

            cypher_list = (
                f"SELECT * FROM cypher('{graph}', $$ "
                "MATCH (n:Person) WHERE n.name IN $p0 RETURN n.name AS name $$, %s"
                ") AS (name agtype)"
            )
            assert sorted(
                row["name"]
                for row in native_session.execute(cypher_list, {"p0": ["Alice", "Bob"]}).all()
            ) == ["Alice", "Bob"]
        finally:
            native_session.close()
    finally:
        with conn.cursor() as cur:
            cur.execute("LOAD 'age'; SET search_path = ag_catalog, \"$user\", public;")
            cur.execute(f"SELECT drop_graph('{graph}', true)")
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.close()


@pytest.mark.live
def test_pg19_native_and_bridge_typed_uuid_datetime_and_numeric_scale_parity() -> None:
    """Native and psycopg bridge behave identically for UUID/aware-datetime against typed and text columns, and numeric(10,2) scale."""
    conn = _connect(PG19_PORT)
    schema = f"zz_native_uuid_dt_{uuid.uuid4().hex[:10]}"
    u = uuid.UUID("12345678-1234-5678-1234-567812345678")
    aware_midnight = datetime.datetime(2024, 1, 2, 0, 0, 0, tzinfo=datetime.UTC)
    aware_later = datetime.datetime(2024, 1, 2, 3, 0, 0, tzinfo=datetime.UTC)
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"')
            cur.execute(f'SET search_path TO "{schema}", public')
            cur.execute(
                """
                CREATE TABLE typed_parity (
                    id int PRIMARY KEY,
                    u uuid,
                    d date,
                    ts timestamptz,
                    txt text,
                    num numeric(10, 2)
                )
                """
            )
            cur.execute(
                "INSERT INTO typed_parity VALUES (1, %s, DATE '2024-01-02', %s, %s, 1.50)",
                (u, aware_midnight, str(u)),
            )

        bridge_session = Session(bridge=PostgresBridge(conn), dialect="sql_pgq")
        native_session = Session(_uri(PG19_PORT), dialect="sql_pgq", backend="native", pool_size=1)
        try:
            native_session.execute(f'SET search_path TO "{schema}", public')

            # Typed columns match on both native and bridge
            for q, params in (
                ("SELECT id FROM typed_parity WHERE u = $u", {"u": u}),
                ("SELECT id FROM typed_parity WHERE ts = $ts", {"ts": aware_midnight}),
                ("SELECT id FROM typed_parity WHERE txt = $u::text", {"u": u}),
            ):
                assert native_session.execute(q, params).all() == [{"id": 1}]
                assert bridge_session.execute(q, params).all() == [{"id": 1}]

            # Uncast uuid against text column fails on BOTH native and bridge
            for sess in (native_session, bridge_session):
                with pytest.raises((ValueError, RuntimeError, psycopg.Error)):
                    sess.execute("SELECT id FROM typed_parity WHERE txt = $u", {"u": u}).all()

            # date = aware_datetime compares as timestamptz (midnight matches, non-midnight does not) on BOTH
            for sess in (native_session, bridge_session):
                assert sess.execute(
                    "SELECT id FROM typed_parity WHERE d = $dt", {"dt": aware_midnight}
                ).all() == [{"id": 1}]
                assert (
                    sess.execute(
                        "SELECT id FROM typed_parity WHERE d = $dt", {"dt": aware_later}
                    ).all()
                    == []
                )

            # numeric(10, 2) preserves trailing zero scale
            n_num = native_session.execute("SELECT num FROM typed_parity WHERE id = 1").first()
            b_num = bridge_session.execute("SELECT num FROM typed_parity WHERE id = 1").first()
            assert n_num is not None and b_num is not None
            assert isinstance(n_num["num"], Decimal) and isinstance(b_num["num"], Decimal)
            assert str(n_num["num"]) == str(b_num["num"]) == "1.50"
        finally:
            native_session.close()
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()
