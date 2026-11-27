"""Focused live tests for PostgreSQL/AGE JSON parameters and float precision (#421, #422)."""

from __future__ import annotations

import json
import math
import uuid
from typing import Any

import pytest
from conftest import get_test_host, is_port_open
from voyager_ogm import Json, Session
from voyager_ogm.agtype import decode_agtype
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

pytestmark = pytest.mark.live


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


def _same_value(actual: Any, expected: Any) -> bool:
    if isinstance(expected, float) and math.isnan(expected):
        return isinstance(actual, float) and math.isnan(actual)
    if isinstance(expected, dict):
        return (
            isinstance(actual, dict)
            and actual.keys() == expected.keys()
            and all(_same_value(actual[k], expected[k]) for k in expected)
        )
    if isinstance(expected, list):
        return (
            isinstance(actual, list)
            and len(actual) == len(expected)
            and all(_same_value(a, e) for a, e in zip(actual, expected, strict=False))
        )
    return actual == expected


@pytest.fixture(scope="module")
def pg19_connections() -> Any:
    conn = _connect(PG19_PORT)
    bridge = Session(bridge=PostgresBridge(conn), dialect="sql_pgq")
    native = Session(_uri(PG19_PORT), dialect="sql_pgq", backend="native", pool_size=2)
    try:
        yield conn, bridge, native
    finally:
        native.close()
        conn.close()


@pytest.mark.parametrize(
    "value",
    [
        {},
        {"k": 1},
        {"k": [1, {"nested": True}]},
        [],
        [{}],
        [[]],
        [1, 2],
        [1, {"k": 2}],
        "text",
        3,
        True,
    ],
)
def test_pg_jsonb_parameter_matches_psycopg(pg19_connections: Any, value: Any) -> None:
    conn, bridge, native = pg19_connections
    from psycopg.types.json import Jsonb

    with conn.cursor() as cur:
        cur.execute("SELECT %s::jsonb AS v", (Jsonb(value),))
        expected = cur.fetchone()[0]

    native_value = native.execute("SELECT $v::jsonb AS v", {"v": value}).first()["v"]
    bridge_value = bridge.execute("SELECT $v::jsonb AS v", {"v": value}).first()["v"]
    assert _same_value(native_value, expected)
    assert _same_value(bridge_value, expected)


@pytest.mark.parametrize("value", [[], [1, 2], [{}], {"k": [1]}])
def test_pg_json_wrapper_matches_psycopg_without_cast(pg19_connections: Any, value: Any) -> None:
    conn, bridge, native = pg19_connections
    from psycopg.types.json import Jsonb

    with conn.cursor() as cur:
        cur.execute("SELECT %s AS v", (Jsonb(value),))
        expected = cur.fetchone()[0]

    native_value = native.execute("SELECT $v AS v", {"v": Json(value)}).first()["v"]
    bridge_value = bridge.execute("SELECT $v AS v", {"v": Json(value)}).first()["v"]
    assert _same_value(native_value, expected)
    assert _same_value(bridge_value, expected)


def test_pg_native_arrow_and_polars_empty_json_array(
    pg19_connections: Any,
) -> None:
    _, _, native = pg19_connections
    arrow = native.execute_to_arrow("SELECT $v::jsonb AS v", {"v": []})
    assert arrow.column("v")[0].as_py() == "[]"
    polars = native.execute_to_polars("SELECT $v::jsonb AS v", {"v": []})
    assert polars["v"][0] == "[]"


@pytest.mark.parametrize(
    "value",
    [
        2.332717393807508e-41,
        1.7976931348623157e308,
        -1.7976931348623157e308,
        1e-308,
        1.3884398771378935e25,
    ],
)
def test_pg_jsonb_float_round_trips_exactly(pg19_connections: Any, value: float) -> None:
    conn, bridge, native = pg19_connections
    from psycopg.types.json import Jsonb

    with conn.cursor() as cur:
        cur.execute("SELECT %s::jsonb AS v", (Jsonb({"k": value}),))
        expected = cur.fetchone()[0]

    native_value = native.execute("SELECT $v::jsonb AS v", {"v": {"k": value}}).first()["v"]
    bridge_value = bridge.execute("SELECT $v::jsonb AS v", {"v": {"k": value}}).first()["v"]
    assert native_value["k"] == expected["k"]
    assert bridge_value["k"] == expected["k"]


def test_pg_jsonb_rejects_non_finite_like_psycopg(
    pg19_connections: Any,
) -> None:
    conn, bridge, native = pg19_connections
    from psycopg.types.json import Jsonb

    invalid_text = (
        (psycopg.errors.InvalidTextRepresentation, ValueError)
        if psycopg is not None
        else (ValueError,)
    )
    for payload in [
        {"k": float("inf")},
        {"k": float("-inf")},
        {"k": float("nan")},
    ]:
        with pytest.raises(invalid_text):
            with conn.cursor() as cur:
                cur.execute("SELECT %s::jsonb AS v", (Jsonb(payload),))
        with pytest.raises(invalid_text):
            native.execute("SELECT $v::jsonb AS v", {"v": payload})
        with pytest.raises(invalid_text):
            bridge.execute("SELECT $v::jsonb AS v", {"v": payload})


def test_pg_jsonb_out_of_range_and_large_integer_values(
    pg19_connections: Any,
) -> None:
    conn, bridge, native = pg19_connections

    for literal in [
        '{"k": 1e400}',
        '{"k": -1e400}',
        '{"k": 1e-400}',
        '{"k": 13884398771378935000000000}',
        '{"k": -13884398771378935000000000}',
    ]:
        with conn.cursor() as cur:
            cur.execute(f"SELECT '{literal}'::jsonb AS v")
            expected = cur.fetchone()[0]
        native_value = native.execute(f"SELECT '{literal}'::jsonb AS v").first()["v"]
        bridge_value = bridge.execute(f"SELECT '{literal}'::jsonb AS v").first()["v"]
        assert _same_value(native_value, expected)
        assert _same_value(bridge_value, expected)


@pytest.mark.parametrize(
    "value",
    [
        1.3884398771378935e25,
        [-966116.6771473465],
        {"k": 2.332717393807508e-41},
        [1e308, -1e308, 1e-308],
    ],
)
def test_age_float_parameters_round_trip_exactly(value: Any) -> None:
    conn = _connect(AGE_PORT)
    graph = f"zz_json_age_{uuid.uuid4().hex[:8]}"
    bridge = None
    native = None
    try:
        with conn.cursor() as cur:
            cur.execute("LOAD 'age'; SET search_path = ag_catalog, \"$user\", public;")
            cur.execute(f"SELECT ag_catalog.create_graph('{graph}')")

        bridge = Session(bridge=PostgresBridge(conn, graph_name=graph), dialect="age")
        native = Session(
            _uri(AGE_PORT), dialect="age", graph_name=graph, backend="native", pool_size=1
        )
        statement = f"SELECT * FROM cypher('{graph}', $$ RETURN $v $$, %s) AS (v agtype)"
        with conn.cursor() as cur:
            cur.execute(statement, (json.dumps({"v": value}),))
            expected = decode_agtype(cur.fetchone()[0])

        native_value = native.execute(statement, {"v": value}).first()["v"]
        bridge_value = bridge.execute(statement, {"v": value}).first()["v"]
        assert _same_value(native_value, expected)
        assert _same_value(bridge_value, expected)
    finally:
        if native is not None:
            native.close()
        try:
            with conn.cursor() as cur:
                cur.execute("LOAD 'age'; SET search_path = ag_catalog, \"$user\", public;")
                cur.execute(f"SELECT ag_catalog.drop_graph('{graph}', true)")
        finally:
            conn.close()


def test_age_json_wrapper_passes_map_and_list() -> None:
    conn = _connect(AGE_PORT)
    graph = f"zz_json_age_{uuid.uuid4().hex[:8]}"
    bridge = None
    native = None
    try:
        with conn.cursor() as cur:
            cur.execute("LOAD 'age'; SET search_path = ag_catalog, \"$user\", public;")
            cur.execute(f"SELECT ag_catalog.create_graph('{graph}')")

        bridge = Session(bridge=PostgresBridge(conn, graph_name=graph), dialect="age")
        native = Session(
            _uri(AGE_PORT), dialect="age", graph_name=graph, backend="native", pool_size=1
        )
        statement = f"SELECT * FROM cypher('{graph}', $$ RETURN $v $$, %s) AS (v agtype)"
        for value in [{"k": [1, 2]}, [1, {"k": 2}], []]:
            with conn.cursor() as cur:
                cur.execute(statement, (json.dumps({"v": value}),))
                expected = decode_agtype(cur.fetchone()[0])
            native_value = native.execute(statement, {"v": Json(value)}).first()["v"]
            bridge_value = bridge.execute(statement, {"v": Json(value)}).first()["v"]
            assert _same_value(native_value, expected)
            assert _same_value(bridge_value, expected)
    finally:
        if native is not None:
            native.close()
        try:
            with conn.cursor() as cur:
                cur.execute("LOAD 'age'; SET search_path = ag_catalog, \"$user\", public;")
                cur.execute(f"SELECT ag_catalog.drop_graph('{graph}', true)")
        finally:
            conn.close()
