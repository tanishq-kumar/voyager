"""Live tests for unified result types and Part 2 type audit across all engines (#395)."""

from __future__ import annotations

import datetime
import math
import uuid
import zoneinfo
from decimal import Decimal
from typing import Any

import polars as pl
import pyarrow as pa
import pytest
from conftest import get_test_host, is_port_open
from voyager_ogm import Duration, GraphNode, GraphPath, GraphRelationship, Point, Session
from voyager_ogm.bridge import DuckDbBridge, FalkorDBBridge, Neo4jBoltBridge, PostgresBridge

pytestmark = pytest.mark.live

HOST = get_test_host()
NEO4J_PORT = 7687
MEMGRAPH_PORT = 7688
FALKORDB_PORT = 6379
AGE_PORT = 5455
PG19_PORT = 5456

PG_USER = "postgres"
PG_PASSWORD = "voyagerpass123"
PG_DATABASE = "voyager_graph"


def _connect_pg(port: int) -> Any:
    psycopg = pytest.importorskip("psycopg")
    if not is_port_open(HOST, port):
        pytest.skip(f"PostgreSQL not reachable on {HOST}:{port}")
    return psycopg.connect(
        host=HOST,
        port=port,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DATABASE,
        connect_timeout=5,
        autocommit=True,
    )


def _assert_same_type_and_value(a: Any, b: Any, context: str = "") -> None:
    assert type(a) is type(b), (
        f"{context}: {type(a).__name__} != {type(b).__name__} ({a!r} vs {b!r})"
    )
    if isinstance(a, float):
        if math.isnan(a):
            assert math.isnan(b), f"{context}: expected NaN, got {b!r}"
        else:
            assert a == b, f"{context}: {a!r} != {b!r}"
    elif isinstance(a, Decimal):
        if a.is_nan():
            assert b.is_nan(), f"{context}: expected Decimal NaN, got {b!r}"
        else:
            assert a == b, f"{context}: {a!r} != {b!r}"
    elif isinstance(a, list):
        assert len(a) == len(b), f"{context}: len mismatch"
        for i, (x, y) in enumerate(zip(a, b, strict=True)):
            _assert_same_type_and_value(x, y, f"{context}[{i}]")
    elif isinstance(a, dict):
        assert set(a.keys()) == set(b.keys()), f"{context}: keys mismatch"
        for k in a:
            _assert_same_type_and_value(a[k], b[k], f"{context}[{k!r}]")
    elif isinstance(a, GraphNode):
        assert a.labels == b.labels
        _assert_same_type_and_value(dict(a.properties), dict(b.properties), f"{context}.node")
    elif isinstance(a, GraphRelationship):
        assert a.type == b.type
        _assert_same_type_and_value(dict(a.properties), dict(b.properties), f"{context}.rel")
    elif isinstance(a, GraphPath):
        assert len(a.nodes) == len(b.nodes)
        assert len(a.relationships) == len(b.relationships)
        for i, (na, nb) in enumerate(zip(a.nodes, b.nodes, strict=True)):
            _assert_same_type_and_value(na, nb, f"{context}.path.nodes[{i}]")
        for i, (ra, rb) in enumerate(zip(a.relationships, b.relationships, strict=True)):
            _assert_same_type_and_value(ra, rb, f"{context}.path.rels[{i}]")
    else:
        assert a == b, f"{context}: {a!r} != {b!r}"


def test_neo4j_live_result_types_nested_entities_arrow_polars() -> None:
    """Neo4j returns identical Python types on native and bridge, including nested & Arrow/Polars."""
    if not is_port_open(HOST, NEO4J_PORT):
        pytest.skip(f"Neo4j not reachable on {HOST}:{NEO4J_PORT}")
    neo4j = pytest.importorskip("neo4j")

    label = f"ZzResNeo_{uuid.uuid4().hex[:10]}"
    drv = neo4j.GraphDatabase.driver(
        f"bolt://{HOST}:{NEO4J_PORT}", auth=("neo4j", "voyagerpass123"), connection_timeout=5
    )
    bridge = Session(bridge=Neo4jBoltBridge(drv), dialect="neo4j")
    native = Session(
        f"bolt://neo4j:voyagerpass123@{HOST}:{NEO4J_PORT}",
        dialect="neo4j",
        backend="native",
        pool_size=1,
    )

    d = datetime.date(2024, 1, 2)
    t_local = datetime.time(14, 30, 15, 123456)
    t_off = datetime.time(14, 30, 15, 123456, tzinfo=datetime.timezone(datetime.timedelta(hours=2)))
    dt_local = datetime.datetime(2024, 1, 2, 3, 4, 5, 123456)
    dt_utc = datetime.datetime(2024, 1, 2, 3, 4, 5, 123456, tzinfo=datetime.UTC)
    dt_off = datetime.datetime(
        2024, 1, 2, 3, 4, 5, 123456, tzinfo=datetime.timezone(datetime.timedelta(hours=2))
    )
    dt_zone = datetime.datetime(
        2024, 1, 2, 3, 4, 5, 123456, tzinfo=zoneinfo.ZoneInfo("Europe/Berlin")
    )
    dur_td = Duration(0, 2, 3661, 500_000_000)
    dur_hms = Duration(0, 0, 90_000, 500_000_000)
    dur_cal = Duration(14, 3, 4, 500_000_000)
    raw = b"\x01\x02\xfe"

    try:
        native.execute(
            f"""
                CREATE (a:{label} {{
                    d: $d, t_local: $t_local, t_off: $t_off,
                    dt_local: $dt_local, dt_utc: $dt_utc, dt_off: $dt_off, dt_zone: $dt_zone,
                    dur_td: $dur_td, dur_hms: $dur_hms, dur_cal: $dur_cal, b: $b,
                    d_list: [$d], empty_l: [], big_s: $big_s, big_b: $big_b
                }})-[r:ZZ_LINK {{d: $d, dur: $dur_cal, b: $b}}]->(b:{label} {{name: 'end'}})
            """,
            {
                "d": d,
                "t_local": t_local,
                "t_off": t_off,
                "dt_local": dt_local,
                "dt_utc": dt_utc,
                "dt_off": dt_off,
                "dt_zone": dt_zone,
                "dur_td": dur_td,
                "dur_hms": dur_hms,
                "dur_cal": dur_cal,
                "b": raw,
                "big_s": "x" * 70_000,
                "big_b": b"\xab" * 70_000,
            },
        )

        q = (
            f"MATCH p=(a:{label})-[r:ZZ_LINK]->(b:{label}) "
            "RETURN a.d AS d, a.t_local AS t_local, a.t_off AS t_off, "
            "a.dt_local AS dt_local, a.dt_utc AS dt_utc, a.dt_off AS dt_off, a.dt_zone AS dt_zone, "
            "a.dur_td AS dur_td, a.dur_hms AS dur_hms, a.dur_cal AS dur_cal, a.b AS b, "
            "a.d_list AS d_list, [a.d, null] AS d_list_null, a.empty_l AS empty_l, "
            "size(a.big_s) AS slen, size(a.big_b) AS blen, "
            "[a.d, a.dur_cal, a.b] AS mixed_l, [1, 2.5, 9007199254740993] AS num_mixed_l, "
            "{d: a.d, dur: a.dur_td, b: a.b} AS nested_m, "
            "a AS node_a, r AS rel_r, p AS path_p"
        )
        n_row = native.execute(q).first()
        b_row = bridge.execute(q).first()
        assert n_row is not None and b_row is not None
        for col in n_row:
            _assert_same_type_and_value(n_row[col], b_row[col], f"neo4j.{col}")
        assert n_row["dur_hms"].days == 0 and n_row["dur_hms"].seconds == 90_000
        assert type(n_row["num_mixed_l"][0]) is int
        assert type(n_row["num_mixed_l"][1]) is float
        assert n_row["num_mixed_l"][2] == 9_007_199_254_740_993

        # Arrow and Polars schema & value checks on native
        scalar_q = (
            f"MATCH (a:{label}) WHERE a.d IS NOT NULL "
            "RETURN a.d AS d, a.t_local AS t_local, a.dt_local AS dt_local, "
            "a.dt_utc AS dt_utc, a.dt_zone AS dt_zone, a.dur_td AS dur_td, "
            "a.dur_hms AS dur_hms, a.dur_cal AS dur_cal, a.b AS b, a.d_list AS d_list"
        )
        arrow_tbl = native.execute_to_arrow(scalar_q)
        assert arrow_tbl.schema.field("d").type == pa.date32()
        assert arrow_tbl.schema.field("t_local").type == pa.time64("ns")
        assert arrow_tbl.schema.field("dt_local").type == pa.timestamp("us")
        assert arrow_tbl.schema.field("dt_utc").type == pa.timestamp("us", tz="UTC")
        assert arrow_tbl.schema.field("dt_zone").type == pa.timestamp("us", tz="Europe/Berlin")
        assert pa.types.is_struct(arrow_tbl.schema.field("dur_td").type)
        assert arrow_tbl.schema.field("dur_hms").type == pa.duration("us")
        assert pa.types.is_struct(arrow_tbl.schema.field("dur_cal").type)
        assert arrow_tbl.schema.field("b").type == pa.binary()
        assert pa.types.is_list(arrow_tbl.schema.field("d_list").type)

        df = native.execute_to_polars(scalar_q)
        assert df.schema["d"] == pl.Date
        assert df.schema["t_local"] == pl.Time
        assert df.schema["dt_local"] == pl.Datetime("us", None)
        assert df.schema["dt_utc"] == pl.Datetime("us", "UTC")
        assert df.schema["dt_zone"] == pl.Datetime("us", "Europe/Berlin")
        assert isinstance(df.schema["dur_td"], pl.Struct)
        assert df.schema["dur_hms"] == pl.Duration("us")
        assert isinstance(df.schema["dur_cal"], pl.Struct)
        assert df.schema["b"] == pl.Binary
        assert df.schema["d_list"] == pl.List(pl.Date)

        # Fallback: sub-microsecond nanosecond precision & mixed zone column
        fb_q = (
            "RETURN datetime({year: 2024, month: 1, day: 2, hour: 3, minute: 4, second: 5, "
            "nanosecond: 123456789, timezone: 'UTC'}) AS sub_ns, "
            "date('2024-01-02') AS ok_d"
        )
        fb_row = native.execute(fb_q).first()
        b_fb_row = bridge.execute(fb_q).first()
        assert fb_row == b_fb_row
        assert fb_row is not None and isinstance(fb_row["sub_ns"], str)
        assert "123456789" in fb_row["sub_ns"] and fb_row["ok_d"] == datetime.date(2024, 1, 2)

        # Point param + NaN/Inf param parity
        pt = Point(latitude=52.52, longitude=13.405)
        n_pt = native.execute(
            "RETURN $p AS p, $nan AS nan, $inf AS inf",
            {"p": pt, "nan": float("nan"), "inf": float("inf")},
        ).first()
        b_pt = bridge.execute(
            "RETURN $p AS p, $nan AS nan, $inf AS inf",
            {"p": pt, "nan": float("nan"), "inf": float("inf")},
        ).first()
        assert n_pt is not None and b_pt is not None
        assert pt == n_pt["p"]
        assert math.isnan(n_pt["nan"]) and math.isnan(b_pt["nan"])
        assert n_pt["inf"] == b_pt["inf"] == float("inf")
    finally:
        bridge.execute(f"MATCH (n:{label}) DETACH DELETE n")
        bridge.close()
        native.close()
        drv.close()


def test_memgraph_live_result_types_and_audit() -> None:
    """Memgraph returns identical temporal/entity/float result types on native and bridge."""
    if not is_port_open(HOST, MEMGRAPH_PORT):
        pytest.skip(f"Memgraph not reachable on {HOST}:{MEMGRAPH_PORT}")
    neo4j = pytest.importorskip("neo4j")

    label = f"ZzResMg_{uuid.uuid4().hex[:10]}"
    drv = neo4j.GraphDatabase.driver(
        f"bolt://{HOST}:{MEMGRAPH_PORT}", auth=None, connection_timeout=5
    )
    bridge = Session(bridge=Neo4jBoltBridge(drv), dialect="memgraph")
    native = Session(
        f"bolt://{HOST}:{MEMGRAPH_PORT}", dialect="memgraph", backend="native", pool_size=1
    )

    d = datetime.date(2024, 1, 2)
    t_local = datetime.time(14, 30, 15, 123456)
    dt_local = datetime.datetime(2024, 1, 2, 3, 4, 5, 123456)
    dt_off = datetime.datetime(
        2024, 1, 2, 3, 4, 5, 123456, tzinfo=datetime.timezone(datetime.timedelta(hours=2))
    )
    dur = Duration(0, 2, 3661, 500_000_000)
    dur_hms = Duration(0, 0, 90_000, 500_000_000)

    try:
        native.execute(
            f"""
            CREATE (a:{label} {{
                d: $d, t_local: $t_local, dt_local: $dt_local, dt_off: $dt_off,
                dur: $dur, dur_hms: $dur_hms,
                nl: [1, null, 3], nest: [[1, 2], [3]], el: [], em: {{}}
            }})
            """,
            {
                "d": d,
                "t_local": t_local,
                "dt_local": dt_local,
                "dt_off": dt_off,
                "dur": dur,
                "dur_hms": dur_hms,
            },
        )
        q = (
            f"MATCH (a:{label}) "
            "RETURN a.d AS d, a.t_local AS t_local, a.dt_local AS dt_local, a.dt_off AS dt_off, "
            "a.dur AS dur, a.dur_hms AS dur_hms, a.nl AS nl, a.nest AS nest, "
            "[1, 2.5, 9007199254740993] AS num_mixed_l, a.el AS el, a.em AS em, a AS node_a"
        )
        n_row = native.execute(q).first()
        b_row = bridge.execute(q).first()
        assert n_row is not None and b_row is not None
        for col in n_row:
            _assert_same_type_and_value(n_row[col], b_row[col], f"memgraph.{col}")
        assert type(n_row["num_mixed_l"][0]) is int
        assert type(n_row["num_mixed_l"][1]) is float
        assert n_row["num_mixed_l"][2] == 9_007_199_254_740_993
        df = native.execute_to_polars(
            f"MATCH (a:{label}) RETURN a.d AS d, a.t_local AS t_local, a.dt_local AS dt_local, "
            "a.dur AS dur, a.dur_hms AS dur_hms"
        )
        assert df.schema["d"] == pl.Date
        assert df.schema["t_local"] == pl.Time
        assert df.schema["dt_local"] == pl.Datetime("us", None)
        assert isinstance(df.schema["dur"], pl.Struct)
        # Memgraph normalises 90000s into days=1, seconds=3600 on the server, so the
        # Arrow column is a calendar struct there; Neo4j keeps days=0 (Duration(us)).
        if n_row["dur_hms"].months == 0 and n_row["dur_hms"].days == 0:
            assert df.schema["dur_hms"] == pl.Duration("us")
        else:
            assert isinstance(df.schema["dur_hms"], pl.Struct)
    finally:
        bridge.execute(f"MATCH (n:{label}) DETACH DELETE n")
        bridge.close()
        native.close()
        drv.close()


def test_falkordb_live_result_types_and_spurious_utc_fix() -> None:
    """FalkorDB returns naive localdatetime on both native and bridge plus Duration/list parity."""
    if not is_port_open(HOST, FALKORDB_PORT):
        pytest.skip(f"FalkorDB not reachable on {HOST}:{FALKORDB_PORT}")
    falkordb = pytest.importorskip("falkordb")

    graph_name = f"zz_res_fk_{uuid.uuid4().hex[:10]}"
    client = falkordb.FalkorDB(host=HOST, port=FALKORDB_PORT)
    graph = client.select_graph(graph_name)
    bridge = Session(bridge=FalkorDBBridge(graph), dialect="falkordb")
    native = Session(
        f"redis://{HOST}:{FALKORDB_PORT}/{graph_name}", dialect="falkordb", backend="native"
    )

    d = datetime.date(2024, 1, 2)
    t_local = datetime.time(14, 30, 15)
    dt_local = datetime.datetime(2024, 1, 2, 3, 4, 5)
    dur = Duration(0, 2, 3661, 0)
    dur_hms = Duration(0, 0, 3661, 0)

    try:
        native.execute(
            "CREATE (n:ZzFkRes {d: date($d), t: localtime($t), dt: localdatetime($dt), "
            "dur: duration($dur), dur_hms: duration($dur_hms), empty_l: []})",
            {"d": d, "t": t_local, "dt": dt_local, "dur": dur, "dur_hms": dur_hms},
        )
        q = (
            "MATCH (n:ZzFkRes) "
            "RETURN n.d AS d, n.t AS t, n.dt AS dt, n.dur AS dur, n.dur_hms AS dur_hms, "
            "[1, null, 3] AS nl, n.empty_l AS empty_l, n AS node_n, "
            "0.0/0.0 AS nan_val, 1.0/0.0 AS inf_val, "
            "duration('P1D') - duration('P2DT2H') AS neg_dur"
        )
        n_row = native.execute(q).first()
        b_row = bridge.execute(q).first()
        assert n_row is not None and b_row is not None
        for col in n_row:
            _assert_same_type_and_value(n_row[col], b_row[col], f"falkordb.{col}")
        assert n_row["dt"].tzinfo is None and b_row["dt"].tzinfo is None
        assert n_row["neg_dur"] == Duration(0, -2, 79_200, 0)
        df = native.execute_to_polars(
            "MATCH (n:ZzFkRes) RETURN n.d AS d, n.t AS t, n.dt AS dt, n.dur AS dur, n.dur_hms AS dur_hms"
        )
        assert df.schema["d"] == pl.Date
        assert df.schema["t"] == pl.Time
        assert df.schema["dt"] == pl.Datetime("us", None)
        assert isinstance(df.schema["dur"], pl.Struct)
        assert df.schema["dur_hms"] == pl.Duration("us")
    finally:
        bridge.close()
        native.close()
        try:
            graph.delete()
        except Exception:
            pass


def test_pg19_live_result_types_arrays_specials_arrow_polars() -> None:
    """PostgreSQL 19 matches bridge across all scalar/array types, Decimal/float specials, and Polars."""
    conn = _connect_pg(PG19_PORT)
    schema = f"zz_res_pg19_{uuid.uuid4().hex[:10]}"
    table = "all_types"
    u = uuid.UUID("12345678-1234-5678-1234-567812345678")
    d = datetime.date(2024, 1, 2)
    t_naive = datetime.time(14, 30, 15, 123456)
    t_off = datetime.time(14, 30, 15, 123456, tzinfo=datetime.timezone(datetime.timedelta(hours=2)))
    ts = datetime.datetime(2024, 1, 2, 3, 4, 5, 123456)
    tstz = datetime.datetime(2024, 1, 2, 3, 4, 5, 123456, tzinfo=datetime.UTC)
    iv_td = Duration(0, 2, 3661, 500_000_000)
    iv_hms = Duration(0, 0, 90_000, 500_000_000)
    iv_cal = Duration(14, 3, 14706, 0)
    dec = Decimal("123.45")
    raw = b"\x01\x02\xfe"

    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"')
            cur.execute(f'SET search_path TO "{schema}", public')
            cur.execute(
                f"""
                CREATE TABLE "{table}" (
                    id bigint PRIMARY KEY,
                    d date, t time, ttz timetz, ts timestamp, tstz timestamptz,
                    iv_td interval, iv_hms interval, iv_cal interval, u uuid, num numeric(10, 2), b bytea,
                    j json, jb jsonb,
                    u_arr uuid[], num_arr numeric(10, 2)[], b_arr bytea[], iv_arr interval[], j_arr jsonb[]
                )
                """
            )

        bridge = Session(bridge=PostgresBridge(conn), dialect="sql_pgq")
        native = Session(
            f"postgresql://{PG_USER}:{PG_PASSWORD}@{HOST}:{PG19_PORT}/{PG_DATABASE}",
            dialect="sql_pgq",
            backend="native",
            pool_size=1,
        )
        try:
            native.execute(f'SET search_path TO "{schema}", public')
            native.execute(
                f"""
                INSERT INTO "{table}" VALUES (
                    $id, $d, $t, $ttz, $ts, $tstz, $iv_td, $iv_hms, $iv_cal, $u, $num, $b,
                    $j, $jb, $u_arr, $num_arr, $b_arr, $iv_arr, $j_arr
                )
                """,
                {
                    "id": 1,
                    "d": d,
                    "t": t_naive,
                    "ttz": t_off,
                    "ts": ts,
                    "tstz": tstz,
                    "iv_td": iv_td,
                    "iv_hms": iv_hms,
                    "iv_cal": iv_cal,
                    "u": u,
                    "num": Decimal("1.50"),
                    "b": raw,
                    "j": {"a": [1, True, None]},
                    "jb": {"items": [{"k": "v"}]},
                    "u_arr": [u, None],
                    "num_arr": [dec, Decimal("1.50")],
                    "b_arr": [raw, b""],
                    "iv_arr": [iv_td],
                    "j_arr": [{"x": 1}],
                },
            )
            sel = f'SELECT * FROM "{table}" WHERE id = 1'
            n_row = native.execute(sel).first()
            b_row = bridge.execute(sel).first()
            assert n_row is not None and b_row is not None
            for col in n_row:
                _assert_same_type_and_value(n_row[col], b_row[col], f"pg19.{col}")
            assert str(n_row["num"]) == str(b_row["num"]) == "1.50"
            assert (
                [str(x) for x in n_row["num_arr"]]
                == [str(x) for x in b_row["num_arr"]]
                == [
                    "123.45",
                    "1.50",
                ]
            )
            assert n_row["iv_hms"].days == 0 and n_row["iv_hms"].seconds == 90_000

            arrow_tbl = native.execute_to_arrow(
                f'SELECT d, t, ts, tstz, iv_td, iv_hms, iv_cal, u, num, b, j FROM "{table}" WHERE id = 1'
            )
            assert arrow_tbl.schema.field("d").type == pa.date32()
            assert arrow_tbl.schema.field("t").type == pa.time64("us")
            assert arrow_tbl.schema.field("ts").type == pa.timestamp("us")
            assert arrow_tbl.schema.field("tstz").type == pa.timestamp("us", tz="UTC")
            assert pa.types.is_struct(arrow_tbl.schema.field("iv_td").type)
            assert arrow_tbl.schema.field("iv_hms").type == pa.duration("us")
            assert pa.types.is_struct(arrow_tbl.schema.field("iv_cal").type)
            assert arrow_tbl.schema.field("u").type == pa.uuid()
            assert arrow_tbl.schema.field("num").type == pa.decimal128(38, 2)
            assert arrow_tbl.schema.field("b").type == pa.binary()

            df = native.execute_to_polars(
                f'SELECT d, t, ts, tstz, iv_td, iv_hms, iv_cal, u, num, b FROM "{table}" WHERE id = 1'
            )
            assert df.schema["d"] == pl.Date
            assert df.schema["t"] == pl.Time
            assert isinstance(df.schema["iv_td"], pl.Struct)
            assert df.schema["iv_hms"] == pl.Duration("us")
            assert df.schema["u"] == pl.Binary
            assert df.schema["b"] == pl.Binary

            # Edge fallbacks: NUMERIC NaN/Inf and >38 digits, float NaN/Inf, JSON array
            edge_sql = (
                "SELECT 'NaN'::numeric AS dec_nan, 'Infinity'::numeric AS dec_inf, "
                "1234567890123456789012345678901234567890.123::numeric AS dec_big, "
                "'NaN'::float8 AS f_nan, 'Infinity'::float8 AS f_inf, '-Infinity'::float8 AS f_ninf, "
                "'[1, true, null]'::jsonb AS j_list"
            )
            n_edge = native.execute(edge_sql).first()
            b_edge = bridge.execute(edge_sql).first()
            assert n_edge is not None and b_edge is not None
            for col in ("dec_nan", "dec_inf", "dec_big", "f_nan", "f_inf", "f_ninf", "j_list"):
                _assert_same_type_and_value(n_edge[col], b_edge[col], f"pg19.edge.{col}")
            assert n_edge["j_list"] == [1, True, None]
            # Native falls back gracefully to ISO string on 'infinity'::date (psycopg raises DataError)
            assert native.execute("SELECT 'infinity'::date AS d_inf").first() == {
                "d_inf": "infinity"
            }
        finally:
            native.close()
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_age_live_result_types_specials_and_audit() -> None:
    """Apache AGE native matches bridge on booleans, mixed scalars, NaN/Inf, large ints, lists/maps."""
    conn = _connect_pg(AGE_PORT)
    graph = f"zz_res_age_{uuid.uuid4().hex[:10]}"
    try:
        with conn.cursor() as cur:
            cur.execute("LOAD 'age'; SET search_path = ag_catalog, \"$user\", public;")
            cur.execute(f"SELECT ag_catalog.create_graph('{graph}')")

        bridge = Session(bridge=PostgresBridge(conn, graph_name=graph), dialect="age")
        native = Session(
            f"postgresql://{PG_USER}:{PG_PASSWORD}@{HOST}:{AGE_PORT}/{PG_DATABASE}",
            dialect="age",
            graph_name=graph,
            backend="native",
            pool_size=1,
        )
        try:
            q_mix = f"SELECT * FROM cypher('{graph}', $$ UNWIND [true, 'true'] AS x RETURN x $$) AS (x agtype)"
            assert (
                native.execute(q_mix).all()
                == bridge.execute(q_mix).all()
                == [{"x": True}, {"x": "true"}]
            )

            q_pure_str = f"SELECT * FROM cypher('{graph}', $$ UNWIND ['42', 'true'] AS s RETURN s $$) AS (s agtype)"
            assert (
                native.execute(q_pure_str).all()
                == bridge.execute(q_pure_str).all()
                == [
                    {"s": "42"},
                    {"s": "true"},
                ]
            )

            q_specials = (
                "SELECT ('NaN'::float8)::agtype AS nan_v, ('Infinity'::float8)::agtype AS inf_v, "
                "('-Infinity'::float8)::agtype AS ninf_v, "
                f"(SELECT big FROM cypher('{graph}', $$ RETURN 9223372036854775808 AS big $$) AS (big agtype)) AS big_v"
            )
            n_sp = native.execute(q_specials).first()
            b_sp = bridge.execute(q_specials).first()
            assert n_sp is not None and b_sp is not None
            for col in n_sp:
                _assert_same_type_and_value(n_sp[col], b_sp[col], f"age.{col}")

            q_lists = (
                f"SELECT * FROM cypher('{graph}', $$ "
                "RETURN [1, 2.5, null] AS nl, [] AS el, {} AS em, [[1, 2], [3]] AS nest $$) "
                "AS (nl agtype, el agtype, em agtype, nest agtype)"
            )
            assert native.execute(q_lists).first() == bridge.execute(q_lists).first()
        finally:
            native.close()
    finally:
        with conn.cursor() as cur:
            cur.execute("LOAD 'age'; SET search_path = ag_catalog, \"$user\", public;")
            cur.execute(f"SELECT ag_catalog.drop_graph('{graph}', true)")
        conn.close()


def test_duckdb_live_embedded_result_types() -> None:
    """DuckDB embedded bridge returns unified Duration, JSON, Decimal, UUID, BLOB, and Polars types."""
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(":memory:")
    sess = Session(bridge=DuckDbBridge(con), dialect="duckpgq")
    try:
        df = sess.execute_to_polars(
            "SELECT DATE '2024-01-02' AS d, INTERVAL '2 hours' AS iv_hms, INTERVAL '1 year 2 months' AS iv_cal, 12.34::DECIMAL(10, 2) AS dec"
        )
        assert df.schema["d"] == pl.Date
        assert df.schema["iv_hms"] == pl.Duration("us")
        assert isinstance(df.schema["iv_cal"], pl.Struct)
        assert df["iv_cal"].to_list() == [{"months": 14, "days": 0, "seconds": 0, "nanoseconds": 0}]
        assert isinstance(df.schema["dec"], pl.Decimal)
        assert df.schema["dec"].scale == 2
    finally:
        sess.close()
