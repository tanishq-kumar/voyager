"""Live native vs bridge typed parameter parity tests (#391).

Covers Neo4j, Memgraph, FalkorDB, Apache AGE, and PostgreSQL 19 Beta 3, verifying:
- Equality (`=`) and range (`>=` / `<`) comparisons for temporal, UUID, Decimal, and bytes parameters.
- `SET` / `CREATE` / `INSERT` followed by server-side type checks (`valueType` / `pg_typeof`).
- Round-tripping stored values back to Python (recording expected native Arrow/ISO vs bridge Python types).
- Genuine strings shaped like dates/UUIDs/decimals stay strings and are never coerced.

All labels, graphs, schemas, and tables use unique `zz`-prefixed names and are dropped in cleanup.
"""

from __future__ import annotations

import datetime
import math
import uuid
import zoneinfo
from decimal import Decimal
from typing import Any

import pytest
from conftest import get_test_host, is_port_open
from voyager_ogm import Duration, Session
from voyager_ogm.bridge import FalkorDBBridge, Neo4jBoltBridge, PostgresBridge

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


def _neo4j_uri() -> str:
    return f"bolt://neo4j:voyagerpass123@{HOST}:{NEO4J_PORT}"


def _memgraph_uri() -> str:
    return f"bolt://{HOST}:{MEMGRAPH_PORT}"


def _falkordb_uri(graph: str) -> str:
    return f"redis://{HOST}:{FALKORDB_PORT}/{graph}"


def _pg_uri(port: int) -> str:
    return f"postgresql://{PG_USER}:{PG_PASSWORD}@{HOST}:{port}/{PG_DATABASE}"


def _connect_psycopg(port: int) -> Any:
    psycopg = pytest.importorskip("psycopg")
    if not is_port_open(HOST, port):
        pytest.skip(f"PostgreSQL on {HOST}:{port} is not available")
    return psycopg.connect(
        host=HOST,
        port=port,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DATABASE,
        connect_timeout=5,
        autocommit=True,
    )


def _normalize_temporal_or_scalar(value: Any) -> Any:
    """Normalizes bridge driver types (neo4j.time, datetime, Decimal, UUID) for value parity."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, bytes):
        return list(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime.timedelta):
        from voyager_ogm.types import to_iso_duration

        return to_iso_duration(value)
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    if isinstance(value, (datetime.date, datetime.time)):
        return value.isoformat()
    # neo4j.time.Date / Time / DateTime / Duration
    iso_method = getattr(value, "iso_format", None)
    if callable(iso_method):
        return str(iso_method())
    if isinstance(value, list):
        return [_normalize_temporal_or_scalar(item) for item in value]
    if isinstance(value, dict):
        return {k: _normalize_temporal_or_scalar(v) for k, v in value.items()}
    return str(value)


@pytest.mark.live
def test_neo4j_native_typed_params_parity() -> None:
    """Neo4j native vs bridge: temporal, UUID, Decimal, bytes, nested lists/maps, and strings."""
    if not is_port_open(HOST, NEO4J_PORT):
        pytest.skip(f"Neo4j not reachable on {HOST}:{NEO4J_PORT}")
    neo4j = pytest.importorskip("neo4j")
    pytz = pytest.importorskip("pytz")

    tag = uuid.uuid4().hex[:10]
    label = f"ZzTypedNeo4j_{tag}"
    driver = neo4j.GraphDatabase.driver(
        f"bolt://{HOST}:{NEO4J_PORT}", auth=("neo4j", "voyagerpass123"), connection_timeout=5
    )
    bridge_session = Session(bridge=Neo4jBoltBridge(driver), dialect="neo4j")
    native_session = Session(_neo4j_uri(), dialect="neo4j", backend="native", pool_size=1)

    d = datetime.date(2024, 1, 2)
    d_lo, d_hi = datetime.date(2024, 1, 1), datetime.date(2024, 1, 3)
    t_naive = datetime.time(14, 30, 15, 123456)
    t_naive_lo, t_naive_hi = datetime.time(14, 0, 0), datetime.time(15, 0, 0)
    t_offset = datetime.time(
        14, 30, 15, 123456, tzinfo=datetime.timezone(datetime.timedelta(hours=2))
    )
    dt_naive = datetime.datetime(2024, 1, 2, 3, 4, 5, 123456)
    dt_naive_lo, dt_naive_hi = datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 3)
    dt_offset = datetime.datetime(
        2024, 1, 2, 3, 4, 5, 123456, tzinfo=datetime.timezone(datetime.timedelta(hours=2))
    )
    dt_offset_lo = datetime.datetime(
        2024, 1, 2, 0, 0, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=2))
    )
    dt_offset_hi = datetime.datetime(
        2024, 1, 2, 6, 0, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=2))
    )
    dt_zone = datetime.datetime(
        2024, 1, 2, 3, 4, 5, 123456, tzinfo=zoneinfo.ZoneInfo("Europe/Berlin")
    )
    dt_zone_lo = datetime.datetime(2024, 1, 2, 0, 0, 0, tzinfo=zoneinfo.ZoneInfo("Europe/Berlin"))
    dt_zone_hi = datetime.datetime(2024, 1, 2, 6, 0, 0, tzinfo=zoneinfo.ZoneInfo("Europe/Berlin"))
    berlin_tz = pytz.timezone("Europe/Berlin")
    bridge_dt_zone = berlin_tz.localize(dt_naive)
    bridge_dt_zone_lo = berlin_tz.localize(datetime.datetime(2024, 1, 2, 0, 0, 0))
    bridge_dt_zone_hi = berlin_tz.localize(datetime.datetime(2024, 1, 2, 6, 0, 0))
    td = datetime.timedelta(days=2, seconds=3661, microseconds=500000)
    raw_bytes = b"\x01\x02\xfe"
    u = uuid.UUID("12345678-1234-5678-1234-567812345678")
    dec = Decimal("123.45")

    try:
        # Native writes typed properties directly (bridge does not support UUID/Decimal/offset-time)
        with native_session.transaction():
            native_session.execute(
                f"""
                CREATE (n:{label} {{tag: 'native'}})
                SET n.d = $d,
                    n.t_naive = $t_naive,
                    n.t_offset = $t_offset,
                    n.dt_naive = $dt_naive,
                    n.dt_offset = $dt_offset,
                    n.dt_zone = $dt_zone,
                    n.td = $td,
                    n.b = $b,
                    n.u = $u,
                    n.dec = $dec,
                    n.dates = $dates,
                    n.raw_str = $raw_str
                """,
                {
                    "d": d,
                    "t_naive": t_naive,
                    "t_offset": t_offset,
                    "dt_naive": dt_naive,
                    "dt_offset": dt_offset,
                    "dt_zone": dt_zone,
                    "td": td,
                    "b": raw_bytes,
                    "u": u,
                    "dec": dec,
                    "dates": [d_lo, d],
                    "raw_str": "2024-01-02",
                },
            )

        # Bridge writes equivalent node for parity comparison on driver-supported types
        bridge_session.execute(
            f"""
            CREATE (n:{label} {{tag: 'bridge'}})
            SET n.d = $d,
                n.t_naive = $t_naive,
                n.dt_naive = $dt_naive,
                n.dt_offset = $dt_offset,
                n.dt_zone = $dt_zone,
                n.td = $td,
                n.b = $b,
                n.dates = $dates,
                n.raw_str = $raw_str
            """,
            {
                "d": d,
                "t_naive": t_naive,
                "dt_naive": dt_naive,
                "dt_offset": dt_offset,
                "dt_zone": bridge_dt_zone,
                "td": td,
                "b": raw_bytes,
                "dates": [d_lo, d],
                "raw_str": "2024-01-02",
            },
        )

        # Check server-side valueType on both nodes
        vt_query = (
            f"MATCH (n:{label} {{tag: 'native'}}) "
            "RETURN valueType(n.d) AS d, valueType(n.t_naive) AS t_naive, "
            "valueType(n.t_offset) AS t_offset, valueType(n.dt_naive) AS dt_naive, "
            "valueType(n.dt_offset) AS dt_offset, valueType(n.dt_zone) AS dt_zone, "
            "valueType(n.td) AS td, valueType(n.u) AS u, valueType(n.dec) AS dec, "
            "valueType(n.raw_str) AS raw_str"
        )
        vt = native_session.execute(vt_query).first()
        assert vt == {
            "d": "DATE NOT NULL",
            "t_naive": "LOCAL TIME NOT NULL",
            "t_offset": "ZONED TIME NOT NULL",
            "dt_naive": "LOCAL DATETIME NOT NULL",
            "dt_offset": "ZONED DATETIME NOT NULL",
            "dt_zone": "ZONED DATETIME NOT NULL",
            "td": "DURATION NOT NULL",
            "u": "STRING NOT NULL",
            "dec": "FLOAT NOT NULL",
            "raw_str": "STRING NOT NULL",
        }

        # Equality and range comparisons: native vs bridge
        for prop, val, lo, hi, b_val, b_lo, b_hi in (
            ("d", d, d_lo, d_hi, d, d_lo, d_hi),
            ("t_naive", t_naive, t_naive_lo, t_naive_hi, t_naive, t_naive_lo, t_naive_hi),
            ("dt_naive", dt_naive, dt_naive_lo, dt_naive_hi, dt_naive, dt_naive_lo, dt_naive_hi),
            (
                "dt_offset",
                dt_offset,
                dt_offset_lo,
                dt_offset_hi,
                dt_offset,
                dt_offset_lo,
                dt_offset_hi,
            ),
            (
                "dt_zone",
                dt_zone,
                dt_zone_lo,
                dt_zone_hi,
                bridge_dt_zone,
                bridge_dt_zone_lo,
                bridge_dt_zone_hi,
            ),
        ):
            eq_q = f"MATCH (n:{label}) WHERE n.{prop} = $p RETURN count(n) AS c"
            rng_q = (
                f"MATCH (n:{label}) WHERE n.{prop} >= $lo AND n.{prop} < $hi RETURN count(n) AS c"
            )
            assert native_session.execute(eq_q, {"p": val}).all() == [{"c": 2}]
            assert bridge_session.execute(eq_q, {"p": b_val}).all() == [{"c": 2}]
            assert native_session.execute(rng_q, {"lo": lo, "hi": hi}).all() == [{"c": 2}]
            assert bridge_session.execute(rng_q, {"lo": b_lo, "hi": b_hi}).all() == [{"c": 2}]

        # Offset time, duration, bytes, UUID, Decimal equality on native
        assert native_session.execute(
            f"MATCH (n:{label} {{tag: 'native'}}) WHERE n.t_offset = $p RETURN count(n) AS c",
            {"p": t_offset},
        ).all() == [{"c": 1}]
        assert native_session.execute(
            f"MATCH (n:{label} {{tag: 'native'}}) WHERE n.td = $p RETURN count(n) AS c",
            {"p": td},
        ).all() == [{"c": 1}]
        assert native_session.execute(
            f"MATCH (n:{label} {{tag: 'native'}}) WHERE n.b = $p RETURN count(n) AS c",
            {"p": raw_bytes},
        ).all() == [{"c": 1}]
        assert native_session.execute(
            f"MATCH (n:{label} {{tag: 'native'}}) WHERE n.u = $p RETURN count(n) AS c",
            {"p": u},
        ).all() == [{"c": 1}]
        assert native_session.execute(
            f"MATCH (n:{label} {{tag: 'native'}}) WHERE n.dec = $p RETURN count(n) AS c",
            {"p": dec},
        ).all() == [{"c": 1}]

        # Genuine string never matches DATE property
        assert native_session.execute(
            f"MATCH (n:{label} {{tag: 'native'}}) WHERE n.d = $p RETURN count(n) AS c",
            {"p": "2024-01-02"},
        ).all() == [{"c": 0}]

        # Nested map containing both a date and a genuine string
        nested_row = native_session.execute(
            f"MATCH (n:{label} {{tag: 'native'}}) WHERE n.d = $nested.d "
            "RETURN valueType($nested.d) AS d_vt, valueType($nested.raw) AS raw_vt, $nested AS nested",
            {"nested": {"d": d, "raw": "2024-01-02"}},
        ).first()
        assert nested_row is not None
        assert nested_row["d_vt"] == "DATE NOT NULL"
        assert nested_row["raw_vt"] == "STRING NOT NULL"
        assert nested_row["nested"] == {"d": d, "raw": "2024-01-02"}

        # Round-trip back to Python (native and bridge both return unified Python types)
        ret_q = (
            f"MATCH (n:{label} {{tag: 'native'}}) "
            "RETURN n.d AS d, n.t_naive AS t_naive, n.dt_naive AS dt_naive, "
            "n.dt_offset AS dt_offset, n.dt_zone AS dt_zone, n.td AS td, "
            "n.b AS b, n.u AS u, n.dec AS dec, n.dates AS dates"
        )
        native_row = native_session.execute(ret_q).first()
        bridge_row = bridge_session.execute(ret_q).first()
        assert native_row is not None and bridge_row is not None
        assert isinstance(native_row["d"], datetime.date) and native_row["d"] == d
        assert type(native_row["d"]) is type(bridge_row["d"]) and native_row["d"] == bridge_row["d"]
        assert native_row["t_naive"] == bridge_row["t_naive"] == t_naive
        assert native_row["dt_naive"] == bridge_row["dt_naive"] == dt_naive
        assert native_row["dt_offset"] == bridge_row["dt_offset"] == dt_offset
        assert native_row["dt_zone"] == bridge_row["dt_zone"] == dt_zone
        assert native_row["td"] == bridge_row["td"] == Duration.from_timedelta(td)
        assert native_row["b"] == bridge_row["b"] == raw_bytes
        assert native_row["u"] == bridge_row["u"] == str(u)
        assert native_row["dec"] == bridge_row["dec"] == 123.45
        assert native_row["dates"] == bridge_row["dates"] == [d_lo, d]

        # Non-finite Decimal sent as Float NaN/Inf on Bolt
        dec_sp = native_session.execute(
            "RETURN $nan AS nan_v, $inf AS inf_v, $ninf AS ninf_v",
            {
                "nan": Decimal("NaN"),
                "inf": Decimal("Infinity"),
                "ninf": Decimal("-Infinity"),
            },
        ).first()
        assert dec_sp is not None
        assert math.isnan(dec_sp["nan_v"])
        assert dec_sp["inf_v"] == float("inf")
        assert dec_sp["ninf_v"] == float("-inf")
    finally:
        try:
            bridge_session.execute(f"MATCH (n:{label}) DETACH DELETE n")
        finally:
            bridge_session.close()
            native_session.close()
            driver.close()


@pytest.mark.live
def test_memgraph_native_typed_params_parity() -> None:
    """Memgraph (Bolt v4.4 legacy temporal tags) native vs bridge parity."""
    if not is_port_open(HOST, MEMGRAPH_PORT):
        pytest.skip(f"Memgraph not reachable on {HOST}:{MEMGRAPH_PORT}")
    neo4j = pytest.importorskip("neo4j")
    pytz = pytest.importorskip("pytz")

    tag = uuid.uuid4().hex[:10]
    label = f"ZzTypedMg_{tag}"
    driver = neo4j.GraphDatabase.driver(
        f"bolt://{HOST}:{MEMGRAPH_PORT}", auth=None, connection_timeout=5
    )
    bridge_session = Session(bridge=Neo4jBoltBridge(driver), dialect="memgraph")
    native_session = Session(_memgraph_uri(), dialect="memgraph", backend="native", pool_size=1)

    d = datetime.date(2024, 1, 2)
    d_lo, d_hi = datetime.date(2024, 1, 1), datetime.date(2024, 1, 3)
    t_naive = datetime.time(14, 30, 15, 123456)
    dt_naive = datetime.datetime(2024, 1, 2, 3, 4, 5, 123456)
    dt_offset = datetime.datetime(
        2024, 1, 2, 3, 4, 5, 123456, tzinfo=datetime.timezone(datetime.timedelta(hours=2))
    )
    dt_offset_lo = datetime.datetime(
        2024, 1, 2, 0, 0, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=2))
    )
    dt_offset_hi = datetime.datetime(
        2024, 1, 2, 6, 0, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=2))
    )
    dt_zone = datetime.datetime(
        2024, 1, 2, 3, 4, 5, 123456, tzinfo=zoneinfo.ZoneInfo("Europe/Berlin")
    )
    bridge_dt_zone = pytz.timezone("Europe/Berlin").localize(dt_naive)
    td = datetime.timedelta(days=2, seconds=3661, microseconds=500000)
    u = uuid.UUID("12345678-1234-5678-1234-567812345678")
    dec = Decimal("123.45")

    try:
        native_session.execute(
            f"""
            CREATE (n:{label} {{tag: 'native'}})
            SET n.d = $d,
                n.t_naive = $t_naive,
                n.dt_naive = $dt_naive,
                n.dt_offset = $dt_offset,
                n.dt_zone = $dt_zone,
                n.td = $td,
                n.u = $u,
                n.dec = $dec,
                n.raw_str = $raw_str
            """,
            {
                "d": d,
                "t_naive": t_naive,
                "dt_naive": dt_naive,
                "dt_offset": dt_offset,
                "dt_zone": dt_zone,
                "td": td,
                "u": u,
                "dec": dec,
                "raw_str": "2024-01-02",
            },
        )

        bridge_session.execute(
            f"""
            CREATE (n:{label} {{tag: 'bridge'}})
            SET n.d = $d,
                n.t_naive = $t_naive,
                n.dt_naive = $dt_naive,
                n.dt_offset = $dt_offset,
                n.dt_zone = $dt_zone,
                n.td = $td,
                n.raw_str = $raw_str
            """,
            {
                "d": d,
                "t_naive": t_naive,
                "dt_naive": dt_naive,
                "dt_offset": dt_offset,
                "dt_zone": bridge_dt_zone,
                "td": td,
                "raw_str": "2024-01-02",
            },
        )

        vt = native_session.execute(
            f"MATCH (n:{label} {{tag: 'native'}}) "
            "RETURN valueType(n.d) AS d, valueType(n.t_naive) AS t_naive, "
            "valueType(n.dt_naive) AS dt_naive, valueType(n.dt_offset) AS dt_offset, "
            "valueType(n.dt_zone) AS dt_zone, valueType(n.td) AS td, "
            "valueType(n.u) AS u, valueType(n.dec) AS dec, valueType(n.raw_str) AS raw_str"
        ).first()
        assert vt == {
            "d": "DATE",
            "t_naive": "LOCAL_TIME",
            "dt_naive": "LOCAL_DATE_TIME",
            "dt_offset": "ZONED_DATE_TIME",
            "dt_zone": "ZONED_DATE_TIME",
            "td": "DURATION",
            "u": "STRING",
            "dec": "FLOAT",
            "raw_str": "STRING",
        }

        for prop, val, lo, hi in (
            ("d", d, d_lo, d_hi),
            ("dt_offset", dt_offset, dt_offset_lo, dt_offset_hi),
        ):
            eq_q = f"MATCH (n:{label}) WHERE n.{prop} = $p RETURN count(n) AS c"
            rng_q = (
                f"MATCH (n:{label}) WHERE n.{prop} >= $lo AND n.{prop} < $hi RETURN count(n) AS c"
            )
            assert native_session.execute(eq_q, {"p": val}).all() == [{"c": 2}]
            assert bridge_session.execute(eq_q, {"p": val}).all() == [{"c": 2}]
            assert native_session.execute(rng_q, {"lo": lo, "hi": hi}).all() == [{"c": 2}]
            assert bridge_session.execute(rng_q, {"lo": lo, "hi": hi}).all() == [{"c": 2}]

        assert native_session.execute(
            f"MATCH (n:{label} {{tag: 'native'}}) WHERE n.t_naive = $p RETURN count(n) AS c",
            {"p": t_naive},
        ).all() == [{"c": 1}]
        assert native_session.execute(
            f"MATCH (n:{label} {{tag: 'native'}}) WHERE n.dt_naive = $p RETURN count(n) AS c",
            {"p": dt_naive},
        ).all() == [{"c": 1}]
        assert native_session.execute(
            f"MATCH (n:{label} {{tag: 'native'}}) WHERE n.dt_zone = $p RETURN count(n) AS c",
            {"p": dt_zone},
        ).all() == [{"c": 1}]
        assert native_session.execute(
            f"MATCH (n:{label} {{tag: 'native'}}) WHERE n.td = $p RETURN count(n) AS c",
            {"p": td},
        ).all() == [{"c": 1}]
        assert native_session.execute(
            f"MATCH (n:{label} {{tag: 'native'}}) WHERE n.u = $p RETURN count(n) AS c",
            {"p": u},
        ).all() == [{"c": 1}]
        assert native_session.execute(
            f"MATCH (n:{label} {{tag: 'native'}}) WHERE n.dec = $p RETURN count(n) AS c",
            {"p": dec},
        ).all() == [{"c": 1}]
        assert native_session.execute(
            f"MATCH (n:{label} {{tag: 'native'}}) WHERE n.d = $p RETURN count(n) AS c",
            {"p": "2024-01-02"},
        ).all() == [{"c": 0}]

        # Round-trip check
        ret_mg_q = (
            f"MATCH (n:{label} {{tag: 'native'}}) "
            "RETURN n.d AS d, n.t_naive AS t_naive, n.dt_naive AS dt_naive, "
            "n.dt_offset AS dt_offset, n.dt_zone AS dt_zone, n.td AS td, n.u AS u, n.dec AS dec"
        )
        row = native_session.execute(ret_mg_q).first()
        bridge_mg_row = bridge_session.execute(ret_mg_q).first()
        expected_mg = {
            "d": d,
            "t_naive": t_naive,
            "dt_naive": dt_naive,
            "dt_offset": dt_offset,
            "dt_zone": dt_zone,
            "td": Duration.from_timedelta(td),
            "u": str(u),
            "dec": 123.45,
        }
        assert row == bridge_mg_row == expected_mg
    finally:
        try:
            bridge_session.execute(f"MATCH (n:{label}) DETACH DELETE n")
        finally:
            bridge_session.close()
            native_session.close()
            driver.close()


@pytest.mark.live
def test_falkordb_native_typed_params_parity() -> None:
    """FalkorDB native vs bridge: temporal ISO adapters, Decimal, UUID, and compact temporal decode."""
    if not is_port_open(HOST, FALKORDB_PORT):
        pytest.skip(f"FalkorDB not reachable on {HOST}:{FALKORDB_PORT}")
    falkordb_pkg = pytest.importorskip("falkordb")

    graph_name = f"zz_typed_fk_{uuid.uuid4().hex[:10]}"
    client = falkordb_pkg.FalkorDB(host=HOST, port=FALKORDB_PORT)
    graph = client.select_graph(graph_name)
    bridge_session = Session(bridge=FalkorDBBridge(graph), dialect="falkordb")
    native_session = Session(_falkordb_uri(graph_name), dialect="falkordb", backend="native")

    d = datetime.date(2024, 1, 2)
    d_lo, d_hi = datetime.date(2024, 1, 1), datetime.date(2024, 1, 3)
    t_naive = datetime.time(14, 30, 15)
    dt_naive = datetime.datetime(2024, 1, 2, 3, 4, 5)
    td = datetime.timedelta(days=2, seconds=3661)
    dec = Decimal("123.45")
    u = uuid.UUID("12345678-1234-5678-1234-567812345678")

    try:
        # FalkorDB uses date($p)/localtime($p)/localdatetime($p)/duration($p) over ISO params
        create_q = (
            "CREATE (n:ZzFkTyped {d: date($d), t: localtime($t), dt: localdatetime($dt), "
            "dur: duration($td), dec: $dec, raw: $raw})"
        )
        params_common = {
            "d": d,
            "t": t_naive,
            "dt": dt_naive,
            "td": td,
            "dec": dec,
            "raw": "2024-01-02",
        }
        bridge_session.execute(create_q, params_common)
        native_session.execute(
            "CREATE (n:ZzFkNative {d: date($d), u: $u, dec: $dec})",
            {"d": d, "u": u, "dec": dec},
        )

        # Equality and range comparisons match bridge
        for q, p in (
            ("MATCH (n:ZzFkTyped) WHERE n.d = date($p) RETURN count(n) AS c", {"p": d}),
            (
                "MATCH (n:ZzFkTyped) WHERE n.d >= date($lo) AND n.d < date($hi) RETURN count(n) AS c",
                {"lo": d_lo, "hi": d_hi},
            ),
            ("MATCH (n:ZzFkTyped) WHERE n.t = localtime($p) RETURN count(n) AS c", {"p": t_naive}),
            (
                "MATCH (n:ZzFkTyped) WHERE n.dt = localdatetime($p) RETURN count(n) AS c",
                {"p": dt_naive},
            ),
            ("MATCH (n:ZzFkTyped) WHERE n.dur = duration($p) RETURN count(n) AS c", {"p": td}),
            ("MATCH (n:ZzFkTyped) WHERE n.dec = $p RETURN count(n) AS c", {"p": dec}),
            (
                "MATCH (n:ZzFkTyped) WHERE n.dec >= $lo AND n.dec < $hi RETURN count(n) AS c",
                {"lo": Decimal("100.0"), "hi": Decimal("200.0")},
            ),
        ):
            assert native_session.execute(q, p).all() == [{"c": 1}], q
            assert bridge_session.execute(q, p).all() == [{"c": 1}], q

        # Native UUID string comparison
        assert native_session.execute(
            "MATCH (n:ZzFkNative) WHERE n.u = $p RETURN count(n) AS c", {"p": u}
        ).all() == [{"c": 1}]

        # Round-trip back to Python (native and bridge both return unified Python types)
        ret_q = "MATCH (n:ZzFkTyped) RETURN n.d AS d, n.t AS t, n.dt AS dt, n.dur AS dur, n.dec AS dec, n.raw AS raw"
        native_row = native_session.execute(ret_q).first()
        bridge_row = bridge_session.execute(ret_q).first()
        expected_fk = {
            "d": d,
            "t": t_naive,
            "dt": dt_naive,
            "dur": Duration.from_timedelta(td),
            "dec": 123.45,
            "raw": "2024-01-02",
        }
        assert native_row == bridge_row == expected_fk
        assert type(native_row["dt"]) is datetime.datetime and native_row["dt"].tzinfo is None
        assert type(bridge_row["dt"]) is datetime.datetime and bridge_row["dt"].tzinfo is None
    finally:
        bridge_session.close()
        native_session.close()
        try:
            graph.delete()
        except Exception:
            pass


def _run_postgres_typed_param_suite(port: int, dialect: str) -> None:
    """Shared live PostgreSQL typed-parameter suite (PostgreSQL 19 Beta 3 and AGE server)."""
    conn = _connect_psycopg(port)
    schema = f"zz_typed_params_{uuid.uuid4().hex[:10]}"
    table = "typed_items"

    d = datetime.date(2024, 1, 2)
    d_lo, d_hi = datetime.date(2024, 1, 1), datetime.date(2024, 1, 3)
    t_naive = datetime.time(14, 30, 15, 123456)
    t_offset = datetime.time(
        14, 30, 15, 123456, tzinfo=datetime.timezone(datetime.timedelta(hours=2))
    )
    ts_naive = datetime.datetime(2024, 1, 2, 3, 4, 5, 123456)
    tstz = datetime.datetime(
        2024, 1, 2, 3, 4, 5, 123456, tzinfo=datetime.timezone(datetime.timedelta(hours=2))
    )
    tstz_zone = datetime.datetime(
        2024, 1, 2, 3, 4, 5, 123456, tzinfo=zoneinfo.ZoneInfo("Europe/Berlin")
    )
    td = datetime.timedelta(days=2, seconds=3661, microseconds=500000)
    u = uuid.UUID("12345678-1234-5678-1234-567812345678")
    dec = Decimal("123.45")
    raw_bytes = b"\x01\x02\xfe"

    fq_table = f'"{schema}"."{table}"'

    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"')
            cur.execute(
                f"""
                CREATE TABLE {fq_table} (
                    id integer PRIMARY KEY,
                    d date,
                    t time,
                    tz timetz,
                    ts timestamp,
                    tstz timestamptz,
                    iv interval,
                    u uuid,
                    num numeric,
                    b bytea,
                    d_arr date[],
                    u_arr uuid[],
                    raw_text text
                )
                """
            )

        bridge_session = Session(bridge=PostgresBridge(conn), dialect=dialect)
        native_session = Session(_pg_uri(port), dialect=dialect, backend="native", pool_size=1)
        try:
            native_session.execute(f'SET search_path TO "{schema}", public')

            # 1. Bare parameter pg_typeof() proves OIDs are sent (no column context!)
            typeof_sql = (
                "SELECT pg_typeof($d)::text AS d, pg_typeof($t)::text AS t, "
                "pg_typeof($tz)::text AS tz, pg_typeof($ts)::text AS ts, "
                "pg_typeof($tstz)::text AS tstz, pg_typeof($iv)::text AS iv, "
                "pg_typeof($u)::text AS u, pg_typeof($num)::text AS num, "
                "pg_typeof($b)::text AS b, pg_typeof($d_arr)::text AS d_arr, "
                "pg_typeof($u_arr)::text AS u_arr"
            )
            typeof_params = {
                "d": d,
                "t": t_naive,
                "tz": t_offset,
                "ts": ts_naive,
                "tstz": tstz,
                "iv": td,
                "u": u,
                "num": dec,
                "b": raw_bytes,
                "d_arr": [d_lo, d],
                "u_arr": [u],
            }
            native_types = native_session.execute(typeof_sql, typeof_params).first()
            bridge_types = bridge_session.execute(typeof_sql, typeof_params).first()
            assert native_types == {
                "d": "date",
                "t": "time without time zone",
                "tz": "time with time zone",
                "ts": "timestamp without time zone",
                "tstz": "timestamp with time zone",
                "iv": "interval",
                "u": "uuid",
                "num": "numeric",
                "b": "bytea",
                "d_arr": "date[]",
                "u_arr": "uuid[]",
            }
            assert bridge_types is not None
            for k, expected in native_types.items():
                assert bridge_types[k] == expected, k

            # Genuine strings deliberately stay untyped (OID 0 -> 42P18 in bare pg_typeof)
            with pytest.raises(ValueError, match="42P18"):
                native_session.execute("SELECT pg_typeof($raw)::text AS raw", {"raw": "2024-01-02"})

            # 2. Parameterized INSERT through native, then read/compare via both native and bridge
            native_session.execute(
                f"""
                INSERT INTO {fq_table} (id, d, t, tz, ts, tstz, iv, u, num, b, d_arr, u_arr, raw_text)
                VALUES ($id, $d, $t, $tz, $ts, $tstz, $iv, $u, $num, $b, $d_arr, $u_arr, $raw)
                """,
                {"id": 1, **typeof_params, "raw": "2024-01-02"},
            )
            native_session.execute(
                f"INSERT INTO {fq_table} (id, tstz) VALUES ($id, $tstz)",
                {"id": 2, "tstz": tstz_zone},
            )

            # Equality & range comparisons match bridge
            for col_name, val, lo, hi in (
                ("d", d, d_lo, d_hi),
                ("t", t_naive, datetime.time(14, 0), datetime.time(15, 0)),
                ("ts", ts_naive, datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 3)),
                (
                    "tstz",
                    tstz,
                    datetime.datetime(2024, 1, 2, 0, 0, tzinfo=datetime.UTC),
                    datetime.datetime(2024, 1, 2, 6, 0, tzinfo=datetime.UTC),
                ),
                ("iv", td, datetime.timedelta(days=1), datetime.timedelta(days=3)),
                ("num", dec, Decimal("100.00"), Decimal("200.00")),
            ):
                eq_sql = f"SELECT id FROM {fq_table} WHERE {col_name} = $p ORDER BY id"
                rng_sql = (
                    f"SELECT id FROM {fq_table} WHERE {col_name} >= $lo AND {col_name} < $hi "
                    "ORDER BY id"
                )
                assert native_session.execute(eq_sql, {"p": val}).all() == (
                    bridge_session.execute(eq_sql, {"p": val}).all()
                ), col_name
                assert native_session.execute(rng_sql, {"lo": lo, "hi": hi}).all() == (
                    bridge_session.execute(rng_sql, {"lo": lo, "hi": hi}).all()
                ), col_name

            for col_name, val in (
                ("tz", t_offset),
                ("u", u),
                ("b", raw_bytes),
                ("d_arr", [d_lo, d]),
            ):
                eq_sql = f"SELECT id FROM {fq_table} WHERE {col_name} = $p"
                assert native_session.execute(eq_sql, {"p": val}).all() == [{"id": 1}]
                assert bridge_session.execute(eq_sql, {"p": val}).all() == [{"id": 1}]

            # Round-trip check
            ret_pg_q = f"SELECT d, t, ts, tstz, iv, u, num, b, d_arr, u_arr, raw_text FROM {fq_table} WHERE id = 1"
            native_row = native_session.execute(ret_pg_q).first()
            bridge_row = bridge_session.execute(ret_pg_q).first()
            expected_pg = {
                "d": d,
                "t": t_naive,
                "ts": ts_naive,
                "tstz": datetime.datetime(2024, 1, 2, 1, 4, 5, 123456, tzinfo=datetime.UTC),
                "iv": Duration.from_timedelta(td),
                "u": u,
                "num": dec,
                "b": raw_bytes,
                "d_arr": [d_lo, d],
                "u_arr": [u],
                "raw_text": "2024-01-02",
            }
            assert native_row == bridge_row == expected_pg
            for key, expected_val in expected_pg.items():
                assert type(native_row[key]) is type(bridge_row[key]) is type(expected_val), key
        finally:
            native_session.close()
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


@pytest.mark.live
def test_pg19_native_typed_params_parity() -> None:
    """PostgreSQL 19 Beta 3 native vs bridge typed parameter parity."""
    _run_postgres_typed_param_suite(PG19_PORT, "sql_pgq")


@pytest.mark.live
def test_age_native_typed_params_and_cypher_maps() -> None:
    """Apache AGE server SQL typed params match bridge and cypher() parameter maps keep working."""
    _run_postgres_typed_param_suite(AGE_PORT, "sql_pgq")

    conn = _connect_psycopg(AGE_PORT)
    graph = f"zz_age_typed_{uuid.uuid4().hex[:10]}"
    try:
        with conn.cursor() as cur:
            cur.execute("LOAD 'age'; SET search_path = ag_catalog, \"$user\", public;")
            cur.execute(f"SELECT ag_catalog.create_graph('{graph}')")

        bridge_session = Session(bridge=PostgresBridge(conn, graph_name=graph), dialect="age")
        native_session = Session(
            _pg_uri(AGE_PORT), dialect="age", graph_name=graph, backend="native", pool_size=1
        )
        try:
            create_sql = (
                f"SELECT * FROM cypher('{graph}', $$ "
                "CREATE (n:ZzAgeTyped {name: $name, d: $d, u: $u, dec: $dec}) "
                "RETURN n.name AS name, n.d AS d, n.u AS u, n.dec AS dec $$, %s) "
                "AS (name agtype, d agtype, u agtype, dec agtype)"
            )
            native_row = native_session.execute(
                create_sql,
                {
                    "name": "Alice",
                    "d": datetime.date(2024, 1, 2),
                    "u": uuid.UUID("12345678-1234-5678-1234-567812345678"),
                    "dec": Decimal("123.45"),
                },
            ).first()
            bridge_row = bridge_session.execute(
                create_sql,
                {
                    "name": "Alice",
                    "d": "2024-01-02",
                    "u": "12345678-1234-5678-1234-567812345678",
                    "dec": 123.45,
                },
            ).first()
            assert (
                native_row
                == bridge_row
                == {
                    "name": "Alice",
                    "d": "2024-01-02",
                    "u": "12345678-1234-5678-1234-567812345678",
                    "dec": 123.45,
                }
            )
        finally:
            native_session.close()
    finally:
        with conn.cursor() as cur:
            cur.execute("LOAD 'age'; SET search_path = ag_catalog, \"$user\", public;")
            cur.execute(f"SELECT ag_catalog.drop_graph('{graph}', true)")
        conn.close()
