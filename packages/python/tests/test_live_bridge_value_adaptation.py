"""Focused live regression tests for bridge value adaptation (#418, #423, #424)."""

from __future__ import annotations

import datetime
import uuid
from zoneinfo import ZoneInfo

import pytest
from conftest import get_test_host, is_port_open
from voyager_ogm import Duration, Session
from voyager_ogm.bridge import DuckDbBridge, FalkorDBBridge, Neo4jBoltBridge

pytestmark = pytest.mark.live

HOST = get_test_host()


def _neo4j_session(port: int, dialect: str) -> tuple[Session, object]:
    neo4j = pytest.importorskip("neo4j")
    if not is_port_open(HOST, port):
        pytest.skip(f"{dialect} is not reachable on {HOST}:{port}")
    auth = ("neo4j", "voyagerpass123") if port == 7687 else None
    driver = neo4j.GraphDatabase.driver(f"bolt://{HOST}:{port}", auth=auth, connection_timeout=5.0)
    session = Session(bridge=Neo4jBoltBridge(driver), dialect=dialect)
    return session, driver


@pytest.mark.parametrize(("port", "dialect"), [(7687, "neo4j"), (7688, "memgraph")])
def test_live_bolt_bridge_zoneinfo_and_uuid(port: int, dialect: str) -> None:
    """ZoneInfo datetimes keep zone name/instant/fold and UUIDs arrive as strings."""
    session, driver = _neo4j_session(port, dialect)
    value = datetime.datetime(2024, 10, 27, 2, 30, 0, fold=1, tzinfo=ZoneInfo("Europe/Berlin"))
    try:
        row = session.execute(
            "RETURN $dt AS dt, $u AS u", {"dt": value, "u": uuid.UUID(int=0)}
        ).first()
        assert row is not None
        assert row["dt"].astimezone(datetime.UTC) == value.astimezone(datetime.UTC)
        assert getattr(row["dt"].tzinfo, "key", None) == "Europe/Berlin"
        assert row["u"] == str(uuid.UUID(int=0))
    finally:
        session.close()
        driver.close()


def test_live_neo4j_bridge_offset_time() -> None:
    """Offset-aware times bind through ``neo4j.time.Time`` and keep their offset."""
    session, driver = _neo4j_session(7687, "neo4j")
    value = datetime.time(0, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=-9, minutes=-30)))
    try:
        row = session.execute("RETURN $t AS t", {"t": value}).first()
        assert row is not None
        assert row["t"].utcoffset() == value.utcoffset()
        assert (row["t"].hour, row["t"].minute) == (value.hour, value.minute)
    finally:
        session.close()
        driver.close()


def test_live_falkordb_bridge_uuid() -> None:
    """FalkorDB receives UUIDs as strings, matching its official client."""
    falkordb = pytest.importorskip("falkordb")
    if not is_port_open(HOST, 6379):
        pytest.skip(f"falkordb is not reachable on {HOST}:6379")
    client = falkordb.FalkorDB(host=HOST, port=6379)
    graph_name = f"zz_bridge_values_{uuid.uuid4().hex[:8]}"
    graph = client.select_graph(graph_name)
    session = Session(bridge=FalkorDBBridge(graph, graph_name=graph_name), dialect="falkordb")
    value = uuid.UUID(int=0)
    try:
        row = session.execute("RETURN $u AS u", {"u": value}).first()
        assert row is not None
        assert row["u"] == str(value)
    finally:
        session.close()
        try:
            graph.delete()
        except Exception:
            pass


@pytest.mark.parametrize(
    "statement, parameters, expected",
    [
        (
            "SELECT $v AS v",
            {"v": Duration(months=14, days=3, seconds=4, nanoseconds=5_000)},
            Duration(months=14, days=3, seconds=4, nanoseconds=5_000),
        ),
        (
            "SELECT $v::TIMETZ AS v",
            {
                "v": datetime.time(
                    0,
                    0,
                    tzinfo=datetime.timezone(datetime.timedelta(hours=-9, minutes=-30)),
                )
            },
            datetime.time(
                0,
                0,
                tzinfo=datetime.timezone(datetime.timedelta(hours=-9, minutes=-30)),
            ),
        ),
        ("SELECT $v::JSON AS v", {"v": {"a": [1, True, None]}}, {"a": [1, True, None]}),
        ("SELECT $v AS v", {"v": uuid.UUID(int=0)}, uuid.UUID(int=0)),
    ],
)
def test_live_duckdb_bridge_parameter_adaptation(
    statement: str, parameters: dict[str, object], expected: object
) -> None:
    """DuckDB Duration, TIMETZ, JSON and UUID parameters round-trip through execute()."""
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(":memory:")
    session = Session(bridge=DuckDbBridge(con), dialect="duckpgq")
    try:
        row = session.execute(statement, parameters).first()
        assert row is not None
        actual = row["v"]
        if isinstance(expected, datetime.time):
            assert actual.utcoffset() == expected.utcoffset()
            assert (actual.hour, actual.minute) == (expected.hour, expected.minute)
        else:
            assert actual == expected
    finally:
        session.close()


def test_live_duckdb_bridge_arrow_and_transaction_values() -> None:
    """DuckDB Arrow exports keep huge integers, UUIDs, TIMETZ offsets and JSON."""
    duckdb = pytest.importorskip("duckdb")
    pyarrow = pytest.importorskip("pyarrow")
    con = duckdb.connect(":memory:")
    session = Session(bridge=DuckDbBridge(con), dialect="duckpgq")
    huge = 2**63
    value = uuid.UUID(int=0)
    timetz = datetime.time(0, 0, tzinfo=datetime.UTC)
    doc = {"a": [1, True, None]}
    try:
        huge_table = session.execute_to_arrow("SELECT $v::HUGEINT AS v", {"v": huge})
        assert huge_table.schema.field("v").type == pyarrow.decimal128(38, 0)
        assert int(huge_table.column("v")[0].as_py()) == huge

        uuid_table = session.execute_to_arrow("SELECT [$v]::UUID[] AS v", {"v": value})
        assert uuid_table.column("v")[0].as_py() == [value.bytes]

        time_table = session.execute_to_arrow("SELECT $v::TIMETZ AS v", {"v": timetz})
        assert time_table.column("v")[0].as_py() == "00:00:00+00:00"

        with session.transaction():
            tx_json = session.execute_to_arrow("SELECT $v::JSON AS v", {"v": doc})
            tx_uuid = session.execute_to_arrow("SELECT $v AS v", {"v": value})
        assert tx_json.schema.field("v").metadata[b"voyager.encoding"] == b"json"
        assert tx_uuid.column("v")[0].as_py() == value.bytes
    finally:
        session.close()


def test_live_duckdb_bridge_polars_exports() -> None:
    """DuckDB Polars exports use Binary for UUID and exact Decimal for HUGEINT."""
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(":memory:")
    session = Session(bridge=DuckDbBridge(con), dialect="duckpgq")
    value = uuid.UUID(int=0)
    huge = 2**63
    try:
        uuid_frame = session.execute_to_polars("SELECT $v AS v", {"v": value})
        assert uuid_frame["v"].to_list() == [value.bytes]
        huge_frame = session.execute_to_polars("SELECT $v::HUGEINT AS v", {"v": huge})
        assert int(huge_frame["v"].to_list()[0]) == huge
    finally:
        session.close()
