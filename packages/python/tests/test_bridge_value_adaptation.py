"""Regression tests for bridge parameter and Arrow value adaptation (#418, #423, #424)."""

from __future__ import annotations

import datetime
import json
import uuid
from decimal import Decimal
from zoneinfo import ZoneInfo

import pyarrow as pa
import pytest
from voyager_ogm import Duration, Session
from voyager_ogm.bridge import (
    DuckDbBridge,
    _adapt_bolt_parameters,
    _adapt_falkordb_value,
)


def test_bolt_bridge_adapts_uuid_parameter_to_string() -> None:
    """Neo4j/Memgraph have no UUID parameter type; the bridge sends a string."""
    value = uuid.UUID("12345678-1234-5678-1234-567812345678")

    adapted = _adapt_bolt_parameters({"v": value, "nested": [value]})

    assert adapted == {"v": str(value), "nested": [str(value)]}


def test_bolt_bridge_adapts_offset_time_to_driver_time() -> None:
    """Offset-aware ``datetime.time`` becomes ``neo4j.time.Time`` with a fixed offset."""
    pytest.importorskip("neo4j")
    value = datetime.time(
        1,
        2,
        3,
        456789,
        tzinfo=datetime.timezone(datetime.timedelta(hours=-9, minutes=-30)),
    )

    adapted = _adapt_bolt_parameters({"v": value})["v"]

    assert type(adapted).__name__ == "Time"
    assert adapted.utcoffset() == value.utcoffset()


@pytest.mark.parametrize(
    ("fold", "expected_offset"),
    [
        (0, datetime.timedelta(hours=2)),
        (1, datetime.timedelta(hours=1)),
    ],
)
def test_bolt_bridge_adapts_zoneinfo_datetime_with_fold(
    fold: int, expected_offset: datetime.timedelta
) -> None:
    """ZoneInfo datetimes use a pytz zone so both the zone name and ``fold`` survive."""
    pytest.importorskip("neo4j")
    value = datetime.datetime(2024, 10, 27, 2, 30, 0, fold=fold, tzinfo=ZoneInfo("Europe/Berlin"))

    adapted = _adapt_bolt_parameters({"v": value})["v"]

    assert type(adapted).__name__ == "DateTime"
    assert getattr(adapted.tzinfo, "zone", None) == "Europe/Berlin"
    assert adapted.utcoffset() == expected_offset
    assert adapted.to_native().astimezone(datetime.UTC) == value.astimezone(datetime.UTC)


def test_falkordb_bridge_adapts_uuid_parameter_to_string() -> None:
    """FalkorDB stores UUIDs as strings, matching its official client."""
    value = uuid.UUID(int=0)

    assert _adapt_falkordb_value(value) == str(value)
    assert _adapt_falkordb_value([value]) == [str(value)]


@pytest.fixture
def duckdb_session():
    """Yields a Session over an in-memory DuckDB connection."""
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(":memory:")
    session = Session(bridge=DuckDbBridge(con), dialect="duckpgq")
    try:
        yield session
    finally:
        session.close()


def test_duckdb_duration_parameters_round_trip_exactly(duckdb_session: Session) -> None:
    """DuckDB INTERVAL parameters keep calendar months, days and microseconds."""
    value = Duration(months=14, days=3, seconds=4, nanoseconds=5_000)

    row = duckdb_session.execute("SELECT $v AS v", {"v": value}).first()
    assert row is not None
    assert row["v"] == value

    table = duckdb_session.execute_to_arrow("SELECT $v AS v", {"v": value})
    assert table.column("v")[0].as_py() == {
        "months": 14,
        "days": 3,
        "seconds": 4,
        "nanoseconds": 5_000,
    }

    frame = duckdb_session.execute_to_polars("SELECT $v AS v", {"v": value})
    assert frame["v"].to_list() == [{"months": 14, "days": 3, "seconds": 4, "nanoseconds": 5_000}]


def test_duckdb_negative_offset_time_round_trips_timetz(duckdb_session: Session) -> None:
    """Negative UTC offsets bind as TIMETZ text instead of failing in the DuckDB binder."""
    value = datetime.time(0, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=-9, minutes=-30)))

    row = duckdb_session.execute("SELECT $v::TIMETZ AS v", {"v": value}).first()
    assert row is not None
    assert row["v"].utcoffset() == value.utcoffset()
    assert (row["v"].hour, row["v"].minute) == (value.hour, value.minute)

    table = duckdb_session.execute_to_arrow("SELECT $v::TIMETZ AS v", {"v": value})
    assert table.schema.field("v").type == pa.string()
    assert table.schema.field("v").metadata[b"voyager.type"] == b"time_tz"
    assert table.column("v")[0].as_py() == "00:00:00-09:30"

    frame = duckdb_session.execute_to_polars("SELECT $v::TIMETZ AS v", {"v": value})
    assert frame["v"].to_list() == ["00:00:00-09:30"]


def test_duckdb_json_parameter_is_serialized_not_inferred(duckdb_session: Session) -> None:
    """A dict target-cast to JSON is serialized so mixed lists keep their element types."""
    value = {"a": [1, True, None], "nested": {"k": "v"}}

    row = duckdb_session.execute("SELECT $v::JSON AS v", {"v": value}).first()
    assert row is not None
    assert row["v"] == value

    table = duckdb_session.execute_to_arrow("SELECT $v::JSON AS v", {"v": value})
    field = table.schema.field("v")
    assert field.metadata[b"voyager.encoding"] == b"json"
    assert json.loads(table.column("v")[0].as_py()) == value

    frame = duckdb_session.execute_to_polars("SELECT $v::JSON AS v", {"v": value})
    assert json.loads(frame["v"].to_list()[0]) == value


def test_duckdb_hugeint_exports_are_exact(duckdb_session: Session) -> None:
    """HUGEINT values outside int64 use decimal128(38, 0) in Arrow and Decimal in Polars."""
    value = 2**63

    row = duckdb_session.execute("SELECT $v::HUGEINT AS v", {"v": value}).first()
    assert row is not None
    assert row["v"] == value

    table = duckdb_session.execute_to_arrow("SELECT $v::HUGEINT AS v", {"v": value})
    field = table.schema.field("v")
    assert field.type == pa.decimal128(38, 0)
    assert field.metadata[b"voyager.type"] == b"hugeint"
    assert int(table.column("v")[0].as_py()) == value

    frame = duckdb_session.execute_to_polars("SELECT $v::HUGEINT AS v", {"v": value})
    assert frame["v"].to_list() == [Decimal(value)]


def test_duckdb_uuid_exports_are_fixed_size_binary(duckdb_session: Session) -> None:
    """UUID and UUID[] columns export as 16-byte fixed-size binary values."""
    value = uuid.UUID("12345678-1234-5678-1234-567812345678")

    row = duckdb_session.execute("SELECT $v AS v", {"v": value}).first()
    assert row is not None
    assert row["v"] == value

    table = duckdb_session.execute_to_arrow("SELECT $v AS v", {"v": value})
    field = table.schema.field("v")
    assert field.type == pa.binary(16)
    assert field.metadata[b"voyager.type"] == b"uuid"
    assert table.column("v")[0].as_py() == value.bytes

    frame = duckdb_session.execute_to_polars("SELECT $v AS v", {"v": value})
    assert frame["v"].to_list() == [value.bytes]

    array_table = duckdb_session.execute_to_arrow("SELECT [$v]::UUID[] AS v", {"v": value})
    assert array_table.schema.field("v").type == pa.list_(pa.binary(16))
    assert array_table.column("v")[0].as_py() == [value.bytes]

    array_frame = duckdb_session.execute_to_polars("SELECT [$v]::UUID[] AS v", {"v": value})
    assert array_frame["v"].to_list() == [[value.bytes]]


def test_duckdb_transaction_arrow_matches_auto_commit(duckdb_session: Session) -> None:
    """The transactional Arrow path uses the same canonical conversion as auto-commit."""
    value = uuid.UUID(int=0)
    doc = {"a": [1, True, None]}
    huge = 2**63

    auto_json = duckdb_session.execute_to_arrow("SELECT $v::JSON AS v", {"v": doc})
    auto_uuid = duckdb_session.execute_to_arrow("SELECT [$v]::UUID[] AS v", {"v": value})
    auto_huge = duckdb_session.execute_to_arrow("SELECT $v::HUGEINT AS v", {"v": huge})

    with duckdb_session.transaction():
        tx_json = duckdb_session.execute_to_arrow("SELECT $v::JSON AS v", {"v": doc})
        tx_uuid = duckdb_session.execute_to_arrow("SELECT [$v]::UUID[] AS v", {"v": value})
        tx_huge = duckdb_session.execute_to_arrow("SELECT $v::HUGEINT AS v", {"v": huge})

    assert tx_json.schema.field("v").metadata[b"voyager.encoding"] == b"json"
    assert json.loads(tx_json.column("v")[0].as_py()) == doc
    assert tx_json.equals(auto_json)

    assert tx_uuid.schema.field("v").type == pa.list_(pa.binary(16))
    assert tx_uuid.column("v")[0].as_py() == [value.bytes]
    assert tx_uuid.equals(auto_uuid)

    assert tx_huge.schema.field("v").type == pa.decimal128(38, 0)
    assert int(tx_huge.column("v")[0].as_py()) == huge
    assert tx_huge.equals(auto_huge)
