"""Regression tests for DuckDB TIMETZ export with calendar INTERVAL columns (#439)."""

from __future__ import annotations

import pyarrow as pa
import pytest
from voyager_ogm import Session
from voyager_ogm.bridge import DuckDbBridge


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


def test_duckdb_timetz_with_calendar_interval_keeps_arrow_months(
    duckdb_session: Session,
) -> None:
    """A TIMETZ column no longer forces DB-API rows, so calendar INTERVAL months survive."""
    statement = "SELECT '10:00:00+02:00'::TIMETZ AS t, INTERVAL '1 year 2 months' AS iv"
    expected_interval = {"months": 14, "days": 0, "seconds": 0, "nanoseconds": 0}

    def assert_arrow(table: pa.Table) -> None:
        time_field = table.schema.field("t")
        assert time_field.type == pa.string()
        assert time_field.metadata[b"voyager.type"] == b"time_tz"
        assert table.column("t")[0].as_py() == "10:00:00+02:00"
        assert table.column("iv")[0].as_py() == expected_interval

    def assert_polars(frame) -> None:
        assert frame["t"].to_list() == ["10:00:00+02:00"]
        assert frame["iv"].to_list() == [expected_interval]

    assert_arrow(duckdb_session.execute_to_arrow(statement))
    assert_polars(duckdb_session.execute_to_polars(statement))

    with duckdb_session.transaction():
        assert_arrow(duckdb_session.execute_to_arrow(statement))
        assert_polars(duckdb_session.execute_to_polars(statement))


def test_duckdb_timetz_array_with_interval_keeps_list_shape(duckdb_session: Session) -> None:
    """TIMETZ[] exports as ISO-text lists while the interval keeps its calendar months."""
    statement = "SELECT ['10:00:00+02:00'::TIMETZ, NULL] AS t, INTERVAL '1 year 2 months' AS iv"

    table = duckdb_session.execute_to_arrow(statement)
    time_type = table.schema.field("t").type
    assert pa.types.is_list(time_type)
    assert time_type.value_type == pa.string()
    assert table.column("t")[0].as_py() == ["10:00:00+02:00", None]
    assert table.column("iv")[0].as_py() == {
        "months": 14,
        "days": 0,
        "seconds": 0,
        "nanoseconds": 0,
    }


def test_duckdb_timetz_exports_match_python_isoformat(duckdb_session: Session) -> None:
    """Whole-hour offsets, sub-seconds and TIMETZ[] all match ``datetime.time.isoformat``."""
    statement = """
        SELECT
            '00:00:00+00'::TIMETZ AS whole,
            '10:11:12.12-09:30'::TIMETZ AS sub,
            '10:11:12.000001+05:45'::TIMETZ AS minute,
            ['00:00:00+00'::TIMETZ, '10:11:12.5+05:45'::TIMETZ, NULL] AS arr
    """
    expected = {
        "whole": "00:00:00+00:00",
        "sub": "10:11:12.120000-09:30",
        "minute": "10:11:12.000001+05:45",
        "arr": ["00:00:00+00:00", "10:11:12.500000+05:45", None],
    }

    def assert_arrow(table: pa.Table) -> None:
        for column, value in expected.items():
            assert table.column(column)[0].as_py() == value

    def assert_polars(frame) -> None:
        for column, value in expected.items():
            assert frame[column].to_list() == [value]

    assert_arrow(duckdb_session.execute_to_arrow(statement))
    assert_polars(duckdb_session.execute_to_polars(statement))

    with duckdb_session.transaction():
        assert_arrow(duckdb_session.execute_to_arrow(statement))
        assert_polars(duckdb_session.execute_to_polars(statement))


def test_duckdb_timetz_eager_exports_match_python_isoformat(duckdb_session: Session) -> None:
    """DML ``RETURNING`` and multi-statement input keep the same ISO text form."""
    duckdb_session.execute("CREATE TABLE zz_timetz_values (v TIMETZ, a TIMETZ[])")
    returning = (
        "INSERT INTO zz_timetz_values VALUES "
        "('00:00:00+00'::TIMETZ, ['10:11:12.5+05:45'::TIMETZ, NULL]) RETURNING v, a"
    )

    table = duckdb_session.execute_to_arrow(returning)
    assert table.column("v").to_pylist() == ["00:00:00+00:00"]
    assert table.column("a").to_pylist() == [["10:11:12.500000+05:45", None]]

    frame = duckdb_session.execute_to_polars(returning)
    assert frame["v"].to_list() == ["00:00:00+00:00"]
    assert frame["a"].to_list() == [["10:11:12.500000+05:45", None]]

    multi = "SELECT 1 AS n; SELECT '10:11:12.12-09:30'::TIMETZ AS t"
    assert duckdb_session.execute_to_arrow(multi).column("t").to_pylist() == [
        "10:11:12.120000-09:30"
    ]
    assert duckdb_session.execute_to_polars(multi)["t"].to_list() == ["10:11:12.120000-09:30"]
