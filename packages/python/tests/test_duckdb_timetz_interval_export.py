"""Regression tests for DuckDB TIMETZ export with calendar INTERVAL columns (#439)."""

from __future__ import annotations

import datetime
import uuid

import pyarrow as pa
import pytest
from voyager_ogm import Duration, Session
from voyager_ogm.bridge import DuckDbBridge, _duckdb_timetz_text_iso


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


def test_duckdb_execute_preserves_timetz_offset_with_interval_and_scalars(
    duckdb_session: Session,
) -> None:
    """execute() keeps TIMETZ offsets alongside calendar INTERVAL, HUGEINT, UUID, and JSON."""
    statement = """
        SELECT
            '10:00:00+02'::TIMETZ AS t,
            INTERVAL '1 year 2 months' AS iv,
            170141183460469231731687303715884105727::HUGEINT AS huge,
            UUID '12345678-1234-5678-1234-567812345678' AS u,
            '{"a": 1}'::JSON AS j
    """
    expected_time = datetime.time(10, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=2)))
    expected_iv = Duration(months=14)
    expected_huge = 170141183460469231731687303715884105727
    expected_uuid = uuid.UUID("12345678-1234-5678-1234-567812345678")

    def assert_row(row: dict | None) -> None:
        assert row is not None
        assert row["t"] == expected_time
        assert row["t"].utcoffset() == datetime.timedelta(hours=2)
        assert row["iv"] == expected_iv
        assert type(row["huge"]) is int
        assert row["huge"] == expected_huge
        assert row["u"] == expected_uuid
        assert row["j"] == {"a": 1}

    assert_row(duckdb_session.execute(statement).first())
    with duckdb_session.transaction():
        assert_row(duckdb_session.execute(statement).first())


def test_duckdb_dml_returning_and_multi_statement_keep_timetz_and_interval(
    duckdb_session: Session,
) -> None:
    """INSERT/UPDATE/DELETE RETURNING and multi-statement SQL keep both offset and months."""
    duckdb_session.execute("CREATE TABLE zz_dml_iv (id INT, t TIMETZ, iv INTERVAL)")
    expected_time = datetime.time(10, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=2)))
    expected_iv = Duration(months=14)
    expected_arrow_iv = {"months": 14, "days": 0, "seconds": 0, "nanoseconds": 0}

    for idx, stmt_factory in enumerate(
        (
            lambda k: (
                f"INSERT INTO zz_dml_iv VALUES ({k}, '10:00:00+02'::TIMETZ, "
                "INTERVAL '1 year 2 months') RETURNING t, iv"
            ),
            lambda k: f"UPDATE zz_dml_iv SET id = {k + 100} WHERE id = {k} RETURNING t, iv",
            lambda k: f"DELETE FROM zz_dml_iv WHERE id = {k + 100} RETURNING t, iv",
            lambda k: (
                f"INSERT INTO zz_dml_iv VALUES ({k + 200}, '10:00:00+02'::TIMETZ, "
                f"INTERVAL '1 year 2 months'); SELECT t, iv FROM zz_dml_iv WHERE id = {k + 200}"
            ),
        )
    ):
        _ = idx
        row = duckdb_session.execute(stmt_factory(1)).first()
        assert row == {"t": expected_time, "iv": expected_iv}
        assert row["t"].utcoffset() == datetime.timedelta(hours=2)

        table = duckdb_session.execute_to_arrow(stmt_factory(2))
        assert table.column("t").to_pylist() == ["10:00:00+02:00"]
        assert table.column("iv").to_pylist() == [expected_arrow_iv]

        frame = duckdb_session.execute_to_polars(stmt_factory(3))
        assert frame["t"].to_list() == ["10:00:00+02:00"]
        assert frame["iv"].to_list() == [expected_arrow_iv]


def test_duckdb_nested_timetz_in_struct_map_list_and_array(
    duckdb_session: Session,
) -> None:
    """TIMETZ inside STRUCT, MAP, LIST<STRUCT>, ARRAY, and UNION keeps its offset on every path."""
    statement = """
        SELECT
            {'t': '10:00:00+02'::TIMETZ, 'iv': INTERVAL '1 year 2 months', 'note': 'plain'} AS s,
            MAP {'k': '10:11:12.5-09:30'::TIMETZ} AS m,
            [{'t': '10:00:00+02'::TIMETZ}] AS ls,
            ['10:00:00+02'::TIMETZ, NULL]::TIMETZ[2] AS arr
    """
    union_statement = "SELECT union_value(t := '10:00:00+02'::TIMETZ)::UNION(t TIMETZ, n INT) AS u"
    t_pos = datetime.time(10, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=2)))
    t_neg = datetime.time(
        10, 11, 12, 500_000, tzinfo=datetime.timezone(datetime.timedelta(hours=-9, minutes=-30))
    )
    iv_dur = Duration(months=14)
    iv_struct = {"months": 14, "days": 0, "seconds": 0, "nanoseconds": 0}

    def check_all() -> None:
        row = duckdb_session.execute(statement).first()
        assert row is not None
        assert row["s"] == {"t": t_pos, "iv": iv_dur, "note": "plain"}
        assert row["s"]["t"].utcoffset() == datetime.timedelta(hours=2)
        assert row["m"] == {"k": t_neg}
        assert row["ls"] == [{"t": t_pos}]
        assert row["arr"] == (t_pos, None)
        assert duckdb_session.execute(union_statement).first() == {"u": t_pos}

        table = duckdb_session.execute_to_arrow(statement)
        assert table.column("s").to_pylist() == [
            {"t": "10:00:00+02:00", "iv": iv_struct, "note": "plain"}
        ]
        assert table.column("m").to_pylist() == [[("k", "10:11:12.500000-09:30")]]
        assert table.column("ls").to_pylist() == [[{"t": "10:00:00+02:00"}]]
        assert table.column("arr").to_pylist() == [["10:00:00+02:00", None]]
        assert duckdb_session.execute_to_arrow(union_statement).column("u").to_pylist() == [
            "10:00:00+02:00"
        ]

        frame = duckdb_session.execute_to_polars(statement)
        assert frame["s"].to_list() == [{"t": "10:00:00+02:00", "iv": iv_struct, "note": "plain"}]
        assert frame["m"].to_list() == [[{"key": "k", "value": "10:11:12.500000-09:30"}]]
        assert frame["ls"].to_list() == [[{"t": "10:00:00+02:00"}]]
        assert frame["arr"].to_list() == [["10:00:00+02:00", None]]

    check_all()
    with duckdb_session.transaction():
        check_all()


def test_duckdb_nested_interval_without_top_level_interval_keeps_months(
    duckdb_session: Session,
) -> None:
    """Nested INTERVAL inside LIST, STRUCT, MAP, and ARRAY keeps months even with no top-level INTERVAL."""
    statement = """
        SELECT
            [INTERVAL '1 year 2 months', INTERVAL '3 seconds'] AS iv_list,
            {'iv': INTERVAL '1 year 2 months', 'n': 7} AS iv_struct,
            MAP {'a': INTERVAL '1 year 2 months'} AS iv_map,
            [INTERVAL '1 year 2 months', NULL]::INTERVAL[2] AS iv_arr
    """
    dur_cal = Duration(months=14)
    dur_sec = Duration(seconds=3)
    cal_dict = {"months": 14, "days": 0, "seconds": 0, "nanoseconds": 0}
    sec_dict = {"months": 0, "days": 0, "seconds": 3, "nanoseconds": 0}

    row = duckdb_session.execute(statement).first()
    assert row == {
        "iv_list": [dur_cal, dur_sec],
        "iv_struct": {"iv": dur_cal, "n": 7},
        "iv_map": {"a": dur_cal},
        "iv_arr": (dur_cal, None),
    }

    table = duckdb_session.execute_to_arrow(statement)
    assert table.column("iv_list").to_pylist() == [[cal_dict, sec_dict]]
    assert table.column("iv_struct").to_pylist() == [{"iv": cal_dict, "n": 7}]
    assert table.column("iv_map").to_pylist() == [[("a", cal_dict)]]
    assert table.column("iv_arr").to_pylist() == [[cal_dict, None]]

    frame = duckdb_session.execute_to_polars(statement)
    assert frame["iv_list"].to_list() == [[cal_dict, sec_dict]]
    assert frame["iv_struct"].to_list() == [{"iv": cal_dict, "n": 7}]
    assert frame["iv_map"].to_list() == [[{"key": "a", "value": cal_dict}]]
    assert frame["iv_arr"].to_list() == [[cal_dict, None]]

    # Mixed rows (calendar months in row 0, pure seconds in row 1) and empty/NULL maps.
    mixed_rows_sql = """
        SELECT [INTERVAL '1 year'] AS l, MAP {'a': INTERVAL '1 year'} AS m
        UNION ALL
        SELECT [INTERVAL '3 seconds'] AS l, MAP {}::MAP(VARCHAR, INTERVAL) AS m
        UNION ALL
        SELECT NULL::INTERVAL[] AS l, NULL::MAP(VARCHAR, INTERVAL) AS m
    """
    mixed_table = duckdb_session.execute_to_arrow(mixed_rows_sql)
    assert mixed_table.column("l").to_pylist() == [
        [{"months": 12, "days": 0, "seconds": 0, "nanoseconds": 0}],
        [{"months": 0, "days": 0, "seconds": 3, "nanoseconds": 0}],
        None,
    ]
    assert mixed_table.column("m").to_pylist() == [
        [("a", {"months": 12, "days": 0, "seconds": 0, "nanoseconds": 0})],
        [],
        None,
    ]
    mixed_frame = duckdb_session.execute_to_polars(mixed_rows_sql)
    assert mixed_frame["m"].to_list() == [
        [{"key": "a", "value": {"months": 12, "days": 0, "seconds": 0, "nanoseconds": 0}}],
        [],
        None,
    ]

    # 0-row empty result preserves Arrow schema without crashing.
    empty_table = duckdb_session.execute_to_arrow(
        "SELECT MAP {'a': INTERVAL '1 year'} AS m, {'t': '10:00:00+02'::TIMETZ} AS s WHERE 1=0"
    )
    assert empty_table.num_rows == 0
    assert pa.types.is_map(empty_table.schema.field("m").type)
    assert pa.types.is_struct(empty_table.schema.field("s").type)


def test_duckdb_timetz_text_iso_normalizes_and_rejects_invalid_inputs() -> None:
    """_duckdb_timetz_text_iso normalizes valid TIMETZ forms and rejects invalid values."""
    assert _duckdb_timetz_text_iso(None) is None
    assert _duckdb_timetz_text_iso("00:00:00+00") == "00:00:00+00:00"
    assert _duckdb_timetz_text_iso("10:11:12+02") == "10:11:12+02:00"
    assert _duckdb_timetz_text_iso("10:11:12.1-09:30") == "10:11:12.100000-09:30"
    assert _duckdb_timetz_text_iso("10:11:12+00:00:30") == "10:11:12+00:00:30"
    assert _duckdb_timetz_text_iso("10:11:12Z") == "10:11:12+00:00"
    assert _duckdb_timetz_text_iso("10:11:12+0200") == "10:11:12+02:00"
    assert _duckdb_timetz_text_iso("23:59:59.999999+15:59:59") == "23:59:59.999999+15:59:59"
    assert _duckdb_timetz_text_iso("24:00:00+00") == "24:00:00+00:00"
    assert _duckdb_timetz_text_iso("24:00:00-00") == "24:00:00+00:00"
    assert _duckdb_timetz_text_iso("24:00:00Z") == "24:00:00+00:00"
    assert _duckdb_timetz_text_iso("24:00:00+0200") == "24:00:00+02:00"
    assert _duckdb_timetz_text_iso("24:00:00-09:30") == "24:00:00-09:30"
    assert _duckdb_timetz_text_iso("24:00:00+00:00:30") == "24:00:00+00:00:30"
    assert _duckdb_timetz_text_iso("24:00:00+15:59:59") == "24:00:00+15:59:59"
    assert _duckdb_timetz_text_iso("24:00:00-15:59:59") == "24:00:00-15:59:59"
    assert _duckdb_timetz_text_iso("24:00:00.000000+00") == "24:00:00+00:00"
    assert _duckdb_timetz_text_iso(["10:00:00+02", "24:00:00+00", None]) == [
        "10:00:00+02:00",
        "24:00:00+00:00",
        None,
    ]

    for bad in (
        "10:00:00",
        "24:00:00",
        "not-a-time",
        "24:00:00.000001+00",
        "24:00:01+00",
        "24:01:00+00",
        "25:00:00+00",
        "10:60:00+02",
        "10:00:00.1234567+02",
        "10:00:00+25:00",
        "24:00:00+24:00",
        "24:00:00+00:60",
    ):
        with pytest.raises(ValueError):
            _duckdb_timetz_text_iso(bad)

    with pytest.raises(TypeError):
        _duckdb_timetz_text_iso(12345)


def test_duckdb_timetz_24_00_00_all_paths(duckdb_session: Session) -> None:
    """24:00:00 TIMETZ succeeds and normalizes consistently across execute, Arrow, Polars, nested, and tx."""
    no_iv_sql = """
        SELECT
            '24:00:00+00'::TIMETZ AS t24,
            '24:00:00-09:30'::TIMETZ AS t24_min,
            '24:00:00+15:59:59'::TIMETZ AS t24_sec,
            '23:59:59.123456-05:30:15'::TIMETZ AS t_sub,
            '24:00:00+00'::VARCHAR AS raw_str,
            ['24:00:00+00'::TIMETZ, NULL] AS l,
            ['24:00:00+00'::TIMETZ, NULL]::TIMETZ[2] AS arr,
            {'t': '24:00:00+00'::TIMETZ, 'note': 'plain'} AS s,
            [{'t': '24:00:00+00'::TIMETZ}] AS ls,
            MAP {'k': '24:00:00+00'::TIMETZ} AS m,
            MAP {'24:00:00+00'::TIMETZ: 'v'} AS mk
    """
    with_iv_sql = """
        SELECT
            '24:00:00+00'::TIMETZ AS t24,
            '24:00:00-09:30'::TIMETZ AS t24_min,
            '24:00:00+15:59:59'::TIMETZ AS t24_sec,
            '23:59:59.123456-05:30:15'::TIMETZ AS t_sub,
            '24:00:00+00'::VARCHAR AS raw_str,
            ['24:00:00+00'::TIMETZ, NULL] AS l,
            ['24:00:00+00'::TIMETZ, NULL]::TIMETZ[2] AS arr,
            {'t': '24:00:00+00'::TIMETZ, 'iv': INTERVAL '1 year 2 months', 'note': 'plain'} AS s,
            [{'t': '24:00:00+00'::TIMETZ}] AS ls,
            MAP {'k': '24:00:00+00'::TIMETZ} AS m,
            MAP {'24:00:00+00'::TIMETZ: 'v'} AS mk,
            INTERVAL '1 year 2 months' AS iv
    """
    union_sql = "SELECT union_value(t := '24:00:00+00'::TIMETZ)::UNION(t TIMETZ, n INT) AS u"
    union_iv_sql = (
        "SELECT union_value(t := '24:00:00+00'::TIMETZ)::UNION(t TIMETZ, n INT) AS u, "
        "INTERVAL '1 year 2 months' AS iv"
    )
    t_sub_py = datetime.time(
        23,
        59,
        59,
        123456,
        tzinfo=datetime.timezone(-datetime.timedelta(hours=5, minutes=30, seconds=15)),
    )
    iv_dur = Duration(months=14)
    iv_struct = {"months": 14, "days": 0, "seconds": 0, "nanoseconds": 0}

    def check_all() -> None:
        row_no_iv = duckdb_session.execute(no_iv_sql).first()
        assert row_no_iv == {
            "t24": "24:00:00+00:00",
            "t24_min": "24:00:00-09:30",
            "t24_sec": "24:00:00+15:59:59",
            "t_sub": t_sub_py,
            "raw_str": "24:00:00+00",
            "l": ["24:00:00+00:00", None],
            "arr": ("24:00:00+00:00", None),
            "s": {"t": "24:00:00+00:00", "note": "plain"},
            "ls": [{"t": "24:00:00+00:00"}],
            "m": {"k": "24:00:00+00:00"},
            "mk": {"24:00:00+00:00": "v"},
        }
        assert duckdb_session.execute(union_sql).first() == {"u": "24:00:00+00:00"}

        row_iv = duckdb_session.execute(with_iv_sql).first()
        assert row_iv == {
            "t24": "24:00:00+00:00",
            "t24_min": "24:00:00-09:30",
            "t24_sec": "24:00:00+15:59:59",
            "t_sub": t_sub_py,
            "raw_str": "24:00:00+00",
            "l": ["24:00:00+00:00", None],
            "arr": ("24:00:00+00:00", None),
            "s": {"t": "24:00:00+00:00", "iv": iv_dur, "note": "plain"},
            "ls": [{"t": "24:00:00+00:00"}],
            "m": {"k": "24:00:00+00:00"},
            "mk": {"24:00:00+00:00": "v"},
            "iv": iv_dur,
        }
        assert duckdb_session.execute(union_iv_sql).first() == {
            "u": "24:00:00+00:00",
            "iv": iv_dur,
        }

        for sql, has_iv in ((no_iv_sql, False), (with_iv_sql, True)):
            table = duckdb_session.execute_to_arrow(sql)
            assert table.schema.field("t24").metadata[b"voyager.type"] == b"time_tz"
            assert table.column("t24").to_pylist() == ["24:00:00+00:00"]
            assert table.column("t24_min").to_pylist() == ["24:00:00-09:30"]
            assert table.column("t24_sec").to_pylist() == ["24:00:00+15:59:59"]
            assert table.column("t_sub").to_pylist() == ["23:59:59.123456-05:30:15"]
            assert table.column("raw_str").to_pylist() == ["24:00:00+00"]
            assert table.column("l").to_pylist() == [["24:00:00+00:00", None]]
            assert table.column("arr").to_pylist() == [["24:00:00+00:00", None]]
            expected_s = (
                {"t": "24:00:00+00:00", "iv": iv_struct, "note": "plain"}
                if has_iv
                else {"t": "24:00:00+00:00", "note": "plain"}
            )
            assert table.column("s").to_pylist() == [expected_s]
            assert table.column("ls").to_pylist() == [[{"t": "24:00:00+00:00"}]]
            assert table.column("m").to_pylist() == [[("k", "24:00:00+00:00")]]
            assert table.column("mk").to_pylist() == [[("24:00:00+00:00", "v")]]

            frame = duckdb_session.execute_to_polars(sql)
            assert frame["t24"].to_list() == ["24:00:00+00:00"]
            assert frame["t24_min"].to_list() == ["24:00:00-09:30"]
            assert frame["t24_sec"].to_list() == ["24:00:00+15:59:59"]
            assert frame["t_sub"].to_list() == ["23:59:59.123456-05:30:15"]
            assert frame["raw_str"].to_list() == ["24:00:00+00"]
            assert frame["l"].to_list() == [["24:00:00+00:00", None]]
            assert frame["arr"].to_list() == [["24:00:00+00:00", None]]
            assert frame["s"].to_list() == [expected_s]
            assert frame["ls"].to_list() == [[{"t": "24:00:00+00:00"}]]
            assert frame["m"].to_list() == [[{"key": "k", "value": "24:00:00+00:00"}]]
            assert frame["mk"].to_list() == [[{"key": "24:00:00+00:00", "value": "v"}]]
            if has_iv:
                assert table.column("iv").to_pylist() == [iv_struct]
                assert frame["iv"].to_list() == [iv_struct]

        assert duckdb_session.execute_to_arrow(union_sql).column("u").to_pylist() == [
            "24:00:00+00:00"
        ]
        u_iv_table = duckdb_session.execute_to_arrow(union_iv_sql)
        assert u_iv_table.column("u").to_pylist() == ["24:00:00+00:00"]
        assert u_iv_table.column("iv").to_pylist() == [iv_struct]

    check_all()
    with duckdb_session.transaction():
        check_all()
