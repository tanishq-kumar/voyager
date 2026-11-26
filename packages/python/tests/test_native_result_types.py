"""Unit tests for unified result type conversions (#395 and Part 2 audit)."""

from __future__ import annotations

import datetime
import json
import math
import uuid
import zoneinfo
from decimal import Decimal
from typing import Any

import duckdb
import pytest
from dateutil.relativedelta import relativedelta
from voyager_ogm import Duration, GraphNode, GraphPath, GraphRelationship, Session
from voyager_ogm.agtype import decode_agtype
from voyager_ogm.bridge import DuckDbBridge, _strip_falkordb_utc
from voyager_ogm.entities import (
    convert_driver_value,
    json_default,
    serialize_value_for_arrow,
)
from voyager_ogm.types import parse_pg_interval_text


class _FakeNeo4jDate:
    def __init__(self, year: int, month: int, day: int) -> None:
        self.year = year
        self.month = month
        self.day = day

    def iso_format(self) -> str:
        return f"{self.year:04d}-{self.month:02d}-{self.day:02d}"


class _FakeNeo4jTime:
    def __init__(
        self,
        hour: int,
        minute: int,
        second: int,
        nanosecond: int = 0,
        tzinfo: datetime.tzinfo | None = None,
    ) -> None:
        self.hour = hour
        self.minute = minute
        self.second = second
        self.nanosecond = nanosecond
        self.tzinfo = tzinfo

    def utcoffset(self) -> datetime.timedelta | None:
        return self.tzinfo.utcoffset(None) if self.tzinfo is not None else None

    def iso_format(self) -> str:
        base = f"{self.hour:02d}:{self.minute:02d}:{self.second:02d}.{self.nanosecond:09d}"
        return base + ("+02:00" if self.tzinfo else "")


class _FakeNeo4jDateTime:
    def __init__(
        self,
        year: int,
        month: int,
        day: int,
        hour: int,
        minute: int,
        second: int,
        nanosecond: int = 0,
        tzinfo: Any = None,
        iso: str | None = None,
    ) -> None:
        self.year = year
        self.month = month
        self.day = day
        self.hour = hour
        self.minute = minute
        self.second = second
        self.nanosecond = nanosecond
        self.tzinfo = tzinfo
        self._iso = iso

    def utcoffset(self) -> datetime.timedelta | None:
        if hasattr(self.tzinfo, "utcoffset"):
            return self.tzinfo.utcoffset(
                datetime.datetime(
                    self.year, self.month, self.day, self.hour, self.minute, self.second
                )
            )
        return None

    def iso_format(self) -> str:
        if self._iso is not None:
            return self._iso
        return (
            f"{self.year:04d}-{self.month:02d}-{self.day:02d}"
            f"T{self.hour:02d}:{self.minute:02d}:{self.second:02d}"
        )


class _FakeNeo4jDuration:
    def __init__(
        self, months: int = 0, days: int = 0, seconds: int = 0, nanoseconds: int = 0
    ) -> None:
        self.months = months
        self.days = days
        self.seconds = seconds
        self.nanoseconds = nanoseconds


_FakeNeo4jDate.__name__ = "Date"
_FakeNeo4jTime.__name__ = "Time"
_FakeNeo4jDateTime.__name__ = "DateTime"
_FakeNeo4jDuration.__name__ = "Duration"


class _UnknownZoneTz:
    key = "Mars/Olympus_Mons"


def test_duration_value_semantics_roundtrip_and_errors() -> None:
    """Duration equality/hash normalize only seconds/nanoseconds carry without folding days."""
    d1 = Duration(0, 2, 3661, 500_000_000)
    d2 = Duration(0, 0, 2 * 86_400 + 3661, 500_000_000)
    d3 = Duration.from_timedelta(datetime.timedelta(days=2, seconds=3661, microseconds=500_000))
    assert d1 == d3
    assert hash(d1) == hash(d3)
    # Days are NOT folded into seconds (matching Neo4j and PostgreSQL semantics)
    assert d1 != d2
    assert Duration(0, 2, 0, 0) != Duration(0, 0, 172_800, 0)
    assert Duration(0, -1, 86_395, 0) != Duration(0, 0, -5, 0)

    # Seconds/nanoseconds carry IS normalized
    assert Duration(0, 0, 1, 1_500_000_000) == Duration(0, 0, 2, 500_000_000)
    assert hash(Duration(0, 0, 1, 1_500_000_000)) == hash(Duration(0, 0, 2, 500_000_000))
    assert Duration(0, 0, -1, 500_000_000) == Duration(0, 0, 0, -500_000_000)
    assert hash(Duration(0, 0, -1, 500_000_000)) == hash(Duration(0, 0, 0, -500_000_000))

    assert d1.to_timedelta() == datetime.timedelta(days=2, seconds=3661, microseconds=500_000)
    assert Duration(0, 0, -5, 0).to_timedelta() == datetime.timedelta(seconds=-5)
    assert d1.to_iso() == "P2DT1H1M1.5S"
    assert Duration.from_iso("P2DT1H1M1.500S") == d1

    cal = Duration(months=14, days=3, seconds=4, nanoseconds=500)
    assert cal.to_iso() == "P1Y2M3DT4.0000005S"
    assert Duration.from_iso("P1Y2M3DT4.0000005S") == cal

    # Mixed signs with months != 0 and seconds/nanoseconds differing in sign
    assert Duration(months=1, nanoseconds=-500_000_000).to_iso() == "P1MT-0.5S"
    assert Duration(months=1, seconds=-1, nanoseconds=500_000_000).to_iso() == "P1MT-0.5S"
    assert str(Duration(months=1, seconds=-1, nanoseconds=500_000_000)) == "P1MT-0.5S"
    assert json_default(Duration(months=1, seconds=-1, nanoseconds=500_000_000)) == "P1MT-0.5S"

    # Grid / property-style round-trip over all sign combinations of (months, days, seconds, nanos)
    for m in (-14, -1, 0, 1, 12, 14):
        for d in (-2, 0, 2):
            for s in (-3661, -1, 0, 1, 90_000):
                for ns in (-500_000_000, 0, 500_000_000):
                    dur = Duration(months=m, days=d, seconds=s, nanoseconds=ns)
                    iso = dur.to_iso()
                    assert Duration.from_iso(iso) == dur, (dur, iso)

    with pytest.raises(ValueError, match="months"):
        cal.to_timedelta()
    with pytest.raises(ValueError, match="(?i)sub-microsecond|nanoseconds"):
        Duration(0, 0, 1, 500).to_timedelta()


def test_duration_is_immutable() -> None:
    """Duration raises AttributeError on attribute mutation without static bypasses."""
    dur = Duration(1, 2, 3, 0)
    attr = "months"
    with pytest.raises(AttributeError):
        setattr(dur, attr, 5)


def test_parse_pg_interval_text_formats() -> None:
    """parse_pg_interval_text parses postgres, postgres_verbose, sql_standard, and ISO interval strings."""
    assert parse_pg_interval_text("2 days 01:01:01.5") == Duration(
        months=0, days=2, seconds=3661, nanoseconds=500_000_000
    )
    assert parse_pg_interval_text("1 year 2 mons -3 days +04:05:06.123") == Duration(
        months=14, days=-3, seconds=14_706, nanoseconds=123_000_000
    )
    assert parse_pg_interval_text("@ 1 year 2 mons ago") == Duration(months=-14)
    assert parse_pg_interval_text("@ 0") == Duration()
    assert parse_pg_interval_text("@ 1 year 2 mons 3 days 4 hours 5 mins 6.50 secs") == Duration(
        months=14, days=3, seconds=14_706, nanoseconds=500_000_000
    )
    assert parse_pg_interval_text("@ 5.25 secs ago") == Duration(
        seconds=-5, nanoseconds=-250_000_000
    )
    assert parse_pg_interval_text("+1-2 +3 +4:05:06.5") == Duration(
        months=14, days=3, seconds=14_706, nanoseconds=500_000_000
    )
    assert parse_pg_interval_text("-1-2") == Duration(months=-14)
    assert parse_pg_interval_text("-3") == Duration(days=-3)
    assert parse_pg_interval_text("0") == Duration()
    assert parse_pg_interval_text("P1Y2M3DT4H5M6.5S") == Duration(
        months=14, days=3, seconds=14_706, nanoseconds=500_000_000
    )
    assert parse_pg_interval_text("-00:00:05.25") == Duration(
        months=0, days=0, seconds=-5, nanoseconds=-250_000_000
    )


def test_convert_driver_value_temporals_relativedelta_and_fallbacks() -> None:
    """Bridge converter normalizes neo4j.time.*, relativedelta, entities, and out-of-range fallbacks."""
    _FakeNeo4jDate.__module__ = "neo4j.time"
    _FakeNeo4jTime.__module__ = "neo4j.time"
    _FakeNeo4jDateTime.__module__ = "neo4j.time"
    _FakeNeo4jDuration.__module__ = "neo4j.time"

    d = _FakeNeo4jDate(2024, 1, 2)
    t_local = _FakeNeo4jTime(14, 30, 15, 123_456_000)
    t_off = _FakeNeo4jTime(14, 30, 15, 0, datetime.timezone(datetime.timedelta(hours=2)))
    dt_local = _FakeNeo4jDateTime(2024, 1, 2, 3, 4, 5, 123_456_000)
    dt_zone = _FakeNeo4jDateTime(2024, 1, 2, 3, 4, 5, 0, tzinfo=zoneinfo.ZoneInfo("Europe/Berlin"))
    dur = _FakeNeo4jDuration(14, 3, 4, 500_000_000)

    assert convert_driver_value(d) == datetime.date(2024, 1, 2)
    assert type(convert_driver_value(d)) is datetime.date
    assert convert_driver_value(t_local) == datetime.time(14, 30, 15, 123456)
    assert convert_driver_value(t_off) == datetime.time(
        14, 30, 15, tzinfo=datetime.timezone(datetime.timedelta(hours=2))
    )
    assert convert_driver_value(dt_local) == datetime.datetime(2024, 1, 2, 3, 4, 5, 123456)
    assert convert_driver_value(dt_zone) == datetime.datetime(
        2024, 1, 2, 3, 4, 5, tzinfo=zoneinfo.ZoneInfo("Europe/Berlin")
    )
    assert convert_driver_value(dur) == Duration(14, 3, 4, 500_000_000)
    assert convert_driver_value(relativedelta(days=2, hours=1, minutes=1, seconds=1)) == Duration(
        0, 2, 3661, 0
    )
    assert convert_driver_value(relativedelta(years=1, months=2, days=3, hours=4)) == Duration(
        0, 427, 14_400, 0
    )

    # Fallbacks for values Python cannot represent cleanly
    sub_us_dt = _FakeNeo4jDateTime(
        2024, 1, 2, 3, 4, 5, 123_456_789, iso="2024-01-02T03:04:05.123456789"
    )
    assert convert_driver_value(sub_us_dt) == "2024-01-02T03:04:05.123456789"
    bad_year = _FakeNeo4jDate(0, 1, 1)
    assert convert_driver_value(bad_year) == "0000-01-01"
    bad_zone = _FakeNeo4jDateTime(
        2024,
        1,
        2,
        3,
        4,
        5,
        0,
        tzinfo=_UnknownZoneTz(),
        iso="2024-01-02T03:04:05Z[Mars/Olympus_Mons]",
    )
    assert convert_driver_value(bad_zone) == "2024-01-02T03:04:05Z[Mars/Olympus_Mons]"


def test_strip_falkordb_utc_recursive() -> None:
    """_strip_falkordb_utc strips UTC tzinfo inside nested lists, dicts, and entities."""
    aware = datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=datetime.UTC)
    node = GraphNode._from_trusted(1, "1", frozenset({"A"}), {"dt": aware, "items": [aware]})
    rel = GraphRelationship._from_trusted(2, "2", "R", 1, 3, {"dt": aware}, "1", "3")
    path = GraphPath._from_trusted((node,), (rel,))

    cleaned = _strip_falkordb_utc({"path": path, "list": [aware]})
    assert cleaned["list"] == [datetime.datetime(2024, 1, 2, 3, 4, 5)]
    assert cleaned["path"].nodes[0]["dt"].tzinfo is None
    assert cleaned["path"].nodes[0]["items"][0].tzinfo is None
    assert cleaned["path"].relationships[0]["dt"].tzinfo is None


def test_json_default_and_arrow_serialization() -> None:
    """json_default and serialize_value_for_arrow handle Duration, UUID, Decimal, bytes, and temporals."""
    u = uuid.UUID("12345678-1234-5678-1234-567812345678")
    dur_td = Duration(0, 2, 3661, 500_000_000)
    dur_cal = Duration(14, 3, 4, 500)
    payload = {
        "d": datetime.date(2024, 1, 2),
        "dur": dur_td,
        "cal": dur_cal,
        "u": u,
        "dec": Decimal("12.34"),
        "b": b"\x01\xfe",
    }
    encoded = json.loads(json.dumps(payload, default=json_default))
    assert encoded == {
        "d": "2024-01-02",
        "dur": "P2DT1H1M1.5S",
        "cal": "P1Y2M3DT4.0000005S",
        "u": str(u),
        "dec": "12.34",
        "b": [1, 254],
    }

    dur_hms = Duration(0, 0, 90_000, 500_000_000)
    assert serialize_value_for_arrow(dur_hms) == datetime.timedelta(
        seconds=90_000, microseconds=500_000
    )
    assert serialize_value_for_arrow(dur_td) == {
        "months": 0,
        "days": 2,
        "seconds": 3661,
        "nanoseconds": 500_000_000,
    }
    assert serialize_value_for_arrow(dur_cal) == {
        "months": 14,
        "days": 3,
        "seconds": 4,
        "nanoseconds": 500,
    }


def test_agtype_python_float_specials_and_nested() -> None:
    """decode_agtype handles NaN, Infinity, -Infinity, and nested lists/maps."""
    assert math.isnan(decode_agtype("NaN"))
    assert decode_agtype("Infinity") == float("inf")
    assert decode_agtype("-Infinity") == float("-inf")
    assert decode_agtype("+Infinity") == float("inf")
    nested = decode_agtype("[1, NaN, Infinity, true]")
    assert (
        nested[0] == 1 and math.isnan(nested[1]) and nested[2] == float("inf") and nested[3] is True
    )


def test_duckdb_bridge_embedded_result_types() -> None:
    """DuckDbBridge decodes temporal, Duration, UUID, Decimal, bytes, JSON, NaN/Inf, HUGEINT, lists."""
    con = duckdb.connect(":memory:")
    sess = Session(bridge=DuckDbBridge(con), dialect="duckpgq")
    try:
        row = sess.execute(
            """
            SELECT
                DATE '2024-01-02' AS d,
                TIME '14:30:15.123456' AS t,
                TIMESTAMP '2024-01-02 03:04:05.123456' AS dt,
                INTERVAL '2 days 01:01:01.5' AS iv,
                UUID '12345678-1234-5678-1234-567812345678' AS u,
                123.45::DECIMAL(10, 2) AS dec,
                '\\x0102fe'::BLOB AS blob,
                '{"a": [1, true, null]}'::JSON AS j,
                'nan'::DOUBLE AS nan_val,
                'inf'::DOUBLE AS inf_val,
                170141183460469231731687303715884105727::HUGEINT AS huge,
                []::INT[] AS empty_l,
                [[1, 2], [3, NULL]] AS nested_l
            """
        ).first()
        assert row is not None
        assert row["d"] == datetime.date(2024, 1, 2)
        assert row["t"] == datetime.time(14, 30, 15, 123456)
        assert row["dt"] == datetime.datetime(2024, 1, 2, 3, 4, 5, 123456)
        assert row["iv"] == Duration(0, 2, 3661, 500_000_000)
        assert row["u"] == uuid.UUID("12345678-1234-5678-1234-567812345678")
        assert row["dec"] == Decimal("123.45")
        assert isinstance(row["blob"], bytes)
        assert row["j"] == {"a": [1, True, None]}
        assert math.isnan(row["nan_val"])
        assert row["inf_val"] == float("inf")
        assert row["huge"] == 170141183460469231731687303715884105727
        assert row["empty_l"] == []
        assert row["nested_l"] == [[1, 2], [3, None]]
    finally:
        sess.close()
