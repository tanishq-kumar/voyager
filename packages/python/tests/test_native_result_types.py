"""Unit tests for unified result type conversions (#395 and Part 2 audit)."""

from __future__ import annotations

import datetime
import json
import math
import uuid
import zoneinfo
from decimal import Decimal
from typing import Any

import pytest
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
    """Bridge converter normalizes neo4j.time.*, entities, and out-of-range fallbacks."""
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


def test_convert_driver_value_relativedelta() -> None:
    """relativedelta conversion when python-dateutil is installed (it is absent on Python 3.14)."""
    relativedelta = pytest.importorskip("dateutil.relativedelta").relativedelta
    assert convert_driver_value(relativedelta(days=2, hours=1, minutes=1, seconds=1)) == Duration(
        0, 2, 3661, 0
    )
    assert convert_driver_value(relativedelta(years=1, months=2, days=3, hours=4)) == Duration(
        0, 427, 14_400, 0
    )


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
    duckdb = pytest.importorskip("duckdb")
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


def test_hydrate_datetime_offsetless_bracket_zone_uses_local_wall_clock() -> None:
    """Offset-less bracketed zoned strings are interpreted in the named zone, never as UTC (#420)."""
    from voyager_ogm.types import _hydrate_datetime

    dt = _hydrate_datetime("2024-01-02T03:04:05.123456[Europe/Berlin]")
    expected = datetime.datetime(
        2024, 1, 2, 3, 4, 5, 123456, tzinfo=zoneinfo.ZoneInfo("Europe/Berlin")
    )
    assert isinstance(dt, datetime.datetime)
    assert dt == expected
    assert dt.utcoffset() == datetime.timedelta(hours=1)
    assert dt.astimezone(datetime.UTC) == datetime.datetime(
        2024, 1, 2, 2, 4, 5, 123456, tzinfo=datetime.UTC
    )

    # Offset-bearing bracketed string still converts instant accurately
    dt_with_off = _hydrate_datetime("2024-01-02T03:04:05.123456+01:00[Europe/Berlin]")
    assert dt_with_off == expected
    assert dt_with_off.astimezone(datetime.UTC) == datetime.datetime(
        2024, 1, 2, 2, 4, 5, 123456, tzinfo=datetime.UTC
    )


def _ps_int(v: int) -> bytes:
    import struct

    if -16 <= v <= 127:
        return struct.pack(">b", v)
    if -128 <= v <= 127:
        return b"\xc8" + struct.pack(">b", v)
    if -32768 <= v <= 32767:
        return b"\xc9" + struct.pack(">h", v)
    if -(2**31) <= v <= 2**31 - 1:
        return b"\xca" + struct.pack(">i", v)
    return b"\xcb" + struct.pack(">q", v)


def _ps_str(s: str) -> bytes:
    b = s.encode("utf-8")
    if len(b) <= 15:
        return bytes([0x80 | len(b)]) + b
    return bytes([0xD0, len(b)]) + b


def _ps_list(items: list[bytes]) -> bytes:
    assert len(items) <= 15
    return bytes([0x90 | len(items)]) + b"".join(items)


def _ps_map(items: list[tuple[str, bytes]]) -> bytes:
    assert len(items) <= 15
    return bytes([0xA0 | len(items)]) + b"".join(_ps_str(k) + v for k, v in items)


def _ps_struct(tag: int, fields: list[bytes]) -> bytes:
    assert len(fields) <= 15
    return bytes([0xB0 | len(fields), tag]) + b"".join(fields)


def _ps_float(v: float) -> bytes:
    import struct

    return b"\xc1" + struct.pack(">d", v)


def _bolt_send_frame(conn: Any, payload: bytes) -> None:
    import struct

    conn.sendall(struct.pack(">H", len(payload)) + payload + b"\x00\x00")


def _bolt_recv_frame(conn: Any) -> bytes:
    import struct

    chunks: list[bytes] = []
    while True:
        hdr = conn.recv(2)
        if not hdr:
            return b""
        while len(hdr) < 2:
            hdr += conn.recv(2 - len(hdr))
        (size,) = struct.unpack(">H", hdr)
        if size == 0:
            return b"".join(chunks)
        buf = b""
        while len(buf) < size:
            part = conn.recv(size - len(buf))
            if not part:
                raise EOFError("unexpected EOF in Bolt chunk")
            buf += part
        chunks.append(buf)


def test_native_bolt_e2e_stub_points_v52_and_legacy_v44() -> None:
    """End-to-end PyO3 + BoltConnection test over a local TCP stub for #414, #419, and #420."""
    import socket
    import threading

    import polars as pl
    import pyarrow as pa
    from voyager_ogm import Point

    def start_stub(
        negotiated: bytes, cols: list[str], row_fields: list[bytes]
    ) -> tuple[int, list[bytes], threading.Thread]:
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.bind(("127.0.0.1", 0))
        srv.listen(4)
        port = srv.getsockname()[1]
        handshakes: list[bytes] = []

        def serve() -> None:
            try:
                conn, _ = srv.accept()
                with conn:
                    hs = b""
                    while len(hs) < 20:
                        chunk = conn.recv(20 - len(hs))
                        if not chunk:
                            return
                        hs += chunk
                    handshakes.append(hs)
                    conn.sendall(negotiated)
                    while True:
                        frame = _bolt_recv_frame(conn)
                        if not frame:
                            break
                        tag = frame[1] if len(frame) >= 2 else 0
                        if tag == 0x02:  # GOODBYE
                            break
                        if tag == 0x01:  # HELLO
                            _bolt_send_frame(
                                conn,
                                _ps_struct(0x70, [_ps_map([("server", _ps_str("Memgraph/2.20"))])]),
                            )
                        elif tag == 0x6A:  # LOGON
                            _bolt_send_frame(conn, _ps_struct(0x70, [_ps_map([])]))
                        elif tag == 0x10:  # RUN
                            _bolt_send_frame(
                                conn,
                                _ps_struct(
                                    0x70,
                                    [_ps_map([("fields", _ps_list([_ps_str(c) for c in cols]))])],
                                ),
                            )
                        elif tag == 0x3F:  # PULL
                            _bolt_send_frame(conn, _ps_struct(0x71, [_ps_list(row_fields)]))
                            _bolt_send_frame(
                                conn,
                                _ps_struct(0x70, [_ps_map([("t_last", _ps_int(1))])]),
                            )
                        else:
                            _bolt_send_frame(conn, _ps_struct(0x70, [_ps_map([])]))
            finally:
                srv.close()

        t = threading.Thread(target=serve, daemon=True)
        t.start()
        return port, handshakes, t

    # Case 1: Bolt 5.2 (#414, #419, #420) returning Point2D, Point3D, nested Point, negative Duration
    pt2 = _ps_struct(0x58, [_ps_int(4326), _ps_float(13.405), _ps_float(52.52)])
    pt3 = _ps_struct(0x59, [_ps_int(9157), _ps_float(1.0), _ps_float(2.0), _ps_float(3.5)])
    neg_dur = _ps_struct(0x45, [_ps_int(0), _ps_int(0), _ps_int(0), _ps_int(-1000)])
    node_val = _ps_struct(
        0x4E,
        [
            _ps_int(7),
            _ps_list([_ps_str("Spot")]),
            _ps_map([("pt", pt2), ("dur", neg_dur)]),
            _ps_str("4:7"),
        ],
    )
    cols = ["p2", "p3", "plist", "pmap", "neg_d", "node_v"]
    row = [
        pt2,
        pt3,
        _ps_list([pt2, pt3]),
        _ps_map([("pt", pt2), ("d", neg_dur)]),
        neg_dur,
        node_val,
    ]

    # Run execute(), execute_to_arrow(), execute_to_polars() against 3 stub accepts
    for mode in ("execute", "arrow", "polars"):
        port, handshakes, thread = start_stub(b"\x00\x00\x02\x05", cols, row)
        sess = Session(
            f"bolt://127.0.0.1:{port}", dialect="memgraph", backend="native", pool_size=1
        )
        try:
            assert sess._native_client is not None
            assert sess._native_client.protocol_version_sync() == "5.2"
            assert handshakes[0][4:12] == b"\x00\x04\x04\x05\x00\x02\x04\x04"
            if mode == "execute":
                rec = sess.execute("RETURN 1").first()
                assert rec is not None
                assert isinstance(rec["p2"], Point) and rec["p2"] == Point(
                    latitude=52.52, longitude=13.405
                )
                assert isinstance(rec["p3"], Point) and rec["p3"] == Point(
                    x=1.0, y=2.0, z=3.5, srid=9157
                )
                assert rec["plist"] == [
                    Point(latitude=52.52, longitude=13.405),
                    Point(x=1.0, y=2.0, z=3.5, srid=9157),
                ]
                assert rec["pmap"]["pt"] == Point(latitude=52.52, longitude=13.405)
                assert rec["pmap"]["d"] == Duration(0, 0, 0, -1000)
                assert rec["neg_d"] == Duration(0, 0, 0, -1000)
                assert isinstance(rec["node_v"], GraphNode)
                assert rec["node_v"].properties["pt"] == Point(latitude=52.52, longitude=13.405)
                assert rec["node_v"].properties["dur"] == Duration(0, 0, 0, -1000)
            elif mode == "arrow":
                tbl = sess.execute_to_arrow("RETURN 1")
                assert pa.types.is_struct(tbl.schema.field("p2").type)
                assert tbl.schema.field("p2").metadata.get(b"voyager.type") == b"point"
                assert Point.from_arrow(tbl.column("p2")[0]) == Point(
                    latitude=52.52, longitude=13.405
                )
                assert Point.from_arrow(tbl.column("p3")[0]) == Point(
                    x=1.0, y=2.0, z=3.5, srid=9157
                )
                assert pa.types.is_struct(tbl.schema.field("neg_d").type)
                assert Duration(**tbl.column("neg_d")[0].as_py()) == Duration(0, 0, 0, -1000)
            else:
                df = sess.execute_to_polars("RETURN 1")
                assert isinstance(df.schema["p2"], pl.Struct)
                assert Point.from_polars(df["p2"]) == Point(latitude=52.52, longitude=13.405)
                assert isinstance(df.schema["neg_d"], pl.Struct)
                assert Duration(**df["neg_d"][0]) == Duration(0, 0, 0, -1000)
        finally:
            sess.close()
            thread.join(timeout=2)

    # Case 2: Bolt 4.4 legacy 0x66 local zoned datetime (#420) carries the exact instant in execute/Arrow/Polars
    local_berlin_secs = 1_704_164_645  # 2024-01-02T03:04:05 Europe/Berlin (+01:00) == 02:04:05Z
    legacy_0x66 = _ps_struct(
        0x66,
        [_ps_int(local_berlin_secs), _ps_int(123_456_000), _ps_str("Europe/Berlin")],
    )
    expected_dt = datetime.datetime(
        2024, 1, 2, 3, 4, 5, 123456, tzinfo=zoneinfo.ZoneInfo("Europe/Berlin")
    )
    for mode in ("execute", "arrow", "polars"):
        port, _, thread = start_stub(b"\x00\x00\x04\x04", ["dt"], [legacy_0x66])
        sess = Session(
            f"bolt://127.0.0.1:{port}", dialect="memgraph", backend="native", pool_size=1
        )
        try:
            assert sess._native_client is not None
            assert sess._native_client.protocol_version_sync() == "4.4"
            if mode == "execute":
                rec = sess.execute("RETURN 1").first()
                assert rec is not None
                assert rec["dt"].astimezone(datetime.UTC) == expected_dt.astimezone(datetime.UTC)
            elif mode == "arrow":
                tbl = sess.execute_to_arrow("RETURN 1")
                assert tbl.schema.field("dt").type == pa.timestamp("us", tz="Europe/Berlin")
                assert tbl.column("dt")[0].as_py().astimezone(
                    datetime.UTC
                ) == expected_dt.astimezone(datetime.UTC)
            else:
                df = sess.execute_to_polars("RETURN 1")
                assert df.schema["dt"] == pl.Datetime("us", "Europe/Berlin")
                assert df["dt"][0].astimezone(datetime.UTC) == expected_dt.astimezone(datetime.UTC)
        finally:
            sess.close()
            thread.join(timeout=2)
