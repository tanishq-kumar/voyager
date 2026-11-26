"""Unit tests for Python -> native ParamValue conversion (#391)."""

from __future__ import annotations

import datetime
import json
import math
import uuid
import zoneinfo
from decimal import Decimal

import pytest
from voyager_ogm import Duration, Point
from voyager_ogm._voyager_rs import inspect_native_params
from voyager_ogm.types import serialize_param_value


def test_python_to_native_param_types_preserved() -> None:
    """Every Python temporal, UUID, Decimal, bytes type preserves its ParamValue variant."""
    sample_uuid = uuid.UUID("12345678-1234-5678-1234-567812345678")
    payload = {
        "d": datetime.date(2024, 1, 2),
        "t_naive": datetime.time(14, 30, 15, 123456),
        "t_offset": datetime.time(
            14, 30, 15, 123456, tzinfo=datetime.timezone(datetime.timedelta(hours=2))
        ),
        "dt_naive": datetime.datetime(2024, 1, 2, 3, 4, 5, 123456),
        "dt_utc": datetime.datetime(2024, 1, 2, 3, 4, 5, 123456, tzinfo=datetime.UTC),
        "dt_offset": datetime.datetime(
            2024, 1, 2, 3, 4, 5, 123456, tzinfo=datetime.timezone(datetime.timedelta(hours=2))
        ),
        "dt_zone": datetime.datetime(
            2024, 1, 2, 3, 4, 5, 123456, tzinfo=zoneinfo.ZoneInfo("Europe/Berlin")
        ),
        "td_pos": datetime.timedelta(days=2, seconds=3661, microseconds=500000),
        "td_neg": datetime.timedelta(seconds=-5),
        "u": sample_uuid,
        "dec": Decimal("123.4500"),
        "b": b"\x01\x02\xff",
        "ba": bytearray(b"\x03\x04"),
        "plain_date_str": "2024-01-02",
        "plain_uuid_str": str(sample_uuid),
        "plain_dec_str": "123.4500",
    }

    decoded = json.loads(inspect_native_params(payload))

    assert decoded["d"] == {"Date": {"year": 2024, "month": 1, "day": 2}}
    assert decoded["t_naive"] == {
        "Time": {
            "hour": 14,
            "minute": 30,
            "second": 15,
            "nanosecond": 123_456_000,
            "tz_offset_seconds": None,
        }
    }
    assert decoded["t_offset"] == {
        "Time": {
            "hour": 14,
            "minute": 30,
            "second": 15,
            "nanosecond": 123_456_000,
            "tz_offset_seconds": 7200,
        }
    }
    assert decoded["dt_naive"] == {
        "LocalDateTime": {
            "year": 2024,
            "month": 1,
            "day": 2,
            "hour": 3,
            "minute": 4,
            "second": 5,
            "nanosecond": 123_456_000,
        }
    }
    assert decoded["dt_utc"] == {
        "DateTime": {
            "utc_seconds": 1_704_164_645,
            "nanosecond": 123_456_000,
            "tz": {"OffsetSeconds": 0},
        }
    }
    assert decoded["dt_offset"] == {
        "DateTime": {
            "utc_seconds": 1_704_157_445,
            "nanosecond": 123_456_000,
            "tz": {"OffsetSeconds": 7200},
        }
    }
    assert decoded["dt_zone"] == {
        "DateTime": {
            "utc_seconds": 1_704_161_045,
            "nanosecond": 123_456_000,
            "tz": {"ZoneId": {"id": "Europe/Berlin", "offset_seconds": 3600}},
        }
    }
    assert decoded["td_pos"] == {
        "Duration": {
            "months": 0,
            "days": 2,
            "seconds": 3661,
            "nanoseconds": 500_000_000,
        }
    }
    assert decoded["td_neg"] == {
        "Duration": {
            "months": 0,
            "days": -1,
            "seconds": 86395,
            "nanoseconds": 0,
        }
    }
    assert decoded["u"] == {"Uuid": "12345678-1234-5678-1234-567812345678"}
    assert decoded["dec"] == {"Decimal": "123.4500"}
    assert decoded["b"] == {"Bytes": [1, 2, 255]}
    assert decoded["ba"] == {"Bytes": [3, 4]}

    # Genuine strings stay plain Json strings and are never reinterpreted.
    assert decoded["plain_date_str"] == {"Json": "2024-01-02"}
    assert decoded["plain_uuid_str"] == {"Json": str(sample_uuid)}
    assert decoded["plain_dec_str"] == {"Json": "123.4500"}


def test_python_to_native_param_nesting_lists_tuples_dicts() -> None:
    """Typed parameters survive recursion inside lists, tuples, and dicts."""
    payload = {
        "items": [
            datetime.date(2024, 1, 2),
            (
                Decimal("9.99"),
                uuid.UUID("12345678-1234-5678-1234-567812345678"),
            ),
            {"bytes": b"\xaa", "text": "2024-01-02"},
        ]
    }
    decoded = json.loads(inspect_native_params(payload))
    assert decoded == {
        "items": {
            "List": [
                {"Date": {"year": 2024, "month": 1, "day": 2}},
                {
                    "List": [
                        {"Decimal": "9.99"},
                        {"Uuid": "12345678-1234-5678-1234-567812345678"},
                    ]
                },
                {
                    "Map": {
                        "bytes": {"Bytes": [170]},
                        "text": {"Json": "2024-01-02"},
                    }
                },
            ]
        }
    }


def test_python_to_native_param_duration_point_floats_and_overflow() -> None:
    """Duration, Point (object and tagged dict), NaN/Inf floats, and >i64 integer handling."""
    dur = Duration(months=14, days=2, seconds=3, nanoseconds=500)
    p2 = Point(latitude=52.52, longitude=13.405)
    p3 = Point(x=1.0, y=2.0, z=3.0)
    payload = {
        "dur": dur,
        "p2": p2,
        "p3": p3,
        "tagged_p": serialize_param_value(p2),
        "inf": float("inf"),
        "ninf": float("-inf"),
    }
    decoded = json.loads(inspect_native_params(payload))
    assert decoded["dur"] == {
        "Duration": {"months": 14, "days": 2, "seconds": 3, "nanoseconds": 500}
    }
    assert decoded["p2"] == {"Point2D": {"srid": 4326, "x": 13.405, "y": 52.52}}
    assert decoded["p3"] == {"Point3D": {"srid": 9157, "x": 1.0, "y": 2.0, "z": 3.0}}
    assert decoded["tagged_p"] == {"Point2D": {"srid": 4326, "x": 13.405, "y": 52.52}}
    # serde_json renders non-finite f64 inside ParamValue::Float as null in JSON output,
    # distinct from ParamValue::Json(null)
    assert decoded["inf"] == {"Float": None}
    assert decoded["ninf"] == {"Float": None}

    nan_decoded = json.loads(inspect_native_params({"nan": float("nan")}))
    assert nan_decoded == {"nan": {"Float": None}}

    # Integers outside signed 64-bit raise OverflowError instead of silently mutating type
    with pytest.raises(OverflowError):
        inspect_native_params({"huge": 2**63})
    assert math.isnan(float("nan"))


def test_spatial_mapping_defaults_srid_and_falls_back_on_unknown_shape() -> None:
    """Tagged spatial dicts without srid infer default SRIDs and unknown shapes stay plain maps."""
    decoded = json.loads(
        inspect_native_params(
            {
                "geo_2d": {"__voyager_spatial__": {"latitude": 52.52, "longitude": 13.405}},
                "geo_3d": {
                    "__voyager_spatial__": {"latitude": 52.52, "longitude": 13.405, "height": 34.0}
                },
                "cart_2d": {"__voyager_spatial__": {"x": 1.0, "y": 2.0}},
                "cart_3d": {"__voyager_spatial__": {"x": 1.0, "y": 2.0, "z": 3.0}},
                "unknown": {"__voyager_spatial__": {"foo": 1}},
                "non_dict": {"__voyager_spatial__": "raw"},
            }
        )
    )
    assert decoded["geo_2d"] == {"Point2D": {"srid": 4326, "x": 13.405, "y": 52.52}}
    assert decoded["geo_3d"] == {"Point3D": {"srid": 4979, "x": 13.405, "y": 52.52, "z": 34.0}}
    assert decoded["cart_2d"] == {"Point2D": {"srid": 7203, "x": 1.0, "y": 2.0}}
    assert decoded["cart_3d"] == {"Point3D": {"srid": 9157, "x": 1.0, "y": 2.0, "z": 3.0}}
    assert decoded["unknown"] == {"Map": {"__voyager_spatial__": {"Map": {"foo": {"Json": 1}}}}}
    assert decoded["non_dict"] == {"Map": {"__voyager_spatial__": {"Json": "raw"}}}
