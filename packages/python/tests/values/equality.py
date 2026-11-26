"""Canonical value and Python type equality helpers for value conformance (#299)."""

from __future__ import annotations

import datetime
import math
from decimal import Decimal
from typing import Any

from voyager_ogm import Duration, GraphNode, GraphPath, GraphRelationship, Point


def _tz_zone_name(tz: datetime.tzinfo | None) -> str | None:
    """Extracts an IANA zone name from ``ZoneInfo`` or ``pytz`` tzinfo objects."""
    if tz is None:
        return None
    key = getattr(tz, "key", None)
    if isinstance(key, str):
        return key
    zone = getattr(tz, "zone", None)
    if isinstance(zone, str):
        return zone
    return None


def _compare_float(actual: float, expected: float, context: str) -> list[str]:
    if math.isnan(expected):
        if not math.isnan(actual):
            return [f"{context}: expected NaN, got {actual!r}"]
        return []
    if math.isnan(actual):
        return [f"{context}: expected {expected!r}, got NaN"]
    if math.isinf(expected):
        if actual != expected:
            return [f"{context}: expected {expected!r}, got {actual!r}"]
        return []
    if expected == 0.0 and actual == 0.0:
        if math.copysign(1.0, actual) != math.copysign(1.0, expected):
            return [f"{context}: zero sign mismatch ({actual!r} != {expected!r})"]
        return []
    if actual != expected:
        return [f"{context}: float {actual!r} != {expected!r}"]
    return []


def _compare_decimal(actual: Decimal, expected: Decimal, context: str) -> list[str]:
    if expected.is_nan():
        if not actual.is_nan():
            return [f"{context}: expected Decimal('NaN'), got {actual!r}"]
        return []
    if actual.is_nan():
        return [f"{context}: expected {expected!r}, got Decimal('NaN')"]
    if expected.is_infinite():
        if not (actual.is_infinite() and actual.is_signed() == expected.is_signed()):
            return [f"{context}: Decimal infinity mismatch ({actual!r} != {expected!r})"]
        return []
    if actual != expected:
        return [f"{context}: Decimal {actual!r} != {expected!r}"]
    if actual.as_tuple().exponent != expected.as_tuple().exponent:
        return [
            f"{context}: Decimal scale mismatch "
            f"(exponent {actual.as_tuple().exponent!r} != {expected.as_tuple().exponent!r}, "
            f"{actual!r} vs {expected!r})"
        ]
    return []


def _compare_time(actual: datetime.time, expected: datetime.time, context: str) -> list[str]:
    if (actual.tzinfo is None) != (expected.tzinfo is None):
        return [f"{context}: time tz-awareness mismatch ({actual!r} vs {expected!r})"]
    if actual.tzinfo is None:
        if actual != expected:
            return [f"{context}: time {actual!r} != {expected!r}"]
        return []
    actual_off = actual.utcoffset()
    expected_off = expected.utcoffset()
    if actual_off != expected_off:
        return [
            f"{context}: time offset mismatch ({actual_off!r} != {expected_off!r}, "
            f"{actual!r} vs {expected!r})"
        ]
    actual_hms = (actual.hour, actual.minute, actual.second, actual.microsecond)
    expected_hms = (expected.hour, expected.minute, expected.second, expected.microsecond)
    if actual_hms != expected_hms:
        return [f"{context}: time wall-clock {actual!r} != {expected!r}"]
    return []


def _compare_datetime(
    actual: datetime.datetime, expected: datetime.datetime, context: str
) -> list[str]:
    if (actual.tzinfo is None) != (expected.tzinfo is None):
        return [f"{context}: datetime tz-awareness mismatch ({actual!r} vs {expected!r})"]
    if actual.tzinfo is None:
        if actual != expected:
            return [f"{context}: naive datetime {actual!r} != {expected!r}"]
        return []
    actual_utc = actual.astimezone(datetime.UTC)
    expected_utc = expected.astimezone(datetime.UTC)
    if actual_utc != expected_utc:
        return [
            f"{context}: datetime instant {actual_utc.isoformat()} != {expected_utc.isoformat()} "
            f"({actual!r} vs {expected!r})"
        ]
    actual_off = actual.utcoffset()
    expected_off = expected.utcoffset()
    if actual_off != expected_off:
        return [
            f"{context}: datetime offset mismatch ({actual_off!r} != {expected_off!r}, "
            f"{actual!r} vs {expected!r})"
        ]
    actual_zone = _tz_zone_name(actual.tzinfo)
    expected_zone = _tz_zone_name(expected.tzinfo)
    if actual_zone != expected_zone:
        return [
            f"{context}: datetime zone mismatch ({actual_zone!r} != {expected_zone!r}, "
            f"{actual!r} vs {expected!r})"
        ]
    return []


def _compare_point(actual: Point, expected: Point, context: str) -> list[str]:
    if actual.srid != expected.srid:
        return [f"{context}: Point SRID {actual.srid} != {expected.srid}"]
    if actual.is_3d != expected.is_3d:
        return [f"{context}: Point is_3d {actual.is_3d} != {expected.is_3d}"]
    if not (
        math.isclose(actual.x, expected.x, rel_tol=0.0, abs_tol=1e-9)
        and math.isclose(actual.y, expected.y, rel_tol=0.0, abs_tol=1e-9)
    ):
        return [f"{context}: Point coords ({actual.x}, {actual.y}) != ({expected.x}, {expected.y})"]
    if actual.is_3d and not math.isclose(
        actual.z or 0.0, expected.z or 0.0, rel_tol=0.0, abs_tol=1e-9
    ):
        return [f"{context}: Point z {actual.z} != {expected.z}"]
    return []


def compare_value_and_type(actual: Any, expected: Any, *, context: str = "value") -> list[str]:
    """Compares ``actual`` against ``expected`` for both Python type and semantic value.

    Handles ``NaN``, ``-0.0`` vs ``+0.0``, tz-aware vs naive temporals, ``Decimal`` scale
    and specials, ``Duration`` component semantics, ``Point``, and nested containers/entities.

    Returns:
        A list of difference descriptions (empty when equal).
    """
    if expected is None:
        if actual is not None:
            return [f"{context}: expected None, got {type(actual).__name__}({actual!r})"]
        return []
    if actual is None:
        return [f"{context}: expected {type(expected).__name__}({expected!r}), got None"]

    if type(actual) is not type(expected):
        return [
            f"{context}: type mismatch: actual {type(actual).__name__} ({actual!r}) "
            f"!= expected {type(expected).__name__} ({expected!r})"
        ]

    if isinstance(expected, float):
        return _compare_float(actual, expected, context)
    if isinstance(expected, Decimal):
        return _compare_decimal(actual, expected, context)
    if isinstance(expected, datetime.datetime):
        return _compare_datetime(actual, expected, context)
    if isinstance(expected, datetime.time):
        return _compare_time(actual, expected, context)
    if isinstance(expected, datetime.date):
        if actual != expected:
            return [f"{context}: date {actual!r} != {expected!r}"]
        return []
    if isinstance(expected, Duration):
        if actual != expected:
            return [f"{context}: Duration {actual!r} != {expected!r}"]
        return []
    if isinstance(expected, Point):
        return _compare_point(actual, expected, context)
    if isinstance(expected, list):
        if len(actual) != len(expected):
            return [f"{context}: list length {len(actual)} != {len(expected)}"]
        diffs: list[str] = []
        for idx, (a_item, e_item) in enumerate(zip(actual, expected, strict=True)):
            diffs.extend(compare_value_and_type(a_item, e_item, context=f"{context}[{idx}]"))
        return diffs
    if isinstance(expected, dict):
        if set(actual.keys()) != set(expected.keys()):
            return [
                f"{context}: dict keys {sorted(actual.keys())!r} != {sorted(expected.keys())!r}"
            ]
        diffs = []
        for key in expected:
            diffs.extend(
                compare_value_and_type(actual[key], expected[key], context=f"{context}[{key!r}]")
            )
        return diffs
    if isinstance(expected, GraphNode):
        if actual.labels != expected.labels:
            return [f"{context}.labels: {actual.labels!r} != {expected.labels!r}"]
        return compare_value_and_type(
            dict(actual.properties), dict(expected.properties), context=f"{context}.properties"
        )
    if isinstance(expected, GraphRelationship):
        if actual.type != expected.type:
            return [f"{context}.type: {actual.type!r} != {expected.type!r}"]
        return compare_value_and_type(
            dict(actual.properties), dict(expected.properties), context=f"{context}.properties"
        )
    if isinstance(expected, GraphPath):
        if len(actual.nodes) != len(expected.nodes):
            return [f"{context}.nodes: len {len(actual.nodes)} != {len(expected.nodes)}"]
        if len(actual.relationships) != len(expected.relationships):
            return [
                f"{context}.relationships: len {len(actual.relationships)} != {len(expected.relationships)}"
            ]
        diffs = []
        for idx, (a_n, e_n) in enumerate(zip(actual.nodes, expected.nodes, strict=True)):
            diffs.extend(compare_value_and_type(a_n, e_n, context=f"{context}.nodes[{idx}]"))
        for idx, (a_r, e_r) in enumerate(
            zip(actual.relationships, expected.relationships, strict=True)
        ):
            diffs.extend(
                compare_value_and_type(a_r, e_r, context=f"{context}.relationships[{idx}]")
            )
        return diffs

    if actual != expected:
        return [f"{context}: {actual!r} != {expected!r}"]
    return []


def values_and_types_equal(actual: Any, expected: Any) -> bool:
    """Boolean convenience wrapper over :func:`compare_value_and_type`."""
    return not compare_value_and_type(actual, expected)
