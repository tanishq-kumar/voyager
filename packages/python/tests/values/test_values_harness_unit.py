"""Offline unit tests for the value conformance harness helpers (#299)."""

from __future__ import annotations

import datetime
import json
import zoneinfo
from decimal import Decimal

import pytest
from voyager_ogm import Duration, Point

from .equality import compare_value_and_type, values_and_types_equal
from .known_differences import (
    KnownDifferenceEntry,
    find_matching_entry,
    find_stale_entries,
    load_known_differences,
)
from .restrictions import ENGINES, VALUE_KINDS, is_supported, strategy_for


def test_equality_float_nan_and_signed_zero() -> None:
    assert values_and_types_equal(float("nan"), float("nan"))
    assert values_and_types_equal(-0.0, -0.0)
    assert not values_and_types_equal(-0.0, 0.0)
    assert not values_and_types_equal(1, 1.0)
    assert not values_and_types_equal(True, 1)


def test_equality_decimal_scale_and_specials() -> None:
    assert values_and_types_equal(Decimal("1.50"), Decimal("1.50"))
    assert not values_and_types_equal(Decimal("1.50"), Decimal("1.5"))
    assert values_and_types_equal(Decimal("NaN"), Decimal("NaN"))
    assert values_and_types_equal(Decimal("Infinity"), Decimal("Infinity"))
    assert not values_and_types_equal(Decimal("-Infinity"), Decimal("Infinity"))


def test_equality_temporals_and_duration() -> None:
    t_naive = datetime.time(12, 0, 0)
    t_utc = datetime.time(12, 0, 0, tzinfo=datetime.UTC)
    assert not values_and_types_equal(t_naive, t_utc)
    assert values_and_types_equal(t_utc, datetime.time(12, 0, 0, tzinfo=datetime.UTC))

    dt_berlin = datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=zoneinfo.ZoneInfo("Europe/Berlin"))
    dt_offset = datetime.datetime(
        2024, 1, 2, 3, 4, 5, tzinfo=datetime.timezone(datetime.timedelta(hours=1))
    )
    assert not values_and_types_equal(dt_berlin, dt_offset)

    assert values_and_types_equal(
        Duration(0, 0, 1, -500_000_000),
        Duration(0, 0, 0, 500_000_000),
    )
    assert not values_and_types_equal(
        Duration(0, 1, 0, 0),
        Duration(0, 0, 86_400, 0),
    )
    assert values_and_types_equal(
        Point(latitude=52.52, longitude=13.405), Point(latitude=52.52, longitude=13.405)
    )
    assert compare_value_and_type({"a": 1}, {"a": 1.0})


def test_restrictions_and_strategies_exist_for_all_engines_and_kinds() -> None:
    for engine in ENGINES:
        for kind in VALUE_KINDS:
            if is_supported(engine, kind):
                strat = strategy_for(engine, kind)
                assert strat is not None


def test_known_difference_matching_and_stale_detection() -> None:
    entries = [
        KnownDifferenceEntry(
            index=0,
            engine="neo4j",
            backend="native",
            path="param",
            value_kind="point",
            category="bug",
            reason="Point returns dict",
            repro="Session(...).execute('RETURN $v', {'v': Point(0, 0)})",
            match="dict",
        ),
        KnownDifferenceEntry(
            index=1,
            engine="memgraph",
            backend="native",
            path="param",
            value_kind="int",
            category="bug",
            reason="Stale entry",
            repro="repro",
        ),
    ]
    matched = find_matching_entry(
        entries,
        engine="neo4j",
        backend="native",
        path="param",
        value_kind="point",
        message="type mismatch: dict != WGS84Point",
    )
    assert matched is not None and matched.index == 0

    executed = {
        ("neo4j", "native", "param", "point"),
        ("memgraph", "native", "param", "int"),
    }
    stale = find_stale_entries(entries, executed, {0})
    assert [e.index for e in stale] == [1]


def test_compare_value_and_type_raises_nothing_on_mismatch() -> None:
    diffs = compare_value_and_type([1, 2], [1, 2.0])
    assert len(diffs) == 1
    assert "type mismatch" in diffs[0]
    with pytest.raises(ValueError, match="Unknown value kind"):
        strategy_for("neo4j", "does_not_exist")


def test_inspect_native_wire_truncates_multibyte_utf8_on_char_boundary() -> None:
    from voyager_ogm import _voyager_rs

    payload = json.loads(_voyager_rs.inspect_native_wire("pg19", "SELECT $v", {"v": "é" * 300}))
    value = payload["values"][0]
    assert value.endswith("é... (600 bytes)")
    assert "\ufffd" not in value
    assert len(value.encode("utf-8")) <= 256 + len("... (600 bytes)")


def test_known_differences_file_loads_and_has_bug_repros() -> None:
    entries = load_known_differences()
    assert entries
    for entry in entries:
        assert entry.engine in ENGINES
        assert entry.reason
        if entry.category == "bug":
            assert entry.repro
