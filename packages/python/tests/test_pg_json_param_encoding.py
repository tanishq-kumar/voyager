"""Offline wire/unit tests for PostgreSQL JSON parameter encoding (#421, #422)."""

from __future__ import annotations

import importlib
import json
from typing import Any
from unittest.mock import MagicMock

import pytest
from voyager_ogm import Json, Session
from voyager_ogm._voyager_rs import inspect_native_wire
from voyager_ogm.bridge import MockBridge, PostgresBridge

Jsonb: Any = None
try:
    Jsonb = importlib.import_module("psycopg.types.json").Jsonb
except ImportError:  # pragma: no cover - psycopg is a test dependency
    pass


def _wire(statement: str, params: dict) -> dict:
    return json.loads(inspect_native_wire("pg19", statement, params))


def test_json_wrapper_is_exported() -> None:
    assert Json([1, 2]).value == [1, 2]
    assert Json.__voyager_json__ is True


def test_native_dict_and_json_cast_bind_as_jsonb() -> None:
    assert _wire("SELECT $v::jsonb AS v", {"v": {"k": 1}})["oids"] == [3802]
    assert _wire("SELECT $v::jsonb AS v", {"v": {"k": 1}})["values"] == ['{"k":1}']

    nested = _wire("SELECT $v::jsonb AS v", {"v": [{}]})
    assert nested["oids"] == [3802]
    assert nested["values"] == ["[{}]"]

    deep = _wire("SELECT $v::jsonb AS v", {"v": [[1, 2], [3]]})
    assert deep["oids"] == [3802]
    assert deep["values"] == ["[[1,2],[3]]"]


def test_native_empty_list_is_json_array_not_json_object() -> None:
    empty = _wire("SELECT $v::jsonb AS v", {"v": []})
    assert empty["oids"] == [3802]
    assert empty["values"] == ["[]"]

    # Without a JSON cast an empty list remains an empty SQL array literal.
    array = _wire("SELECT $v AS v", {"v": []})
    assert array["oids"] == [0]
    assert array["values"] == ["{}"]


def test_native_list_of_dicts_is_jsonb_array_without_a_cast() -> None:
    # psycopg adapts each dict as one jsonb element and the list as an array.
    mapped = _wire("SELECT $v AS v", {"v": [{"x": 1}]})
    assert mapped["oids"] == [3807]
    assert mapped["values"] == ['{"{\\"x\\":1}"}']

    nested = _wire("SELECT $v AS v", {"v": [[{"x": 1}]]})
    assert nested["oids"] == [3807]
    assert nested["values"] == ['{{"{\\"x\\":1}"}}']

    # With an explicit JSON cast the same list is one JSON document.
    cast = _wire("SELECT $v::jsonb AS v", {"v": [{"x": 1}]})
    assert cast["oids"] == [3802]
    assert cast["values"] == ['[{"x":1}]']


def test_native_nested_list_is_a_multidimensional_array_without_a_cast() -> None:
    nested = _wire("SELECT $v AS v", {"v": [[1, 2], [3, 4]]})
    assert nested["oids"] == [1016]
    assert nested["values"] == ["{{1,2},{3,4}}"]

    # Ragged nested lists are not representable as PostgreSQL arrays; the
    # server rejects the array literal, just like psycopg's adaptation does.
    ragged = _wire("SELECT $v AS v", {"v": [[1, 2], []]})
    assert ragged["oids"] == [0]
    assert ragged["values"] == ["{{1,2},{}}"]


def test_native_json_cast_keeps_empty_and_nested_lists_as_json() -> None:
    for value, expected in [([], "[]"), ([{}], "[{}]"), ([[]], "[[]]")]:
        wire = _wire("SELECT $v::jsonb AS v", {"v": value})
        assert wire["oids"] == [3802]
        assert wire["values"] == [expected]


def test_native_scalar_list_stays_array_without_cast_but_json_with_cast() -> None:
    plain = _wire("SELECT $v AS v", {"v": [1, 2, 3]})
    assert plain["oids"] == [1016]
    assert plain["values"] == ["{1,2,3}"]

    cast = _wire("SELECT $v::jsonb AS v", {"v": [1, 2, 3]})
    assert cast["oids"] == [3802]
    assert cast["values"] == ["[1,2,3]"]

    spaced = _wire("SELECT $v :: jsonb AS v", {"v": []})
    assert spaced["oids"] == [3802]
    assert spaced["values"] == ["[]"]


def test_native_json_wrapper_forces_jsonb_without_a_cast() -> None:
    wrapped = _wire("SELECT $v AS v", {"v": Json([1, 2, 3])})
    assert wrapped["oids"] == [3802]
    assert wrapped["values"] == ["[1,2,3]"]

    scalar = _wire("SELECT $v AS v", {"v": Json(5)})
    assert scalar["oids"] == [3802]
    assert scalar["values"] == ["5"]

    null = _wire("SELECT $v AS v", {"v": Json(None)})
    assert null["oids"] == [3802]
    assert null["values"] == ["null"]


def test_native_json_wrapper_inside_containers_is_unwrapped() -> None:
    wire = _wire("SELECT $v AS v", {"v": {"items": [Json([1]), Json({"k": 2})]}})
    assert wire["oids"] == [3802]
    assert wire["values"] == ['{"items":[[1],{"k":2}]}']


def test_native_json_non_finite_floats_are_written_as_pg_tokens() -> None:
    wire = _wire("SELECT $v::jsonb AS v", {"v": {"nan": float("nan"), "inf": float("inf")}})
    assert wire["oids"] == [3802]
    assert wire["values"] == ['{"inf":Infinity,"nan":NaN}']


def test_native_float_text_round_trips_extreme_values() -> None:
    wire = _wire("SELECT $v::jsonb AS v", {"v": {"k": 2.332717393807508e-41}})
    assert wire["values"] == ['{"k":2.332717393807508e-41}']

    wire = _wire("SELECT $v::jsonb AS v", {"v": {"k": 1.7976931348623157e308}})
    assert wire["values"] == ['{"k":1.7976931348623157e+308}']


def test_age_parameter_map_honours_json_wrapper() -> None:
    statement = "SELECT * FROM cypher('g', $$ RETURN $v $$, %s) AS (v agtype)"
    wire = _wire(statement, {"v": Json({"k": 1})})
    assert wire["oids"] == [0]
    assert json.loads(wire["values"][0]) == {"v": {"k": 1}}


def test_mock_bridge_receives_unwrapped_json_value() -> None:
    bridge = MockBridge()
    session = Session(bridge=bridge, dialect="sql_pgq")
    session.execute("SELECT $v::jsonb AS v", {"v": Json([1, 2])})
    assert bridge.executed_queries[-1][1]["v"] == Json([1, 2])


@pytest.mark.skipif(Jsonb is None, reason="psycopg is not installed")
def test_postgres_bridge_adapts_json_values() -> None:
    bridge = PostgresBridge(MagicMock())

    formatted, ordered = bridge._format_pgq_statement("SELECT $v::jsonb AS v", {"v": {"k": 1}})
    assert formatted == "SELECT %s::jsonb AS v"
    assert isinstance(ordered[0], Jsonb)
    assert ordered[0].obj == {"k": 1}

    _, ordered = bridge._format_pgq_statement("SELECT $v::jsonb AS v", {"v": []})
    assert isinstance(ordered[0], Jsonb)
    assert ordered[0].obj == []

    _, ordered = bridge._format_pgq_statement("SELECT $v::jsonb AS v", {"v": [{}]})
    assert isinstance(ordered[0], Jsonb)
    assert ordered[0].obj == [{}]


@pytest.mark.skipif(Jsonb is None, reason="psycopg is not installed")
def test_postgres_bridge_keeps_scalar_arrays_and_honours_wrapper() -> None:
    bridge = PostgresBridge(MagicMock())

    _, ordered = bridge._format_pgq_statement("SELECT $v AS v", {"v": [1, 2]})
    assert ordered == [[1, 2]]

    _, ordered = bridge._format_pgq_statement("SELECT $v::jsonb AS v", {"v": [1, 2]})
    assert isinstance(ordered[0], Jsonb)
    assert ordered[0].obj == [1, 2]

    _, ordered = bridge._format_pgq_statement("SELECT $v AS v", {"v": Json([1, 2])})
    assert isinstance(ordered[0], Jsonb)
    assert ordered[0].obj == [1, 2]

    _, ordered = bridge._format_pgq_statement("SELECT $v AS v", {"v": Json(None)})
    assert isinstance(ordered[0], Jsonb)
    assert ordered[0].obj is None


@pytest.mark.skipif(Jsonb is None, reason="psycopg is not installed")
def test_postgres_bridge_adapts_list_of_dicts_as_jsonb_array() -> None:
    bridge = PostgresBridge(MagicMock())

    _, ordered = bridge._format_pgq_statement("SELECT $v AS v", {"v": [{"x": 1}]})
    assert isinstance(ordered[0], list)
    assert len(ordered[0]) == 1
    assert isinstance(ordered[0][0], Jsonb)
    assert ordered[0][0].obj == {"x": 1}

    _, ordered = bridge._format_pgq_statement("SELECT $v AS v", {"v": [[{"x": 1}]]})
    assert isinstance(ordered[0][0], list)
    assert isinstance(ordered[0][0][0], Jsonb)

    # Nested scalar lists stay lists so psycopg builds a multidimensional array.
    _, ordered = bridge._format_pgq_statement("SELECT $v AS v", {"v": [[1, 2], [3, 4]]})
    assert ordered == [[[1, 2], [3, 4]]]

    # An explicit JSON cast always collapses the value into one JSON document.
    _, ordered = bridge._format_pgq_statement("SELECT $v::jsonb AS v", {"v": [{"x": 1}]})
    assert isinstance(ordered[0], Jsonb)
    assert ordered[0].obj == [{"x": 1}]


def test_postgres_bridge_age_map_unwraps_json_wrapper() -> None:
    bridge = PostgresBridge(MagicMock())
    statement = "SELECT * FROM cypher('g', $$ RETURN $v $$, %s) AS (v agtype)"
    formatted, params = bridge._format_pgq_statement(statement, {"v": Json({"k": [Json(1)]})})
    assert formatted == statement
    assert json.loads(params[0]) == {"v": {"k": [1]}}
