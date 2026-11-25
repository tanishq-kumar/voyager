"""Unit tests for the Python Apache AGE agtype decoder."""

from __future__ import annotations

import pytest
from voyager_ogm.agtype import decode_agtype

VERTEX = '{"id": 1, "label": "P", "properties": {"name": "A"}}::vertex'
EDGE = '{"id": 2, "label": "K", "start_id": 1, "end_id": 3, "properties": {"since": 2020}}::edge'
VERTEX_B = '{"id": 3, "label": "P", "properties": {"name": "B"}}::vertex'


def test_top_level_vertex() -> None:
    value = decode_agtype(VERTEX)
    assert value == {
        "kind": "node",
        "id": 1,
        "labels": ["P"],
        "properties": {"name": "A"},
    }


def test_top_level_edge() -> None:
    value = decode_agtype(EDGE)
    assert value == {
        "kind": "relationship",
        "id": 2,
        "type": "K",
        "start_id": 1,
        "end_id": 3,
        "properties": {"since": 2020},
    }


def test_top_level_path() -> None:
    value = decode_agtype(f"[{VERTEX}, {EDGE}, {VERTEX_B}]::path")
    assert value["kind"] == "path"
    assert [node["id"] for node in value["nodes"]] == [1, 3]
    assert [rel["type"] for rel in value["relationships"]] == ["K"]
    assert value["sequence"] == [1]


def test_list_of_vertices() -> None:
    value = decode_agtype(f"[{VERTEX}, {VERTEX_B}]")
    assert isinstance(value, list)
    assert [item["properties"]["name"] for item in value] == ["A", "B"]


def test_map_containing_vertex() -> None:
    value = decode_agtype(f'{{"n": {VERTEX}, "count": 1}}')
    assert value["count"] == 1
    assert value["n"]["kind"] == "node"
    assert value["n"]["properties"]["name"] == "A"


def test_nested_list() -> None:
    value = decode_agtype(f'[[{VERTEX}], {{"inner": [{EDGE}]}}]')
    assert value[0][0]["id"] == 1
    assert value[1]["inner"][0]["type"] == "K"


def test_quoted_string_containing_annotation() -> None:
    assert decode_agtype('"hello ::vertex"') == "hello ::vertex"
    assert decode_agtype('{"text": "a ::edge b"}') == {"text": "a ::edge b"}


def test_plain_map_with_entity_keys_stays_a_map() -> None:
    value = decode_agtype('{"id": 1, "label": "x", "properties": {"city": "Berlin"}}')
    assert value == {"id": 1, "label": "x", "properties": {"city": "Berlin"}}


def test_unannotated_entity_list_stays_a_list() -> None:
    value = decode_agtype(f"[{VERTEX}, {EDGE}, {VERTEX_B}]")
    assert isinstance(value, list)
    assert len(value) == 3
    assert [item["kind"] for item in value] == ["node", "relationship", "node"]


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ('"A"', "A"),
        ("42", 42),
        ("-7", -7),
        ("1.5", 1.5),
        ("123::numeric", 123),
        ("1.25::numeric", 1.25),
        ("true", True),
        ("false", False),
        ("null", None),
        ("[1, 2, 3]", [1, 2, 3]),
        ('{"a": 1, "b": [true, null]}', {"a": 1, "b": [True, None]}),
    ],
)
def test_scalars(text: str, expected: object) -> None:
    assert decode_agtype(text) == expected
