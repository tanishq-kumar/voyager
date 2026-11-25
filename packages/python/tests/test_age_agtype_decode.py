"""Unit tests for the Python Apache AGE agtype decoder."""

from __future__ import annotations

import pytest
from voyager_ogm import GraphNode, GraphPath, GraphRelationship
from voyager_ogm.agtype import decode_agtype

VERTEX = '{"id": 1, "label": "P", "properties": {"name": "A"}}::vertex'
EDGE = '{"id": 2, "label": "K", "start_id": 1, "end_id": 3, "properties": {"since": 2020}}::edge'
VERTEX_B = '{"id": 3, "label": "P", "properties": {"name": "B"}}::vertex'


def test_top_level_vertex() -> None:
    value = decode_agtype(VERTEX)
    assert isinstance(value, GraphNode)
    assert value.id == 1
    assert value.element_id is None
    assert value.labels == frozenset({"P"})
    assert dict(value.properties) == {"name": "A"}
    assert value["name"] == "A"
    assert value.to_dict() == {
        "kind": "node",
        "id": 1,
        "element_id": None,
        "labels": ["P"],
        "properties": {"name": "A"},
    }


def test_top_level_edge() -> None:
    value = decode_agtype(EDGE)
    assert isinstance(value, GraphRelationship)
    assert value.id == 2
    assert value.element_id is None
    assert value.type == "K"
    assert value.start_id == 1
    assert value.end_id == 3
    assert dict(value.properties) == {"since": 2020}
    assert value["since"] == 2020
    assert value.to_dict() == {
        "kind": "relationship",
        "id": 2,
        "element_id": None,
        "type": "K",
        "start_id": 1,
        "end_id": 3,
        "properties": {"since": 2020},
    }


def test_top_level_path() -> None:
    value = decode_agtype(f"[{VERTEX}, {EDGE}, {VERTEX_B}]::path")
    assert isinstance(value, GraphPath)
    assert [node.id for node in value.nodes] == [1, 3]
    assert [rel.type for rel in value.relationships] == ["K"]
    assert value.to_dict()["kind"] == "path"


def test_list_of_vertices() -> None:
    value = decode_agtype(f"[{VERTEX}, {VERTEX_B}]")
    assert isinstance(value, list)
    assert all(isinstance(item, GraphNode) for item in value)
    assert [item["name"] for item in value] == ["A", "B"]


def test_map_containing_vertex() -> None:
    value = decode_agtype(f'{{"n": {VERTEX}, "count": 1}}')
    assert value["count"] == 1
    assert isinstance(value["n"], GraphNode)
    assert value["n"]["name"] == "A"


def test_nested_list() -> None:
    value = decode_agtype(f'[[{VERTEX}], {{"inner": [{EDGE}]}}]')
    assert isinstance(value[0][0], GraphNode)
    assert value[0][0].id == 1
    assert isinstance(value[1]["inner"][0], GraphRelationship)
    assert value[1]["inner"][0].type == "K"


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
    assert [type(item) for item in value] == [GraphNode, GraphRelationship, GraphNode]


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
