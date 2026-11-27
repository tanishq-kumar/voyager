"""Unit tests for unified GraphNode, GraphRelationship, GraphPath entities and converters."""

from __future__ import annotations

import json
import pickle
from collections.abc import Iterator, Mapping
from typing import Any

import pytest
from voyager_ogm import GraphNode, GraphPath, GraphRelationship, Point, json_default
from voyager_ogm.agtype import decode_agtype
from voyager_ogm.entities import (
    convert_driver_value,
    serialize_records_for_arrow,
    serialize_value_for_arrow,
)
from voyager_ogm.viewer.extractor import extract_graph_entities_from_records


class _FakeBoltNode(Mapping[str, Any]):
    """Mimics a neo4j.graph.Node (Mapping + _id + element_id + labels)."""

    def __init__(
        self,
        node_id: int,
        element_id: str | None,
        labels: list[str] | set[str],
        props: dict[str, Any],
    ) -> None:
        self._id = node_id
        self.element_id = element_id
        self.labels = frozenset(labels)
        self._props = dict(props)

    def __getitem__(self, key: str) -> Any:
        return self._props[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._props)

    def __len__(self) -> int:
        return len(self._props)


class _FakeBoltRel(Mapping[str, Any]):
    """Mimics a neo4j.graph.Relationship."""

    def __init__(
        self,
        rel_id: int,
        element_id: str | None,
        rel_type: str,
        start_node: _FakeBoltNode,
        end_node: _FakeBoltNode,
        props: dict[str, Any],
    ) -> None:
        self._id = rel_id
        self.element_id = element_id
        self.type = rel_type
        self.start_node = start_node
        self.end_node = end_node
        self._props = dict(props)

    def __getitem__(self, key: str) -> Any:
        return self._props[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._props)

    def __len__(self) -> int:
        return len(self._props)


class _FakeBoltPath:
    """Mimics a neo4j.graph.Path."""

    def __init__(self, nodes: list[_FakeBoltNode], relationships: list[_FakeBoltRel]) -> None:
        self.nodes = tuple(nodes)
        self.relationships = tuple(relationships)


class _FakeSpatialPoint(tuple[float, ...]):
    """Mimics neo4j.spatial.Point (a tuple subclass with coordinates and srid)."""

    __module__ = "neo4j.spatial"
    srid = 4326

    @property
    def x(self) -> float:
        return self[0]

    @property
    def y(self) -> float:
        return self[1]


class _FakeFalkorNode:
    """Mimics falkordb.node.Node."""

    __module__ = "falkordb.node"

    def __init__(self, node_id: int, labels: list[str], properties: dict[str, Any]) -> None:
        self.id = node_id
        self.labels = labels
        self.properties = properties


class _FakeFalkorEdge:
    """Mimics falkordb.edge.Edge."""

    __module__ = "falkordb.edge"

    def __init__(
        self,
        edge_id: int,
        relation: str,
        src_node: int,
        dest_node: int,
        properties: dict[str, Any],
    ) -> None:
        self.id = edge_id
        self.relation = relation
        self.src_node = src_node
        self.dest_node = dest_node
        self.properties = properties


class _FakeFalkorPath:
    """Mimics falkordb.path.Path where nodes()/edges() are methods."""

    __module__ = "falkordb.path"

    def __init__(self, nodes: list[_FakeFalkorNode], edges: list[_FakeFalkorEdge]) -> None:
        self._nodes = nodes
        self._edges = edges

    def nodes(self) -> list[_FakeFalkorNode]:
        return list(self._nodes)

    def edges(self) -> list[_FakeFalkorEdge]:
        return list(self._edges)


class _FakeRelateNode:
    """Mimics relate._native.Node where metadata and properties are separate."""

    __module__ = "relate._native"

    def __init__(
        self,
        node_id: int,
        element_id: str | None,
        labels: list[str],
        properties: dict[str, Any],
    ) -> None:
        self.id = node_id
        self.element_id = element_id
        self.labels = labels
        self.properties = properties


class _FakeRelateRelationship:
    """Mimics relate._native.Relationship."""

    __module__ = "relate._native"

    def __init__(
        self,
        rel_id: int,
        element_id: str | None,
        rel_type: str,
        start_id: int,
        end_id: int,
        properties: dict[str, Any],
        start_element_id: str | None = None,
        end_element_id: str | None = None,
    ) -> None:
        self.id = rel_id
        self.element_id = element_id
        self.type = rel_type
        self.start_id = start_id
        self.end_id = end_id
        self.start_element_id = start_element_id
        self.end_element_id = end_element_id
        self.properties = properties


class _FakeRelatePath:
    """Mimics relate._native.Path."""

    __module__ = "relate._native"

    def __init__(
        self,
        nodes: list[_FakeRelateNode],
        relationships: list[_FakeRelateRelationship],
    ) -> None:
        self.nodes = nodes
        self.relationships = relationships


def test_graph_node_mapping_immutability_equality_and_pickle() -> None:
    node = GraphNode(
        id=42,
        element_id="4:db:42",
        labels=frozenset({"Person", "Employee"}),
        properties={"name": "Alice", "age": 30},
    )

    # Mapping protocol
    assert node["name"] == "Alice"
    assert node.get("age") == 30
    assert node.get("missing", "fallback") == "fallback"
    assert "name" in node
    assert "missing" not in node
    assert len(node) == 2
    assert set(node.keys()) == {"name", "age"}
    assert dict(node.items()) == {"name": "Alice", "age": 30}
    assert node.labels == frozenset({"Person", "Employee"})

    # Immutability (frozen slots + read-only properties proxy)
    props_any: Any = node.properties
    node_any: Any = node
    with pytest.raises(AttributeError):
        node_any.id = 99
    with pytest.raises(AttributeError):
        delattr(node_any, "labels")
    with pytest.raises(TypeError):
        props_any["name"] = "Bob"
    with pytest.raises(TypeError):
        node_any["name"] = "Bob"

    # Identity equality and hashing
    same_eid = GraphNode(
        id=999,
        element_id="4:db:42",
        labels=frozenset({"Other"}),
        properties={"name": "DifferentSnapshot"},
    )
    diff_eid = GraphNode(
        id=42,
        element_id="4:db:43",
        labels=frozenset({"Person", "Employee"}),
        properties={"name": "Alice", "age": 30},
    )
    assert node == same_eid
    assert hash(node) == hash(same_eid)
    assert node != diff_eid
    assert len({node, same_eid, diff_eid}) == 2

    # Fallback to numeric id when element_id is None
    no_eid_a = GraphNode(id=7, element_id=None, labels=["A"], properties={"x": 1})
    no_eid_b = GraphNode(id=7, element_id=None, labels=["B"], properties={"x": 2})
    no_eid_c = GraphNode(id=8, element_id=None, labels=["A"], properties={"x": 1})
    assert no_eid_a == no_eid_b
    assert hash(no_eid_a) == hash(no_eid_b)
    assert no_eid_a != no_eid_c

    # to_dict & repr
    assert node.to_dict() == {
        "kind": "node",
        "id": 42,
        "element_id": "4:db:42",
        "labels": ["Employee", "Person"],
        "properties": {"name": "Alice", "age": 30},
    }
    assert "GraphNode(" in repr(node)
    assert "'name': 'Alice'" in repr(node)

    # Pickle round-trip across all protocols
    for proto in range(pickle.HIGHEST_PROTOCOL + 1):
        restored = pickle.loads(pickle.dumps(node, protocol=proto))
        assert isinstance(restored, GraphNode)
        assert restored == node
        assert restored.id == 42
        assert restored.element_id == "4:db:42"
        assert restored.labels == frozenset({"Person", "Employee"})
        assert dict(restored.properties) == {"name": "Alice", "age": 30}
        with pytest.raises(TypeError):
            restored.properties["x"] = 1


def test_graph_relationship_mapping_identity_to_dict_and_pickle() -> None:
    rel = GraphRelationship(
        id=10,
        element_id="5:db:10",
        type="KNOWS",
        start_id=1,
        end_id=2,
        properties={"since": 2020},
        start_element_id="4:db:1",
        end_element_id="4:db:2",
    )

    assert rel["since"] == 2020
    assert rel.get("weight", 1.0) == 1.0
    assert "since" in rel
    assert len(rel) == 1
    assert rel.start_id == 1
    assert rel.end_id == 2
    assert rel.start_element_id == "4:db:1"
    assert rel.end_element_id == "4:db:2"

    # Cross-class inequality even with matching element_id
    node_same_eid = GraphNode(id=10, element_id="5:db:10", labels=["X"], properties={})
    assert rel != node_same_eid

    assert rel.to_dict() == {
        "kind": "relationship",
        "id": 10,
        "element_id": "5:db:10",
        "type": "KNOWS",
        "start_id": 1,
        "end_id": 2,
        "properties": {"since": 2020},
        "start_element_id": "4:db:1",
        "end_element_id": "4:db:2",
    }

    rel_compact = GraphRelationship(
        id=11,
        element_id=None,
        type="LIKES",
        start_id=1,
        end_id=2,
        properties={},
    )
    assert set(rel_compact.to_dict().keys()) == {
        "kind",
        "id",
        "element_id",
        "type",
        "start_id",
        "end_id",
        "properties",
    }

    rel_any: Any = rel
    with pytest.raises(AttributeError):
        rel_any.type = "OTHER"
    rel_props_any: Any = rel.properties
    with pytest.raises(TypeError):
        rel_props_any["since"] = 2021

    for proto in range(pickle.HIGHEST_PROTOCOL + 1):
        restored = pickle.loads(pickle.dumps(rel, protocol=proto))
        assert restored == rel
        assert restored.to_dict() == rel.to_dict()


def test_graph_path_and_json_default() -> None:
    a = GraphNode(id=1, element_id="n:1", labels=["Person"], properties={"name": "A"})
    b = GraphNode(id=2, element_id="n:2", labels=["Person"], properties={"name": "B"})
    k = GraphRelationship(
        id=3,
        element_id="r:3",
        type="KNOWS",
        start_id=1,
        end_id=2,
        properties={"w": 1},
    )
    path = GraphPath(nodes=(a, b), relationships=(k,))

    assert path.start_node == a
    assert path.end_node == b
    assert len(path) == 1
    assert list(path) == [k]
    assert (
        path.to_dict()
        == {
            "kind": "path",
            "nodes": [a.to_dict()],
            "relationships": [k.to_dict()],
        }
        if False
        else {
            "kind": "path",
            "nodes": [a.to_dict(), b.to_dict()],
            "relationships": [k.to_dict()],
        }
    )

    for proto in range(pickle.HIGHEST_PROTOCOL + 1):
        restored = pickle.loads(pickle.dumps(path, protocol=proto))
        assert restored == path
        assert len(restored) == 1

    payload = json.loads(json.dumps({"node": a, "path": path}, default=json_default))
    assert payload["node"] == a.to_dict()
    assert payload["path"] == path.to_dict()

    point = Point(latitude=52.52, longitude=13.405)
    point_payload = json.loads(json.dumps({"p": point}, default=json_default))
    assert point_payload["p"] == {"srid": 4326, "x": 13.405, "y": 52.52}

    with pytest.raises(TypeError):
        json.dumps(object(), default=json_default)


def test_convert_driver_value_handles_all_driver_shapes_and_nesting() -> None:
    # 1. Neo4j / Bolt objects (Bolt spatial points become voyager_ogm.Point)
    bn1 = _FakeBoltNode(1, "4:x:1", ["ZzA", "ZzB"], {"name": "A"})
    bn2 = _FakeBoltNode(2, "4:x:2", ["ZzA"], {"name": "B"})
    br = _FakeBoltRel(3, "5:x:3", "ZK", bn1, bn2, {"w": 1})
    bp = _FakeBoltPath([bn1, bn2], [br])
    pt = _FakeSpatialPoint((1.0, 2.0))

    converted_bolt = convert_driver_value({"path": bp, "list": [bn1, br], "point": pt})
    assert isinstance(converted_bolt["path"], GraphPath)
    assert isinstance(converted_bolt["path"].nodes[0], GraphNode)
    assert converted_bolt["path"].nodes[0].labels == frozenset({"ZzA", "ZzB"})
    assert isinstance(converted_bolt["path"].relationships[0], GraphRelationship)
    assert converted_bolt["path"].relationships[0].start_element_id == "4:x:1"
    assert isinstance(converted_bolt["point"], Point)
    assert converted_bolt["point"] == Point(x=1.0, y=2.0, srid=4326)

    # 2. FalkorDB objects
    fn1 = _FakeFalkorNode(10, ["Person", "VIP"], {"name": "Alice"})
    fn2 = _FakeFalkorNode(11, ["City"], {"name": "Paris"})
    fe = _FakeFalkorEdge(20, "LIVES_IN", 10, 11, {"since": 2021})
    fp = _FakeFalkorPath([fn1, fn2], [fe])

    converted_fk = convert_driver_value([fn1, fe, fp])
    assert isinstance(converted_fk[0], GraphNode)
    assert converted_fk[0].element_id is None
    assert converted_fk[0].labels == frozenset({"Person", "VIP"})
    assert isinstance(converted_fk[1], GraphRelationship)
    assert converted_fk[1].type == "LIVES_IN"
    assert isinstance(converted_fk[2], GraphPath)
    assert [n["name"] for n in converted_fk[2].nodes] == ["Alice", "Paris"]

    # 3. Apache AGE agtype decoder (including nested lists/maps; plain dict stays dict)
    v_a = '{"id": 100, "label": "P", "properties": {"name": "AgeA"}}::vertex'
    e_k = '{"id": 101, "label": "K", "start_id": 100, "end_id": 102, "properties": {"w": 2}}::edge'
    v_b = '{"id": 102, "label": "P", "properties": {"name": "AgeB"}}::vertex'
    age_path = decode_agtype(f"[{v_a}, {e_k}, {v_b}]::path")
    assert isinstance(age_path, GraphPath)
    assert [n["name"] for n in age_path.nodes] == ["AgeA", "AgeB"]
    assert age_path.relationships[0].type == "K"
    age_map = decode_agtype(f'{{"n": {v_a}, "nested": [{e_k}]}}')
    assert isinstance(age_map["n"], GraphNode)
    assert isinstance(age_map["nested"][0], GraphRelationship)
    assert decode_agtype('{"id": 1, "label": "plain"}') == {"id": 1, "label": "plain"}

    # 4. Relate objects: properties named id / labels / type / element_id are NOT shadowed
    rn1 = _FakeRelateNode(
        50,
        "r-eid-50",
        ["User"],
        {"id": "user-uuid-1", "labels": ["user-tag"], "name": "R1"},
    )
    rn2 = _FakeRelateNode(51, "r-eid-51", ["User"], {"name": "R2"})
    rr = _FakeRelateRelationship(
        60,
        "r-eid-60",
        "FOLLOWS",
        50,
        51,
        {"type": "direct", "element_id": "prop-eid"},
        start_element_id="r-eid-50",
        end_element_id="r-eid-51",
    )
    rp = _FakeRelatePath([rn1, rn2], [rr])
    converted_rel = convert_driver_value({"n": rn1, "r": rr, "p": rp})
    assert isinstance(converted_rel["n"], GraphNode)
    assert converted_rel["n"].id == 50
    assert converted_rel["n"].element_id == "r-eid-50"
    assert converted_rel["n"].labels == frozenset({"User"})
    assert converted_rel["n"]["id"] == "user-uuid-1"
    assert converted_rel["n"]["labels"] == ["user-tag"]
    assert isinstance(converted_rel["r"], GraphRelationship)
    assert converted_rel["r"].type == "FOLLOWS"
    assert converted_rel["r"]["type"] == "direct"
    assert converted_rel["r"]["element_id"] == "prop-eid"
    assert isinstance(converted_rel["p"], GraphPath)


def test_convert_driver_value_unifies_point_mappings() -> None:
    """FalkorDB point maps and Bolt wire dicts both hydrate to ``voyager_ogm.Point``."""
    bolt_wire = convert_driver_value({"srid": 4326, "x": 13.405, "y": 52.52})
    falkor_map = convert_driver_value({"latitude": 52.52, "longitude": 13.405})
    cartesian = convert_driver_value({"srid": 7203, "x": 1.0, "y": 2.0})

    assert bolt_wire == Point(latitude=52.52, longitude=13.405)
    assert falkor_map == Point(latitude=52.52, longitude=13.405)
    assert cartesian == Point(x=1.0, y=2.0, srid=7203)
    assert convert_driver_value({"x": 1.0, "y": 2.0}) == {"x": 1.0, "y": 2.0}
    # A user map that also carries non-coordinate keys stays a map (no data loss).
    user_map = {"srid": 4326, "x": 1.0, "y": 2.0, "name": "a"}
    assert convert_driver_value(user_map) == user_map
    geo_map = {"latitude": 52.52, "longitude": 13.405, "city": "Berlin"}
    assert convert_driver_value(geo_map) == geo_map


def test_arrow_serialization_helpers_and_viewer_extractor() -> None:
    a = GraphNode(id=1, element_id="e:1", labels=["ZzS"], properties={"name": "A"})
    b = GraphNode(id=2, element_id="e:2", labels=["ZzS"], properties={"name": "B"})
    k = GraphRelationship(
        id=3,
        element_id="e:3",
        type="ZK",
        start_id=1,
        end_id=2,
        properties={"w": 1},
        start_element_id="e:1",
        end_element_id="e:2",
    )
    p = GraphPath(nodes=(a, b), relationships=(k,))

    assert json.loads(serialize_value_for_arrow(a)) == a.to_dict()
    assert json.loads(serialize_value_for_arrow([b])) == [b.to_dict()]
    rows = serialize_records_for_arrow([{"a": a, "k": k, "p": p, "m": {"node": b}, "s": "hi"}])
    assert json.loads(rows[0]["a"]) == a.to_dict()
    assert json.loads(rows[0]["m"]) == {"node": b.to_dict()}
    assert rows[0]["s"] == "hi"

    nodes, edges = extract_graph_entities_from_records(
        [{"p": p, "extra": [a], "nested": {"rel": k}}]
    )
    assert {n["data"]["name"] for n in nodes} == {"A", "B"}
    assert len(edges) == 1
    assert edges[0]["label"] == "ZK"
    assert edges[0]["source"] == "e:1"
    assert edges[0]["target"] == "e:2"
