"""Read-only graph entity value types returned by query execution.

Every engine and backend (Neo4j, Memgraph, FalkorDB, Apache AGE, Relate;
Python bridges and native Rust client) returns whole-entity projections as
:class:`GraphNode`, :class:`GraphRelationship`, and :class:`GraphPath`.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator, Mapping
from types import MappingProxyType
from typing import Any


class GraphNode(Mapping[str, Any]):
    """Immutable value representing a graph node returned by a query.

    Metadata lives on attributes (:attr:`id`, :attr:`element_id`,
    :attr:`labels`, :attr:`properties`). The instance itself implements
    :class:`collections.abc.Mapping` over :attr:`properties`, so
    ``node["name"]``, ``"name" in node``, ``node.get(...)``, and ``len(node)``
    read node properties without ever colliding with metadata.

    Equality and hashing use identity (:attr:`element_id` when not ``None``,
    otherwise :attr:`id`, scoped by class). Two snapshots of the same node
    with different property values therefore compare equal.
    """

    __slots__ = ("_element_id", "_id", "_labels", "_properties")
    _element_id: str | None
    _id: int | str | None
    _labels: frozenset[str]
    _properties: MappingProxyType[str, Any]

    def __init__(
        self,
        id: int | str | None = None,
        element_id: str | None = None,
        labels: Iterable[str] = (),
        properties: Mapping[str, Any] | None = None,
    ) -> None:
        frozen_labels = (
            labels if isinstance(labels, frozenset) else frozenset(str(lbl) for lbl in labels)
        )
        frozen_props = MappingProxyType(dict(properties) if properties is not None else {})
        object.__setattr__(self, "_id", id)
        object.__setattr__(self, "_element_id", element_id)
        object.__setattr__(self, "_labels", frozen_labels)
        object.__setattr__(self, "_properties", frozen_props)

    @classmethod
    def _from_trusted(
        cls,
        node_id: int | str | None,
        element_id: str | None,
        labels: frozenset[str],
        properties: dict[str, Any],
    ) -> GraphNode:
        """Internal fast-path constructor taking ownership of fresh containers."""
        inst = object.__new__(cls)
        object.__setattr__(inst, "_id", node_id)
        object.__setattr__(inst, "_element_id", element_id)
        object.__setattr__(inst, "_labels", labels)
        object.__setattr__(inst, "_properties", MappingProxyType(properties))
        return inst

    @property
    def id(self) -> int | str | None:
        """Engine-assigned integer (or string) node identifier, if any."""
        return self._id

    @property
    def element_id(self) -> str | None:
        """Engine-assigned string element identifier, or ``None`` when absent."""
        return self._element_id

    @property
    def labels(self) -> frozenset[str]:
        """Immutable set of node labels."""
        return self._labels

    @property
    def properties(self) -> Mapping[str, Any]:
        """Read-only mapping view of node properties."""
        return self._properties

    def __getitem__(self, key: str) -> Any:
        return self._properties[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._properties)

    def __len__(self) -> int:
        return len(self._properties)

    def __contains__(self, key: object) -> bool:
        return key in self._properties

    def _identity(self) -> Any:
        return self._element_id if self._element_id is not None else self._id

    def __eq__(self, other: object) -> bool:
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, GraphNode)
        return self._identity() == other._identity()

    def __hash__(self) -> int:
        return hash((type(self), self._identity()))

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError(f"{type(self).__name__} is immutable")

    def __delattr__(self, name: str) -> None:
        raise AttributeError(f"{type(self).__name__} is immutable")

    def __reduce__(self) -> tuple[Any, ...]:
        return (type(self), (self._id, self._element_id, self._labels, dict(self._properties)))

    def to_dict(self) -> dict[str, Any]:
        """Returns the canonical kind-tagged dictionary representation."""
        return {
            "kind": "node",
            "id": self._id,
            "element_id": self._element_id,
            "labels": sorted(self._labels),
            "properties": dict(self._properties),
        }

    def __repr__(self) -> str:
        return (
            f"GraphNode(id={self._id!r}, element_id={self._element_id!r}, "
            f"labels={sorted(self._labels)!r}, properties={dict(self._properties)!r})"
        )


class GraphRelationship(Mapping[str, Any]):
    """Immutable value representing a graph relationship returned by a query.

    Metadata lives on attributes (:attr:`id`, :attr:`element_id`, :attr:`type`,
    :attr:`start_id`, :attr:`end_id`, :attr:`start_element_id`,
    :attr:`end_element_id`, :attr:`properties`). The instance implements
    :class:`collections.abc.Mapping` over :attr:`properties`.

    Equality and hashing use identity (:attr:`element_id` when not ``None``,
    otherwise :attr:`id`, scoped by class). Two snapshots of the same
    relationship with different property values compare equal.
    """

    __slots__ = (
        "_element_id",
        "_end_element_id",
        "_end_id",
        "_id",
        "_properties",
        "_start_element_id",
        "_start_id",
        "_type",
    )
    _element_id: str | None
    _end_element_id: str | None
    _end_id: int | str | None
    _id: int | str | None
    _properties: MappingProxyType[str, Any]
    _start_element_id: str | None
    _start_id: int | str | None
    _type: str

    def __init__(
        self,
        id: int | str | None = None,
        element_id: str | None = None,
        type: str = "",
        start_id: int | str | None = None,
        end_id: int | str | None = None,
        properties: Mapping[str, Any] | None = None,
        start_element_id: str | None = None,
        end_element_id: str | None = None,
    ) -> None:
        frozen_props = MappingProxyType(dict(properties) if properties is not None else {})
        object.__setattr__(self, "_id", id)
        object.__setattr__(self, "_element_id", element_id)
        object.__setattr__(self, "_type", str(type))
        object.__setattr__(self, "_start_id", start_id)
        object.__setattr__(self, "_end_id", end_id)
        object.__setattr__(self, "_properties", frozen_props)
        object.__setattr__(self, "_start_element_id", start_element_id)
        object.__setattr__(self, "_end_element_id", end_element_id)

    @classmethod
    def _from_trusted(
        cls,
        rel_id: int | str | None,
        element_id: str | None,
        rel_type: str,
        start_id: int | str | None,
        end_id: int | str | None,
        properties: dict[str, Any],
        start_element_id: str | None = None,
        end_element_id: str | None = None,
    ) -> GraphRelationship:
        """Internal fast-path constructor taking ownership of a fresh dict."""
        inst = object.__new__(cls)
        object.__setattr__(inst, "_id", rel_id)
        object.__setattr__(inst, "_element_id", element_id)
        object.__setattr__(inst, "_type", rel_type)
        object.__setattr__(inst, "_start_id", start_id)
        object.__setattr__(inst, "_end_id", end_id)
        object.__setattr__(inst, "_properties", MappingProxyType(properties))
        object.__setattr__(inst, "_start_element_id", start_element_id)
        object.__setattr__(inst, "_end_element_id", end_element_id)
        return inst

    @property
    def id(self) -> int | str | None:
        """Engine-assigned integer (or string) relationship identifier, if any."""
        return self._id

    @property
    def element_id(self) -> str | None:
        """Engine-assigned string element identifier, or ``None`` when absent."""
        return self._element_id

    @property
    def type(self) -> str:
        """Relationship type name."""
        return self._type

    @property
    def start_id(self) -> int | str | None:
        """Identifier of the start node."""
        return self._start_id

    @property
    def end_id(self) -> int | str | None:
        """Identifier of the end node."""
        return self._end_id

    @property
    def start_element_id(self) -> str | None:
        """String element identifier of the start node, or ``None`` when absent."""
        return self._start_element_id

    @property
    def end_element_id(self) -> str | None:
        """String element identifier of the end node, or ``None`` when absent."""
        return self._end_element_id

    @property
    def properties(self) -> Mapping[str, Any]:
        """Read-only mapping view of relationship properties."""
        return self._properties

    def __getitem__(self, key: str) -> Any:
        return self._properties[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._properties)

    def __len__(self) -> int:
        return len(self._properties)

    def __contains__(self, key: object) -> bool:
        return key in self._properties

    def _identity(self) -> Any:
        return self._element_id if self._element_id is not None else self._id

    def __eq__(self, other: object) -> bool:
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, GraphRelationship)
        return self._identity() == other._identity()

    def __hash__(self) -> int:
        return hash((type(self), self._identity()))

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError(f"{type(self).__name__} is immutable")

    def __delattr__(self, name: str) -> None:
        raise AttributeError(f"{type(self).__name__} is immutable")

    def __reduce__(self) -> tuple[Any, ...]:
        return (
            type(self),
            (
                self._id,
                self._element_id,
                self._type,
                self._start_id,
                self._end_id,
                dict(self._properties),
                self._start_element_id,
                self._end_element_id,
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        """Returns the canonical kind-tagged dictionary representation."""
        out: dict[str, Any] = {
            "kind": "relationship",
            "id": self._id,
            "element_id": self._element_id,
            "type": self._type,
            "start_id": self._start_id,
            "end_id": self._end_id,
            "properties": dict(self._properties),
        }
        if self._start_element_id is not None:
            out["start_element_id"] = self._start_element_id
        if self._end_element_id is not None:
            out["end_element_id"] = self._end_element_id
        return out

    def __repr__(self) -> str:
        return (
            f"GraphRelationship(id={self._id!r}, element_id={self._element_id!r}, "
            f"type={self._type!r}, start_id={self._start_id!r}, end_id={self._end_id!r}, "
            f"properties={dict(self._properties)!r})"
        )


class GraphPath:
    """Immutable value representing a graph path returned by a query.

    ``len(path)`` and iteration ``iter(path)`` operate over
    :attr:`relationships` (matching ``neo4j.graph.Path`` and Cypher
    ``length(path)`` hop count semantics).
    """

    __slots__ = ("_nodes", "_relationships")
    _nodes: tuple[GraphNode, ...]
    _relationships: tuple[GraphRelationship, ...]

    def __init__(
        self,
        nodes: Iterable[GraphNode] = (),
        relationships: Iterable[GraphRelationship] = (),
    ) -> None:
        node_tuple = nodes if isinstance(nodes, tuple) else tuple(nodes)
        rel_tuple = relationships if isinstance(relationships, tuple) else tuple(relationships)
        object.__setattr__(self, "_nodes", node_tuple)
        object.__setattr__(self, "_relationships", rel_tuple)

    @classmethod
    def _from_trusted(
        cls,
        nodes: tuple[GraphNode, ...],
        relationships: tuple[GraphRelationship, ...],
    ) -> GraphPath:
        """Internal fast-path constructor taking ownership of tuples."""
        inst = object.__new__(cls)
        object.__setattr__(inst, "_nodes", nodes)
        object.__setattr__(inst, "_relationships", relationships)
        return inst

    @property
    def nodes(self) -> tuple[GraphNode, ...]:
        """Ordered sequence of nodes along the path."""
        return self._nodes

    @property
    def relationships(self) -> tuple[GraphRelationship, ...]:
        """Ordered sequence of relationships along the path."""
        return self._relationships

    @property
    def start_node(self) -> GraphNode | None:
        """First node in the path, or ``None`` if empty."""
        return self._nodes[0] if self._nodes else None

    @property
    def end_node(self) -> GraphNode | None:
        """Last node in the path, or ``None`` if empty."""
        return self._nodes[-1] if self._nodes else None

    def __len__(self) -> int:
        return len(self._relationships)

    def __iter__(self) -> Iterator[GraphRelationship]:
        return iter(self._relationships)

    def __eq__(self, other: object) -> bool:
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, GraphPath)
        return self._nodes == other._nodes and self._relationships == other._relationships

    def __hash__(self) -> int:
        return hash((type(self), self._nodes, self._relationships))

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError(f"{type(self).__name__} is immutable")

    def __delattr__(self, name: str) -> None:
        raise AttributeError(f"{type(self).__name__} is immutable")

    def __reduce__(self) -> tuple[Any, ...]:
        return (type(self), (self._nodes, self._relationships))

    def to_dict(self) -> dict[str, Any]:
        """Returns the canonical kind-tagged dictionary representation."""
        return {
            "kind": "path",
            "nodes": [node.to_dict() for node in self._nodes],
            "relationships": [rel.to_dict() for rel in self._relationships],
        }

    def __repr__(self) -> str:
        return f"GraphPath(nodes={self._nodes!r}, relationships={self._relationships!r})"


def json_default(obj: Any) -> Any:
    """JSON serialization helper for ``json.dumps(..., default=json_default)``."""
    if isinstance(obj, (GraphNode, GraphRelationship, GraphPath)):
        return obj.to_dict()
    if isinstance(obj, MappingProxyType):
        return dict(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _extract_raw_id(obj: Any) -> Any:
    """Reads ``_id`` before ``id`` to avoid Neo4j 6 deprecation warnings."""
    if hasattr(obj, "_id"):
        return obj._id
    return getattr(obj, "id", None)


def _extract_properties(obj: Any) -> dict[str, Any]:
    """Extracts and recursively converts properties from a driver node or relationship."""
    props = getattr(obj, "properties", None)
    if isinstance(props, Mapping):
        return {str(k): convert_driver_value(v) for k, v in props.items()}
    priv_props = getattr(obj, "_properties", None)
    if isinstance(priv_props, Mapping):
        return {str(k): convert_driver_value(v) for k, v in priv_props.items()}
    if isinstance(obj, Mapping):
        return {str(k): convert_driver_value(v) for k, v in obj.items()}
    return {}


def convert_driver_value(value: Any) -> Any:
    """Converts a driver result value into Voyager's unified entity/value representation.

    Handles Neo4j/Memgraph Bolt driver entities, FalkorDB entities, Relate
    entities, and recurses through lists and dicts while preserving spatial and
    scalar driver objects (such as ``neo4j.spatial.WGS84Point``).
    """
    if isinstance(value, (GraphNode, GraphRelationship, GraphPath)):
        return value
    if value is None or isinstance(value, (bool, int, float, str, bytes)):
        return value

    # 1. Paths: Neo4j/Relate (.nodes + .relationships) or FalkorDB (.nodes() + .edges())
    if not isinstance(value, Mapping):
        if hasattr(value, "nodes") and hasattr(value, "relationships"):
            raw_nodes = value.nodes() if callable(value.nodes) else value.nodes
            raw_rels = (
                value.relationships() if callable(value.relationships) else value.relationships
            )
            return GraphPath._from_trusted(
                tuple(convert_driver_value(n) for n in raw_nodes),
                tuple(convert_driver_value(r) for r in raw_rels),
            )
        if callable(getattr(value, "nodes", None)) and callable(getattr(value, "edges", None)):
            return GraphPath._from_trusted(
                tuple(convert_driver_value(n) for n in value.nodes()),
                tuple(convert_driver_value(e) for e in value.edges()),
            )

    # 2. Relationships: FalkorDB Edge (.relation, .src_node, .dest_node)
    if (
        not isinstance(value, Mapping)
        and hasattr(value, "relation")
        and hasattr(value, "src_node")
        and hasattr(value, "dest_node")
    ):
        return GraphRelationship._from_trusted(
            rel_id=getattr(value, "id", None),
            element_id=None,
            rel_type=str(value.relation),
            start_id=value.src_node,
            end_id=value.dest_node,
            properties=_extract_properties(value),
        )

    # 3. Relationships: Neo4j / Relate (.type + start/end node or id)
    if hasattr(value, "type") and (
        hasattr(value, "start_node") or hasattr(value, "start_id") or hasattr(value, "start")
    ):
        start_obj = getattr(value, "start_node", None)
        end_obj = getattr(value, "end_node", None)
        start_eid = getattr(value, "start_element_id", None) or getattr(
            value, "start_node_element_id", None
        )
        end_eid = getattr(value, "end_element_id", None) or getattr(
            value, "end_node_element_id", None
        )

        if start_obj is None:
            start_id = getattr(value, "start_id", None)
            if start_id is None:
                start_id = getattr(value, "start", None)
        elif isinstance(start_obj, (int, str)):
            start_id = start_obj
        else:
            start_id = _extract_raw_id(start_obj)
            if start_eid is None:
                start_eid = getattr(start_obj, "element_id", None)

        if end_obj is None:
            end_id = getattr(value, "end_id", None)
            if end_id is None:
                end_id = getattr(value, "end", None)
        elif isinstance(end_obj, (int, str)):
            end_id = end_obj
        else:
            end_id = _extract_raw_id(end_obj)
            if end_eid is None:
                end_eid = getattr(end_obj, "element_id", None)

        return GraphRelationship._from_trusted(
            rel_id=_extract_raw_id(value),
            element_id=getattr(value, "element_id", None),
            rel_type=str(value.type),
            start_id=start_id,
            end_id=end_id,
            properties=_extract_properties(value),
            start_element_id=start_eid,
            end_element_id=end_eid,
        )

    # 4. Nodes: Neo4j / FalkorDB / Relate (.labels + properties)
    if hasattr(value, "labels") and (
        hasattr(value, "properties") or hasattr(value, "_properties") or isinstance(value, Mapping)
    ):
        raw_labels = getattr(value, "labels", None) or ()
        return GraphNode._from_trusted(
            node_id=_extract_raw_id(value),
            element_id=getattr(value, "element_id", None),
            labels=frozenset(str(lbl) for lbl in raw_labels),
            properties=_extract_properties(value),
        )

    # 5. Relate spatial Point and Duration (must not intercept neo4j.spatial.* or neo4j.time.*)
    mod = type(value).__module__
    if not mod.startswith("neo4j"):
        if hasattr(value, "srid") and hasattr(value, "x") and hasattr(value, "y"):
            from voyager_ogm.types import Point

            return Point(
                x=value.x,
                y=value.y,
                z=getattr(value, "z", None),
                srid=value.srid,
            )
        if hasattr(value, "months") and hasattr(value, "seconds") and hasattr(value, "nanoseconds"):
            to_td = getattr(value, "to_timedelta", None)
            if callable(to_td):
                try:
                    return to_td()
                except Exception:
                    return str(value)

    # 6. Plain mappings and lists/tuples (preserving tuple subclasses such as neo4j.spatial.Point)
    if isinstance(value, Mapping):
        return {k: convert_driver_value(v) for k, v in value.items()}
    if isinstance(value, list) or type(value) is tuple:
        return [convert_driver_value(item) for item in value]
    return value


def _contains_graph_entity(value: Any) -> bool:
    """Returns whether ``value`` is or contains a GraphNode/GraphRelationship/GraphPath."""
    if isinstance(value, (GraphNode, GraphRelationship, GraphPath)):
        return True
    if isinstance(value, list) or type(value) is tuple:
        return any(_contains_graph_entity(item) for item in value)
    if isinstance(value, Mapping):
        return any(_contains_graph_entity(v) for v in value.values())
    return False


def serialize_value_for_arrow(value: Any) -> Any:
    """Serializes a single cell to kind-tagged JSON text when it contains entities."""
    if _contains_graph_entity(value):
        return json.dumps(value, default=json_default)
    return value


def serialize_records_for_arrow(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Serializes graph entity cells into kind-tagged JSON strings for Arrow/Polars."""
    if not records:
        return records
    return [{k: serialize_value_for_arrow(v) for k, v in row.items()} for row in records]


__all__ = [
    "GraphNode",
    "GraphPath",
    "GraphRelationship",
    "convert_driver_value",
    "json_default",
    "serialize_records_for_arrow",
    "serialize_value_for_arrow",
]
