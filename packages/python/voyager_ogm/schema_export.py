"""Model schema export, live schema introspection and per-model JSON Schema.

``export_schema`` describes declared ``Node`` and ``Relationship`` models as a
JSON-serialisable dict built from the same native registry entries that drive
DDL generation. ``introspect_schema`` reads the same structure from a live
database. ``model_json_schema`` (exposed as ``Model.to_json_schema()``) emits a
JSON Schema draft 2020-12 document for the properties of one model.
"""

from __future__ import annotations

import copy
import datetime
import enum
import json
import math
import types
import typing
from collections.abc import Iterable, Mapping
from typing import Any

from voyager_ogm.models import (
    _BUILTIN_TYPES,
    Field,
    Node,
    Relationship,
    VectorProperty,
    _model_identity,
)
from voyager_ogm.types import Point, to_iso_duration

JSON_SCHEMA_DIALECT = "https://json-schema.org/draft/2020-12/schema"

_NO_JSON = object()

#: Default number of entities sampled per label or relationship type by
#: ``introspect_schema`` (matches the Rust and CLI default).
DEFAULT_SAMPLE_SIZE = 1000

# Keyword names used by ``export_schema`` for Field value constraints.
_CONSTRAINT_ATTRS = ("pattern", "ge", "gt", "le", "lt", "min_length", "max_length", "choices")

_POINT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "x": {"type": "number"},
        "y": {"type": "number"},
        "z": {"type": "number"},
        "latitude": {"type": "number"},
        "longitude": {"type": "number"},
        "height": {"type": "number"},
        "srid": {"type": "integer"},
        "crs": {"type": "string"},
    },
    "required": ["x", "y"],
}


def _to_json_value(value: Any) -> Any:
    """Converts a Python default or choice value to a JSON value, or ``_NO_JSON``.

    Strings, finite numbers, booleans, ``None``, lists/tuples and string-keyed
    mappings convert recursively. Dates, times and datetimes use ISO 8601
    strings, ``timedelta`` an ISO 8601 duration, ``Point`` its dictionary form
    and ``Enum`` members their value. Anything else is not JSON-representable.
    """
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else _NO_JSON
    if isinstance(value, enum.Enum):
        return _to_json_value(value.value)
    if isinstance(value, (datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, datetime.timedelta):
        return to_iso_duration(value)
    if isinstance(value, Point):
        return value.to_dict()
    if isinstance(value, (list, tuple)):
        items = [_to_json_value(v) for v in value]
        return _NO_JSON if any(v is _NO_JSON for v in items) else items
    if isinstance(value, Mapping):
        out: dict[str, Any] = {}
        for k, v in value.items():
            converted = _to_json_value(v)
            if not isinstance(k, str) or converted is _NO_JSON:
                return _NO_JSON
            out[k] = converted
        return out
    return _NO_JSON


def _static_default(field: Field[Any]) -> Any:
    """Returns the JSON form of a field's plain default, or ``_NO_JSON``.

    A ``default_factory`` is never called: it may be non-deterministic (for
    example ``uuid4`` or ``datetime.now``) or have side effects, so a factory
    default is reported only through ``has_default``.
    """
    if field.default_factory is not None or field.default is ...:
        return _NO_JSON
    return _to_json_value(field.default)


def _check_model(model: Any) -> None:
    if not (
        isinstance(model, type)
        and (issubclass(model, Node) or issubclass(model, Relationship))
        and model not in (Node, Relationship)
    ):
        raise TypeError(
            f"expected a Node or Relationship model class, got {model!r}; "
            "pass models declared with Node/Relationship subclasses, @node or @relationship"
        )


def _registry_entry(model: type[Any]) -> dict[str, Any]:
    """Registers ``model`` into a private native registry and returns its entry.

    Reusing the native registry keeps types, flags and labels identical to the
    ``NativeSchemaRegistry`` snapshot and generated DDL, without touching the
    shared global registry.
    """
    from voyager_ogm._voyager_rs import NativeSchemaRegistry

    registry = NativeSchemaRegistry()
    model.register_schema(registry=registry)
    if issubclass(model, Relationship):
        return registry.relationship_schemas()[0]
    return registry.node_schemas()[0]


def _export_property(attr: str, field: Field[Any], entry: Mapping[str, Any]) -> dict[str, Any]:
    prop: dict[str, Any] = {
        "name": entry["name"],
        "attribute": attr,
        "type": entry["type"],
        "nullable": entry["nullable"],
        "primary_key": entry["primary_key"],
        "unique": entry["unique"],
        "indexed": entry["indexed"],
        "index_type": entry["index_type"],
        "description": entry["description"],
        "has_default": field.has_default,
    }
    default = _static_default(field)
    if default is not _NO_JSON:
        prop["default"] = default
    if "dimensions" in entry:
        prop["vector"] = {
            "dimensions": entry["dimensions"],
            "similarity": entry["similarity"],
            "index_name": entry["index_name"],
        }
    constraints: dict[str, Any] = {}
    for key in _CONSTRAINT_ATTRS:
        value = getattr(field, key, None)
        if value is None:
            continue
        if key == "pattern" and not isinstance(value, str):
            value = value.pattern
        elif key == "choices":
            value = [_to_json_value(v) for v in value]
        constraints[key] = value
    prop["constraints"] = constraints
    return prop


def _export_properties(model: type[Any], entry: Mapping[str, Any]) -> dict[str, Any]:
    registry_fields: Mapping[str, Any] = entry.get("fields", {})
    props: dict[str, Any] = {}
    for attr, field in getattr(model, "_schema_fields", {}).items():
        reg = registry_fields.get(field.name or attr)
        if reg is None:
            continue
        props[reg["name"]] = _export_property(attr, field, reg)
    return props


def export_schema(models: Iterable[type[Node] | type[Relationship]]) -> dict[str, Any]:
    """Describes declared models as a JSON-serialisable graph schema dict.

    Node and relationship entries are built from the native schema registry
    entries that also drive DDL generation, so property ``type`` names (for
    example ``STRING``, ``INTEGER``, ``LIST``) and the ``nullable``, ``unique``,
    ``indexed``, ``index_type`` and ``primary_key`` flags match the registry
    snapshot. Properties are keyed by database property name; ``attribute``
    gives the Python attribute name. ``default`` is present only for a plain
    default that is JSON-representable (``has_default`` also covers
    ``default_factory``, which is never called). ``constraints`` holds the
    declared ``Field`` value constraints under their ``Field`` keyword names.

    Args:
        models: Node and Relationship model classes, in the order to report them.

    Returns:
        ``{"nodes": [...], "relationships": [...]}``. Each node has ``name``,
        ``model`` (module-qualified identity), ``labels``, ``description``,
        ``primary_key``, ``properties`` and ``relationship_attributes`` (the
        declarative navigation attributes, or an empty list); each relationship
        has ``name``, ``model``, ``type``, ``source_labels``,
        ``target_labels`` (empty when the model declares no endpoints),
        ``directed``, ``description`` and ``properties``.

    Raises:
        TypeError: If ``models`` is a single class or contains anything other
            than Node or Relationship model classes.

    Example:
        >>> schema = export_schema([Person, Movie, ActedIn])
        >>> schema["relationships"][0]["source_labels"]
        ['Person']
    """
    if isinstance(models, type):
        raise TypeError("export_schema expects an iterable of model classes, e.g. [Person]")
    nodes: list[dict[str, Any]] = []
    rels: list[dict[str, Any]] = []
    for model in models:
        _check_model(model)
        entry = _registry_entry(model)
        if issubclass(model, Relationship):
            rels.append(
                {
                    "name": entry["name"],
                    "model": _model_identity(model),
                    "type": entry["type_name"],
                    "source_labels": list(entry["source_labels"]),
                    "target_labels": list(entry["target_labels"]),
                    "directed": entry["directed"],
                    "description": entry["description"],
                    "properties": _export_properties(model, entry),
                }
            )
        else:
            nodes.append(
                {
                    "name": entry["name"],
                    "model": _model_identity(model),
                    "labels": list(entry["labels"]),
                    "description": entry["description"],
                    "primary_key": entry["primary_key"],
                    "properties": _export_properties(model, entry),
                    "relationship_attributes": list(entry.get("relationship_declarations", [])),
                }
            )
    return {"nodes": nodes, "relationships": rels}


def _introspection_uri(source: Any) -> str:
    """Returns the connection URI of a URI string, ``NativeClient`` or session."""
    if isinstance(source, str):
        return source
    native = getattr(source, "_native_client", None)
    for candidate in (native, source):
        uri = getattr(candidate, "uri", None) if candidate is not None else None
        if isinstance(uri, str) and "://" in uri:
            return uri
    raise TypeError(
        "introspect_schema expects a connection URI, a NativeClient, or a Session or "
        f"AsyncSession that uses the native backend; got {type(source).__name__}. "
        "Pass the connection URI for sessions built on a Python driver bridge."
    )


def introspect_schema(source: Any, *, sample_size: int = DEFAULT_SAMPLE_SIZE) -> dict[str, Any]:
    """Reads a live database's graph schema in the shape of ``export_schema``.

    Introspection runs in Rust (``voyager_drivers::introspect``), the same code
    as ``voyager introspect --format json``, on its own short-lived connection;
    the GIL is released while it runs. Supported engines: Neo4j 5.13+ and
    Memgraph (``bolt://``), FalkorDB (``redis://`` / ``falkordb://``, the graph
    named in the URI) and PostgreSQL SQL/PGQ (experimental: PostgreSQL 19 Beta 1-3 and 20 development builds) property graphs
    (``postgresql://...?graph=<name>``).

    ``nodes`` and ``relationships`` have the keys of ``export_schema``, so the
    two can be compared. Keys the database cannot know are ``None``: ``model``,
    ``description``, ``attribute``, ``has_default``, ``constraints`` (the
    ``Field`` value constraints) and a node's ``relationship_attributes``;
    ``default`` is absent. ``primary_key`` is
    ``None`` unless the engine has a key constraint (Neo4j Enterprise node key,
    PostgreSQL primary key). ``nullable`` is ``False`` only with an existence
    (``NOT NULL``) or key constraint. ``indexed`` / ``index_type`` describe a
    single-property index that does not back a constraint. Property ``type``
    comes from a type constraint or declared column type, otherwise from the
    values of at most ``sample_size`` entities per label or relationship type
    (``ANY`` for mixed values, ``None`` when no value was seen); relationship
    endpoints are sampled the same way except on PostgreSQL, where they are
    declared. The extra top-level keys ``constraints``, ``indexes`` and
    ``source`` list the catalog as the engine reports it.

    Args:
        source: A connection URI (credentials in the userinfo), a
            ``NativeClient``, or a ``Session`` / ``AsyncSession`` using the
            native backend.
        sample_size: Maximum entities sampled per label or relationship type;
            ``0`` disables sampling.

    Returns:
        ``{"nodes": [...], "relationships": [...], "constraints": [...],
        "indexes": [...], "source": {...}}``.

    Raises:
        TypeError: If ``source`` is not a URI, ``NativeClient`` or native session.
        ValueError: If ``sample_size`` is negative.
        VoyagerError: On connection or catalog query failures, and with code
            ``UNSUPPORTED_FEATURE`` for Apache AGE and DuckDB.

    Example:
        >>> live = introspect_schema("bolt://neo4j:secret@localhost:7687")
        >>> [node["name"] for node in live["nodes"]]
        ['Movie', 'Person']
    """
    if isinstance(sample_size, bool) or not isinstance(sample_size, int) or sample_size < 0:
        raise ValueError(f"sample_size must be a non-negative integer, got {sample_size!r}")
    from voyager_ogm._voyager_rs import introspect_schema_json

    result: dict[str, Any] = json.loads(
        introspect_schema_json(_introspection_uri(source), sample_size)
    )
    return result


def _type_schema(annotation: Any) -> dict[str, Any]:
    """Maps a field type annotation to a JSON Schema fragment (without null)."""
    if isinstance(annotation, str):
        annotation = _BUILTIN_TYPES.get(annotation, typing.Any)
    if annotation is None or annotation is typing.Any or annotation is type(None):
        return {}

    origin = typing.get_origin(annotation)
    args = typing.get_args(annotation)
    if origin is typing.Union or origin is types.UnionType:
        members = [_type_schema(a) for a in args if a is not type(None)]
        if len(members) == 1:
            return members[0]
        if any(m == {} for m in members):
            return {}
        return {"anyOf": members}
    if origin is typing.Literal:
        values = [_to_json_value(a) for a in args]
        return {"enum": [v for v in values if v is not _NO_JSON]}
    if origin is typing.Annotated:
        return _type_schema(args[0])
    if origin in (list, tuple, set, frozenset):
        schema: dict[str, Any] = {"type": "array"}
        if origin is list and len(args) == 1:
            schema["items"] = _type_schema(args[0])
        elif origin is tuple and len(args) == 2 and args[1] is Ellipsis:
            schema["items"] = _type_schema(args[0])
        elif origin is tuple and args:
            schema["prefixItems"] = [_type_schema(a) for a in args]
            schema["items"] = False
        elif origin in (set, frozenset):
            schema["uniqueItems"] = True
            if len(args) == 1:
                schema["items"] = _type_schema(args[0])
        return schema
    if origin is dict:
        schema = {"type": "object"}
        if len(args) == 2:
            schema["additionalProperties"] = _type_schema(args[1])
        return schema

    if not isinstance(annotation, type):
        return {}
    # bool is checked before int because bool subclasses int.
    if issubclass(annotation, bool):
        return {"type": "boolean"}
    if issubclass(annotation, enum.Enum):
        values = [_to_json_value(m.value) for m in annotation]
        return {"enum": [v for v in values if v is not _NO_JSON]}
    if issubclass(annotation, str):
        return {"type": "string"}
    if issubclass(annotation, int):
        return {"type": "integer"}
    if issubclass(annotation, float):
        return {"type": "number"}
    # datetime before date because datetime subclasses date.
    if issubclass(annotation, datetime.datetime):
        return {"type": "string", "format": "date-time"}
    if issubclass(annotation, datetime.date):
        return {"type": "string", "format": "date"}
    if issubclass(annotation, datetime.time):
        return {"type": "string", "format": "time"}
    if issubclass(annotation, datetime.timedelta):
        return {"type": "string", "format": "duration"}
    if issubclass(annotation, Point):
        return copy.deepcopy(_POINT_SCHEMA)
    if issubclass(annotation, (list, tuple)):
        return {"type": "array"}
    if issubclass(annotation, (set, frozenset)):
        return {"type": "array", "uniqueItems": True}
    if issubclass(annotation, dict):
        return {"type": "object"}
    return {}


def _allow_null(schema: dict[str, Any]) -> dict[str, Any]:
    """Widens a property schema so that ``null`` is also accepted."""
    if "enum" in schema:
        if None not in schema["enum"]:
            schema["enum"] = [*schema["enum"], None]
        if "type" in schema and isinstance(schema["type"], str):
            schema["type"] = [schema["type"], "null"]
        return schema
    if isinstance(schema.get("type"), str):
        schema["type"] = [schema["type"], "null"]
        return schema
    if "anyOf" in schema:
        schema["anyOf"] = [*schema["anyOf"], {"type": "null"}]
        return schema
    return schema


def _property_json_schema(field: Field[Any]) -> dict[str, Any]:
    schema = _type_schema(field.type_annotation)
    if isinstance(field, VectorProperty):
        schema.setdefault("minItems", field.dimensions)
        schema.setdefault("maxItems", field.dimensions)
    constraints = field.to_json_schema_constraints()
    if "enum" in constraints:
        values = [_to_json_value(v) for v in constraints["enum"]]
        constraints["enum"] = [v for v in values if v is not _NO_JSON]
    schema.update(constraints)
    if field.nullable:
        schema = _allow_null(schema)
    if field.description is not None:
        schema["description"] = field.description
    default = _static_default(field)
    if default is not _NO_JSON:
        schema["default"] = default
    return schema


def model_json_schema(model: type[Node] | type[Relationship]) -> dict[str, Any]:
    """Builds a JSON Schema draft 2020-12 document for one model's properties.

    See ``Node.to_json_schema`` for the mapping rules.

    Args:
        model: A Node or Relationship model class.

    Returns:
        A JSON-serialisable JSON Schema document.

    Raises:
        TypeError: If ``model`` is not a Node or Relationship model class.
    """
    _check_model(model)
    properties: dict[str, Any] = {}
    required: list[str] = []
    for attr, field in getattr(model, "_schema_fields", {}).items():
        properties[attr] = _property_json_schema(field)
        if not field.nullable and not field.has_default:
            required.append(attr)
    schema: dict[str, Any] = {"$schema": JSON_SCHEMA_DIALECT, "title": model.__name__}
    description = model.__dict__.get("__description__")
    if description is not None:
        schema["description"] = description
    schema["type"] = "object"
    schema["properties"] = properties
    if required:
        schema["required"] = required
    schema["additionalProperties"] = False
    return schema
