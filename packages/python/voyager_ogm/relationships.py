"""Declarative relationship navigation attributes for Voyager OGM nodes.

``RelationshipTo`` and ``RelationshipFrom`` are descriptors that record mapping
metadata only (relationship type, direction, target model and optional
relationship model). Accessing one on an instance returns a manager; the
manager performs explicit database reads through the session and stages
``connect`` / ``disconnect`` operations in the session's unit of work.

Attribute access itself never queries the database, and no loading mode is
attached to the descriptor. Eager loading belongs to a query.
"""

from __future__ import annotations

import json
import threading
import typing
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Generic, Literal, TypeVar, cast

from voyager_ogm._voyager_rs import VoyagerError
from voyager_ogm.types import serialize_param_value

if typing.TYPE_CHECKING:
    from voyager_ogm.session import AsyncSession, Session

__all__ = [
    "AsyncRelationshipManager",
    "RelationshipDescriptor",
    "RelationshipFrom",
    "RelationshipManager",
    "RelationshipTo",
]

_T = TypeVar("_T")
_Direction = Literal["outgoing", "incoming", "undirected"]

_MODEL_REGISTRY: dict[str, type[Any]] = {}
_MODEL_REGISTRY_LOCK = threading.RLock()


def _model_registry_key(model: type[Any]) -> str:
    """Returns the module-qualified registry key for a model class."""
    return f"{model.__module__}.{model.__name__}"


def register_model_class(model: type[Any]) -> None:
    """Registers a model class by its module-qualified name.

    Bare class names stay usable as forward references while exactly one
    registered model has that name. The registry is keyed by
    ``module.ClassName`` so same-named classes in different modules stay
    distinct and can be referenced with a qualified string.
    """
    with _MODEL_REGISTRY_LOCK:
        _MODEL_REGISTRY[_model_registry_key(model)] = model


def _clear_model_registry() -> None:
    """Clears the forward-reference registry.

    Intended for test isolation; production code registers models at class
    creation time.
    """
    with _MODEL_REGISTRY_LOCK:
        _MODEL_REGISTRY.clear()


def _resolve_model_reference(reference: Any, *, role: str, attribute: str) -> type[Any]:
    """Resolves a class or string model reference to a registered model class.

    A string with a dot is treated as a module-qualified name and must match
    exactly. A bare string resolves only when exactly one registered model has
    that class name; when several do, the error lists every candidate.
    """
    if isinstance(reference, type):
        return reference
    if isinstance(reference, str):
        with _MODEL_REGISTRY_LOCK:
            if "." in reference:
                resolved = _MODEL_REGISTRY.get(reference)
                known = sorted(_MODEL_REGISTRY)
                if resolved is not None:
                    return resolved
                candidates: list[str] = []
            else:
                candidates = sorted(
                    key for key in _MODEL_REGISTRY if key.rsplit(".", 1)[-1] == reference
                )
                if len(candidates) == 1:
                    return _MODEL_REGISTRY[candidates[0]]
                known = sorted(_MODEL_REGISTRY)
        if len(candidates) > 1:
            raise LookupError(
                f"Relationship attribute '{attribute}' {role} '{reference}' is "
                f"ambiguous: {', '.join(candidates)}. Use a module-qualified name."
            )
        hint = f" Known models: {', '.join(known)}." if known else ""
        raise LookupError(
            f"Relationship attribute '{attribute}' {role} '{reference}' is not a "
            f"registered Node or Relationship model.{hint}"
        )
    raise TypeError(
        f"Relationship attribute '{attribute}' {role} must be a model class or a "
        f"model name string, got {type(reference).__name__}"
    )


def _property_attr(model: type[Any], prop_name: str) -> str:
    """Returns the Python attribute name for a database property name."""
    for attr, field in getattr(model, "_schema_fields", {}).items():
        if (getattr(field, "name", None) or attr) == prop_name:
            return attr
    return prop_name


def _instance_value(instance: Any, prop_name: str) -> Any:
    """Reads a property by database property name from an entity instance."""
    attr = _property_attr(type(instance), prop_name)
    getter = getattr(instance, "get", None)
    if callable(getter):
        return getter(attr)
    return getattr(instance, attr, None)


def _model_labels(model: type[Any]) -> list[str]:
    """Returns the labels used in a MATCH pattern for a node model."""
    labels = getattr(model, "__labels__", None)
    if labels:
        return list(labels)
    cached = getattr(model, "_cached_labels", None)
    if cached:
        return list(cached)
    return [model.__name__]


def _is_age_dialect(dialect: str) -> bool:
    """Returns whether the session dialect speaks Apache AGE agtype values."""
    return str(dialect or "").lower() in ("age", "apache_age", "postgres_age")


def _decode_age_scalar(value: Any) -> Any:
    """Decodes one projected Apache AGE agtype scalar.

    AGE returns every projected property as agtype text, so a string property
    arrives as a JSON-quoted string (``'"Bob"'``) and an integer arrives as its
    JSON text (``'18'`` or ``18`` depending on the backend). Decoding the JSON
    text restores the declared Python types. Values that are already native or
    are not JSON text pass through unchanged.

    Args:
        value: A projected scalar from an AGE result row.

    Returns:
        The decoded scalar.
    """
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except (json.JSONDecodeError, ValueError):
        return value


def _projection_columns(model: type[Any]) -> tuple[tuple[str, str], ...]:
    """Returns ``(attribute, database_property)`` pairs for a model's fields.

    Only declared scalar fields are included. Relationship descriptors are not
    properties, so they are never projected.
    """
    columns: list[tuple[str, str]] = []
    for attr, field in getattr(model, "_schema_fields", {}).items():
        prop_name = str(getattr(field, "name", None) or attr)
        columns.append((str(attr), prop_name))
    return tuple(columns)


def _bind_predicate(expr: Any, target_model: type[Any], target_alias: str, query: Any) -> Any:
    """Binds class-level target predicates to the navigation target alias.

    Class-level field resolution in :class:`Query` finds patterns by label, so a
    self-referential relationship (for example ``Person.friends``) would see both
    the source and the target pattern as candidates. The manager knows the exact
    target alias, so it binds target-model expressions directly instead.
    """
    from voyager_ogm.expressions import (
        AliasedExpr,
        BinaryExpr,
        FunctionExpr,
        PropExpr,
        UnaryExpr,
    )
    from voyager_ogm.models import AggregationExpr, BoundField, Field, PredicateExpr

    if isinstance(expr, Field):
        owner = getattr(expr, "_owner", None)
        if owner is target_model:
            return BoundField(target_alias, expr.name or "", model_class=owner)
        return query._resolve_expr(expr)

    if isinstance(expr, BoundField):
        if not expr.target_alias and expr.model_class is target_model:
            return BoundField(target_alias, expr.field_name, model_class=expr.model_class)
        if not expr.target_alias:
            return query._resolve_expr(expr)
        return expr

    if isinstance(expr, PropExpr):
        if not expr.target_alias and expr.model_class is target_model:
            return PropExpr(target_alias, expr.field_name, model_class=expr.model_class)
        if not expr.target_alias:
            return query._resolve_expr(expr)
        return expr

    if isinstance(expr, PredicateExpr):
        target = expr.target
        if not target and expr.model_class is target_model:
            target = target_alias
        elif not target:
            resolved = query._resolve_expr(expr)
            return resolved
        return PredicateExpr(
            target,
            expr.field,
            expr.op,
            _bind_predicate(expr.value, target_model, target_alias, query),
            model_class=expr.model_class,
        )

    if isinstance(expr, AggregationExpr):
        target = expr.target
        if not target and expr.model_class is target_model:
            target = target_alias
        elif not target:
            return query._resolve_expr(expr)
        return AggregationExpr(
            target,
            expr.field,
            expr.func,
            model_class=expr.model_class,
        )

    if isinstance(expr, BinaryExpr):
        return BinaryExpr(
            _bind_predicate(expr.left, target_model, target_alias, query),
            expr.op,
            _bind_predicate(expr.right, target_model, target_alias, query),
        )

    if isinstance(expr, UnaryExpr):
        return UnaryExpr(
            expr.op,
            _bind_predicate(expr.operand, target_model, target_alias, query),
        )

    if isinstance(expr, FunctionExpr):
        return FunctionExpr(
            expr.name,
            [_bind_predicate(arg, target_model, target_alias, query) for arg in expr.args],
        )

    if isinstance(expr, AliasedExpr):
        return AliasedExpr(
            _bind_predicate(expr.expr, target_model, target_alias, query),
            expr.alias,
        )

    if isinstance(expr, (list, tuple)):
        return type(expr)(
            [_bind_predicate(item, target_model, target_alias, query) for item in expr]
        )

    return expr


@dataclass
class _PendingRelationship:
    """One staged connect or disconnect operation in the unit of work."""

    action: Literal["connect", "disconnect"]
    source: Any
    descriptor: RelationshipDescriptor
    target_model: type[Any]
    target: Any
    properties: dict[str, Any] | None


class RelationshipDescriptor(Generic[_T]):
    """Declarative relationship navigation attribute on a Node model.

    The descriptor stores mapping metadata and resolves string forward
    references lazily, so circular references between models are supported.
    Class access returns the descriptor itself; instance access returns a
    manager and performs no I/O.

    Attributes:
        target: Target node model class or model name string.
        rel_type: Graph relationship type (e.g. ``"KNOWS"``).
        model: Optional relationship model class or model name string.
        direction: Traversal direction from the declaring node.
    """

    direction: _Direction = "outgoing"

    def __init__(
        self,
        target: type[Any] | str | None = None,
        rel_type: str | None = None,
        model: type[Any] | str | None = None,
        *,
        relationship_model: type[Any] | str | None = None,
        direction: _Direction | None = None,
    ) -> None:
        """Initializes a relationship navigation descriptor.

        Args:
            target: Target node model class or model name string.
            rel_type: Graph relationship type.
            model: Optional relationship model carrying edge properties.
            relationship_model: Alias for ``model``.
            direction: Optional direction override (defaults to the descriptor
                subclass direction).
        """
        if target is None:
            raise TypeError(f"{type(self).__name__} requires a 'target' model class or model name")
        if not rel_type or not isinstance(rel_type, str):
            raise TypeError(f"{type(self).__name__} requires a non-empty 'rel_type' string")
        if relationship_model is not None and model is not None and relationship_model is not model:
            raise TypeError("Pass only one of 'model' or 'relationship_model'")
        self.target = target
        self.rel_type = rel_type
        self.model = relationship_model if relationship_model is not None else model
        if direction is not None and direction not in (
            "outgoing",
            "incoming",
            "undirected",
        ):
            raise ValueError(
                f"direction must be 'outgoing', 'incoming' or 'undirected', got {direction!r}"
            )
        if direction is not None:
            self.direction = direction
        self._attr_name: str | None = None
        self._owner: type[Any] | None = None
        self._resolved_target: type[Any] | None = None
        self._resolved_model: type[Any] | None = None
        self._resolved = False

    def __class_getitem__(cls, item: Any) -> type[RelationshipDescriptor[Any]]:
        """Accepts ``RelationshipTo["Person"]`` in type annotations.

        The target comes from the descriptor instance, so the subscription is
        purely a typing convenience and returns the descriptor class.
        """
        del item
        return cls

    def __set_name__(self, owner: type[Any], name: str) -> None:
        """Records the owning model and attribute name."""
        self._owner = owner
        self._attr_name = name
        register_model_class(owner)

    def _set_owner(self, owner: type[Any], name: str) -> None:
        """Binds the descriptor to a class when discovered through annotations."""
        self._owner = owner
        if self._attr_name is None:
            self._attr_name = name

    def __get__(
        self, instance: Any, owner: type[Any] | None = None
    ) -> RelationshipDescriptor[_T] | RelationshipManager[_T] | AsyncRelationshipManager[_T]:
        """Returns the descriptor on class access or a manager on instance access."""
        if instance is None:
            return self
        self.resolve()
        from voyager_ogm.session import AsyncSession

        ref = getattr(instance, "_session_ref", None)
        session = ref() if ref is not None else None
        if isinstance(session, AsyncSession):
            return AsyncRelationshipManager(instance, self)
        return RelationshipManager(instance, self)

    def resolve(self) -> type[Any]:
        """Resolves and returns the target node model, or raises a clear error."""
        if not self._resolved:
            self._resolved_target = _resolve_model_reference(
                self.target,
                role="target",
                attribute=self._attr_name or "",
            )
            if self.model is not None:
                self._resolved_model = _resolve_model_reference(
                    self.model,
                    role="relationship model",
                    attribute=self._attr_name or "",
                )
            self._resolved = True
        assert self._resolved_target is not None
        return self._resolved_target

    @property
    def resolved_model(self) -> type[Any] | None:
        """Returns the optional resolved relationship model class."""
        self.resolve()
        return self._resolved_model

    def to_declaration(self) -> dict[str, Any]:
        """Returns the native registry declaration for this attribute.

        String references are stored unresolved so the registry can round-trip
        before every target model has been defined.
        """
        target = self.target
        target_model = target.__name__ if isinstance(target, type) else str(target)
        model = self.model
        relationship_model = (
            model.__name__ if isinstance(model, type) else (str(model) if model else None)
        )
        return {
            "name": self._attr_name or "",
            "rel_type": self.rel_type,
            "direction": self.direction,
            "target_model": target_model,
            "relationship_model": relationship_model,
        }

    def __repr__(self) -> str:
        owner = self._owner.__name__ if self._owner is not None else ""
        attr = self._attr_name or ""
        prefix = f"{owner}.{attr}" if owner and attr else type(self).__name__
        return (
            f"{type(self).__name__}({prefix!r}, target={self.target!r}, rel_type={self.rel_type!r})"
        )


class RelationshipTo(RelationshipDescriptor[_T]):
    """Outgoing relationship navigation attribute: ``(source)-[:TYPE]->(target)``."""

    direction: _Direction = "outgoing"


class RelationshipFrom(RelationshipDescriptor[_T]):
    """Incoming relationship navigation attribute: ``(source)<-[:TYPE]-(target)``."""

    direction: _Direction = "incoming"


def collect_relationship_descriptors(
    cls: type[Any],
) -> dict[str, RelationshipDescriptor[Any]]:
    """Collects inherited and directly declared relationship descriptors.

    Args:
        cls: Node model class being created.

    Returns:
        Descriptors keyed by attribute name, inherited declarations first.
    """
    descriptors: dict[str, RelationshipDescriptor[Any]] = {}
    for base in reversed(cls.__mro__):
        inherited = base.__dict__.get("_schema_relationships")
        if isinstance(inherited, dict):
            descriptors.update(inherited)
    for name, value in cls.__dict__.items():
        if isinstance(value, RelationshipDescriptor):
            value._set_owner(cls, name)
            descriptors[name] = value
    cls._schema_relationships = descriptors
    return descriptors


def _relationship_write_dialect(session: Any) -> str:
    """Returns the normalized dialect that will compile relationship writes."""
    return str(getattr(session, "dialect", "") or "cypher").lower()


def _require_relationship_write_support(session: Any, attribute: str) -> None:
    """Rejects staged edge writes on read-only query dialects before any I/O.

    SQL/PGQ (``sql_pgq`` and ``duckpgq``) compiles to the read-only
    ``GRAPH_TABLE`` operator, which has no edge mutation form.

    Args:
        session: Session that will execute the staged operation.
        attribute: Relationship attribute name used in the error message.

    Raises:
        VoyagerError: With code ``UNSUPPORTED_FEATURE`` when the dialect cannot
            express a relationship write.
    """
    dialect = _relationship_write_dialect(session)
    if dialect in ("sql_pgq", "duckpgq"):
        raise VoyagerError(
            f"Relationship '{attribute}' cannot connect or disconnect on dialect "
            f"'{dialect}': SQL/PGQ GRAPH_TABLE is a read-only query operator and "
            "cannot express relationship writes.",
            code="UNSUPPORTED_FEATURE",
        )


@dataclass(frozen=True)
class _NavigationProjection:
    """Projected target columns for one navigation query.

    Attributes:
        target_alias: Query alias of the navigated target node, used in errors.
        columns: ``(attribute, database_property)`` pairs in projection order.
            The attribute name doubles as the stable result-column alias.
    """

    target_alias: str
    columns: tuple[tuple[str, str], ...]


class _RelationshipManagerBase(Generic[_T]):
    """Shared binding, query building, hydration and staging for managers."""

    def __init__(self, instance: Any, descriptor: RelationshipDescriptor[_T]) -> None:
        """Binds a manager to an instance and its descriptor."""
        self._instance = instance
        self._descriptor = descriptor

    @property
    def _session(self) -> Session:
        """Returns the instance's session or raises a clear error."""
        ref = getattr(self._instance, "_session_ref", None)
        session = ref() if ref is not None else None
        if session is None:
            raise RuntimeError(
                f"Cannot use relationship '{self._descriptor._attr_name}' on a "
                f"{type(self._instance).__name__} instance with no session. "
                "Create it with session=..., register it with a session, or load it "
                "through a session query."
            )
        return cast("Session", session)

    @property
    def descriptor(self) -> RelationshipDescriptor[_T]:
        """The owning descriptor."""
        return self._descriptor

    def _source_key(self) -> tuple[str, Any]:
        """Returns the source primary key property name and value."""
        from voyager_ogm.models import get_primary_key_field

        source_model = type(self._instance)
        key_field = get_primary_key_field(source_model)
        if key_field is None:
            raise RuntimeError(
                f"Cannot navigate relationship '{self._descriptor._attr_name}': "
                f"{source_model.__name__} declares no primary key."
            )
        value = _instance_value(self._instance, key_field)
        if value is None:
            raise RuntimeError(
                f"Cannot navigate relationship '{self._descriptor._attr_name}': "
                f"{source_model.__name__} is not saved yet (primary key "
                f"'{key_field}' is unset)."
            )
        return key_field, value

    def _target_key(self, target: Any) -> tuple[str, Any]:
        """Returns the target primary key property name and value."""
        from voyager_ogm.models import get_primary_key_field

        target_model = type(target)
        key_field = get_primary_key_field(target_model)
        if key_field is None:
            raise RuntimeError(
                f"Cannot connect relationship '{self._descriptor._attr_name}': "
                f"{target_model.__name__} declares no primary key."
            )
        value = _instance_value(target, key_field)
        if value is None:
            raise RuntimeError(
                f"Cannot connect relationship '{self._descriptor._attr_name}': "
                f"{target_model.__name__} is not saved yet (primary key "
                f"'{key_field}' is unset)."
            )
        return key_field, value

    def _validate_target(self, target: Any) -> type[Any]:
        """Validates that a connect/disconnect target is the declared model."""
        from voyager_ogm.models import Node

        if not isinstance(target, Node):
            raise TypeError(
                f"Relationship '{self._descriptor._attr_name}' target must be a Node "
                f"instance, got {type(target).__name__}"
            )
        target_model = self._descriptor.resolve()
        if not isinstance(target, target_model):
            raise TypeError(
                f"Relationship '{self._descriptor._attr_name}' targets "
                f"{target_model.__name__}, got {type(target).__name__}"
            )
        return target_model

    def _build_query(
        self, *, filters: tuple[Any, ...] = (), kwargs: Any = None
    ) -> tuple[Any, _NavigationProjection]:
        """Builds the navigation query and its target projection.

        The target node is projected as one column per declared scalar field,
        aliased by the model attribute. A named relationship variable is kept
        in the MATCH pattern because SQL/PGQ engines (notably DuckPGQ) require
        every pattern element to be bound to a variable.
        """
        from voyager_ogm.models import _get_next_alias
        from voyager_ogm.query import Query

        target_model = self._descriptor.resolve()
        source_key, source_value = self._source_key()
        target_alias = _get_next_alias(target_model.__name__)
        columns = _projection_columns(target_model)

        query = Query()
        query._native.navigate_relationship(
            {
                "source": self._instance.alias,
                "source_labels": list(self._instance.labels),
                "source_key": source_key,
                "source_key_value": serialize_param_value(source_value),
                "rel_type": self._descriptor.rel_type,
                "direction": self._descriptor.direction,
                "rel_variable": _get_next_alias("rel"),
                "target": target_alias,
                "target_labels": _model_labels(target_model),
            }
        )
        predicates = list(filters)
        if kwargs:
            for name, value in kwargs.items():
                field = getattr(target_model, name)
                predicates.append(field == value)
        if predicates:
            bound = [
                _bind_predicate(predicate, target_model, target_alias, query)
                for predicate in predicates
            ]
            query.where(*bound)
        query._native.return_()
        for attr, prop_name in columns:
            query._native.field(target_alias, prop_name, attr)
        return query, _NavigationProjection(target_alias, columns)

    def _hydrate(self, records: Any, projection: _NavigationProjection) -> list[_T]:
        """Hydrates target model instances through the session identity map.

        Every row must be a mapping with one column per projected attribute. A
        non-mapping row, a missing alias, or a row with no primary key raises a
        clear error instead of silently dropping the row.

        Args:
            records: Executed result rows.
            projection: Target alias and projected attribute columns.

        Returns:
            Hydrated target instances.
        """
        from voyager_ogm.models import get_primary_key_field

        target_model = self._descriptor.resolve()
        session = self._session
        attribute = self._descriptor._attr_name or ""
        decode = _is_age_dialect(session.dialect)
        primary_key = get_primary_key_field(target_model)
        key_attr = _property_attr(target_model, primary_key) if primary_key is not None else None
        hydrated: list[_T] = []
        for record in records:
            if not isinstance(record, Mapping):
                raise VoyagerError(
                    f"Cannot hydrate relationship '{attribute}': expected each row "
                    f"to be a mapping, got {type(record).__name__}.",
                    code="SCHEMA_ERROR",
                )
            lowered = {str(key).lower(): key for key in record if isinstance(key, str)}
            props: dict[str, Any] = {}
            for attr, _prop_name in projection.columns:
                column = attr if attr in record else lowered.get(attr.lower())
                if column is None:
                    available = ", ".join(sorted(str(key) for key in record)) or "<none>"
                    raise VoyagerError(
                        f"Cannot hydrate relationship '{attribute}': record for "
                        f"target '{projection.target_alias}' has no '{attr}' column "
                        f"(available columns: {available}).",
                        code="SCHEMA_ERROR",
                    )
                value = record[column]
                props[attr] = _decode_age_scalar(value) if decode else value
            if key_attr is not None and props.get(key_attr) is None:
                raise VoyagerError(
                    f"Cannot hydrate relationship '{attribute}': target column "
                    f"'{key_attr}' is null, so the row has no {target_model.__name__} "
                    "identity.",
                    code="SCHEMA_ERROR",
                )
            instance = target_model(**props)
            registered = session.register(instance, key_field=key_attr, is_clean=True)
            attach = getattr(registered, "_attach_session", None)
            if callable(attach):
                attach(session)
            hydrated.append(registered)
        return hydrated

    def connect(self, target: Any, properties: dict[str, Any] | None = None) -> None:
        """Stages a connect operation, written on ``session.flush()``."""
        session = self._session
        _require_relationship_write_support(session, self._descriptor._attr_name or "")
        target_model = self._validate_target(target)
        self._source_key()
        self._target_key(target)
        self._session._stage_relationship(
            _PendingRelationship(
                action="connect",
                source=self._instance,
                descriptor=self._descriptor,
                target_model=target_model,
                target=target,
                properties=dict(properties) if properties else None,
            )
        )

    def disconnect(self, target: Any) -> None:
        """Stages a disconnect operation, written on ``session.flush()``."""
        session = self._session
        _require_relationship_write_support(session, self._descriptor._attr_name or "")
        target_model = self._validate_target(target)
        self._source_key()
        self._target_key(target)
        self._session._stage_relationship(
            _PendingRelationship(
                action="disconnect",
                source=self._instance,
                descriptor=self._descriptor,
                target_model=target_model,
                target=target,
                properties=None,
            )
        )


class RelationshipManager(_RelationshipManagerBase[_T]):
    """Synchronous manager for a relationship attribute on one node instance.

    ``.all()`` and ``.filter(...)`` execute explicitly and hydrate target nodes
    through the session identity map. ``.connect(...)`` and ``.disconnect(...)``
    stage operations written by ``session.flush()``.
    """

    def all(self) -> list[_T]:
        """Executes the navigation query and returns hydrated target instances."""
        query, projection = self._build_query()
        return self._hydrate(self._session.execute(query), projection)

    def filter(self, *predicates: Any, **kwargs: Any) -> list[_T]:
        """Executes a filtered navigation query and returns hydrated targets.

        Args:
            *predicates: Fluent predicates, e.g. ``Person.age >= 18``.
            **kwargs: Property equality filters on the target model.

        Returns:
            Hydrated target instances.
        """
        query, projection = self._build_query(filters=predicates, kwargs=kwargs)
        return self._hydrate(self._session.execute(query), projection)


class AsyncRelationshipManager(_RelationshipManagerBase[_T]):
    """Manager for a relationship attribute bound to an ``AsyncSession``.

    ``.all()`` and ``.filter(...)`` are awaitable; staging of connect and
    disconnect is identical to the synchronous manager and is written by
    ``await session.flush()``.
    """

    @property
    def _async_session(self) -> AsyncSession:
        return cast("AsyncSession", self._session)

    async def all(self) -> list[_T]:
        """Asynchronously executes the navigation query and hydrates targets."""
        query, projection = self._build_query()
        records = await self._async_session.execute(query)
        return self._hydrate(records, projection)

    async def filter(self, *predicates: Any, **kwargs: Any) -> list[_T]:
        """Asynchronously executes a filtered navigation query."""
        query, projection = self._build_query(filters=predicates, kwargs=kwargs)
        records = await self._async_session.execute(query)
        return self._hydrate(records, projection)


def _relationship_query_for_operation(operation: _PendingRelationship, use_batch: bool) -> Any:
    """Builds the native connect or disconnect query for one operation group."""
    from voyager_ogm.models import _get_next_alias
    from voyager_ogm.query import Query

    descriptor: RelationshipDescriptor[Any] = cast(
        RelationshipDescriptor[Any], operation.descriptor
    )
    target_model = operation.target_model
    source_key, source_value = _relationship_key(operation.source)
    query = Query()
    source_alias = operation.source.alias
    target_alias = _get_next_alias(target_model.__name__)
    target_key, target_value = _relationship_key(operation.target)
    spec: dict[str, Any] = {
        "source": source_alias,
        "source_labels": list(operation.source.labels),
        "source_key": source_key,
        "source_key_value": serialize_param_value(source_value),
        "target": target_alias,
        "target_labels": _model_labels(target_model),
        "target_key": target_key,
        "rel_type": descriptor.rel_type,
        "direction": descriptor.direction,
    }
    rel_alias = _get_next_alias("rel")
    if use_batch:
        spec.update(
            {
                "properties": _property_pairs(operation.properties),
                "rel_variable": rel_alias if operation.action == "disconnect" else None,
                "batch_param": "targets",
                "row_alias": "target_key",
            }
        )
        query._native.relationship_batch(spec)
        return query

    spec["target_key_value"] = serialize_param_value(target_value)
    if operation.action == "connect":
        spec["properties"] = _property_pairs(operation.properties)
        query._native.connect_relationship(spec)
    else:
        spec["rel_variable"] = rel_alias
        query._native.disconnect_relationship(spec)
    return query


def _relationship_key(instance: Any) -> tuple[str, Any]:
    """Returns an instance's primary key property name and value."""
    from voyager_ogm.models import get_primary_key_field

    key_field = get_primary_key_field(type(instance))
    if key_field is None:
        raise RuntimeError(
            f"{type(instance).__name__} declares no primary key; cannot connect or disconnect it."
        )
    value = _instance_value(instance, key_field)
    if value is None:
        raise RuntimeError(
            f"{type(instance).__name__} is not saved yet (primary key '{key_field}' is unset)."
        )
    return key_field, value


def _property_pairs(properties: dict[str, Any] | None) -> list[tuple[str, Any]]:
    """Converts a property mapping to native literal pairs."""
    if not properties:
        return []
    return [(name, serialize_param_value(value)) for name, value in properties.items()]


def _group_operations(
    operations: list[_PendingRelationship],
) -> list[list[_PendingRelationship]]:
    """Groups consecutive operations that can share one batched statement."""
    groups: list[list[_PendingRelationship]] = []
    for operation in operations:
        if groups:
            group = groups[-1]
            first = group[0]
            same = (
                first.action == operation.action
                and first.source is operation.source
                and first.descriptor is operation.descriptor
                and first.target_model is operation.target_model
                and first.properties == operation.properties
            )
            if same:
                group.append(operation)
                continue
        groups.append([operation])
    return groups


def flush_pending_relationships(session: Any, operations: list[_PendingRelationship]) -> list[Any]:
    """Executes staged relationship operations and returns execution results.

    Operations on the same source, descriptor, target model and properties are
    coalesced into one batched statement; everything else runs individually.
    """
    results: list[Any] = []
    for group in _group_operations(operations):
        if len(group) > 1:
            query = _relationship_query_for_operation(group[0], use_batch=True)
            target_keys = [_relationship_key(op.target)[1] for op in group]
            params = {"targets": [serialize_param_value(key) for key in target_keys]}
            results.append(session.execute(query, params))
        else:
            query = _relationship_query_for_operation(group[0], use_batch=False)
            results.append(session.execute(query))
    return results


async def async_flush_pending_relationships(
    session: Any, operations: list[_PendingRelationship]
) -> list[Any]:
    """Asynchronously executes staged relationship operations."""
    results: list[Any] = []
    for group in _group_operations(operations):
        if len(group) > 1:
            query = _relationship_query_for_operation(group[0], use_batch=True)
            target_keys = [_relationship_key(op.target)[1] for op in group]
            params = {"targets": [serialize_param_value(key) for key in target_keys]}
            results.append(await session.execute(query, params))
        else:
            query = _relationship_query_for_operation(group[0], use_batch=False)
            results.append(await session.execute(query))
    return results
