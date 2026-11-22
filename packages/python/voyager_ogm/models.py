"""Voyager OGM High-Level Graph Entity Models, Decorators & Type Annotations.

Provides declarative schema modeling for Graph Nodes and Relationships with
automatic descriptor binding, base class injection, and type reflection.
"""

from __future__ import annotations

import datetime
import difflib
import inspect
import logging
import re
import threading
import types
import typing
import weakref
from collections import defaultdict
from collections.abc import Sequence
from typing import Any, ClassVar, Generic, TypeVar, cast, dataclass_transform

from voyager_ogm.expressions import (
    BinaryExpr,
    Expression,
    FunctionExpr,
    PropExpr,
    to_expression,
)
from voyager_ogm.types import (
    Point,
    _hydrate_date,
    _hydrate_datetime,
    _hydrate_time,
    _hydrate_timedelta,
    serialize_param_value,
)

try:
    from voyager_ogm._voyager_rs import NativeSchemaRegistry
except ImportError:
    NativeSchemaRegistry = None  # type: ignore[assignment, misc]

logger = logging.getLogger("voyager_ogm")

_T = TypeVar("_T")

# Thread-local alias counter for deterministic auto-aliasing
_alias_state = threading.local()

# Relationships whose alias was generated automatically. Tracking them weakly keeps
# "was this alias explicit?" derivable without storing a per-instance flag.
_auto_aliased_relationships: weakref.WeakSet[Relationship] = weakref.WeakSet()

_BUILTIN_TYPES = {
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
    "list": list,
    "dict": dict,
    "Point": Point,
    "date": datetime.date,
    "time": datetime.time,
    "datetime": datetime.datetime,
    "timedelta": datetime.timedelta,
}

# Registry for automatic field hydration from raw database values (e.g. dicts, driver spatial objects, timestamps)
_HYDRATORS: dict[Any, Any] = {
    Point: Point.from_spatial,
    datetime.date: _hydrate_date,
    datetime.time: _hydrate_time,
    datetime.datetime: _hydrate_datetime,
    datetime.timedelta: _hydrate_timedelta,
}


def _hydrate_field_value(field_desc: Field | None, value: Any) -> Any:
    """Hydrates a raw property value (such as a dictionary or driver spatial object) into a rich type via the _HYDRATORS registry."""
    if field_desc is None or value is None:
        return value
    ann = getattr(field_desc, "type_annotation", None)
    if ann is None:
        return value
    if isinstance(ann, type) and isinstance(value, ann):
        # Special case: datetime is a subclass of date in Python stdlib.
        # If the annotation is specifically date, but value is datetime, convert via date hydrator.
        if ann is datetime.date and isinstance(value, datetime.datetime):
            return _hydrate_date(value)
        return value
    hydrator = _HYDRATORS.get(ann)
    if hydrator is not None:
        try:
            return hydrator(value)
        except Exception as exc:
            logger.debug(
                "Failed to hydrate field '%s' with value %r via %s: %s",
                getattr(field_desc, "name", "unknown"),
                value,
                hydrator,
                exc,
            )
            return value
    ann_name = getattr(ann, "__name__", "")
    for registered_type, h in _HYDRATORS.items():
        if getattr(registered_type, "__name__", "") == ann_name:
            try:
                return h(value)
            except Exception as exc:
                logger.debug(
                    "Failed to hydrate field '%s' with value %r via %s: %s",
                    getattr(field_desc, "name", "unknown"),
                    value,
                    h,
                    exc,
                )
                return value
    return value


def _get_next_alias(label: str) -> str:
    if not hasattr(_alias_state, "counters"):
        _alias_state.counters = defaultdict(int)
    key = label.lower()
    idx = _alias_state.counters[key]
    _alias_state.counters[key] += 1
    return f"_{key}_{idx}"


def reset_alias_counters() -> None:
    """Resets the thread-local auto-alias counters.

    Useful in unit tests and deterministic query generation scenarios to ensure
    starting aliases always begin from index 0.

    Example:
        >>> reset_alias_counters()
        >>> p = Person()
        >>> p.alias
        '_person_0'
    """
    _alias_state.counters = defaultdict(int)


class SchemaValidationError(Exception):
    """Raised when a graph entity or query topology fails schema validation."""


class FieldConstraintError(SchemaValidationError, ValueError):
    """Raised when a field value violates declared field constraints.

    Attributes:
        model: Model class or model name.
        model_name: String name of the model.
        field: Property name.
        constraint: Name of the constraint violated ('pattern', 'ge', 'gt', 'le', 'lt', 'min_length', 'max_length', 'choices').
        value: The offending value.
    """

    def __init__(
        self,
        model: type | str | None,
        field: str,
        constraint: str,
        value: Any,
        message: str | None = None,
    ) -> None:
        self.model = model
        self.model_name = getattr(model, "__name__", str(model)) if model is not None else ""
        self.field = field
        self.constraint = constraint
        self.value = value
        if message is None:
            prefix = f"{self.model_name}.{field}" if self.model_name else field
            message = f"{prefix}: value {value!r} violates {constraint} constraint"
        super().__init__(message)


def _format_model_prop(model: Any, prop: str) -> str:
    name = getattr(model, "__name__", str(model)) if model is not None else ""
    return f"{name}.{prop}" if name else prop


def _extract_base_types(type_hint: Any) -> set[type]:
    """Extracts non-None concrete base types from a type hint."""
    if type_hint is None or type_hint is typing.Any:
        return set()

    if isinstance(type_hint, str):
        if type_hint in _BUILTIN_TYPES:
            return {_BUILTIN_TYPES[type_hint]}
        return set()

    origin = typing.get_origin(type_hint)
    if origin is not None:
        if origin is typing.Union or (hasattr(types, "UnionType") and origin is types.UnionType):
            args = typing.get_args(type_hint)
            res: set[type] = set()
            for arg in args:
                if arg is not type(None):
                    res.update(_extract_base_types(arg))
            return res
        if isinstance(origin, type):
            return {origin}

    if isinstance(type_hint, type):
        return {type_hint}

    return set()


def _validate_field_declaration(cls: type, attr_name: str, field: Field[Any]) -> None:
    """Validates Field constraint declarations against the class annotation at definition time."""
    field._owner = cls
    if field.name is None:
        field.name = attr_name

    # Check for empty ranges, negative lengths, invalid pattern if not already caught
    if field.min_length is not None and field.min_length < 0:
        raise FieldConstraintError(
            cls,
            attr_name,
            "min_length",
            field.min_length,
            f"{cls.__name__}.{attr_name}: min_length cannot be negative ({field.min_length})",
        )
    if field.max_length is not None and field.max_length < 0:
        raise FieldConstraintError(
            cls,
            attr_name,
            "max_length",
            field.max_length,
            f"{cls.__name__}.{attr_name}: max_length cannot be negative ({field.max_length})",
        )
    if (
        field.min_length is not None
        and field.max_length is not None
        and field.min_length > field.max_length
    ):
        raise FieldConstraintError(
            cls,
            attr_name,
            "min_length",
            field.min_length,
            f"{cls.__name__}.{attr_name}: min_length ({field.min_length}) cannot be greater than max_length ({field.max_length})",
        )
    if field.choices is not None and len(field.choices) == 0:
        raise FieldConstraintError(
            cls,
            attr_name,
            "choices",
            field.choices,
            f"{cls.__name__}.{attr_name}: choices cannot be empty",
        )
    if field.ge is not None and field.le is not None and field.ge > field.le:
        raise FieldConstraintError(
            cls,
            attr_name,
            "ge",
            field.ge,
            f"{cls.__name__}.{attr_name}: ge ({field.ge}) cannot be greater than le ({field.le})",
        )
    if field.ge is not None and field.lt is not None and field.ge >= field.lt:
        raise FieldConstraintError(
            cls,
            attr_name,
            "ge",
            field.ge,
            f"{cls.__name__}.{attr_name}: ge ({field.ge}) cannot be greater than or equal to lt ({field.lt})",
        )
    if field.gt is not None and field.le is not None and field.gt >= field.le:
        raise FieldConstraintError(
            cls,
            attr_name,
            "gt",
            field.gt,
            f"{cls.__name__}.{attr_name}: gt ({field.gt}) cannot be greater than or equal to le ({field.le})",
        )
    if field.gt is not None and field.lt is not None and field.gt >= field.lt:
        raise FieldConstraintError(
            cls,
            attr_name,
            "gt",
            field.gt,
            f"{cls.__name__}.{attr_name}: gt ({field.gt}) cannot be greater than or equal to lt ({field.lt})",
        )

    # Check constraint compatibility with annotated type
    type_hint = field.type_annotation
    base_types = _extract_base_types(type_hint)
    if not base_types:
        return

    type_name = getattr(type_hint, "__name__", str(type_hint))

    # pattern: only str
    if field.pattern is not None:
        if not any(issubclass(t, str) for t in base_types if isinstance(t, type)):
            raise FieldConstraintError(
                cls,
                attr_name,
                "pattern",
                field.pattern,
                f"{cls.__name__}.{attr_name}: constraint 'pattern' is not supported for type '{type_name}'",
            )

    # ge, gt, le, lt: numeric (int, float), NOT bool
    has_numeric = (
        field.ge is not None or field.gt is not None or field.le is not None or field.lt is not None
    )
    if has_numeric:
        c_name = (
            "ge"
            if field.ge is not None
            else ("gt" if field.gt is not None else ("le" if field.le is not None else "lt"))
        )
        val = getattr(field, c_name)
        if any(issubclass(t, bool) for t in base_types if isinstance(t, type)):
            raise FieldConstraintError(
                cls,
                attr_name,
                c_name,
                val,
                f"{cls.__name__}.{attr_name}: bool fields do not support numeric bounds",
            )
        if not any(issubclass(t, (int, float)) for t in base_types if isinstance(t, type)):
            raise FieldConstraintError(
                cls,
                attr_name,
                c_name,
                val,
                f"{cls.__name__}.{attr_name}: constraint '{c_name}' is not supported for type '{type_name}'",
            )

    # min_length, max_length: str and list
    if field.min_length is not None or field.max_length is not None:
        c_name = "min_length" if field.min_length is not None else "max_length"
        val = getattr(field, c_name)
        if not any(issubclass(t, (str, list)) for t in base_types if isinstance(t, type)):
            raise FieldConstraintError(
                cls,
                attr_name,
                c_name,
                val,
                f"{cls.__name__}.{attr_name}: constraint '{c_name}' is not supported for type '{type_name}'",
            )


class Field(Generic[_T]):
    r"""Property descriptor supporting type-safe predicate expressions, value constraints, and schema reflection.

    Attributes:
        default: Default value if not assigned during model instantiation.
        name: Property key name in the underlying graph database.
        unique: Flag indicating a UNIQUE property database constraint.
        index: Flag indicating a database index should be created on this property.
        primary_key: Convenience flag setting both unique=True and index=True (and nullable=False).
        nullable: Whether the field can store null values. Defaults to True (False for primary_key).
        type_annotation: The resolved Python type class (e.g. `str`, `int`).
        pattern: Regex pattern string or compiled Pattern for string validation.
            Maps to JSON Schema keyword `pattern`.
        ge: Numeric lower bound (greater than or equal).
            Maps to JSON Schema keyword `minimum`.
        gt: Numeric exclusive lower bound (strictly greater than).
            Maps to JSON Schema keyword `exclusiveMinimum`.
        le: Numeric upper bound (less than or equal).
            Maps to JSON Schema keyword `maximum`.
        lt: Numeric exclusive upper bound (strictly less than).
            Maps to JSON Schema keyword `exclusiveMaximum`.
        min_length: Minimum length for strings and lists.
            Maps to JSON Schema keyword `minLength`.
        max_length: Maximum length for strings and lists.
            Maps to JSON Schema keyword `maxLength`.
        choices: Allowed enumeration values compared by equality.
            Maps to JSON Schema keyword `enum`.

    Example:
        >>> class Person(Node):
        ...     name: str = Field(min_length=1, max_length=200)
        ...     email: str = Field(unique=True, pattern=r"^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$")
        ...     age: int = Field(ge=0, le=150)
        ...     status: str = Field(choices=["active", "inactive"])
    """

    def __init__(
        self,
        default: Any = ...,
        *,
        default_factory: Any = None,
        name: str | None = None,
        unique: bool = False,
        index: bool = False,
        primary_key: bool = False,
        nullable: bool = True,
        type_annotation: Any = None,
        index_type: str | None = None,
        description: str | None = None,
        pattern: str | re.Pattern[str] | None = None,
        ge: int | float | None = None,
        gt: int | float | None = None,
        le: int | float | None = None,
        lt: int | float | None = None,
        min_length: int | None = None,
        max_length: int | None = None,
        choices: Sequence[Any] | None = None,
    ) -> None:
        """Initializes a graph property Field descriptor.

        Args:
            default: Default fallback value for this property.
            default_factory: Zero-argument callable producing a default value.
            name: Custom database property name (defaults to attribute name).
            unique: Whether to enforce a unique constraint.
            index: Whether to create a search index on this property.
            primary_key: Convenience flag setting both unique=True and index=True.
            nullable: Whether the field can store null values. Defaults to True (False for primary_key).
            type_annotation: Python type annotation class.
            index_type: Optional index type string ('BTREE', 'TEXT', 'POINT', 'VECTOR').
            description: Optional human-readable description of this property.
            pattern: Regular expression pattern string or compiled Pattern (JSON Schema: pattern).
            ge: Lower bound: greater than or equal (JSON Schema: minimum).
            gt: Exclusive lower bound: strictly greater than (JSON Schema: exclusiveMinimum).
            le: Upper bound: less than or equal (JSON Schema: maximum).
            lt: Exclusive upper bound: strictly less than (JSON Schema: exclusiveMaximum).
            min_length: Minimum length for string or list (JSON Schema: minLength).
            max_length: Maximum length for string or list (JSON Schema: maxLength).
            choices: Allowed values sequence compared by equality (JSON Schema: enum).
        """
        if default is not ... and default_factory is not None:
            raise TypeError("Field cannot specify both 'default' and 'default_factory'.")
        self.default = default
        self.default_factory = default_factory
        self.name = name
        self.primary_key = primary_key
        self.unique = unique or primary_key
        self.index = index or primary_key
        self.nullable = False if primary_key else nullable
        self.index_type = index_type or ("BTREE" if self.index else None)
        self.description = description
        if isinstance(type_annotation, str) and type_annotation in _BUILTIN_TYPES:
            self.type_annotation = _BUILTIN_TYPES[type_annotation]
        else:
            self.type_annotation = type_annotation
        self._owner: type | None = None

        # Value constraints (#282)
        # 1. pattern check
        self.pattern = pattern
        self._compiled_pattern: re.Pattern[str] | None = None
        if pattern is not None:
            if isinstance(pattern, re.Pattern):
                self._compiled_pattern = pattern
            elif isinstance(pattern, str):
                try:
                    self._compiled_pattern = re.compile(pattern)
                except re.error as exc:
                    raise FieldConstraintError(
                        None,
                        name or "",
                        "pattern",
                        pattern,
                        f"Invalid regex pattern {pattern!r}: {exc}",
                    ) from exc
            else:
                raise FieldConstraintError(
                    None,
                    name or "",
                    "pattern",
                    pattern,
                    f"pattern must be a str or compiled re.Pattern, got {type(pattern).__name__}",
                )

        # 2. bool is not allowed for numeric bounds (Requirement 2 & 4)
        for bound_name, bound_val in (("ge", ge), ("gt", gt), ("le", le), ("lt", lt)):
            if bound_val is not None:
                if isinstance(bound_val, bool) or not isinstance(bound_val, (int, float)):
                    raise FieldConstraintError(
                        None,
                        name or "",
                        bound_name,
                        bound_val,
                        f"bool is not allowed for numeric {bound_name} constraint",
                    )

        # Range checks: ge/gt greater than le/lt (or empty range)
        if ge is not None and le is not None and ge > le:
            raise FieldConstraintError(
                None,
                name or "",
                "ge",
                ge,
                f"ge ({ge}) cannot be greater than le ({le})",
            )
        if ge is not None and lt is not None and ge >= lt:
            raise FieldConstraintError(
                None,
                name or "",
                "ge",
                ge,
                f"ge ({ge}) cannot be greater than or equal to lt ({lt})",
            )
        if gt is not None and le is not None and gt >= le:
            raise FieldConstraintError(
                None,
                name or "",
                "gt",
                gt,
                f"gt ({gt}) cannot be greater than or equal to le ({le})",
            )
        if gt is not None and lt is not None and gt >= lt:
            raise FieldConstraintError(
                None,
                name or "",
                "gt",
                gt,
                f"gt ({gt}) cannot be greater than or equal to lt ({lt})",
            )

        self.ge = ge
        self.gt = gt
        self.le = le
        self.lt = lt

        # 3. min_length / max_length
        if min_length is not None and min_length < 0:
            raise FieldConstraintError(
                None,
                name or "",
                "min_length",
                min_length,
                f"min_length cannot be negative ({min_length})",
            )
        if max_length is not None and max_length < 0:
            raise FieldConstraintError(
                None,
                name or "",
                "max_length",
                max_length,
                f"max_length cannot be negative ({max_length})",
            )
        if min_length is not None and max_length is not None and min_length > max_length:
            raise FieldConstraintError(
                None,
                name or "",
                "min_length",
                min_length,
                f"min_length ({min_length}) cannot be greater than max_length ({max_length})",
            )

        self.min_length = min_length
        self.max_length = max_length

        # 4. choices
        if choices is not None:
            if len(choices) == 0:
                raise FieldConstraintError(
                    None,
                    name or "",
                    "choices",
                    choices,
                    "choices cannot be empty",
                )
            self.choices: Sequence[Any] | None = list(choices)
        else:
            self.choices = None

    def to_json_schema_constraints(self) -> dict[str, Any]:
        """Returns declared value constraints mapped to JSON Schema keywords (#125).

        Keyword mapping:
            - pattern -> pattern
            - ge -> minimum
            - gt -> exclusiveMinimum
            - le -> maximum
            - lt -> exclusiveMaximum
            - min_length -> minLength
            - max_length -> maxLength
            - choices -> enum

        Returns:
            Dictionary of JSON Schema keyword constraints.
        """
        mapping: dict[str, Any] = {}
        if self.pattern is not None:
            mapping["pattern"] = (
                self.pattern if isinstance(self.pattern, str) else self.pattern.pattern
            )
        if self.ge is not None:
            mapping["minimum"] = self.ge
        if self.gt is not None:
            mapping["exclusiveMinimum"] = self.gt
        if self.le is not None:
            mapping["maximum"] = self.le
        if self.lt is not None:
            mapping["exclusiveMaximum"] = self.lt
        if self.min_length is not None:
            mapping["minLength"] = self.min_length
        if self.max_length is not None:
            mapping["maxLength"] = self.max_length
        if self.choices is not None:
            mapping["enum"] = list(self.choices)
        return mapping

    def validate_value(self, value: Any, model: type | str | None = None) -> None:
        """Validates a property value against this field's declared value constraints.

        Args:
            value: Property value to validate.
            model: Optional owning model class or model name for error reporting.

        Raises:
            FieldConstraintError: If the value violates any declared constraint.
        """
        owner = model if model is not None else self._owner
        prop_name = self.name or ""

        # None handling: None skips all value constraint checks, regardless of nullable.
        if value is None:
            return

        # pattern
        if self._compiled_pattern is not None:
            if not isinstance(value, str):
                raise FieldConstraintError(
                    owner,
                    prop_name,
                    "pattern",
                    value,
                    f"{_format_model_prop(owner, prop_name)}: pattern constraint requires str, got {type(value).__name__}",
                )
            if not self._compiled_pattern.fullmatch(value):
                pat_str = (
                    self.pattern.pattern
                    if isinstance(self.pattern, re.Pattern)
                    else str(self.pattern)
                )
                raise FieldConstraintError(
                    owner,
                    prop_name,
                    "pattern",
                    value,
                    f"{_format_model_prop(owner, prop_name)}: value {value!r} does not match pattern {pat_str!r}",
                )

        # ge, gt, le, lt (bool must NOT pass as int)
        has_numeric = (
            self.ge is not None or self.gt is not None or self.le is not None or self.lt is not None
        )
        if has_numeric:
            if isinstance(value, bool):
                c_name = (
                    "ge"
                    if self.ge is not None
                    else ("gt" if self.gt is not None else ("le" if self.le is not None else "lt"))
                )
                raise FieldConstraintError(
                    owner,
                    prop_name,
                    c_name,
                    value,
                    f"{_format_model_prop(owner, prop_name)}: bool value {value!r} is not allowed for numeric {c_name} constraint",
                )
            if not isinstance(value, (int, float)):
                c_name = (
                    "ge"
                    if self.ge is not None
                    else ("gt" if self.gt is not None else ("le" if self.le is not None else "lt"))
                )
                raise FieldConstraintError(
                    owner,
                    prop_name,
                    c_name,
                    value,
                    f"{_format_model_prop(owner, prop_name)}: value {value!r} is not numeric",
                )
            if self.ge is not None and value < self.ge:
                raise FieldConstraintError(
                    owner,
                    prop_name,
                    "ge",
                    value,
                    f"{_format_model_prop(owner, prop_name)}: value {value!r} violates ge >= {self.ge}",
                )
            if self.gt is not None and value <= self.gt:
                raise FieldConstraintError(
                    owner,
                    prop_name,
                    "gt",
                    value,
                    f"{_format_model_prop(owner, prop_name)}: value {value!r} violates gt > {self.gt}",
                )
            if self.le is not None and value > self.le:
                raise FieldConstraintError(
                    owner,
                    prop_name,
                    "le",
                    value,
                    f"{_format_model_prop(owner, prop_name)}: value {value!r} violates le <= {self.le}",
                )
            if self.lt is not None and value >= self.lt:
                raise FieldConstraintError(
                    owner,
                    prop_name,
                    "lt",
                    value,
                    f"{_format_model_prop(owner, prop_name)}: value {value!r} violates lt < {self.lt}",
                )

        # min_length, max_length
        if self.min_length is not None or self.max_length is not None:
            if not isinstance(value, (str, list)):
                c_name = "min_length" if self.min_length is not None else "max_length"
                raise FieldConstraintError(
                    owner,
                    prop_name,
                    c_name,
                    value,
                    f"{_format_model_prop(owner, prop_name)}: length constraints apply to str and list, got {type(value).__name__}",
                )
            length = len(value)
            if self.min_length is not None and length < self.min_length:
                raise FieldConstraintError(
                    owner,
                    prop_name,
                    "min_length",
                    value,
                    f"{_format_model_prop(owner, prop_name)}: length {length} is less than min_length {self.min_length}",
                )
            if self.max_length is not None and length > self.max_length:
                raise FieldConstraintError(
                    owner,
                    prop_name,
                    "max_length",
                    value,
                    f"{_format_model_prop(owner, prop_name)}: length {length} is greater than max_length {self.max_length}",
                )

        # choices (compares by equality)
        if self.choices is not None:
            if not any(value == choice for choice in self.choices):
                raise FieldConstraintError(
                    owner,
                    prop_name,
                    "choices",
                    value,
                    f"{_format_model_prop(owner, prop_name)}: value {value!r} is not in choices {list(self.choices)!r}",
                )

    @property
    def has_default(self) -> bool:
        """Returns whether this field declares a default value or factory."""
        return self.default is not ... or self.default_factory is not None

    def resolve_default(self) -> Any:
        """Returns a fresh default value for a new instance.

        Plain defaults are returned as-is; a default factory is called for each
        invocation so mutable values are never shared between instances.
        """
        if self.default_factory is not None:
            return self.default_factory()
        return self.default

    def __class_getitem__(cls, item: Any) -> Any:
        """Enables generic typing support like `Field[str]` or `Field[int]`.

        Args:
            item: Generic type parameter.

        Returns:
            A Field descriptor initialized with the type annotation.
        """
        return cls(type_annotation=item)

    def __set_name__(self, owner: type, name: str) -> None:
        """Captures the assigned class attribute name.

        Args:
            owner: The owning class object.
            name: The attribute name string.
        """
        if self.name is None:
            self.name = name
        self._owner = owner

    def __get__(self, instance: Any, owner: type | None = None) -> Any:
        """Returns the BoundField descriptor for an instance or the descriptor itself.

        Args:
            instance: Instance of Node or Relationship, or None when accessed from class.
            owner: Owning class.

        Returns:
            BoundField bound to instance's alias, or Field on class-level access.
        """
        if instance is None:
            return self
        alias = getattr(instance, "alias", None)
        if alias is None:
            return self
        assert self.name is not None
        if hasattr(instance, "_bound_fields") and isinstance(instance._bound_fields, dict):
            bound = instance._bound_fields.get(self.name)
            if bound is None:
                bound = BoundField(alias, self.name)
                instance._bound_fields[self.name] = bound
            return bound
        return BoundField(alias, self.name)

    def bind(self, alias: str) -> BoundField:
        """Explicitly binds this property field to a target variable alias.

        Args:
            alias: Target query variable alias string (e.g. 'p', 'actor').

        Returns:
            A BoundField instance referencing the variable and property.
        """
        assert self.name is not None
        return BoundField(alias, self.name)

    def __repr__(self) -> str:
        owner_name = self._owner.__name__ if self._owner else ""
        if owner_name:
            return f"{owner_name}.{self.name}"
        return f"Field({self.name})"

    def __hash__(self) -> int:
        # Defining __eq__ would otherwise make Field unhashable; keep identity hashing.
        return id(self)

    def __eq__(self, other: Any) -> Any:  # type: ignore[override]
        if isinstance(other, Field) and self is other:
            return True
        return PredicateExpr("", self.name or "", "eq", other, model_class=self._owner)

    def __ne__(self, other: Any) -> Any:  # type: ignore[override]
        if isinstance(other, Field) and self is other:
            return False
        return PredicateExpr("", self.name or "", "neq", other, model_class=self._owner)

    def __gt__(self, other: Any) -> PredicateExpr:
        return PredicateExpr("", self.name or "", "gt", other, model_class=self._owner)

    def __ge__(self, other: Any) -> PredicateExpr:
        return PredicateExpr("", self.name or "", "gte", other, model_class=self._owner)

    def __lt__(self, other: Any) -> PredicateExpr:
        return PredicateExpr("", self.name or "", "lt", other, model_class=self._owner)

    def __le__(self, other: Any) -> PredicateExpr:
        return PredicateExpr("", self.name or "", "lte", other, model_class=self._owner)

    def contains(self, substring: Any) -> PredicateExpr:
        """Creates a string substring predicate `self CONTAINS substring`."""
        return PredicateExpr("", self.name or "", "contains", substring, model_class=self._owner)

    def in_(self, other: Any) -> PredicateExpr:
        """Creates an IN list membership predicate `self IN values`."""
        val_list = list(other) if isinstance(other, (list, tuple, set)) else other
        return PredicateExpr("", self.name or "", "in", val_list, model_class=self._owner)

    def not_in(self, other: Any) -> PredicateExpr:
        """Creates a NOT IN list membership predicate `self NOT IN values`."""
        val_list = list(other) if isinstance(other, (list, tuple, set)) else other
        return PredicateExpr("", self.name or "", "not_in", val_list, model_class=self._owner)

    def startswith(self, prefix: Any) -> PredicateExpr:
        """Creates a prefix predicate `self STARTS WITH prefix`."""
        return PredicateExpr("", self.name or "", "starts_with", prefix, model_class=self._owner)

    def endswith(self, suffix: Any) -> PredicateExpr:
        """Creates a suffix predicate `self ENDS WITH suffix`."""
        return PredicateExpr("", self.name or "", "ends_with", suffix, model_class=self._owner)

    def __add__(self, other: Any) -> BinaryExpr:
        from voyager_ogm.expressions import BinaryExpr, PropExpr, to_expression

        return BinaryExpr(
            PropExpr("", self.name or "", model_class=self._owner), "+", to_expression(other)
        )

    def __radd__(self, other: Any) -> BinaryExpr:
        from voyager_ogm.expressions import BinaryExpr, PropExpr, to_expression

        return BinaryExpr(
            to_expression(other), "+", PropExpr("", self.name or "", model_class=self._owner)
        )

    def __sub__(self, other: Any) -> BinaryExpr:
        from voyager_ogm.expressions import BinaryExpr, PropExpr, to_expression

        return BinaryExpr(
            PropExpr("", self.name or "", model_class=self._owner), "-", to_expression(other)
        )

    def __rsub__(self, other: Any) -> BinaryExpr:
        from voyager_ogm.expressions import BinaryExpr, PropExpr, to_expression

        return BinaryExpr(
            to_expression(other), "-", PropExpr("", self.name or "", model_class=self._owner)
        )

    def __mul__(self, other: Any) -> BinaryExpr:
        from voyager_ogm.expressions import BinaryExpr, PropExpr, to_expression

        return BinaryExpr(
            PropExpr("", self.name or "", model_class=self._owner), "*", to_expression(other)
        )

    def __rmul__(self, other: Any) -> BinaryExpr:
        from voyager_ogm.expressions import BinaryExpr, PropExpr, to_expression

        return BinaryExpr(
            to_expression(other), "*", PropExpr("", self.name or "", model_class=self._owner)
        )

    def __truediv__(self, other: Any) -> BinaryExpr:
        from voyager_ogm.expressions import BinaryExpr, PropExpr, to_expression

        return BinaryExpr(
            PropExpr("", self.name or "", model_class=self._owner), "/", to_expression(other)
        )

    def __rtruediv__(self, other: Any) -> BinaryExpr:
        from voyager_ogm.expressions import BinaryExpr, PropExpr, to_expression

        return BinaryExpr(
            to_expression(other), "/", PropExpr("", self.name or "", model_class=self._owner)
        )

    def count(self) -> AggregationExpr:
        """Returns a `COUNT(self)` column aggregation expression."""
        return AggregationExpr("", self.name or "", "count", model_class=self._owner)

    def avg(self) -> AggregationExpr:
        """Returns an `AVG(self)` column aggregation expression."""
        return AggregationExpr("", self.name or "", "avg", model_class=self._owner)

    def sum(self) -> AggregationExpr:
        """Returns a `SUM(self)` column aggregation expression."""
        return AggregationExpr("", self.name or "", "sum", model_class=self._owner)

    def min(self) -> AggregationExpr:
        """Returns a `MIN(self)` column aggregation expression."""
        return AggregationExpr("", self.name or "", "min", model_class=self._owner)

    def max(self) -> AggregationExpr:
        """Returns a `MAX(self)` column aggregation expression."""
        return AggregationExpr("", self.name or "", "max", model_class=self._owner)

    def collect(self) -> AggregationExpr:
        """Returns a `COLLECT(self)` column aggregation expression."""
        return AggregationExpr("", self.name or "", "collect", model_class=self._owner)

    def distance_to(self, other: Any) -> FunctionExpr:
        """Computes spatial distance expression `point.distance(field, other)`.

        Args:
            other: A Point, BoundField, or Expression.

        Returns:
            FunctionExpr representing the spatial distance expression.
        """
        name = self.name or ""
        bf = BoundField("", name, model_class=self._owner)
        return bf.distance_to(other)


class VectorProperty(Field):
    """Declarative vector embedding property descriptor for Vector Search and GraphRAG.

    Attributes:
        dimensions: Dimensionality of the embedding vector (e.g. 1536, 768, 384).
        similarity: Distance/similarity metric ('cosine', 'euclidean', 'dot').
        index_name: Optional explicit index identifier name.

    Example:
        >>> class Article(Node):
        ...     __labels__ = ["Article"]
        ...     id: str = Field(primary_key=True)
        ...     embedding: list[float] = VectorProperty(
        ...         dimensions=1536,
        ...         similarity="cosine",
        ...         index_name="article_embedding_idx"
        ...     )
    """

    def __init__(
        self,
        dimensions: int,
        similarity: str = "cosine",
        *,
        index_name: str | None = None,
        default: Any = ...,
        default_factory: Any = None,
        name: str | None = None,
        nullable: bool = True,
        type_annotation: Any = None,
    ) -> None:
        """Initializes a VectorProperty descriptor.

        Args:
            dimensions: Dimensionality of the vector (positive integer).
            similarity: Distance/similarity function ('cosine', 'euclidean', 'dot').
            index_name: Optional custom index name.
            default: Default fallback value for this property.
            default_factory: Zero-argument callable producing a default value.
            name: Custom database property name (defaults to attribute name).
            nullable: Whether the property can store null values. Defaults to True.
            type_annotation: Python type annotation class (defaults to list[float]).

        Raises:
            ValueError: If dimensions is not a positive integer or similarity is invalid.
        """
        if not isinstance(dimensions, int) or dimensions <= 0:
            raise ValueError(f"Vector dimensions must be a positive integer, got {dimensions!r}")
        sim_norm = similarity.strip().lower()
        if sim_norm not in ("cosine", "euclidean", "l2", "dot", "inner_product", "ip"):
            raise ValueError(
                f"Unsupported vector similarity metric '{similarity}'. "
                "Expected 'cosine', 'euclidean' (or 'l2'), or 'dot' (or 'inner_product')."
            )
        canonical_sim = (
            "euclidean"
            if sim_norm == "l2"
            else ("dot" if sim_norm in ("inner_product", "ip") else sim_norm)
        )
        self.dimensions = dimensions
        self.similarity = canonical_sim
        self.index_name = index_name
        super().__init__(
            default=default,
            default_factory=default_factory,
            name=name,
            unique=False,
            index=True,
            primary_key=False,
            nullable=nullable,
            type_annotation=type_annotation or list[float],
            index_type="VECTOR",
        )


class BoundField(Expression):
    """A field bound to a specific node or relationship alias instance.

    Overloads comparison, arithmetic, and boolean operators to produce AST `Expression`
    and `PredicateExpr` objects for query building.

    Attributes:
        target_alias: Variable alias of the node or edge (e.g. 'p', 'm').
        field_name: Property name on the entity.
    """

    def __init__(
        self,
        target_alias: str,
        field_name: str,
        model_class: type | None = None,
    ) -> None:
        """Initializes a BoundField.

        Args:
            target_alias: Entity variable alias in the query.
            field_name: Property name on the entity.
            model_class: Optional owning model class for class-level resolution.
        """
        self.target_alias = target_alias
        self.field_name = field_name
        self.target = target_alias
        self.field = field_name
        self.model_class = model_class

    def to_spec(self) -> tuple[str, str, str]:
        """Returns the AST property specification tuple `('prop', var, prop)`."""
        return ("prop", self.target_alias, self.field_name)

    def __hash__(self) -> int:
        """Returns the hash of this bound field based on its target alias and property name."""
        return hash((self.target_alias, self.field_name))

    def __bool__(self) -> bool:
        """Explicitly disallows evaluating a BoundField in a boolean context."""
        raise TypeError(
            f"Evaluating a BoundField ('{self.target_alias}.{self.field_name}') in a boolean context "
            "(e.g. 'if node.prop:') is not supported. "
            f"To inspect in-memory property values, use entity.get('{self.field_name}') instead."
        )

    def __repr__(self) -> str:
        return f"{self.target_alias}.{self.field_name}"

    def __eq__(self, other: Any) -> Any:  # type: ignore[override]
        """Creates an equality predicate `alias.prop = value`, or returns True if comparing identical bound fields."""
        if isinstance(other, BoundField) and (self.target_alias, self.field_name) == (
            other.target_alias,
            other.field_name,
        ):
            return True
        return PredicateExpr(
            self.target_alias, self.field_name, "eq", other, model_class=self.model_class
        )

    def __gt__(self, other: Any) -> PredicateExpr:
        """Creates a greater-than predicate `alias.prop > value`."""
        return PredicateExpr(
            self.target_alias, self.field_name, "gt", other, model_class=self.model_class
        )

    def __ge__(self, other: Any) -> PredicateExpr:
        """Creates a greater-than-or-equal predicate `alias.prop >= value`."""
        return PredicateExpr(
            self.target_alias, self.field_name, "gte", other, model_class=self.model_class
        )

    def __lt__(self, other: Any) -> PredicateExpr:
        """Creates a less-than predicate `alias.prop < value`."""
        return PredicateExpr(
            self.target_alias, self.field_name, "lt", other, model_class=self.model_class
        )

    def __le__(self, other: Any) -> PredicateExpr:
        """Creates a less-than-or-equal predicate `alias.prop <= value`."""
        return PredicateExpr(
            self.target_alias, self.field_name, "lte", other, model_class=self.model_class
        )

    def __ne__(self, other: Any) -> PredicateExpr:  # type: ignore[override]
        """Creates a not-equal predicate `alias.prop != value`."""
        return PredicateExpr(
            self.target_alias, self.field_name, "ne", other, model_class=self.model_class
        )

    def in_(self, other: Any) -> PredicateExpr:
        """Creates an IN list membership predicate `alias.prop IN values`."""
        val_list = list(other) if isinstance(other, (list, tuple, set)) else other
        return PredicateExpr(
            self.target_alias, self.field_name, "in", val_list, model_class=self.model_class
        )

    def not_in(self, other: Any) -> PredicateExpr:
        """Creates a NOT IN list membership predicate `alias.prop NOT IN values`."""
        val_list = list(other) if isinstance(other, (list, tuple, set)) else other
        return PredicateExpr(
            self.target_alias, self.field_name, "not_in", val_list, model_class=self.model_class
        )

    def startswith(self, prefix: Any) -> PredicateExpr:
        """Creates a prefix predicate `alias.prop STARTS WITH prefix`."""
        return PredicateExpr(
            self.target_alias, self.field_name, "starts_with", prefix, model_class=self.model_class
        )

    def endswith(self, suffix: Any) -> PredicateExpr:
        """Creates a suffix predicate `alias.prop ENDS WITH suffix`."""
        return PredicateExpr(
            self.target_alias, self.field_name, "ends_with", suffix, model_class=self.model_class
        )

    def contains(self, substring: Any) -> PredicateExpr:
        """Creates a string substring predicate `alias.prop CONTAINS value`."""
        return PredicateExpr(
            self.target_alias, self.field_name, "contains", substring, model_class=self.model_class
        )

    def count(self) -> AggregationExpr:
        """Returns a `COUNT(alias.prop)` aggregation expression."""
        return AggregationExpr(
            self.target_alias, self.field_name, "count", model_class=self.model_class
        )

    def avg(self) -> AggregationExpr:
        """Returns an `AVG(alias.prop)` aggregation expression."""
        return AggregationExpr(
            self.target_alias, self.field_name, "avg", model_class=self.model_class
        )

    def sum(self) -> AggregationExpr:
        """Returns a `SUM(alias.prop)` aggregation expression."""
        return AggregationExpr(
            self.target_alias, self.field_name, "sum", model_class=self.model_class
        )

    def min(self) -> AggregationExpr:
        """Returns a `MIN(alias.prop)` aggregation expression."""
        return AggregationExpr(
            self.target_alias, self.field_name, "min", model_class=self.model_class
        )

    def max(self) -> AggregationExpr:
        """Returns a `MAX(alias.prop)` aggregation expression."""
        return AggregationExpr(
            self.target_alias, self.field_name, "max", model_class=self.model_class
        )

    def collect(self) -> AggregationExpr:
        """Returns a `COLLECT(alias.prop)` aggregation expression."""
        return AggregationExpr(
            self.target_alias, self.field_name, "collect", model_class=self.model_class
        )

    def distance_to(self, other: Any) -> FunctionExpr:
        """Creates a spatial distance expression `point.distance(self, other)`.

        Args:
            other: A Point, BoundField, or Expression.

        Returns:
            FunctionExpr representing the spatial distance expression.
        """
        return FunctionExpr("point.distance", [self, to_expression(other)])


class AggregationExpr(Expression):
    """Container for AST column aggregation expressions (e.g. COUNT, AVG)."""

    _is_aggregation = True

    def __init__(
        self,
        target: str,
        field: str,
        func: str,
        model_class: type | None = None,
    ) -> None:
        self.target = target
        self.target_alias = target
        self.field = field
        self.field_name = field
        self.func = func
        self.model_class = model_class

    def to_spec(self) -> tuple[str, str, str, str, None]:
        """Converts this aggregation into an AST spec descriptor tuple."""
        return ("agg", self.target, self.field, self.func, None)

    def __repr__(self) -> str:
        tgt = self.target or (self.model_class.__name__ if self.model_class else "")
        if tgt:
            return f"{self.func.upper()}({tgt}.{self.field})"
        return f"{self.func.upper()}(.{self.field})"


class PredicateExpr(BinaryExpr):
    """Container for AST predicate conditions.

    Attributes:
        target: Variable alias of the entity.
        field: Property name.
        op: Comparison operator string ('eq', 'gt', 'gte', 'lt', 'lte', 'contains').
        value: Right-hand side literal comparison value.
    """

    def __init__(
        self,
        target: str,
        field: str,
        op: str,
        value: Any,
        model_class: type | None = None,
    ) -> None:
        """Initializes a PredicateExpr.

        Args:
            target: Entity variable alias.
            field: Property name.
            op: Operator string.
            value: Right-hand comparison value.
            model_class: Optional owning model class for class-level resolution.
        """
        self.target = target
        self.target_alias = target
        self.field = field
        self.field_name = field
        self.op = op
        self.value = value
        self.model_class = model_class
        super().__init__(PropExpr(target, field, model_class=model_class), op, to_expression(value))

    def to_spec(self) -> tuple[str, str, Any, Any]:
        """Converts to native AST binary expression tuple."""
        return (
            "bin",
            self.op,
            ("prop", self.target, self.field),
            to_expression(self.value).to_spec(),
        )

    def __repr__(self) -> str:
        tgt = self.target or (self.model_class.__name__ if self.model_class else "")
        if tgt:
            return f"({tgt}.{self.field} {self.op} {self.value!r})"
        return f"(.{self.field} {self.op} {self.value!r})"

    def __bool__(self) -> bool:
        """Explicitly disallows evaluating a PredicateExpr in a boolean context."""
        raise TypeError(
            f"Evaluating a PredicateExpr ('{self.target}.{self.field} {self.op} {self.value!r}') in a boolean context "
            "(e.g. 'if node.prop == val:' or 'expr and expr') is not supported. "
            "To filter queries, pass this predicate to Query.where(...). "
            f"To check an in-memory property value, use entity.get('{self.field}') == {self.value!r}."
        )


def _model_identity(model: type[Any]) -> str:
    """Returns the stable native schema identity for a model class.

    The identity combines the defining module and qualified class name, so two
    models with the same class name in different modules stay distinct.
    """
    module = str(getattr(model, "__module__", "") or "")
    qualname = str(getattr(model, "__qualname__", None) or getattr(model, "__name__", ""))
    return f"{module}.{qualname}" if module else qualname


_NATIVE_KEY_CACHE_MISS = object()
_NATIVE_KEY_CACHE_ATTR = "_native_key_cache"


def _native_key_cache(model: type[Any]) -> dict[str, Any]:
    """Returns this model class's own native key cache, creating it on first use.

    The cache lives directly on the class (``cls.__dict__``) so a subclass never
    reuses a parent's cached answers.
    """
    cache = model.__dict__.get(_NATIVE_KEY_CACHE_ATTR)
    if cache is None:
        cache = {"primary_key": _NATIVE_KEY_CACHE_MISS, "merge_keys": {}}
        setattr(model, _NATIVE_KEY_CACHE_ATTR, cache)
    return cache


def _invalidate_native_key_cache(model: type[Any]) -> None:
    """Drops cached native key answers for a model class after re-registration."""
    if _NATIVE_KEY_CACHE_ATTR in model.__dict__:
        delattr(model, _NATIVE_KEY_CACHE_ATTR)


def _ensure_model_registered(model: type[Any], registry: Any) -> str | None:
    """Ensures the model has a native schema entry and returns its identity."""
    register_schema = getattr(model, "register_schema", None)
    if not callable(register_schema):
        return None
    identity = _model_identity(model)
    if not registry.has_node(identity) and not registry.has_relationship(identity):
        register_schema()
    return identity


def get_primary_key_field(model: type[Any]) -> str | None:
    """Returns the primary key property name for a model, if any.

    Primary keys are declared with ``Field(primary_key=True)``. The decision is
    made by the native schema registry from the field descriptors; this helper
    only maps a model class to its native schema identity.

    Args:
        model: Node or Relationship model class to inspect.

    Returns:
        The primary key property name, or None when the model declares no
        primary key or has no registered native schema.
    """
    if NativeSchemaRegistry is None:
        return None
    cache = _native_key_cache(model)
    cached = cache["primary_key"]
    if cached is not _NATIVE_KEY_CACHE_MISS:
        return cached
    registry = NativeSchemaRegistry.global_registry()
    identity = _ensure_model_registered(model, registry)
    if identity is None:
        return None
    value = registry.primary_key_for(identity)
    cache["primary_key"] = value
    return value


def _native_merge_key_fields(
    model: type[Any],
    supplied: list[str],
    defaulted: set[str],
) -> list[str]:
    """Delegates MERGE key resolution to the native schema registry."""
    if NativeSchemaRegistry is None:
        raise RuntimeError("NativeSchemaRegistry is required to resolve MERGE keys")
    cache = _native_key_cache(model)
    cache_key = (tuple(supplied), tuple(sorted(defaulted)))
    cached = cache["merge_keys"].get(cache_key)
    if cached is not None:
        return list(cached)
    registry = NativeSchemaRegistry.global_registry()
    identity = _ensure_model_registered(model, registry)
    if identity is None:
        raise RuntimeError(f"{model!r} is not a registrable graph model")
    key_fields = registry.merge_key_for(identity, supplied, list(defaulted))
    if key_fields is None:
        raise RuntimeError(f"No native schema registered for model '{identity}'")
    cache["merge_keys"][cache_key] = tuple(key_fields)
    return list(key_fields)


def _process_type_annotations(cls: Any) -> dict[str, Field]:
    """Extracts type annotations and injects Field descriptors automatically.

    Args:
        cls: Class to inspect.

    Returns:
        Dictionary mapping field attribute names to Field descriptors.
    """
    fields_map: dict[str, Field] = {}

    # Inherit schema fields from base classes in MRO
    for base in reversed(cls.__mro__):
        if hasattr(base, "_schema_fields") and isinstance(base._schema_fields, dict):
            fields_map.update(base._schema_fields)

    try:
        annotations = inspect.get_annotations(cls, eval_str=True)
    except Exception:
        annotations = getattr(cls, "__annotations__", {})

    for attr_name, type_hint in annotations.items():
        if attr_name.startswith("_"):
            continue
        if isinstance(type_hint, str) and type_hint in _BUILTIN_TYPES:
            type_hint = _BUILTIN_TYPES[type_hint]

        existing_val = getattr(cls, attr_name, None)
        if isinstance(existing_val, Field):
            existing_val.type_annotation = type_hint
            if existing_val.name is None:
                existing_val.name = attr_name
            fields_map[attr_name] = existing_val
        elif not inspect.isroutine(existing_val):
            field_desc: Field[Any] = Field(
                default=existing_val if existing_val is not None else ...,
                name=attr_name,
                type_annotation=type_hint,
            )
            setattr(cls, attr_name, field_desc)
            fields_map[attr_name] = field_desc

    # Discover any explicit Field descriptors defined without type annotations
    for attr_name in dir(cls):
        if attr_name.startswith("_") or attr_name in fields_map:
            continue
        val = getattr(cls, attr_name, None)
        if isinstance(val, Field):
            if val.name is None:
                val.name = attr_name
            fields_map[attr_name] = val

    for f in fields_map.values():
        f._owner = cls

    for attr_name, field_desc in fields_map.items():
        _validate_field_declaration(cls, attr_name, field_desc)

    cls._schema_fields = fields_map
    return fields_map


def _apply_field_defaults(
    values: dict[str, Any],
    schema: dict[str, Field],
) -> set[str]:
    """Fills in declared defaults for fields absent from ``values``.

    Explicitly supplied keys, including ones mapped to ``None``, are never
    overwritten. A ``default_factory`` is invoked once per call so every
    instance or bulk record receives an independent default value.

    Args:
        values: Mutable property mapping to complete in place.
        schema: Field descriptors keyed by model attribute name.

    Returns:
        Names of the fields that were filled from a declared default.
    """
    defaulted: set[str] = set()
    for attr_name, field in schema.items():
        if attr_name in values or not field.has_default:
            continue
        values[attr_name] = field.resolve_default()
        defaulted.add(attr_name)
    return defaulted


def _resolve_declared_labels(cls: type) -> list[str] | None:
    """Resolves labels declared directly in a class body.

    ``__label__ = "X"`` is accepted as the single-label form of
    ``__labels__ = ["X"]``. When both are declared, they must agree on the
    primary label (``__labels__[0]``). Inherited attributes are ignored; only
    the class's own ``__dict__`` is inspected.

    Args:
        cls: The class whose declared labels should be resolved.

    Returns:
        The declared label list, or ``None`` when the class declares neither
        ``__label__`` nor a non-empty ``__labels__``.

    Raises:
        TypeError: If ``__label__`` is not a non-empty string, or if both
            attributes are declared with different primary labels.
    """
    if "__label__" not in cls.__dict__ and "__labels__" not in cls.__dict__:
        return None

    has_label = "__label__" in cls.__dict__
    declared_label = cls.__dict__.get("__label__")
    declared_labels = cls.__dict__.get("__labels__")

    if has_label and (not isinstance(declared_label, str) or not declared_label):
        raise TypeError(
            f"'{cls.__name__}.__label__' must be a non-empty string, got {declared_label!r}."
        )

    if has_label and declared_labels and declared_label != declared_labels[0]:
        raise TypeError(
            f"'{cls.__name__}' sets both __label__={declared_label!r} and "
            f"__labels__={list(declared_labels)!r}, but __label__ must equal "
            f"__labels__[0] ({declared_labels[0]!r}). Declare only one, or make "
            "__label__ match the first entry of __labels__."
        )

    if declared_labels:
        return list(declared_labels)
    if has_label:
        return [cast(str, declared_label)]
    return None


def _resolve_declared_type(cls: type) -> str | None:
    """Resolves a relationship type declared directly in a class body.

    Args:
        cls: The class whose own ``__type__`` attribute should be resolved.

    Returns:
        The declared relationship type string, or ``None`` when the class does
        not declare ``__type__`` in its own body.

    Raises:
        TypeError: If ``__type__`` is not a non-empty string.
    """
    if "__type__" not in cls.__dict__:
        return None
    declared_type = cls.__dict__.get("__type__")
    if not isinstance(declared_type, str) or not declared_type:
        raise TypeError(
            f"'{cls.__name__}.__type__' must be a non-empty string, got {declared_type!r}."
        )
    return declared_type


@dataclass_transform(field_specifiers=(Field,))
class Node:
    """Base class for Voyager OGM Graph Node Entities.

    Can be directly subclassed or automatically injected via the `@node` decorator.

    Attributes:
        __labels__: Class-level list of graph database labels. A class may instead
            declare the single-label alias ``__label__ = "X"``, which is equivalent
            to ``__labels__ = ["X"]``. Declaring both is allowed only when
            ``__label__`` equals ``__labels__[0]``.
        _schema_fields: Dictionary of property field descriptors for schema reflection.

    Example:
        >>> class Developer(Node, label=["Person", "Engineer"]):
        ...     name: str
        ...     age: int
        >>> class Film(Node):
        ...     __label__ = "Movie"
        ...     title: str
    """

    __labels__: ClassVar[list[str]] = []
    __description__: ClassVar[str | None] = None
    _schema_fields: ClassVar[dict[str, Field]] = {}
    _cached_labels: ClassVar[list[str]] = ["Node"]
    _cached_label: ClassVar[str] = "Node"

    def __init_subclass__(
        cls,
        label: str | list[str] | None = None,
        labels: list[str] | None = None,
        **kwargs: Any,
    ) -> None:
        """Initializes Node subclass metadata and labels.

        Args:
            label: Optional custom label or list of labels.
            labels: Optional custom label list (synonym for label).
            **kwargs: Extra keyword arguments passed to super.

        Note:
            Precedence for the effective labels is: an explicit ``label``/
            ``labels`` keyword, then a class-body ``__label__`` or ``__labels__``
            declaration, then the class name. A ``@node("X")`` decorator argument
            is explicit at the use site and therefore overrides class-body
            declarations.
        """
        super().__init_subclass__(**kwargs)
        declared_labels = _resolve_declared_labels(cls)
        lbl = label if label is not None else labels
        if isinstance(lbl, str):
            cls.__labels__ = [lbl]
        elif isinstance(lbl, list):
            cls.__labels__ = list(lbl)
        elif declared_labels:
            cls.__labels__ = declared_labels
        elif "__labels__" not in cls.__dict__ or not cls.__labels__:
            cls.__labels__ = [cls.__name__]

        cls._cached_labels = list(cls.__labels__)
        cls._cached_label = cls.__labels__[0] if cls.__labels__ else cls.__name__
        _process_type_annotations(cls)
        cls.register_schema()

    @classmethod
    def register_schema(cls, registry: Any = None) -> None:
        """Registers this Node model's schema into the NativeSchemaRegistry.

        The entry is keyed by the model's module-qualified identity, so models
        with the same class name in different modules stay separate. The native
        registry derives the primary key from the declared field descriptors.

        Args:
            registry: Optional NativeSchemaRegistry instance. If None, registers
                in the shared global registry (NativeSchemaRegistry.global_registry()).
        """
        if NativeSchemaRegistry is None:
            return
        _invalidate_native_key_cache(cls)
        try:
            reg = registry if registry is not None else NativeSchemaRegistry.global_registry()
            name = cls.__name__
            labels = list(getattr(cls, "__labels__", [name]))
            fields = getattr(cls, "_schema_fields", {})
            model_id = _model_identity(cls)
            fulltext_indexes = getattr(cls, "__fulltext_indexes__", None)
            ft_list = None
            if fulltext_indexes is not None:
                ft_list = [
                    ft.to_dict() if hasattr(ft, "to_dict") else ft for ft in fulltext_indexes
                ]
            description = cls.__dict__.get("__description__", None)
            register_with_identity = getattr(reg, "register_node_with_identity", None)
            if callable(register_with_identity):
                register_with_identity(
                    model_id, name, labels, fields, ft_list, description=description
                )
            else:
                reg.register_node(name, labels, fields, None, ft_list, description=description)
        except Exception as e:
            logger.warning("Failed to register schema for node model %s: %s", cls.__name__, e)
            raise

    @classmethod
    def fulltext_search(
        cls,
        query_string: str,
        *,
        yield_node: str = "node",
        yield_score: str = "score",
        dialect: str = "cypher",
    ) -> Any:
        """Starts a fluent Query full-text search against this model's full-text index.

        Note:
            Model-level search methods default to ``dialect='cypher'``. When executing via an active
            session against Memgraph or FalkorDB, prefer ``session.text_search(Model, ...)``, which
            automatically inherits the session's active dialect, or pass ``dialect='memgraph'`` /
            ``dialect='falkordb'`` explicitly.

        Args:
            query_string: Full-text search term or expression (e.g. 'search phrase').
            yield_node: Yielded variable alias for the matched entity (default: 'node').
            yield_score: Yielded variable alias for the relevance score (default: 'score').
            dialect: Query dialect ('cypher', 'memgraph', 'falkordb', etc.). Defaults to 'cypher'.

        Returns:
            Fluent Query instance chained with the search procedure and yield clause.
        """
        from voyager_ogm.query import Query

        return Query.fulltext_search(
            cls,
            query_string,
            yield_node=yield_node,
            yield_score=yield_score,
            dialect=dialect,
        )

    text_search = fulltext_search

    @classmethod
    def vector_search(
        cls,
        query_vector: list[float],
        k: int = 10,
        *,
        yield_node: str = "node",
        yield_score: str = "score",
        dialect: str = "cypher",
    ) -> Any:
        """Starts a fluent Query vector search against this model's vector property.

        Note:
            Model-level search methods default to ``dialect='cypher'``. When executing via an active
            session against Memgraph, prefer ``session.vector_search(Model, ...)``, which automatically
            inherits the session's active dialect, or pass ``dialect='memgraph'`` explicitly.

        Args:
            query_vector: Dense embedding vector to query with (list of floats).
            k: Top-k nearest neighbors to retrieve (default: 10).
            yield_node: Yielded variable alias for the matched entity node (default: 'node').
            yield_score: Yielded variable alias for the similarity score (default: 'score').
            dialect: Query dialect ('cypher', 'memgraph', etc.). Defaults to 'cypher'.

        Returns:
            Fluent Query instance chained with the vector search procedure call and yield clause.
        """
        from voyager_ogm.query import Query

        return Query.vector_search(
            cls,
            query_vector,
            k=k,
            yield_node=yield_node,
            yield_score=yield_score,
            dialect=dialect,
        )

    def __init__(self, alias: str | None = None, **values: Any) -> None:
        """Instantiates a Node entity with a unique query alias.

        Args:
            alias: Optional explicit variable alias. If omitted, an auto-alias
                (e.g. `_person_0`) is deterministically generated.
            **values: Property key-value pairs, or optional `session` reference.
        """
        cached_label = getattr(self.__class__, "_cached_label", None)
        if not cached_label:
            cached_label = self.__labels__[0] if self.__labels__ else self.__class__.__name__
        self._alias = alias or _get_next_alias(cached_label)
        self._cached_alias = self._alias
        self._bound_fields: dict[str, BoundField] = {}
        session = values.pop("session", None)
        self._session_ref: weakref.ref[Any] | None = (
            weakref.ref(session) if session is not None else None
        )
        hydrated_values: dict[str, Any] = {}
        schema = getattr(self, "_schema_fields", {})
        for k, v in values.items():
            hydrated_values[k] = _hydrate_field_value(schema.get(k), v)
        self._defaulted_fields = _apply_field_defaults(hydrated_values, schema)
        for k, v in hydrated_values.items():
            field_desc = schema.get(k)
            if field_desc is not None:
                field_desc.validate_value(v, self.__class__)
        self._values: dict[str, Any] = hydrated_values
        self._dirty_fields: dict[str, Any] = dict(hydrated_values)
        if session is not None and getattr(session, "identity_map_enabled", False):
            session.register(self)

    def _attach_session(self, session: Any) -> None:
        """[Experimental] Attaches a session via weak reference for Active Record `.save()` ergonomics."""
        self._session_ref = weakref.ref(session)

    def _validate_for_save(self) -> None:
        """Validates all schema fields on this node before saving or flushing."""
        schema = getattr(self, "_schema_fields", {})
        for name, field_desc in schema.items():
            if name in self._values:
                val = self._values[name]
                field_desc.validate_value(val, self.__class__)

    def save(self, session: Any = None, key_field: str | None = None) -> Any:
        """[Experimental] Persists dirty property changes to the database using Active Record ergonomics.

        Technique: Active Record + Data Mapper Fusion
        --------------------------------------------
        Combines the intuitive convenience of Active Record (`entity.save()`) with the
        clean architecture of Data Mapper (the entity contains zero database dialect or
        connection logic, delegating persistence to the active Session).

        Args:
            session: Explicit Session to persist through. If omitted, uses the session
                attached during initialization or registration.
            key_field: Unique primary key property name. Defaults to the model's
                declared `Field(primary_key=True)`, falling back to 'id'.

        Returns:
            BulkExecutionResult if flushed, or None if no dirty fields were pending.

        Raises:
            RuntimeError: If no session is passed and no session was previously attached.
        """
        self._validate_for_save()
        active_session = session
        if active_session is None and self._session_ref is not None:
            active_session = self._session_ref()

        if active_session is None:
            msg = (
                f"Cannot save {self.__class__.__name__}: No active Session attached. "
                "Pass session explicitly: node.save(session=session) or register with session first."
            )
            raise RuntimeError(msg)

        if session is not None:
            self._attach_session(session)

        if not self._dirty_fields:
            return None

        resolved_key_field = key_field
        if resolved_key_field is None:
            resolved_key_field = get_primary_key_field(self.__class__) or "id"

        record = {k: serialize_param_value(v) for k, v in self._dirty_fields.items()}
        primary_val = self.get(resolved_key_field)
        if primary_val is not None:
            record[resolved_key_field] = serialize_param_value(primary_val)

        plan = active_session.bulk_upsert(
            model=self.__class__,
            data=[record],
            key_field=resolved_key_field,
            defaulted_fields=[{k for k in self._defaulted_fields if k in record}],
        )
        result = active_session.run_bulk(plan)
        self.clear_dirty()
        return result

    async def async_save(self, session: Any = None, key_field: str | None = None) -> Any:
        """[Experimental] Asynchronously persists dirty property changes to the database using Active Record ergonomics.

        Technique: Active Record + Data Mapper Fusion
        --------------------------------------------
        Combines the intuitive convenience of Active Record (`await entity.async_save()`) with
        the clean architecture of Data Mapper (the entity contains zero database dialect or
        connection logic, delegating persistence to the active AsyncSession).

        Args:
            session: Explicit AsyncSession to persist through. If omitted, uses the session
                attached during initialization or registration.
            key_field: Unique primary key property name. Defaults to the model's
                declared `Field(primary_key=True)`, falling back to 'id'.

        Returns:
            BulkExecutionResult if flushed, or None if no dirty fields were pending.

        Raises:
            RuntimeError: If no session is passed and no session was previously attached.
        """
        self._validate_for_save()
        active_session = session
        if active_session is None and self._session_ref is not None:
            active_session = self._session_ref()

        if active_session is None:
            msg = (
                f"Cannot save {self.__class__.__name__}: No active AsyncSession attached. "
                "Pass session explicitly: await node.async_save(session=session) or register first."
            )
            raise RuntimeError(msg)

        if session is not None:
            self._attach_session(session)

        if not self._dirty_fields:
            return None

        resolved_key_field = key_field
        if resolved_key_field is None:
            resolved_key_field = get_primary_key_field(self.__class__) or "id"

        record = {k: serialize_param_value(v) for k, v in self._dirty_fields.items()}
        primary_val = self.get(resolved_key_field)
        if primary_val is not None:
            record[resolved_key_field] = serialize_param_value(primary_val)

        plan = active_session.bulk_upsert(
            model=self.__class__,
            data=[record],
            key_field=resolved_key_field,
            defaulted_fields=[{k for k in self._defaulted_fields if k in record}],
        )
        result = await active_session.run_bulk(plan)
        self.clear_dirty()
        return result

    @property
    def alias(self) -> str:
        """Returns the variable alias assigned to this node instance."""
        return self._alias

    @property
    def labels(self) -> list[str]:
        """Returns the list of graph labels associated with this node."""
        return self.__labels__ if self.__labels__ else [self.__class__.__name__]

    @property
    def dirty_fields(self) -> dict[str, Any]:
        """Returns a dictionary of modified property names and their updated values."""
        return dict(self._dirty_fields)

    def clear_dirty(self) -> None:
        """Clears recorded dirty fields after a commit or save."""
        self._dirty_fields.clear()

    def get(self, name: str, default: Any = None) -> Any:
        """Retrieves an in-memory property value."""
        return self._values.get(name, default)

    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith("_"):
            super().__setattr__(name, value)
        else:
            if not hasattr(self, "_values"):
                self._values = {}
            if not hasattr(self, "_dirty_fields"):
                self._dirty_fields = {}
            if not hasattr(self, "_defaulted_fields"):
                self._defaulted_fields = set()
            if hasattr(self, "_schema_fields") and name in self._schema_fields:
                field_desc = self._schema_fields[name]
                value = _hydrate_field_value(field_desc, value)
                field_desc.validate_value(value, self.__class__)
            self._values[name] = value
            self._dirty_fields[name] = value
            self._defaulted_fields.discard(name)

    def count(self) -> AggregationExpr:
        """Returns a `COUNT(alias)` node aggregation expression."""
        return AggregationExpr(self._alias, "*", "count")

    def __getattr__(self, name: str) -> BoundField:
        """Dynamically resolves unknown property names into BoundField descriptors.

        Args:
            name: Property name accessed on the node.

        Returns:
            BoundField bound to this node's alias and property name.

        Raises:
            AttributeError: If name starts with an underscore or is not declared in schema.
        """
        if name.startswith("_"):
            raise AttributeError(name)
        if self._schema_fields:
            if name not in self._schema_fields:
                matches = difflib.get_close_matches(
                    name, self._schema_fields.keys(), n=1, cutoff=0.6
                )
                suggestion = f". Did you mean '{matches[0]}'?" if matches else ""
                raise AttributeError(
                    f"'{type(self).__name__}' has no property '{name}'{suggestion}"
                )
        if name not in self._bound_fields:
            self._bound_fields[name] = BoundField(self._alias, name)
        return self._bound_fields[name]


@dataclass_transform(field_specifiers=(Field,))
class Relationship:
    """Base class for Voyager OGM Graph Relationship Entities.

    Can be directly subclassed or automatically injected via `@relationship`.

    Attributes:
        __type__: Relationship type string in the database (e.g. 'ACTED_IN').
            A class may set this directly in its body; an explicit ``type_name``
            keyword or decorator argument overrides it.
        __direction__: Edge traversal direction ('outgoing', 'incoming', 'undirected').
        _schema_fields: Dictionary of property field descriptors.

    Example:
        >>> class ActedIn(Relationship, type_name="ACTED_IN"):
        ...     role: str
        ...     since: int = 2000
        >>> class Directed(Relationship):
        ...     __type__ = "DIRECTED"
        ...     since: int = 2000
    """

    __type__: ClassVar[str] = ""
    __description__: ClassVar[str | None] = None
    __direction__: ClassVar[str] = "outgoing"
    __source_labels__: ClassVar[list[str]] = []
    __target_labels__: ClassVar[list[str]] = []
    _cached_type: ClassVar[str] = ""
    _cached_types: ClassVar[list[str]] = []
    _schema_fields: ClassVar[dict[str, Field]] = {}

    def __init_subclass__(
        cls,
        type_name: str | None = None,
        type_: str | None = None,
        direction: str = "outgoing",
        **kwargs: Any,
    ) -> None:
        """Initializes Relationship subclass metadata.

        Args:
            type_name: Database edge type string. Defaults to uppercase class name.
            type_: Alias for type_name.
            direction: Traversal direction ('outgoing', 'incoming', 'undirected').
            **kwargs: Extra keyword arguments passed to super.

        Note:
            Precedence for the effective type is: an explicit ``type_name``/
            ``type_``/``type`` keyword, then a class-body ``__type__``
            declaration, then the uppercase class name. A decorator argument is
            explicit at the use site and therefore overrides class-body
            declarations.
        """
        super().__init_subclass__(**kwargs)
        declared_type = _resolve_declared_type(cls)
        resolved_type = (
            type_name or type_ or kwargs.get("type") or declared_type or cls.__name__.upper()
        )
        cls.__type__ = resolved_type
        cls.__direction__ = direction
        source_labels = kwargs.get("source_labels") or kwargs.get("from_labels")
        if not source_labels and "source_node" in kwargs:
            src = kwargs["source_node"]
            source_labels = getattr(src, "__labels__", [getattr(src, "__name__", str(src))])
        target_labels = kwargs.get("target_labels") or kwargs.get("to_labels")
        if not target_labels and "target_node" in kwargs:
            tgt = kwargs["target_node"]
            target_labels = getattr(tgt, "__labels__", [getattr(tgt, "__name__", str(tgt))])
        cls.__source_labels__ = list(source_labels) if source_labels else []
        cls.__target_labels__ = list(target_labels) if target_labels else []
        cls._cached_type = cls.__type__
        cls._cached_types = [cls.__type__]
        _process_type_annotations(cls)
        cls.register_schema()

    @classmethod
    def register_schema(cls, registry: Any = None) -> None:
        """Registers this Relationship model's schema into the NativeSchemaRegistry.

        The entry is keyed by the model's module-qualified identity, so models
        with the same class name in different modules stay separate.

        Args:
            registry: Optional NativeSchemaRegistry instance. If None, registers
                in the shared global registry (NativeSchemaRegistry.global_registry()).
        """
        if NativeSchemaRegistry is None:
            return
        _invalidate_native_key_cache(cls)
        try:
            reg = registry if registry is not None else NativeSchemaRegistry.global_registry()
            name = cls.__name__
            type_name = getattr(cls, "__type__", None) or name.upper()
            source_labels = list(getattr(cls, "__source_labels__", []))
            target_labels = list(getattr(cls, "__target_labels__", []))
            fields = getattr(cls, "_schema_fields", {})
            direction = getattr(cls, "__direction__", "outgoing")
            directed = direction != "undirected"
            model_id = _model_identity(cls)
            fulltext_indexes = getattr(cls, "__fulltext_indexes__", None)
            ft_list = None
            if fulltext_indexes is not None:
                ft_list = [
                    ft.to_dict() if hasattr(ft, "to_dict") else ft for ft in fulltext_indexes
                ]
            description = cls.__dict__.get("__description__", None)
            reg.register_relationship(
                name,
                type_name,
                source_labels if source_labels else None,
                target_labels if target_labels else None,
                fields,
                directed,
                ft_list,
                model_id=model_id,
                description=description,
            )
        except Exception as e:
            logger.warning(
                "Failed to register schema for relationship model %s: %s",
                cls.__name__,
                e,
            )
            raise

    @classmethod
    def fulltext_search(
        cls,
        query_string: str,
        *,
        yield_node: str = "node",
        yield_score: str = "score",
        dialect: str = "cypher",
    ) -> Any:
        """Starts a fluent Query full-text search against this relationship model's full-text index.

        Note:
            Model-level search methods default to ``dialect='cypher'``. When executing via an active
            session against Memgraph or FalkorDB, prefer ``session.text_search(RelModel, ...)``, which
            automatically inherits the session's active dialect, or pass ``dialect='memgraph'`` /
            ``dialect='falkordb'`` explicitly.

        Args:
            query_string: Full-text search term or expression (e.g. 'search phrase').
            yield_node: Yielded variable alias for the matched relationship entity (default: 'node').
            yield_score: Yielded variable alias for the relevance score (default: 'score').
            dialect: Query dialect ('cypher', 'memgraph', 'falkordb', etc.). Defaults to 'cypher'.

        Returns:
            Fluent Query instance chained with the search procedure and yield clause.
        """
        from voyager_ogm.query import Query

        return Query.fulltext_search(
            cls,
            query_string,
            yield_node=yield_node,
            yield_score=yield_score,
            dialect=dialect,
        )

    text_search = fulltext_search

    def __init__(self, alias: str | None = None, **values: Any) -> None:
        """Instantiates a Relationship entity with a unique query alias.

        Args:
            alias: Optional explicit variable alias. If omitted, an auto-alias
                (e.g. `_acted_in_0`) is generated.
            **values: Edge property key-value pairs.
        """
        cached_type = getattr(self.__class__, "_cached_type", None)
        rel_type = (
            getattr(self, "__edge_type__", None)
            or cached_type
            or self.__type__
            or self.__class__.__name__.upper()
        )
        if alias is None:
            self._alias = _get_next_alias(rel_type)
        else:
            self._alias = alias
        auto_alias = alias is None
        self._cached_alias = self._alias
        self._bound_fields: dict[str, BoundField] = {}
        hydrated_values: dict[str, Any] = {}
        schema = getattr(self, "_schema_fields", {})
        for k, v in values.items():
            hydrated_values[k] = _hydrate_field_value(schema.get(k), v)
        self._defaulted_fields = _apply_field_defaults(hydrated_values, schema)
        for k, v in hydrated_values.items():
            field_desc = schema.get(k)
            if field_desc is not None:
                field_desc.validate_value(v, self.__class__)
        self._values = hydrated_values
        if auto_alias:
            _auto_aliased_relationships.add(self)

    def _validate_for_save(self) -> None:
        """Validates all schema fields on this relationship before saving or flushing."""
        schema = getattr(self, "_schema_fields", {})
        for name, field_desc in schema.items():
            if name in self._values:
                val = self._values[name]
                field_desc.validate_value(val, self.__class__)

    @property
    def alias(self) -> str:
        """Returns the variable alias assigned to this relationship instance."""
        return self._alias

    @property
    def _alias_is_explicit(self) -> bool:
        """Returns True when the caller supplied the alias instead of auto-generation."""
        return self not in _auto_aliased_relationships

    @property
    def edge_type(self) -> str:
        """Returns the relationship type name (e.g. 'FOLLOWS', 'ACTED_IN')."""
        return (
            getattr(self, "__edge_type__", None) or self.__type__ or self.__class__.__name__.upper()
        )

    def get(self, name: str, default: Any = None) -> Any:
        """Retrieves an in-memory property value."""
        return self._values.get(name, default)

    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith("_"):
            super().__setattr__(name, value)
        else:
            if not hasattr(self, "_values"):
                self._values = {}
            if not hasattr(self, "_defaulted_fields"):
                self._defaulted_fields = set()
            if hasattr(self, "_schema_fields") and name in self._schema_fields:
                field_desc = self._schema_fields[name]
                value = _hydrate_field_value(field_desc, value)
                field_desc.validate_value(value, self.__class__)
            self._values[name] = value
            self._defaulted_fields.discard(name)

    def __getattr__(self, name: str) -> BoundField:
        """Dynamically resolves unknown edge property names into BoundField descriptors.

        Args:
            name: Property name.

        Returns:
            BoundField bound to this edge's alias and property name.

        Raises:
            AttributeError: If name starts with an underscore or is not declared in schema.
        """
        if name.startswith("_"):
            raise AttributeError(name)
        if self._schema_fields:
            if name not in self._schema_fields:
                matches = difflib.get_close_matches(
                    name, self._schema_fields.keys(), n=1, cutoff=0.6
                )
                suggestion = f". Did you mean '{matches[0]}'?" if matches else ""
                raise AttributeError(
                    f"'{type(self).__name__}' has no property '{name}'{suggestion}"
                )
        if name not in self._bound_fields:
            self._bound_fields[name] = BoundField(self._alias, name)
        return self._bound_fields[name]


@dataclass_transform(field_specifiers=(Field,))
def node(target: type | str | list[str] | None = None, **kwargs: Any) -> Any:
    """Decorator to mark a Python class as a Voyager Graph Node.

    Automatically injects `Node` inheritance, extracts Python type annotations,
    and sets up schema reflection.

    Args:
        target: Target class when used as `@node`, or primary label/labels when used as `@node("Person")` or `@node(["User", "Admin"])`.
        **kwargs: Optional configuration parameters, supporting `label="Person"` or `labels=["User", "Admin"]`.

    Returns:
        The decorated Node class with schema descriptors and auto-aliasing.

    Note:
        An explicit decorator label (``@node("Person")``, ``label=...`` or
        ``labels=...``) wins over a class-body ``__label__`` or ``__labels__``
        declaration because it is explicit at the use site. Without an explicit
        decorator label, a class-body ``__label__ = "X"`` is honoured as the
        single-label form of ``__labels__ = ["X"]``.

    Example:
        >>> @node
        ... class User:
        ...     name: str
        ...     age: int
        >>> @node(labels=["User", "Admin"])
        ... class Administrator:
        ...     role: str
        >>> @node
        ... class Film:
        ...     __label__ = "Movie"
        ...     title: str
    """
    raw_label = kwargs.get(
        "label",
        kwargs.get("labels", target if isinstance(target, (str, list)) else None),
    )
    explicit_labels = (
        [raw_label]
        if isinstance(raw_label, str)
        else (list(raw_label) if isinstance(raw_label, list) else None)
    )

    def decorator(cls: type) -> type:
        declared_labels = _resolve_declared_labels(cls)
        cls_labels = explicit_labels or declared_labels or [cls.__name__]

        fields_map = _process_type_annotations(cls)

        if not issubclass(cls, Node):
            ns = dict(cls.__dict__)
            if explicit_labels:
                # The decorator label wins over the class-body alias.
                ns.pop("__label__", None)
            ns["__labels__"] = cls_labels
            ns["_cached_labels"] = list(cls_labels)
            ns["_cached_label"] = cls_labels[0] if cls_labels else cls.__name__
            ns["_schema_fields"] = fields_map
            derived = type(cls.__name__, (cls, Node), ns)
            for f in fields_map.values():
                f._owner = derived
            cast(Any, derived).register_schema()
            return derived
        else:
            if explicit_labels and "__label__" in cls.__dict__:
                # The decorator label wins over the class-body alias.
                delattr(cls, "__label__")
            cls.__labels__ = cls_labels  # type: ignore[attr-defined]
            cls._cached_labels = list(cls_labels)  # type: ignore[attr-defined]
            cls._cached_label = cls_labels[0] if cls_labels else cls.__name__  # type: ignore[attr-defined]
            cls._schema_fields = fields_map  # type: ignore[attr-defined]
            cast(Any, cls).register_schema()
            return cls

    if isinstance(target, type):
        return decorator(target)
    return decorator


@dataclass_transform(field_specifiers=(Field,))
def relationship(target: type | str | None = None, **kwargs: Any) -> Any:
    """Decorator to mark a Python class as a Voyager Graph Relationship.

    Automatically injects `Relationship` inheritance, extracts type hints,
    and configures edge direction and type reflection.

    Args:
        target: Target class when used as `@relationship` or type_name string.
        **kwargs: Optional configuration parameters (e.g. `type_name="WORKS_AT"`).

    Returns:
        The decorated Relationship class with schema descriptors.

    Note:
        An explicit decorator type (``@relationship("FOLLOWS")``,
        ``type_name=...``, ``type_=...`` or ``type=...``) wins over a class-body
        ``__type__`` declaration because it is explicit at the use site.

    Example:
        >>> @relationship(type_name="FOLLOWS")
        ... class Follows:
        ...     since: int = 2024
        >>> @relationship
        ... class Directed:
        ...     __type__ = "DIRECTED"
        ...     since: int = 2024
    """
    direction = kwargs.get("direction", "outgoing")
    explicit_type = (
        kwargs.get("type_name")
        or kwargs.get("type_")
        or kwargs.get("type")
        or (target if isinstance(target, str) else None)
    )

    def decorator(cls: type) -> type:
        declared_type = _resolve_declared_type(cls)
        rel_type = explicit_type or declared_type or cls.__name__.upper()

        fields_map = _process_type_annotations(cls)
        source_labels = kwargs.get("source_labels") or kwargs.get("from_labels")
        if not source_labels and "source_node" in kwargs:
            src = kwargs["source_node"]
            source_labels = getattr(src, "__labels__", [getattr(src, "__name__", str(src))])
        target_labels = kwargs.get("target_labels") or kwargs.get("to_labels")
        if not target_labels and "target_node" in kwargs:
            tgt = kwargs["target_node"]
            target_labels = getattr(tgt, "__labels__", [getattr(tgt, "__name__", str(tgt))])
        src_list = list(source_labels) if source_labels else []
        tgt_list = list(target_labels) if target_labels else []

        if not issubclass(cls, Relationship):
            ns = dict(cls.__dict__)
            ns["__type__"] = rel_type
            ns["__direction__"] = direction
            ns["__source_labels__"] = src_list
            ns["__target_labels__"] = tgt_list
            ns["_cached_type"] = rel_type
            ns["_cached_types"] = [rel_type]
            ns["_schema_fields"] = fields_map
            derived = type(cls.__name__, (cls, Relationship), ns)
            for f in fields_map.values():
                f._owner = derived
            cast(Any, derived).register_schema()
            return derived
        else:
            cls.__type__ = rel_type  # type: ignore[attr-defined]
            cls.__direction__ = direction  # type: ignore[attr-defined]
            cls.__source_labels__ = src_list  # type: ignore[attr-defined]
            cls.__target_labels__ = tgt_list  # type: ignore[attr-defined]
            cls._cached_type = rel_type  # type: ignore[attr-defined]
            cls._cached_types = [rel_type]  # type: ignore[attr-defined]
            cls._schema_fields = fields_map  # type: ignore[attr-defined]
            cast(Any, cls).register_schema()
            return cls

    if isinstance(target, type):
        return decorator(target)
    return decorator
