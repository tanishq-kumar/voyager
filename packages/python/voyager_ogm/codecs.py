"""Shared Arrow/Polars codecs and parameter pre-normalization helpers.

This module owns the one recursive Arrow-to-Polars normalizer used by every
bridge export path, plus the value normalization hooks shared by the driver
adapters. Keeping them here is what makes transaction and non-transaction
execution behave identically.
"""

from __future__ import annotations

import uuid
import warnings
from collections.abc import Mapping, Sequence
from typing import Any

import polars as pl

from voyager_ogm._voyager_rs import (
    ARROW_EXTENSION_NAME,
    ARROW_UUID_EXTENSION,
    META_ENCODING_JSON,
    META_TYPE_DURATION,
    VOYAGER_META_ENCODING,
    VOYAGER_META_TYPE,
)
from voyager_ogm.entities import serialize_records_for_arrow
from voyager_ogm.types import Duration, Json, Point, coerce_point_value

_VOYAGER_META_TYPE = VOYAGER_META_TYPE.encode()
_VOYAGER_META_ENCODING = VOYAGER_META_ENCODING.encode()
_ARROW_EXTENSION_NAME = ARROW_EXTENSION_NAME.encode()
_ARROW_UUID_EXTENSION = ARROW_UUID_EXTENSION.encode()
_META_TYPE_DURATION = META_TYPE_DURATION.encode()


def mdn_to_duration(val: Any) -> Duration:
    """Converts a PyArrow ``MonthDayNano`` value to an exact :class:`Duration`."""
    total_ns = int(val.nanoseconds)
    sign = -1 if total_ns < 0 else 1
    abs_ns = abs(total_ns)
    secs = sign * (abs_ns // 1_000_000_000)
    ns = sign * (abs_ns % 1_000_000_000)
    return Duration(
        months=int(val.months),
        days=int(val.days),
        seconds=secs,
        nanoseconds=ns,
    )


def is_month_day_nano_interval(arrow_type: Any) -> bool:
    """Returns whether ``arrow_type`` is PyArrow's ``month_day_nano_interval``."""
    import pyarrow as pa

    return bool(arrow_type == pa.month_day_nano_interval())


def has_month_day_nano_interval(arrow_type: Any) -> bool:
    """Returns whether ``arrow_type`` is or contains ``month_day_nano_interval``."""
    import pyarrow as pa

    if is_month_day_nano_interval(arrow_type):
        return True
    if (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        return has_month_day_nano_interval(arrow_type.value_type)
    if pa.types.is_struct(arrow_type) or pa.types.is_union(arrow_type):
        return any(has_month_day_nano_interval(f.type) for f in arrow_type)
    if pa.types.is_map(arrow_type):
        return has_month_day_nano_interval(arrow_type.key_type) or has_month_day_nano_interval(
            arrow_type.item_type
        )
    return False


def has_union(arrow_type: Any) -> bool:
    """Returns whether ``arrow_type`` is or contains an Arrow union."""
    import pyarrow as pa

    if pa.types.is_union(arrow_type):
        return True
    if (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        return has_union(arrow_type.value_type)
    if pa.types.is_struct(arrow_type):
        return any(has_union(f.type) for f in arrow_type)
    if pa.types.is_map(arrow_type):
        return has_union(arrow_type.key_type) or has_union(arrow_type.item_type)
    return False


def union_field_names(fields: Sequence[Any]) -> list[str]:
    """Returns unique, non-empty struct field names for a union's members."""
    used: set[str] = set()
    names: list[str] = []
    for idx, field in enumerate(fields):
        base = str(field.name) or f"member_{idx}"
        candidate = base
        suffix = 2
        while candidate in used:
            candidate = f"{base}_{suffix}"
            suffix += 1
        used.add(candidate)
        names.append(candidate)
    return names


def union_tag_name(names: Sequence[str]) -> str:
    """Returns a ``tag`` field name that does not collide with a union member."""
    candidate = "tag"
    suffix = 2
    while candidate in names:
        candidate = f"tag_{suffix}"
        suffix += 1
    return candidate


def union_tag_name_for_type(arrow_type: Any) -> str:
    """Returns the tag field name used for the first union found in ``arrow_type``."""
    import pyarrow as pa

    if pa.types.is_union(arrow_type):
        return union_tag_name(union_field_names(list(arrow_type)))
    if (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        return union_tag_name_for_type(arrow_type.value_type)
    if pa.types.is_struct(arrow_type):
        for field in arrow_type:
            if has_union(field.type):
                return union_tag_name_for_type(field.type)
    if pa.types.is_map(arrow_type):
        if has_union(arrow_type.key_type):
            return union_tag_name_for_type(arrow_type.key_type)
        return union_tag_name_for_type(arrow_type.item_type)
    return "tag"


def union_to_struct(arr: Any) -> Any:
    """Converts an Arrow union array to a struct with one field per member plus ``tag``."""
    import pyarrow as pa

    union_type = arr.type
    fields = list(union_type)
    names = union_field_names(fields)
    tag_name = union_tag_name(names)
    codes = arr.type_codes.to_pylist()
    code_to_name = {
        int(code): name for code, name in zip(union_type.type_codes, names, strict=True)
    }
    tag_array = pa.array([code_to_name[int(code)] for code in codes], type=pa.string())
    num_rows = len(arr)
    children = []
    for idx, code in enumerate(union_type.type_codes):
        selected_code = int(code)
        mask = [value == selected_code for value in codes]
        raw_child = arr.field(idx)
        if union_type.mode == "dense":
            selected = raw_child
            positions = arr.offsets.to_pylist()
        else:
            # Sparse union children are full length and their unselected slots are
            # undefined, so filter before recursing (a nested union's unused slots
            # may not be valid) and scatter the converted values back by position.
            selected = raw_child.filter(pa.array(mask))
            positions = []
            offset = 0
            for selected_row in mask:
                if selected_row:
                    positions.append(offset)
                    offset += 1
                else:
                    positions.append(-1)
        converted = convert_unions(selected)
        if num_rows == 0:
            full_child = pa.nulls(0, type=converted.type)
        else:
            indices = pa.array(
                [
                    int(position) if selected_row else None
                    for position, selected_row in zip(positions, mask, strict=True)
                ],
                type=pa.int64(),
            )
            full_child = converted.take(indices)
        children.append(full_child)
    return pa.StructArray.from_arrays(
        [*children, tag_array],
        names=[*names, tag_name],
    )


def convert_unions(arr: Any) -> Any:
    """Recursively replaces Arrow union arrays with tagged struct arrays."""
    import pyarrow as pa

    arrow_type = arr.type
    if pa.types.is_union(arrow_type):
        return union_to_struct(arr)
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        values = convert_unions(arr.values)
        if values.type == arr.values.type:
            return arr
        if pa.types.is_list(arrow_type):
            return pa.ListArray.from_arrays(arr.offsets, values, mask=arr.is_null())
        return pa.LargeListArray.from_arrays(arr.offsets, values, mask=arr.is_null())
    if pa.types.is_fixed_size_list(arrow_type):
        values = convert_unions(arr.values)
        if values.type == arr.values.type:
            return arr
        return pa.FixedSizeListArray.from_arrays(values, arrow_type.list_size, mask=arr.is_null())
    if pa.types.is_struct(arrow_type):
        if not any(has_union(field.type) for field in arrow_type):
            return arr
        children = [convert_unions(arr.field(idx)) for idx in range(arrow_type.num_fields)]
        return pa.StructArray.from_arrays(
            children,
            names=[field.name for field in arrow_type],
            mask=arr.is_null(),
        )
    if pa.types.is_map(arrow_type):
        if not (has_union(arrow_type.key_type) or has_union(arrow_type.item_type)):
            return arr
        keys = convert_unions(arr.keys)
        items = convert_unions(arr.items)
        return pa.MapArray.from_arrays(arr.offsets, keys, items, mask=arr.is_null())
    return arr


def arrow_type_without_unions(arrow_type: Any) -> Any:
    """Returns the Arrow type produced by replacing every union with a tagged struct."""
    import pyarrow as pa

    if pa.types.is_union(arrow_type):
        fields = list(arrow_type)
        names = union_field_names(fields)
        converted = [
            pa.field(name, arrow_type_without_unions(field.type), nullable=field.nullable)
            for name, field in zip(names, fields, strict=True)
        ]
        converted.append(pa.field(union_tag_name(names), pa.string()))
        return pa.struct(converted)
    if pa.types.is_list(arrow_type):
        return pa.list_(arrow_type_without_unions(arrow_type.value_type))
    if pa.types.is_large_list(arrow_type):
        return pa.large_list(arrow_type_without_unions(arrow_type.value_type))
    if pa.types.is_fixed_size_list(arrow_type):
        return pa.list_(arrow_type_without_unions(arrow_type.value_type), arrow_type.list_size)
    if pa.types.is_struct(arrow_type):
        return pa.struct(
            [
                pa.field(
                    field.name,
                    arrow_type_without_unions(field.type),
                    nullable=field.nullable,
                    metadata=field.metadata,
                )
                for field in arrow_type
            ]
        )
    if pa.types.is_map(arrow_type):
        return pa.map_(
            arrow_type_without_unions(arrow_type.key_type),
            arrow_type_without_unions(arrow_type.item_type),
        )
    return arrow_type


def column_without_unions(column: Any) -> Any:
    """Converts every union in an Arrow column (or ChunkedArray), preserving empty schemas."""
    import pyarrow as pa

    if isinstance(column, pa.ChunkedArray):
        chunks = [convert_unions(chunk) for chunk in column.chunks]
        if not chunks:
            return pa.chunked_array([], type=arrow_type_without_unions(column.type))
        return pa.chunked_array(chunks, type=chunks[0].type)
    return convert_unions(column)


def mdn_column_to_arrow(col: Any) -> Any:
    """Converts a scalar ``month_day_nano_interval`` Arrow column to Polars-safe Arrow."""
    import pyarrow as pa

    values = col.to_pylist()
    can_duration_us = True
    can_duration_ns = True
    for val in values:
        if val is None:
            continue
        if val.months != 0 or val.days != 0:
            can_duration_us = False
            can_duration_ns = False
            break
        ns = int(val.nanoseconds)
        if ns % 1_000 != 0 or not (-(2**63) <= ns // 1_000 < 2**63):
            can_duration_us = False
        if not (-(2**63) <= ns < 2**63):
            can_duration_ns = False

    if can_duration_us:
        converted = [None if v is None else int(v.nanoseconds) // 1_000 for v in values]
        return pa.array(converted, type=pa.duration("us"))
    if can_duration_ns:
        converted = [None if v is None else int(v.nanoseconds) for v in values]
        return pa.array(converted, type=pa.duration("ns"))

    struct_type = pa.struct(
        [
            pa.field("months", pa.int64(), nullable=False),
            pa.field("days", pa.int64(), nullable=False),
            pa.field("seconds", pa.int64(), nullable=False),
            pa.field("nanoseconds", pa.int64(), nullable=False),
        ]
    )
    struct_vals: list[dict[str, int] | None] = []
    for v in values:
        if v is None:
            struct_vals.append(None)
        else:
            dur = mdn_to_duration(v)
            struct_vals.append(
                {
                    "months": dur.months,
                    "days": dur.days,
                    "seconds": dur.seconds,
                    "nanoseconds": dur.nanoseconds,
                }
            )
    return pa.array(struct_vals, type=struct_type)


def _ensure_polars_df(val: Any) -> pl.DataFrame:
    """Narrows ``pl.from_arrow(...)`` to a ``pl.DataFrame``."""
    if isinstance(val, pl.DataFrame):
        return val
    return val.to_frame()


def _convert_mdn_array(arr: Any) -> Any:
    """Recursively replaces ``month_day_nano_interval`` arrays with Polars-safe types."""
    import pyarrow as pa

    arrow_type = arr.type
    if is_month_day_nano_interval(arrow_type):
        return mdn_column_to_arrow(arr)
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        values = _convert_mdn_array(arr.values)
        if values.type == arr.values.type:
            return arr
        value_field = pa.field(
            arrow_type.value_field.name,
            values.type,
            nullable=arrow_type.value_field.nullable,
            metadata=arrow_type.value_field.metadata,
        )
        if pa.types.is_list(arrow_type):
            return pa.ListArray.from_arrays(
                arr.offsets, values, type=pa.list_(value_field), mask=arr.is_null()
            )
        return pa.LargeListArray.from_arrays(
            arr.offsets, values, type=pa.large_list(value_field), mask=arr.is_null()
        )
    if pa.types.is_fixed_size_list(arrow_type):
        values = _convert_mdn_array(arr.values)
        if values.type == arr.values.type:
            return arr
        value_field = pa.field(
            arrow_type.value_field.name,
            values.type,
            nullable=arrow_type.value_field.nullable,
            metadata=arrow_type.value_field.metadata,
        )
        return pa.FixedSizeListArray.from_arrays(
            values, type=pa.list_(value_field, arrow_type.list_size), mask=arr.is_null()
        )
    if pa.types.is_struct(arrow_type):
        if not any(has_month_day_nano_interval(field.type) for field in arrow_type):
            return arr
        children = [_convert_mdn_array(arr.field(idx)) for idx in range(arrow_type.num_fields)]
        fields = [
            pa.field(
                field.name,
                child.type,
                nullable=field.nullable,
                metadata=field.metadata,
            )
            for field, child in zip(arrow_type, children, strict=True)
        ]
        return pa.StructArray.from_arrays(children, fields=fields, mask=arr.is_null())
    if pa.types.is_map(arrow_type):
        if not (
            has_month_day_nano_interval(arrow_type.key_type)
            or has_month_day_nano_interval(arrow_type.item_type)
        ):
            return arr
        keys = _convert_mdn_array(arr.keys)
        items = _convert_mdn_array(arr.items)
        key_field = pa.field(
            arrow_type.key_field.name,
            keys.type,
            nullable=arrow_type.key_field.nullable,
            metadata=arrow_type.key_field.metadata,
        )
        item_field = pa.field(
            arrow_type.item_field.name,
            items.type,
            nullable=arrow_type.item_field.nullable,
            metadata=arrow_type.item_field.metadata,
        )
        return pa.MapArray.from_arrays(
            arr.offsets,
            keys,
            items,
            type=pa.map_(key_field, item_field),
            mask=arr.is_null(),
        )
    return arr


def _convert_mdn_column(column: Any) -> Any:
    """Converts ``month_day_nano_interval`` in an Arrow array or ``ChunkedArray``."""
    import pyarrow as pa

    if isinstance(column, pa.ChunkedArray):
        if column.num_chunks == 0:
            empty = _convert_mdn_array(pa.array([], type=column.type))
            return pa.chunked_array([], type=empty.type)
        chunks = [_convert_mdn_array(chunk) for chunk in column.chunks]
        return pa.chunked_array(chunks, type=chunks[0].type)
    return _convert_mdn_array(column)


def arrow_table_to_polars(table: Any) -> pl.DataFrame:
    """Converts a PyArrow table to Polars, normalizing incompatible Arrow types once.

    Arrow ``UNION`` columns become a tagged ``Struct`` with one field per member
    plus a ``tag`` string naming the active member, with a ``UserWarning`` per
    converted column. ``month_day_nano_interval`` columns (top-level or nested)
    become ``Duration``/``Struct`` values Polars can import. Tables without
    either type take the direct zero-copy path.
    """
    import pyarrow as pa

    schema = table.schema
    needs_conversion = any(
        has_union(field.type) or has_month_day_nano_interval(field.type) for field in schema
    )
    if not needs_conversion:
        return _ensure_polars_df(pl.from_arrow(table))

    arrays: list[Any] = []
    fields: list[Any] = []
    for idx, field in enumerate(schema):
        column = table.column(idx)
        field_type = field.type
        metadata = field.metadata
        if has_union(field_type):
            warnings.warn(
                f"Arrow UNION column {field.name!r} was converted to a struct with a "
                f"{union_tag_name_for_type(field_type)!r} field because Polars cannot "
                "import Arrow union types.",
                UserWarning,
                stacklevel=2,
            )
            column = column_without_unions(column)
            field_type = column.type
            metadata = None
        if has_month_day_nano_interval(field_type):
            column = _convert_mdn_column(column)
            field_type = column.type
            if pa.types.is_struct(field_type):
                meta = dict(metadata) if metadata else {}
                meta[_VOYAGER_META_TYPE] = _META_TYPE_DURATION
                metadata = meta
        arrays.append(column)
        fields.append(pa.field(field.name, field_type, nullable=field.nullable, metadata=metadata))
    return _ensure_polars_df(pl.from_arrow(pa.Table.from_arrays(arrays, schema=pa.schema(fields))))


def records_to_polars(raw_records: Sequence[dict[str, Any]]) -> pl.DataFrame:
    """Converts bridge records (preserving empty-result ``.columns``) to a Polars DataFrame."""
    columns = getattr(raw_records, "columns", None)
    records = serialize_records_for_arrow(list(raw_records))
    if records:
        return pl.DataFrame(records)
    if columns:
        return pl.DataFrame(schema=list(columns))
    return pl.DataFrame()


def field_metadata(field: Any) -> dict[str, str]:
    """Returns a field's decoded string metadata, ignoring unreadable entries."""
    raw = getattr(field, "metadata", None) or {}
    decoded: dict[str, str] = {}
    for key, value in raw.items():
        try:
            decoded[key.decode() if isinstance(key, bytes) else str(key)] = (
                value.decode() if isinstance(value, bytes) else str(value)
            )
        except UnicodeDecodeError:
            continue
    return decoded


def voyager_type_of(field: Any) -> str | None:
    """Returns the ``voyager.type`` annotation of an Arrow field, if any."""
    return field_metadata(field).get(VOYAGER_META_TYPE)


def is_json_encoded(field: Any) -> bool:
    """Returns whether an Arrow field is tagged ``voyager.encoding=json``."""
    return field_metadata(field).get(VOYAGER_META_ENCODING) == META_ENCODING_JSON


def has_uuid_extension(field: Any) -> bool:
    """Returns whether an Arrow field carries the ``arrow.uuid`` extension tag."""
    return field_metadata(field).get(ARROW_EXTENSION_NAME) == ARROW_UUID_EXTENSION


def unwrap_json_marker(value: Any) -> tuple[bool, Any]:
    """Returns ``(was_explicit, unwrapped)`` after recursively removing ``Json`` wrappers."""
    if isinstance(value, Json):
        return True, unwrap_json_marker(value.value)[1]
    if isinstance(value, Mapping):
        return False, {k: unwrap_json_marker(v)[1] for k, v in value.items()}
    if isinstance(value, tuple):
        return False, tuple(unwrap_json_marker(v)[1] for v in value)
    if isinstance(value, list):
        return False, [unwrap_json_marker(v)[1] for v in value]
    return False, value


def strip_json_markers(value: Any) -> Any:
    """Recursively removes ``Json`` wrappers, returning the contained values."""
    return unwrap_json_marker(value)[1]


def normalize_uuid(value: Any) -> Any:
    """Returns the canonical text form of a ``uuid.UUID``, leaving other values alone."""
    if isinstance(value, uuid.UUID):
        return str(value)
    return value


def uuid_bytes(value: Any) -> Any:
    """Converts a UUID or UUID string (recursively) to its 16-byte representation."""
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        return [uuid_bytes(item) for item in value]
    if isinstance(value, bytes):
        return value
    if isinstance(value, uuid.UUID):
        return value.bytes
    return uuid.UUID(str(value)).bytes


def zoneinfo_key(value: Any) -> str | None:
    """Returns a ``zoneinfo.ZoneInfo`` key attached to a datetime, if present."""
    key = getattr(getattr(value, "tzinfo", None), "key", None)
    return key if isinstance(key, str) else None


def coerce_point(value: Any) -> Point | None:
    """Returns a :class:`voyager_ogm.Point` for a spatial value, otherwise ``None``."""
    if isinstance(value, Point):
        return value
    try:
        return coerce_point_value(value)
    except (TypeError, ValueError):
        return None


def point_coordinate_dict(value: Any) -> dict[str, Any] | None:
    """Returns the canonical ``Point`` coordinate dictionary for a spatial value."""
    point = coerce_point(value)
    return point.to_coordinate_dict() if point is not None else None


def point_param_map(value: Any) -> dict[str, Any] | None:
    """Returns the openCypher point map used to bind a spatial parameter."""
    point = coerce_point(value)
    return point.to_cypher_dict() if point is not None else None
