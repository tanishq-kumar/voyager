"""DuckDB-specific Arrow canonicalization and row-to-Arrow helpers."""

from __future__ import annotations

import datetime
import json
import re
from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import Any

from voyager_ogm._voyager_rs import (
    ARROW_EXTENSION_NAME,
    ARROW_UUID_EXTENSION,
    VOYAGER_META_ENCODING,
    VOYAGER_META_TYPE,
)
from voyager_ogm.codecs import (
    has_month_day_nano_interval,
    is_month_day_nano_interval,
    mdn_column_to_arrow,
    uuid_bytes,
)
from voyager_ogm.entities import json_default

_VOYAGER_META_ENCODING = VOYAGER_META_ENCODING.encode()
_VOYAGER_META_TYPE = VOYAGER_META_TYPE.encode()
_ARROW_EXTENSION_NAME = ARROW_EXTENSION_NAME.encode()
_ARROW_UUID_EXTENSION = ARROW_UUID_EXTENSION.encode()


_DUCKDB_TIMETZ_TYPE_IDS = frozenset({"time with time zone"})
_DUCKDB_INTERVAL_TYPE_IDS = frozenset({"interval", "timedelta"})
_DUCKDB_UUID_TYPE_IDS = frozenset({"uuid"})
_DUCKDB_JSON_TYPE_IDS = frozenset({"json"})
_DUCKDB_ARROW_CANONICAL_TYPE_IDS = (
    _DUCKDB_TIMETZ_TYPE_IDS | _DUCKDB_UUID_TYPE_IDS | _DUCKDB_JSON_TYPE_IDS
)


def _duckdb_type_id(dtype: Any) -> str:
    """Returns the normalized DuckDB type identifier for a type object or string."""
    raw_id = getattr(dtype, "id", None)
    if raw_id is not None:
        # DuckDB's JSON logical type reports id='varchar' while str(dtype) == 'JSON'.
        if str(dtype).strip().upper() == "JSON":
            return "json"
        return str(raw_id).lower()
    text = str(dtype).strip().upper()
    for prefix in (
        "TIME WITH TIME ZONE",
        "UHUGEINT",
        "HUGEINT",
        "INTERVAL",
        "TIMEDELTA",
        "UUID",
        "JSON",
    ):
        if text == prefix or text.startswith((f"{prefix}[", f"{prefix}(")):
            return prefix.lower()
    return text.lower()


def _duckdb_type_children_map(dtype: Any, expected_id: str) -> dict[str, Any]:
    """Returns a ``name -> child_type`` mapping for nested DuckDB types matching ``expected_id``."""
    if _duckdb_type_id(dtype) == expected_id and hasattr(dtype, "children"):
        children: Sequence[tuple[Any, Any]] = dtype.children
        return {str(name): child for name, child in children}
    return {}


def _duckdb_type_has(dtype: Any, target_ids: frozenset[str]) -> bool:
    """Returns whether ``dtype`` is or recursively contains any type in ``target_ids``."""
    tid = _duckdb_type_id(dtype)
    if tid in target_ids:
        return True
    if hasattr(dtype, "id"):
        if tid in ("list", "array", "struct", "map", "union"):
            for name, child in dtype.children:
                if tid == "array" and name == "size":
                    continue
                if tid == "union" and not name:
                    continue
                if _duckdb_type_has(child, target_ids):
                    return True
        return False
    upper = str(dtype).upper()
    return any(target.upper() in upper for target in target_ids)


def _duckdb_description_has_timetz(description: Sequence[Any]) -> bool:
    """Returns whether a DuckDB cursor description contains ``TIMETZ`` anywhere."""
    return any(
        len(item) > 1 and _duckdb_type_has(item[1], _DUCKDB_TIMETZ_TYPE_IDS) for item in description
    )


def _duckdb_description_has_interval(description: Sequence[Any]) -> bool:
    """Returns whether a DuckDB cursor description contains ``INTERVAL`` anywhere."""
    return any(
        len(item) > 1 and _duckdb_type_has(item[1], _DUCKDB_INTERVAL_TYPE_IDS)
        for item in description
    )


def _duckdb_type_has_union_json(dtype: Any) -> bool:
    """Returns whether ``dtype`` contains a ``UNION`` with a ``JSON`` member anywhere."""
    tid = _duckdb_type_id(dtype)
    if not hasattr(dtype, "id"):
        upper = str(dtype).upper()
        return "UNION" in upper and "JSON" in upper
    if tid == "union":
        return any(
            name and _duckdb_type_has(child, _DUCKDB_JSON_TYPE_IDS)
            for name, child in dtype.children
        )
    if tid in ("list", "array", "struct", "map"):
        return any(
            not (tid == "array" and name == "size") and _duckdb_type_has_union_json(child)
            for name, child in dtype.children
        )
    return False


def _duckdb_description_has_union_json(description: Sequence[Any]) -> bool:
    """Returns whether a DuckDB cursor description contains a ``UNION`` with ``JSON``."""
    return any(len(item) > 1 and _duckdb_type_has_union_json(item[1]) for item in description)


def _duckdb_quote_identifier(name: str) -> str:
    """Quotes a DuckDB identifier, doubling embedded double quotes."""
    return '"' + name.replace('"', '""') + '"'


def _duckdb_timetz_cast_sql(dtype: Any) -> str:
    """Builds a DuckDB SQL type replacing nested ``TIME WITH TIME ZONE`` with ``VARCHAR``."""
    tid = _duckdb_type_id(dtype)
    if tid == "time with time zone":
        return "VARCHAR"
    if hasattr(dtype, "id"):
        if tid == "list":
            return f"{_duckdb_timetz_cast_sql(dtype.children[0][1])}[]"
        if tid == "array":
            children = dict(dtype.children)
            return f"{_duckdb_timetz_cast_sql(children['child'])}[{children['size']}]"
        if tid == "struct":
            fields = ", ".join(
                f"{_duckdb_quote_identifier(str(name))} {_duckdb_timetz_cast_sql(child)}"
                for name, child in dtype.children
            )
            return f"STRUCT({fields})"
        if tid == "map":
            children = dict(dtype.children)
            return (
                f"MAP({_duckdb_timetz_cast_sql(children['key'])}, "
                f"{_duckdb_timetz_cast_sql(children['value'])})"
            )
        if tid == "union":
            fields = ", ".join(
                f"{_duckdb_quote_identifier(str(name))} {_duckdb_timetz_cast_sql(child)}"
                for name, child in dtype.children
                if name
            )
            return f"UNION({fields})"
        return str(dtype)
    upper = str(dtype).upper()
    depth = upper.count("[")
    return "VARCHAR" + "[]" * depth


def _duckdb_timetz_projection(description: Sequence[Any]) -> str | None:
    """Builds a projection that casts top-level or nested ``TIMETZ`` values to ISO text.

    DuckDB's Arrow export maps ``TIME WITH TIME ZONE`` to a plain ``time64`` and
    loses the offset, while its DB-API rows fold calendar ``INTERVAL`` months
    into 30-day ``timedelta`` values. Projecting only columns containing ``TIMETZ``
    to ``VARCHAR`` (preserving outer ``LIST``/``ARRAY``/``STRUCT``/``MAP``/``UNION``
    structure) keeps Arrow's ``month_day_nano_interval`` precision everywhere.
    """
    projections: list[str] = []
    has_timetz = False
    for idx, item in enumerate(description):
        name = str(item[0])
        dtype = item[1] if len(item) > 1 else ""
        alias = _duckdb_quote_identifier(name)
        if _duckdb_type_has(dtype, _DUCKDB_TIMETZ_TYPE_IDS):
            has_timetz = True
            target_type = _duckdb_timetz_cast_sql(dtype)
            projections.append(f"CAST(#{idx + 1} AS {target_type}) AS {alias}")
        else:
            projections.append(f"#{idx + 1} AS {alias}")
    if not has_timetz:
        return None
    return ", ".join(projections)


def _duckdb_relation_description(relation: Any) -> tuple[tuple[str, Any], ...]:
    """Builds a DB-API-like description preserving DuckDB type objects from a relation."""
    return tuple(
        (str(name), type_obj)
        for name, type_obj in zip(relation.columns, relation.types, strict=True)
    )


def _duckdb_complete_pending(connection: Any) -> None:
    """Clears an unfinished DB-API result so a relation can start.

    ``DuckDBPyConnection.sql`` refuses to run while a prior ``execute()`` result
    is still open (for example after a bare ``fetchone()``), whereas ``execute()``
    implicitly replaces it. ``complete()`` gives the relation path the same
    behaviour without executing the statement twice.
    """
    complete = getattr(connection, "complete", None)
    if not callable(complete):
        return
    try:
        complete()
    except Exception:
        pass


_DUCKDB_TIMETZ_TEXT_PATTERN = re.compile(
    r"^(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})(?:\.(?P<fraction>\d{1,6}))?"
    r"(?P<tz>Z|z|(?P<sign>[+-])(?P<hours>\d{2})(?::?(?P<minutes>\d{2})(?::?(?P<seconds>\d{2}))?)?)$"
)


def _normalize_timetz_offset(match: re.Match[str], original: str) -> str:
    """Validates and normalizes a matched ``TIMETZ`` UTC offset to ``±HH:MM[:SS]``."""
    tz_raw = match.group("tz")
    if tz_raw in ("Z", "z"):
        return "+00:00"
    sign = match.group("sign")
    off_h = int(match.group("hours"))
    off_m = int(match.group("minutes") or "0")
    off_s = int(match.group("seconds") or "0")
    if not (0 <= off_h <= 23 and 0 <= off_m <= 59 and 0 <= off_s <= 59):
        raise ValueError(f"DuckDB TIMETZ UTC offset out of range: {original!r}")
    if off_h == 0 and off_m == 0 and off_s == 0:
        return "+00:00"
    if off_s == 0:
        return f"{sign}{off_h:02d}:{off_m:02d}"
    return f"{sign}{off_h:02d}:{off_m:02d}:{off_s:02d}"


def _duckdb_timetz_text_iso(value: Any) -> Any:
    """Normalizes DuckDB's ``TIMETZ`` text to ISO-8601 ``HH:MM:SS[.ffffff]±HH:MM[:SS]`` form.

    Accepts every valid DuckDB ``TIMETZ`` value, including ``24:00:00`` (which
    Python's ``datetime.time`` cannot hold), fractional seconds up to 6 digits,
    ``Z``, and ``±HH[:MM[:SS]]`` / ``±HHMM[SS]`` offsets. Recurses into
    lists/tuples, passes through ``None``, and raises on non-string values or
    text that is not a valid DuckDB ``TIMETZ``.
    """
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        return [_duckdb_timetz_text_iso(item) for item in value]
    if not isinstance(value, str):
        raise TypeError(
            f"Expected DuckDB TIMETZ text string, got {type(value).__name__}: {value!r}"
        )
    match = _DUCKDB_TIMETZ_TEXT_PATTERN.match(value)
    if match is None:
        raise ValueError(f"Unrecognized DuckDB TIMETZ text format: {value!r}")
    hour = int(match.group("hour"))
    minute = int(match.group("minute"))
    second = int(match.group("second"))
    fraction = match.group("fraction")
    offset = _normalize_timetz_offset(match, value)
    if hour == 24:
        # DuckDB allows 24:00:00 (with optional zero fraction) across the full
        # TIMETZ offset range, which Python's datetime.time cannot represent.
        if minute != 0 or second != 0 or (fraction is not None and int(fraction) != 0):
            raise ValueError(f"DuckDB TIMETZ value out of range: {value!r}")
        return f"24:00:00{offset}"
    if not (0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59):
        raise ValueError(f"DuckDB TIMETZ value out of range: {value!r}")
    frac_us = int(fraction.ljust(6, "0")) if fraction is not None else 0
    time_part = f"{hour:02d}:{minute:02d}:{second:02d}"
    if frac_us:
        time_part += f".{frac_us:06d}"
    return f"{time_part}{offset}"


def _parse_duckdb_timetz_python(value: Any) -> Any:
    """Decodes DuckDB ``TIMETZ`` to ``datetime.time``, or ISO ``str`` when unrepresentable."""
    if isinstance(value, datetime.time):
        return value
    iso = _duckdb_timetz_text_iso(value)
    if iso.startswith("24:"):
        # End of day has no datetime.time equivalent. Python 3.14's fromisoformat
        # silently maps "24:00:00" to midnight, so keep the ISO text on every version.
        return iso
    try:
        return datetime.time.fromisoformat(iso)
    except ValueError:
        return iso


def _duckdb_time_iso(value: Any) -> Any:
    """Converts a ``datetime.time`` (or nested container of them) to ISO text."""
    if isinstance(value, str):
        return _duckdb_timetz_text_iso(value)
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        return [_duckdb_time_iso(item) for item in value]
    if isinstance(value, dict):
        return {k: _duckdb_time_iso(v) for k, v in value.items()}
    return value.isoformat()


def _duckdb_uuid_bytes(value: Any) -> Any:
    """Converts a UUID or UUID string to its 16-byte representation."""
    return uuid_bytes(value)


def _duckdb_arrow_field_metadata(
    dtype: Any,
    arrow_type: Any,
    existing: Mapping[bytes, bytes] | None = None,
) -> dict[bytes, bytes] | None:
    """Returns Voyager/Arrow field metadata for a DuckDB column or nested child field."""
    import pyarrow as pa

    metadata = dict(existing) if existing else {}
    tid = _duckdb_type_id(dtype) if dtype else ""
    if tid == "uuid":
        metadata[_VOYAGER_META_TYPE] = b"uuid"
        if pa.types.is_fixed_size_binary(arrow_type):
            # Same extension tag as the native PostgreSQL backend's UUID columns.
            metadata[_ARROW_EXTENSION_NAME] = _ARROW_UUID_EXTENSION
    elif tid == "json":
        metadata[_VOYAGER_META_ENCODING] = b"json"
        metadata[_VOYAGER_META_TYPE] = b"json"
    elif tid == "time with time zone":
        metadata[_VOYAGER_META_TYPE] = b"time_tz"
    elif tid == "hugeint":
        metadata[_VOYAGER_META_TYPE] = b"hugeint"
    elif (
        is_month_day_nano_interval(arrow_type) or tid in _DUCKDB_INTERVAL_TYPE_IDS
    ) and pa.types.is_struct(arrow_type):
        metadata[_VOYAGER_META_TYPE] = b"duration"
    return metadata or None


def _duckdb_canonicalized_child_field(orig_field: Any, arrow_type: Any, dtype: Any) -> Any:
    """Rebuilds a nested Arrow child ``pa.Field`` with Voyager metadata for ``dtype``."""
    import pyarrow as pa

    metadata = _duckdb_arrow_field_metadata(dtype, arrow_type, orig_field.metadata)
    return pa.field(orig_field.name, arrow_type, nullable=orig_field.nullable, metadata=metadata)


def _duckdb_canonicalize_arrow_array(arr: Any, dtype: Any) -> Any:
    """Recursively converts ``TIMETZ``, ``UUID``, ``JSON``, and ``month_day_nano_interval`` in Arrow."""
    import pyarrow as pa

    if is_month_day_nano_interval(arr.type):
        return mdn_column_to_arrow(arr)
    tid = _duckdb_type_id(dtype) if dtype else ""
    if tid == "time with time zone":
        return pa.array([_duckdb_timetz_text_iso(v) for v in arr.to_pylist()], type=pa.string())
    if tid == "uuid":
        if pa.types.is_fixed_size_binary(arr.type) and arr.type.byte_width == 16:
            return arr
        return pa.array([_duckdb_uuid_bytes(v) for v in arr.to_pylist()], type=pa.binary(16))
    if tid == "json":
        if arr.type == pa.string():
            return arr
        return pa.array(
            [
                v if v is None or isinstance(v, str) else json.dumps(v, default=json_default)
                for v in arr.to_pylist()
            ],
            type=pa.string(),
        )
    if pa.types.is_list(arr.type):
        child_dtype = _duckdb_list_child_dtype(dtype, tid)
        new_values = _duckdb_canonicalize_arrow_array(arr.values, child_dtype)
        value_field = _duckdb_canonicalized_child_field(
            arr.type.value_field, new_values.type, child_dtype
        )
        return pa.ListArray.from_arrays(
            arr.offsets, new_values, type=pa.list_(value_field), mask=arr.is_null()
        )
    if pa.types.is_large_list(arr.type):
        child_dtype = _duckdb_list_child_dtype(dtype, tid)
        new_values = _duckdb_canonicalize_arrow_array(arr.values, child_dtype)
        value_field = _duckdb_canonicalized_child_field(
            arr.type.value_field, new_values.type, child_dtype
        )
        return pa.LargeListArray.from_arrays(
            arr.offsets, new_values, type=pa.large_list(value_field), mask=arr.is_null()
        )
    if pa.types.is_fixed_size_list(arr.type):
        child_dtype = _duckdb_array_child_dtype(dtype, tid)
        new_values = _duckdb_canonicalize_arrow_array(arr.values, child_dtype)
        value_field = _duckdb_canonicalized_child_field(
            arr.type.value_field, new_values.type, child_dtype
        )
        return pa.FixedSizeListArray.from_arrays(
            new_values,
            type=pa.list_(value_field, arr.type.list_size),
            mask=arr.is_null(),
        )
    if pa.types.is_struct(arr.type):
        dtype_fields = _duckdb_type_children_map(dtype, "struct")
        struct_fields = []
        new_fields = []
        for i, orig_field in enumerate(arr.type):
            child_dtype = dtype_fields.get(orig_field.name)
            child_arr = _duckdb_canonicalize_arrow_array(arr.field(i), child_dtype)
            new_fields.append(child_arr)
            struct_fields.append(
                _duckdb_canonicalized_child_field(orig_field, child_arr.type, child_dtype)
            )
        return pa.StructArray.from_arrays(new_fields, fields=struct_fields, mask=arr.is_null())
    if pa.types.is_map(arr.type):
        dtype_children = _duckdb_type_children_map(dtype, "map")
        key_dtype = dtype_children.get("key")
        val_dtype = dtype_children.get("value")
        new_keys = _duckdb_canonicalize_arrow_array(arr.keys, key_dtype)
        new_items = _duckdb_canonicalize_arrow_array(arr.items, val_dtype)
        key_field = _duckdb_canonicalized_child_field(arr.type.key_field, new_keys.type, key_dtype)
        item_field = _duckdb_canonicalized_child_field(
            arr.type.item_field, new_items.type, val_dtype
        )
        return pa.MapArray.from_arrays(
            arr.offsets,
            new_keys,
            new_items,
            type=pa.map_(key_field, item_field),
            mask=arr.is_null(),
        )
    if pa.types.is_union(arr.type):
        dtype_members = _duckdb_type_children_map(dtype, "union")
        union_fields = []
        new_children = []
        for i, orig_field in enumerate(arr.type):
            child_dtype = dtype_members.get(orig_field.name)
            child_arr = _duckdb_canonicalize_arrow_array(arr.field(i), child_dtype)
            new_children.append(child_arr)
            union_fields.append(
                _duckdb_canonicalized_child_field(orig_field, child_arr.type, child_dtype)
            )
        names = [f.name for f in union_fields]
        codes = list(arr.type.type_codes)
        if arr.type.mode == "dense":
            union_arr = pa.UnionArray.from_dense(
                arr.type_codes, arr.offsets, new_children, field_names=names, type_codes=codes
            )
            target_type = pa.dense_union(union_fields, type_codes=codes)
        else:
            union_arr = pa.UnionArray.from_sparse(
                arr.type_codes, new_children, field_names=names, type_codes=codes
            )
            target_type = pa.sparse_union(union_fields, type_codes=codes)
        return union_arr.cast(target_type)
    return arr


def _duckdb_empty_arrow_array(arrow_type: Any) -> Any:
    """Builds an empty PyArrow array for ``arrow_type``, including nested ``UnionType``."""
    import pyarrow as pa

    if pa.types.is_union(arrow_type):
        children = [_duckdb_empty_arrow_array(f.type) for f in arrow_type]
        names = [f.name for f in arrow_type]
        codes = list(arrow_type.type_codes)
        if arrow_type.mode == "dense":
            return pa.UnionArray.from_dense(
                pa.array([], type=pa.int8()),
                pa.array([], type=pa.int32()),
                children,
                field_names=names,
                type_codes=codes,
            )
        return pa.UnionArray.from_sparse(
            pa.array([], type=pa.int8()),
            children,
            field_names=names,
            type_codes=codes,
        )
    if pa.types.is_list(arrow_type):
        return pa.ListArray.from_arrays(
            pa.array([0], type=pa.int32()),
            _duckdb_empty_arrow_array(arrow_type.value_type),
            type=arrow_type,
        )
    if pa.types.is_large_list(arrow_type):
        return pa.LargeListArray.from_arrays(
            pa.array([0], type=pa.int64()),
            _duckdb_empty_arrow_array(arrow_type.value_type),
            type=arrow_type,
        )
    if pa.types.is_fixed_size_list(arrow_type):
        return pa.FixedSizeListArray.from_arrays(
            _duckdb_empty_arrow_array(arrow_type.value_type),
            type=arrow_type,
        )
    if pa.types.is_struct(arrow_type):
        return pa.StructArray.from_arrays(
            [_duckdb_empty_arrow_array(f.type) for f in arrow_type],
            fields=list(arrow_type),
        )
    if pa.types.is_map(arrow_type):
        return pa.MapArray.from_arrays(
            pa.array([0], type=pa.int32()),
            _duckdb_empty_arrow_array(arrow_type.key_type),
            _duckdb_empty_arrow_array(arrow_type.item_type),
            type=arrow_type,
        )
    return pa.array([], type=arrow_type)


def _duckdb_canonicalize_arrow_column(column: Any, dtype: Any) -> Any:
    """Canonicalizes an Arrow column (or ChunkedArray) preserving its schema even on 0 rows."""
    import pyarrow as pa

    if isinstance(column, pa.ChunkedArray):
        if column.num_chunks == 0:
            converted_empty = _duckdb_canonicalize_arrow_array(
                _duckdb_empty_arrow_array(column.type), dtype
            )
            return pa.chunked_array([], type=converted_empty.type)
        return _duckdb_canonicalize_arrow_array(column.combine_chunks(), dtype)
    return _duckdb_canonicalize_arrow_array(column, dtype)


def _duckdb_arrow_table_to_canonical(arrow_table: Any, description: Sequence[Any]) -> Any:
    """Rewrites DuckDB Arrow columns to Voyager's documented export representations.

    ``UUID`` becomes ``fixed_size_binary(16)`` (Polars ``Binary``), ``JSON`` stays
    UTF-8 text tagged ``voyager.encoding=json``, ``TIMETZ`` stays ISO text tagged
    ``voyager.type=time_tz``, ``HUGEINT`` stays ``decimal128(38, 0)`` tagged
    ``voyager.type=hugeint`` (Arrow has no int128), and calendar intervals (top-level
    or nested) become ``month_day_nano_interval``-free duration or struct columns.
    """
    import pyarrow as pa

    arrays = []
    fields = []
    for idx, field in enumerate(arrow_table.schema):
        dtype: Any = (
            description[idx][1] if idx < len(description) and len(description[idx]) > 1 else ""
        )
        description_type = str(dtype).upper()
        tid = _duckdb_type_id(dtype) if dtype else ""
        column = arrow_table.column(idx)
        metadata = dict(field.metadata) if field.metadata else {}
        if tid == "hugeint":
            array = column
            metadata[_VOYAGER_META_TYPE] = b"hugeint"
        elif is_month_day_nano_interval(field.type):
            array = mdn_column_to_arrow(column)
            if pa.types.is_struct(array.type):
                metadata[_VOYAGER_META_TYPE] = b"duration"
        elif _duckdb_type_has(
            dtype, _DUCKDB_ARROW_CANONICAL_TYPE_IDS
        ) or has_month_day_nano_interval(field.type):
            array = _duckdb_canonicalize_arrow_column(column, dtype)
            if tid in ("uuid", "json", "time with time zone"):
                metadata = _duckdb_arrow_field_metadata(dtype, array.type, metadata) or {}
            elif description_type.startswith("UUID["):
                metadata[_VOYAGER_META_TYPE] = b"uuid"
            elif description_type.startswith("TIME WITH TIME ZONE"):
                metadata[_VOYAGER_META_TYPE] = b"time_tz"
        else:
            arrays.append(column)
            fields.append(field)
            continue
        fields.append(
            pa.field(field.name, array.type, nullable=field.nullable, metadata=metadata or None)
        )
        arrays.append(array)
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def _duckdb_python_values_to_arrow(
    name: str, values: list[Any], dtype: Any, *, nullable: bool = True
) -> tuple[Any, Any]:
    """Builds an Arrow ``(field, array)`` from DuckDB DB-API Python values recursively."""
    import pyarrow as pa

    tid = _duckdb_type_id(dtype) if dtype else ""
    if tid == "time with time zone":
        arr = pa.array([_duckdb_time_iso(value) for value in values], type=pa.string())
    elif tid == "json":
        arr = pa.array(
            [
                value
                if value is None or isinstance(value, str)
                else json.dumps(value, default=json_default)
                for value in values
            ],
            type=pa.string(),
        )
    elif tid == "uuid":
        arr = pa.array([_duckdb_uuid_bytes(value) for value in values], type=pa.binary(16))
    elif tid == "hugeint":
        arr = pa.array(
            [None if value is None else Decimal(int(value)) for value in values],
            type=pa.decimal128(38, 0),
        )
    elif tid in ("interval", "timedelta"):
        arr = pa.array(values, type=pa.duration("us"))
    elif tid == "list" and _duckdb_type_has(dtype, _DUCKDB_ARROW_CANONICAL_TYPE_IDS):
        child_dtype = _duckdb_list_child_dtype(dtype, tid)
        offsets = [0]
        flat: list[Any] = []
        null_flags: list[bool] = []
        for item in values:
            if item is None:
                null_flags.append(True)
            else:
                null_flags.append(False)
                flat.extend(item)
            offsets.append(len(flat))
        child_name = (
            str(dtype.children[0][0]) if hasattr(dtype, "children") and dtype.children else "item"
        )
        child_field, child_arr = _duckdb_python_values_to_arrow(child_name, flat, child_dtype)
        arr = pa.ListArray.from_arrays(
            pa.array(offsets, type=pa.int32()),
            child_arr,
            type=pa.list_(child_field),
            mask=pa.array(null_flags, type=pa.bool_()),
        )
    elif tid == "array" and _duckdb_type_has(dtype, _DUCKDB_ARROW_CANONICAL_TYPE_IDS):
        children = _duckdb_type_children_map(dtype, "array")
        child_dtype = _duckdb_array_child_dtype(dtype, tid)
        size = int(children["size"]) if "size" in children else 0
        flat = []
        null_flags = []
        for item in values:
            if item is None:
                null_flags.append(True)
                flat.extend([None] * size)
            else:
                null_flags.append(False)
                flat.extend(item)
        child_field, child_arr = _duckdb_python_values_to_arrow("item", flat, child_dtype)
        arr = pa.FixedSizeListArray.from_arrays(
            child_arr,
            type=pa.list_(child_field, size),
            mask=pa.array(null_flags, type=pa.bool_()),
        )
    elif tid == "struct" and _duckdb_type_has(dtype, _DUCKDB_ARROW_CANONICAL_TYPE_IDS):
        dtype_fields = _duckdb_type_children_map(dtype, "struct")
        struct_fields = []
        field_arrays = []
        null_flags = [item is None for item in values]
        for fname, child_dtype in dtype_fields.items():
            sub_vals = [None if item is None else item.get(fname) for item in values]
            cfield, carr = _duckdb_python_values_to_arrow(fname, sub_vals, child_dtype)
            struct_fields.append(cfield)
            field_arrays.append(carr)
        arr = pa.StructArray.from_arrays(
            field_arrays, fields=struct_fields, mask=pa.array(null_flags, type=pa.bool_())
        )
    elif tid == "map" and _duckdb_type_has(dtype, _DUCKDB_ARROW_CANONICAL_TYPE_IDS):
        children = _duckdb_type_children_map(dtype, "map")
        key_dtype = children.get("key", "")
        val_dtype = children.get("value", "")
        offsets = [0]
        flat_keys: list[Any] = []
        flat_vals: list[Any] = []
        null_flags = []
        for item in values:
            if item is None:
                null_flags.append(True)
            else:
                null_flags.append(False)
                for k, v in item.items():
                    flat_keys.append(k)
                    flat_vals.append(v)
            offsets.append(len(flat_keys))
        key_field, key_arr = _duckdb_python_values_to_arrow(
            "key", flat_keys, key_dtype, nullable=False
        )
        val_field, val_arr = _duckdb_python_values_to_arrow("value", flat_vals, val_dtype)
        arr = pa.MapArray.from_arrays(
            pa.array(offsets, type=pa.int32()),
            key_arr,
            val_arr,
            type=pa.map_(key_field, val_field),
            mask=pa.array(null_flags, type=pa.bool_()),
        )
    else:
        arr = pa.array(values)

    meta = _duckdb_arrow_field_metadata(dtype, arr.type)
    description_type = str(dtype).upper() if dtype else ""
    if meta is None:
        meta_dict: dict[bytes, bytes] = {}
    else:
        meta_dict = meta
    if description_type.startswith("UUID["):
        meta_dict[_VOYAGER_META_TYPE] = b"uuid"
    elif description_type.startswith("TIME WITH TIME ZONE"):
        meta_dict[_VOYAGER_META_TYPE] = b"time_tz"
    return pa.field(name, arr.type, nullable=nullable, metadata=meta_dict or None), arr


def _duckdb_rows_to_arrow(description: Sequence[Any], rows: Sequence[Sequence[Any]]) -> Any:
    """Builds an Arrow table from DB-API rows when DuckDB's Arrow copy loses TIMETZ offsets."""
    import pyarrow as pa

    arrays = []
    fields = []
    for idx, item in enumerate(description):
        name = str(item[0])
        dtype: Any = item[1] if len(item) > 1 else ""
        values = [row[idx] for row in rows]
        field, array = _duckdb_python_values_to_arrow(name, values, dtype)
        fields.append(field)
        arrays.append(array)
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def _duckdb_list_child_dtype(dtype: Any, tid: str) -> Any:
    """Returns the element type of a DuckDB ``LIST`` type or type string."""
    if tid == "list" and hasattr(dtype, "children"):
        return dtype.children[0][1]
    if isinstance(dtype, str):
        text = dtype.strip()
        if text.endswith("[]"):
            return text[:-2]
    return ""


def _duckdb_array_child_dtype(dtype: Any, tid: str) -> Any:
    """Returns the element type of a DuckDB fixed-size ``ARRAY`` type or type string."""
    if tid == "array":
        return _duckdb_type_children_map(dtype, "array").get("child", "")
    if isinstance(dtype, str):
        text = dtype.strip()
        if text.endswith("]") and not text.endswith("[]") and "[" in text:
            return text[: text.rfind("[")]
    return ""
