# Voyager Value Representation Contract

Status: **normative** for the 0.5 line.
Scope: the 19 value kinds exercised by `packages/python/tests/values/`, across the
OLTP `execute()` path and the OLAP `execute_to_arrow()` / `execute_to_polars()`
paths, for every engine and backend (Neo4j, Memgraph, FalkorDB, Apache AGE,
PostgreSQL, DuckDB, Relate; official clients, Python bridges, and native Rust).

This document is the written half of the single value contract. The machine
half is `crates/voyager-drivers/src/value/mod.rs`, which defines the metadata
keys, canonical values, and shared temporal/spatial primitives. The two must
change together; Rust and Python unit tests compare the metadata registry below
against the exported constants.

## 1. Architectural boundary: OLTP versus OLAP

Voyager has two output paths with different jobs. A conversion rule belongs to
exactly one side of this line.

**OLTP path — `Session.execute()` / `AsyncSession.execute()`.**

- Returns rich, unified Python values: `datetime.date`, `datetime.time`,
  `datetime.datetime`, `voyager_ogm.Duration`, `voyager_ogm.Point`,
  `decimal.Decimal`, `uuid.UUID`, `bytes`, `dict` / `list`, `GraphNode`,
  `GraphRelationship`, and `GraphPath`.
- Must never silently corrupt or downgrade a value through a lossy JSON
  round-trip when a richer representation exists. A date is a `datetime.date`,
  not its ISO string; a UUID is a `uuid.UUID`, not 16 bytes or text; a spatial
  value is a `voyager_ogm.Point`, not a coordinate dict.
- Nested values follow the same rule: a date inside a list, map, node property,
  relationship property, or path property is hydrated to its rich Python type.
- String payloads that carry a typed value are decoded exactly once, through the
  metadata / type tag that the producer attached. Genuine strings are never
  reinterpreted as dates, UUIDs, or numbers.

**OLAP path — `Session.execute_to_arrow()` / `execute_to_polars()`.**

- Returns pure columnar Arrow or Polars data with **zero** Python-object
  columns. A column is a native Arrow/Polars type plus field metadata; it is
  not a list of hydrated Python objects.
- OLAP engines bypass the row-oriented `ResultValue`/`BoltValue`/`AgeValue`
  conversion entirely. They stay zero-copy Arrow, passing only through
  canonical metadata attachment and the single shared Arrow-to-Polars
  normalizer.
- Metadata is authoritative. `voyager.type` names a typed fallback that Arrow
  or Polars cannot express natively (zoned time, decimal text, UUID text,
  JSON), `voyager.encoding=json` marks a JSON payload, and `voyager.kind`
  distinguishes a graph entity inside JSON. A consumer must not guess from
  the string contents when metadata is present.
- The Polars export is the same data with Arrow metadata stripped by Polars.
  Consequences are part of the contract, not bugs: a `time_tz` or
  `datetime_tz` fallback arrives as ISO text, a UUID arrives as 16 raw bytes,
  and a JSON column arrives as its JSON string.

Transaction methods (`*Transaction.execute`, `*Transaction.execute_to_arrow`,
`*Transaction.execute_to_polars`) have exactly the same contract as their
auto-commit counterparts. They call the same pipeline; a transaction never has
its own decoding rules.

## 2. The 19 value kinds

The kind names match `VALUE_KINDS` in
`packages/python/tests/values/restrictions.py`. `OLTP` is the Python type
returned by `execute()`, `Arrow` is the `execute_to_arrow()` column type, and
`Polars` is the `execute_to_polars()` column type. `JSON` is the representation
inside a `voyager.encoding=json` column (graph entity, dynamic map, or
heterogeneous container).

| # | Kind | OLTP `execute()` | OLAP Arrow `execute_to_arrow()` | OLAP Polars `execute_to_polars()` | JSON fallback |
|---|---|---|---|---|---|
| 1 | `null` | `None` | `Null` for an all-null column, otherwise a null slot in the inferred column type | `null` in the inferred type | `null` |
| 2 | `bool` | `bool` | `Boolean` | `Boolean` | JSON boolean |
| 3 | `int` | `int` | `Int64`; an integer beyond `Int64` is `Decimal128(38, 0)` + `voyager.type=hugeint` | `Int64`; beyond `Int64`, `Decimal(38, 0)` | JSON number |
| 4 | `float` | `float` | `Float64` | `Float64` | JSON number; non-finite values as the `NaN`, `Infinity`, `-Infinity` tokens |
| 5 | `decimal` | `decimal.Decimal` | `Decimal128(precision, scale)` when exactly representable, otherwise `Utf8` + `voyager.type=decimal` | `Decimal(precision, scale)` when representable, otherwise `String` | decimal text string |
| 6 | `str` | `str` | `Utf8` | `String` | JSON string |
| 7 | `bytes` | `bytes` | `Binary` (`LargeBinary` when the column exceeds the 32-bit offset range) | `Binary` | array of byte integers (`list[int]`) |
| 8 | `date` | `datetime.date` | `Date32` | `Date` | ISO 8601 date string (`YYYY-MM-DD`) |
| 9 | `time_naive` | `datetime.time` (naive) | `Time64(Microsecond)` or `Time64(Nanosecond)` | `Time` | ISO 8601 time string (`HH:MM:SS[.fffffffff]`) |
| 10 | `time_offset` | `datetime.time` with `tzinfo` | `Utf8` + `voyager.type=time_tz` (Arrow has no zoned time type) | `String` (ISO text) | ISO 8601 time string with offset |
| 11 | `datetime_naive` | `datetime.datetime` (naive) | `Timestamp(Microsecond or Nanosecond, None)` | `Datetime(us or ns)` | ISO 8601 local datetime string |
| 12 | `datetime_offset` | `datetime.datetime` with a fixed offset | `Utf8` + `voyager.type=datetime_tz`; a UTC instant uses `Timestamp(us, "UTC")` | `String`; UTC uses `Datetime(us, "UTC")` | ISO 8601 datetime string with offset |
| 13 | `datetime_zone` | `datetime.datetime` with `ZoneInfo` | `Timestamp(us or ns, "<IANA zone>")` when Arrow accepts the zone, otherwise `Utf8` + `voyager.type=datetime_tz` | `Datetime(us or ns, "<IANA zone>")` when representable, otherwise `String` | ISO 8601 datetime string with offset |
| 14 | `duration` | `voyager_ogm.Duration` | `Duration(us or ns)` when `months == 0` and the value fits `Int64`; otherwise `Struct{months, days, seconds, nanoseconds}` + `voyager.type=duration` | `Duration` or the same `Struct` (never `month_day_nano_interval`) | ISO 8601 duration string |
| 15 | `point` | `voyager_ogm.Point` | `Struct{srid: Int64, x: Float64, y: Float64, z: Float64}` + `voyager.type=point` when the engine exposes a native spatial struct; otherwise `Utf8` + `voyager.type=point` | `Struct` or `String`, matching the Arrow column | coordinate object `{"srid": ..., "x": ..., "y": ...[, "z": ...]}` |
| 16 | `uuid` | `uuid.UUID` | `FixedSizeBinary(16)` + `ARROW:extension:name=arrow.uuid` + `voyager.type=uuid`; text fallback is `Utf8` + `voyager.type=uuid` | `Binary` (16 raw bytes; Polars drops the extension metadata) | canonical lowercase hyphenated UUID string |
| 17 | `json_doc` | `dict` / `list` / scalar with decoded nested values | `Utf8` + `voyager.encoding=json`; `voyager.type=json` marks a JSON value rather than a graph entity | `String` (the same JSON payload) | the payload itself |
| 18 | `list` | `list` | `List<T>`, `LargeList<T>`, or `FixedSizeList<T, N>` with canonicalized child fields | `List` or `Array` | JSON array with the nested rules in section 6 |
| 19 | `map` | `dict` | `Map<K, V>` with canonicalized child fields | `List(Struct{key, value})` because the Polars export has no map type | JSON object with the nested rules in section 6 |

### Temporal rules

- Calendar components are not normalized away. A duration keeps `months`,
  `days`, `seconds`, and `nanoseconds` separately; months are never folded into
  days because the result would depend on a reference date.
- A datetime column chooses microsecond resolution when every value fits the
  microsecond range and nanosecond resolution otherwise. The same choice is
  made for `Duration`.
- A naive time or datetime carries no zone metadata. An offset time always
  uses the `time_tz` text fallback because Arrow cannot represent a time with a
  timezone. A fixed-offset datetime uses the `datetime_tz` text fallback,
  except for UTC where the native `Timestamp(..., "UTC")` / `Datetime(..., "UTC")`
  type is used.
- When an Arrow/Polars timestamp carries a named zone, the returned Python
  value is aware in that zone. When it carries `UTC`, the returned value is
  aware in UTC. A text fallback is parsed back into an aware value from its ISO
  offset, exactly once.

### Spatial rules

- The canonical Python value is always `voyager_ogm.Point` for `execute()`,
  including inside lists, maps, nodes, relationships, and paths.
- Arrow spatial columns are `Struct{srid, x, y, z}` with `voyager.type=point`
  where the engine/backend exposes a native spatial struct. `z` is null for a
  2D point.
- A point nested inside a JSON container is a coordinate object with `srid`,
  `x`, `y`, and, for a 3D point, `z`. It is not an ISO string.
- A text fallback (`Utf8` + `voyager.type=point`) exists only for engines whose
  Arrow export has no spatial struct; it uses the same coordinate JSON.

### Nested containers

- Every nested container keeps its child values typed. `List<UUID>` is a list of
  `FixedSizeBinary(16)` items, not a list of UUID strings; `Map<String, Point>`
  has a canonicalized value field; a struct field that holds JSON says
  `voyager.encoding=json` on that child field.
- A container uses its native Arrow shape (`List`, `LargeList`,
  `FixedSizeList`, `Map`, `Struct`, or `Union`) when its leaves are homogeneous
  typed values. A heterogeneous list or dynamic map that cannot be expressed as
  a single uniform child type uses the `voyager.encoding=json` fallback in
  section 5 instead.
- Every Arrow builder, native or Python, propagates `voyager.*` and
  `ARROW:extension:name` metadata recursively onto the child fields of
  `List`, `LargeList`, `FixedSizeList`, `Map`, `Struct`, and `Union`. Metadata
  belongs to the field that holds the value, at whatever depth that field is.
- Polars strips Arrow field metadata. Nested values therefore surface as their
  Polars types (for example `List(Binary)` for `List<UUID>` and `List(String)`
  for `List<JSON>`); the `execute()` path still hydrates them to the rich
  Python type.

## 3. Metadata registry

The constants below are exported from `crates/voyager-drivers/src/value/mod.rs`
and re-exported to Python through `voyager_ogm._voyager_rs`. Both names and
canonical values are fixed by this table.

| Constant | Canonical value |
|---|---|
| `VOYAGER_META_TYPE` | `voyager.type` |
| `VOYAGER_META_ENCODING` | `voyager.encoding` |
| `VOYAGER_META_KIND` | `voyager.kind` |
| `ARROW_EXTENSION_NAME` | `ARROW:extension:name` |
| `ARROW_UUID_EXTENSION` | `arrow.uuid` |
| `META_TYPE_DATE` | `date` |
| `META_TYPE_TIME` | `time` |
| `META_TYPE_TIME_TZ` | `time_tz` |
| `META_TYPE_LOCAL_DATETIME` | `local_datetime` |
| `META_TYPE_DATETIME_TZ` | `datetime_tz` |
| `META_TYPE_DURATION` | `duration` |
| `META_TYPE_DECIMAL` | `decimal` |
| `META_TYPE_UUID` | `uuid` |
| `META_TYPE_HUGEINT` | `hugeint` |
| `META_TYPE_POINT` | `point` |
| `META_TYPE_JSON` | `json` |
| `META_ENCODING_JSON` | `json` |
| `META_KIND_NODE` | `node` |
| `META_KIND_RELATIONSHIP` | `relationship` |
| `META_KIND_PATH` | `path` |

Annotation rules:

- `voyager.type` alone means "this column holds a text or struct fallback for
  the named Voyager type". It never means "decode this string by guessing".
- `voyager.encoding=json` means "the column is UTF-8 JSON text". The payload is
  serialized with the JSON rules in section 6. `voyager.type=json` is added when
  the value is JSON itself rather than a graph entity.
- `voyager.kind` appears only on a `voyager.encoding=json` column and is one of
  `node`, `relationship`, or `path`. It records that the payload is a graph
  entity so the Python path can build `GraphNode`, `GraphRelationship`, or
  `GraphPath` instead of returning a plain `dict`.
- `ARROW:extension:name=arrow.uuid` marks the 16-byte UUID extension type.
  `voyager.type=uuid` is attached alongside it so consumers that ignore Arrow
  extension types still know the bytes are a UUID.
- A field may carry several annotations (for example a UUID text fallback has
  `voyager.type=uuid`; a JSON entity has `voyager.encoding=json` and
  `voyager.kind=node`). Annotations are inherited by every child field that
  holds the value, never by an unrelated sibling.

## 4. Codified OLAP fallback table

The following Arrow/Polars behaviors are intentional columnar boundaries, not
bugs. They are preserved consistently across every engine.

| Value kind | OLTP `execute()` | OLAP `execute_to_arrow()` | OLAP `execute_to_polars()` |
|---|---|---|---|
| `UUID` | `uuid.UUID` | `FixedSizeBinary(16)` + `ARROW:extension:name=arrow.uuid`, `voyager.type=uuid` (recursively on child fields) | `pl.Binary` (16 raw bytes) |
| `HUGEINT` (> `Int64`) | `int` | `Decimal128(38, 0)` + `voyager.type=hugeint` | `pl.Decimal(38, 0)` |
| `TIMETZ` / zoned time | aware `datetime.time` | `Utf8` + `voyager.type=time_tz` (recursively on child fields) | `pl.String` (ISO text) |
| Fixed-offset `DateTime` | aware `datetime.datetime` | `Utf8` + `voyager.type=datetime_tz` (recursively on child fields); UTC uses `Timestamp(us, "UTC")` | `pl.String` (ISO text); UTC uses `pl.Datetime("us", "UTC")` |
| `Duration` / `INTERVAL` | `voyager_ogm.Duration` | `Duration("us"` or `"ns")` when `months == 0` and the value fits `Int64`; otherwise `Struct{months, days, seconds, nanoseconds}` + `voyager.type=duration` | `pl.Duration` or `pl.Struct` (never `month_day_nano_interval`) |
| `UNION` (DuckDB) | active member's Python value | `Union` (sparse/dense) with canonicalized member fields | tagged `pl.Struct(..., tag: pl.String)` + warning |
| `MAP` / `ARRAY` | `dict` / `tuple` (or `list`) | `Map` / `FixedSizeList` with canonicalized child fields | `pl.List(pl.Struct{key, value})` / `pl.Array` |
| Graph entities and dynamic maps | `GraphNode`, `GraphRelationship`, `GraphPath`, `dict` (full nested Python types) | `Utf8` + `voyager.encoding=json` (+ `voyager.kind=node`, `relationship`, or `path`); nested temporals are ISO strings, bytes an array of integers, points a coordinate object | `pl.String` (the same JSON payload) |
| `Point` | `voyager_ogm.Point` (all engines and backends) | `Struct{srid, x, y, z}` + `voyager.type=point` when the engine exposes a native spatial struct, otherwise `Utf8` + `voyager.type=point` (inside JSON containers: a coordinate object) | the matching `pl.Struct` or `pl.String` |

The `Point` row is the one place where the table names two representable Arrow
shapes: native spatial columns use the `Struct`, and engines that expose a point
as text use the `Utf8` fallback. Both carry `voyager.type=point`; the Python
path decodes either to `voyager_ogm.Point`. Engines with a native spatial struct
must not be downgraded to text to make the two shapes uniform.

## 5. JSON fallback rules

A `voyager.encoding=json` column is the escape hatch for values Arrow cannot
model directly: graph entities, dynamic maps, and heterogeneous containers. Its
payload is deterministic:

1. `null`, `bool`, `str`, and finite numbers map to their JSON counterparts.
   Non-finite floats use the `NaN`, `Infinity`, and `-Infinity` tokens.
2. `decimal.Decimal` and `uuid.UUID` become strings; a UUID uses the canonical
   lowercase hyphenated form.
3. `bytes` becomes an array of integers in `0..=255`, never a base64 or UTF-8
   guess.
4. `datetime.date`, naive `datetime.time`, naive `datetime.datetime`, offset
   time, offset datetime, and zoned datetime become ISO 8601 strings. Offset
   and zoned datetimes keep their offset; a named zone may keep its zone
   identifier in the ISO text.
5. `voyager_ogm.Duration` becomes an ISO 8601 duration string.
6. `voyager_ogm.Point` becomes `{"srid": ..., "x": ..., "y": ...[, "z": ...]}`.
7. A list becomes a JSON array and a map becomes a JSON object, with every
   nested value following these same rules.
8. A `GraphNode` carries `id`, optional `element_id`, `labels`, and `properties`.
   A `GraphRelationship` carries `id`, optional `element_id`, `type`,
   `start_node`, `end_node`, and `properties`. A `GraphPath` carries its nodes
   and relationships in order. The column metadata adds
   `voyager.kind=node|relationship|path`; the OLTP path uses that annotation
   to rebuild the entity.

The OLTP path decodes a JSON entity or map back to rich Python values once, at
the boundary; it never returns the raw JSON string when the metadata says the
payload is decodable. The OLAP path deliberately returns the JSON string,
because the contract for `execute_to_arrow()` / `execute_to_polars()` is
columnar data with no Python-object columns.
