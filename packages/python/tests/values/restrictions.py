"""Per-engine capability matrix, restrictions, and Hypothesis value strategies (#299)."""

from __future__ import annotations

import datetime
import uuid
import zoneinfo
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from hypothesis import strategies as st
from voyager_ogm import Duration, Point

ENGINES: tuple[str, ...] = ("neo4j", "memgraph", "falkordb", "age", "pg19", "duckdb")

VALUE_KINDS: tuple[str, ...] = (
    "null",
    "bool",
    "int",
    "float",
    "decimal",
    "str",
    "bytes",
    "date",
    "time_naive",
    "time_offset",
    "datetime_naive",
    "datetime_offset",
    "datetime_zone",
    "duration",
    "point",
    "uuid",
    "json_doc",
    "list",
    "map",
)

ROUND_TRIP_PATHS: tuple[str, ...] = (
    "param",
    "stored",
    "nested_list",
    "nested_map",
    "node_prop",
    "rel_prop",
    "transaction",
)

I64_MIN = -(2**63)
I64_MAX = 2**63 - 1
JS_SAFE_MAX = 2**53 - 1


@dataclass(frozen=True, slots=True)
class EngineRestriction:
    """Documents why a value kind or round-trip path is restricted or narrowed on an engine."""

    engine: str
    value_kind: str
    supported: bool
    reason: str
    paths_excluded: tuple[str, ...] = ()


# Central single-source registry of engine storage/protocol restrictions.
# Every entry explains what the engine itself cannot represent or store.
RESTRICTIONS: dict[tuple[str, str], EngineRestriction] = {
    # --- Neo4j ---
    ("neo4j", "int"): EngineRestriction(
        engine="neo4j",
        value_kind="int",
        supported=True,
        reason="Bolt PackStream integers are signed 64-bit; integers outside [-2^63, 2^63-1] cannot be sent.",
    ),
    ("neo4j", "decimal"): EngineRestriction(
        engine="neo4j",
        value_kind="decimal",
        supported=False,
        reason="Neo4j has no Decimal property/wire type; Cypher only stores 64-bit IEEE-754 floats.",
    ),
    ("neo4j", "uuid"): EngineRestriction(
        engine="neo4j",
        value_kind="uuid",
        supported=True,
        reason="Neo4j has no native UUID type and stores UUIDs as strings; included via known-differences.",
    ),
    ("neo4j", "json_doc"): EngineRestriction(
        engine="neo4j",
        value_kind="json_doc",
        supported=True,
        reason="Neo4j supports maps/lists in expressions (RETURN $v) but rejects maps/nested lists as node/rel properties.",
        paths_excluded=("stored", "node_prop", "rel_prop", "transaction"),
    ),
    ("neo4j", "map"): EngineRestriction(
        engine="neo4j",
        value_kind="map",
        supported=True,
        reason="Neo4j supports maps in RETURN $v and nested expressions, but rejects maps as node/relationship properties.",
        paths_excluded=("stored", "node_prop", "rel_prop", "transaction"),
    ),
    ("neo4j", "list"): EngineRestriction(
        engine="neo4j",
        value_kind="list",
        supported=True,
        reason="Neo4j properties only allow homogeneous primitive arrays without nulls or nested lists (RETURN $v allows all lists).",
    ),
    # --- Memgraph ---
    ("memgraph", "int"): EngineRestriction(
        engine="memgraph",
        value_kind="int",
        supported=True,
        reason="Bolt PackStream integers are signed 64-bit; integers outside [-2^63, 2^63-1] cannot be sent.",
    ),
    ("memgraph", "bytes"): EngineRestriction(
        engine="memgraph",
        value_kind="bytes",
        supported=False,
        reason="Memgraph rejects Bolt ByteArray parameters with Memgraph.ExecutionException.",
    ),
    ("memgraph", "decimal"): EngineRestriction(
        engine="memgraph",
        value_kind="decimal",
        supported=False,
        reason="Memgraph has no Decimal property/wire type; Cypher only stores 64-bit IEEE-754 floats.",
    ),
    ("memgraph", "time_offset"): EngineRestriction(
        engine="memgraph",
        value_kind="time_offset",
        supported=False,
        reason="Memgraph only supports LocalTime and has no offset-aware ZonedTime type.",
    ),
    ("memgraph", "datetime_offset"): EngineRestriction(
        engine="memgraph",
        value_kind="datetime_offset",
        supported=True,
        reason="Memgraph Bolt encodes pre-1970 sub-second instants with negative nanoseconds rejected by official neo4j driver hydration; restricted to >= 1970-01-01.",
    ),
    ("memgraph", "duration"): EngineRestriction(
        engine="memgraph",
        value_kind="duration",
        supported=True,
        reason="Memgraph rejects durations with non-zero calendar months and normalizes seconds >= 86400 into days.",
    ),
    ("memgraph", "uuid"): EngineRestriction(
        engine="memgraph",
        value_kind="uuid",
        supported=True,
        reason="Memgraph has no native UUID type and stores UUIDs as strings; tracked via known-differences.",
    ),
    # --- FalkorDB ---
    ("falkordb", "int"): EngineRestriction(
        engine="falkordb",
        value_kind="int",
        supported=True,
        reason="FalkorDB RESP/Cypher integers are signed 64-bit; integers outside [-2^63, 2^63-1] cannot be sent.",
    ),
    ("falkordb", "float"): EngineRestriction(
        engine="falkordb",
        value_kind="float",
        supported=True,
        reason="FalkorDB CYPHER parameter headers only accept finite float literals (NaN/Inf params are rejected).",
    ),
    ("falkordb", "bytes"): EngineRestriction(
        engine="falkordb",
        value_kind="bytes",
        supported=False,
        reason="FalkorDB has no binary/bytes Cypher parameter or property type.",
    ),
    ("falkordb", "decimal"): EngineRestriction(
        engine="falkordb",
        value_kind="decimal",
        supported=False,
        reason="FalkorDB has no Decimal type; numeric decimals are represented as 64-bit floats.",
    ),
    ("falkordb", "time_naive"): EngineRestriction(
        engine="falkordb",
        value_kind="time_naive",
        supported=True,
        reason="FalkorDB stores localtime at whole-second resolution and truncates sub-second microseconds.",
    ),
    ("falkordb", "time_offset"): EngineRestriction(
        engine="falkordb",
        value_kind="time_offset",
        supported=False,
        reason="FalkorDB has no offset-aware time type (only second-precision localtime).",
    ),
    ("falkordb", "datetime_naive"): EngineRestriction(
        engine="falkordb",
        value_kind="datetime_naive",
        supported=True,
        reason="FalkorDB stores localdatetime at whole-second resolution (year >= 1970 or bounded range) and truncates microseconds.",
    ),
    ("falkordb", "datetime_offset"): EngineRestriction(
        engine="falkordb",
        value_kind="datetime_offset",
        supported=False,
        reason="FalkorDB has no timezone-aware datetime type (only localdatetime).",
    ),
    ("falkordb", "datetime_zone"): EngineRestriction(
        engine="falkordb",
        value_kind="datetime_zone",
        supported=False,
        reason="FalkorDB has no IANA-zoned datetime type.",
    ),
    ("falkordb", "duration"): EngineRestriction(
        engine="falkordb",
        value_kind="duration",
        supported=True,
        reason="FalkorDB duration parameters require whole-second precision (no sub-second nanoseconds).",
    ),
    ("falkordb", "point"): EngineRestriction(
        engine="falkordb",
        value_kind="point",
        supported=False,
        reason="FalkorDB has no native Bolt-style Point parameter type; spatial coordinates are stored as plain dicts/maps.",
    ),
    ("falkordb", "uuid"): EngineRestriction(
        engine="falkordb",
        value_kind="uuid",
        supported=True,
        reason="FalkorDB has no native UUID type and stores UUIDs as strings.",
    ),
    ("falkordb", "json_doc"): EngineRestriction(
        engine="falkordb",
        value_kind="json_doc",
        supported=True,
        reason="FalkorDB supports maps in RETURN $v but rejects maps as node/relationship properties.",
        paths_excluded=("stored", "node_prop", "rel_prop", "transaction"),
    ),
    ("falkordb", "map"): EngineRestriction(
        engine="falkordb",
        value_kind="map",
        supported=True,
        reason="FalkorDB supports maps in RETURN $v but rejects maps as node/relationship properties.",
        paths_excluded=("stored", "node_prop", "rel_prop", "transaction"),
    ),
    # --- Apache AGE ---
    ("age", "float"): EngineRestriction(
        engine="age",
        value_kind="float",
        supported=True,
        reason="AGE cypher() parameter maps are JSON text, so parameter inputs are finite floats (JSON has no NaN/Inf).",
    ),
    ("age", "decimal"): EngineRestriction(
        engine="age",
        value_kind="decimal",
        supported=False,
        reason="AGE cypher() JSON parameter maps do not distinguish Decimal from float/string.",
    ),
    ("age", "bytes"): EngineRestriction(
        engine="age",
        value_kind="bytes",
        supported=False,
        reason="AGE agtype has no binary/bytes scalar type.",
    ),
    ("age", "date"): EngineRestriction(
        engine="age",
        value_kind="date",
        supported=False,
        reason="AGE agtype has no native date scalar type (dates are stored as ISO strings).",
    ),
    ("age", "time_naive"): EngineRestriction(
        engine="age",
        value_kind="time_naive",
        supported=False,
        reason="AGE agtype has no native time scalar type.",
    ),
    ("age", "time_offset"): EngineRestriction(
        engine="age",
        value_kind="time_offset",
        supported=False,
        reason="AGE agtype has no native offset time scalar type.",
    ),
    ("age", "datetime_naive"): EngineRestriction(
        engine="age",
        value_kind="datetime_naive",
        supported=False,
        reason="AGE agtype has no native timestamp scalar type.",
    ),
    ("age", "datetime_offset"): EngineRestriction(
        engine="age",
        value_kind="datetime_offset",
        supported=False,
        reason="AGE agtype has no native timestamptz scalar type.",
    ),
    ("age", "datetime_zone"): EngineRestriction(
        engine="age",
        value_kind="datetime_zone",
        supported=False,
        reason="AGE agtype has no native zoned datetime scalar type.",
    ),
    ("age", "duration"): EngineRestriction(
        engine="age",
        value_kind="duration",
        supported=False,
        reason="AGE agtype has no native duration scalar type.",
    ),
    ("age", "point"): EngineRestriction(
        engine="age",
        value_kind="point",
        supported=False,
        reason="AGE agtype has no native spatial Point type.",
    ),
    ("age", "uuid"): EngineRestriction(
        engine="age",
        value_kind="uuid",
        supported=False,
        reason="AGE agtype has no native UUID type.",
    ),
    ("age", "str"): EngineRestriction(
        engine="age",
        value_kind="str",
        supported=True,
        reason="PostgreSQL/AGE text cannot contain NUL (\\x00) characters.",
    ),
    # --- PostgreSQL 19 (SQL) ---
    ("pg19", "int"): EngineRestriction(
        engine="pg19",
        value_kind="int",
        supported=True,
        reason="PostgreSQL bigint is signed 64-bit; larger integers require numeric/Decimal.",
    ),
    ("pg19", "str"): EngineRestriction(
        engine="pg19",
        value_kind="str",
        supported=True,
        reason="PostgreSQL text/varchar forbids NUL (\\x00) bytes (SQLSTATE 22021).",
    ),
    ("pg19", "duration"): EngineRestriction(
        engine="pg19",
        value_kind="duration",
        supported=True,
        reason="PostgreSQL interval has microsecond precision (sub-microsecond nanoseconds are not preserved).",
    ),
    ("pg19", "point"): EngineRestriction(
        engine="pg19",
        value_kind="point",
        supported=False,
        reason="Voyager has no native PostgreSQL Point OID parameter binding.",
    ),
    ("pg19", "list"): EngineRestriction(
        engine="pg19",
        value_kind="list",
        supported=True,
        reason="PostgreSQL SQL arrays must be homogeneous 1-D arrays (mixed/jagged lists use jsonb).",
    ),
    ("pg19", "map"): EngineRestriction(
        engine="pg19",
        value_kind="map",
        supported=True,
        reason="PostgreSQL stores maps as jsonb (keys are strings; nested values follow JSON).",
    ),
    # --- DuckDB ---
    ("duckdb", "str"): EngineRestriction(
        engine="duckdb",
        value_kind="str",
        supported=True,
        reason="DuckDB VARCHAR/JSON parameters reject embedded NUL (\\x00) characters.",
    ),
    ("duckdb", "decimal"): EngineRestriction(
        engine="duckdb",
        value_kind="decimal",
        supported=True,
        reason="DuckDB DECIMAL has max precision 38 and only supports finite values (no NaN/Inf).",
    ),
    ("duckdb", "datetime_zone"): EngineRestriction(
        engine="duckdb",
        value_kind="datetime_zone",
        supported=False,
        reason="DuckDB TIMESTAMPTZ stores UTC instants without retaining IANA zone names.",
    ),
    ("duckdb", "duration"): EngineRestriction(
        engine="duckdb",
        value_kind="duration",
        supported=True,
        reason="DuckDB INTERVAL stores (months, days, microseconds); sub-microsecond nanoseconds truncate.",
    ),
    ("duckdb", "point"): EngineRestriction(
        engine="duckdb",
        value_kind="point",
        supported=False,
        reason="DuckDB has no built-in Voyager Point type without spatial extension.",
    ),
}


def get_restriction(engine: str, value_kind: str) -> EngineRestriction | None:
    """Returns the documented :class:`EngineRestriction` for ``(engine, value_kind)``, if any."""
    return RESTRICTIONS.get((engine, value_kind))


def is_supported(engine: str, value_kind: str, path: str | None = None) -> bool:
    """Returns whether ``(engine, value_kind)`` is supported on ``path``."""
    restriction = RESTRICTIONS.get((engine, value_kind))
    if restriction is None:
        return True
    if not restriction.supported:
        return False
    if path is not None and path in restriction.paths_excluded:
        return False
    return True


def strategy_for(engine: str, value_kind: str, *, path: str = "param") -> st.SearchStrategy[Any]:
    """Returns the Hypothesis strategy for ``value_kind`` restricted to ``engine`` and ``path``."""
    builder = _STRATEGY_BUILDERS.get(value_kind)
    if builder is None:
        raise ValueError(f"Unknown value kind: {value_kind!r}")
    return builder(engine, path)


def _strat_null(_engine: str, _path: str) -> st.SearchStrategy[None]:
    return st.none()


def _strat_bool(_engine: str, _path: str) -> st.SearchStrategy[bool]:
    return st.booleans()


def _strat_int(engine: str, _path: str) -> st.SearchStrategy[int]:
    if engine == "duckdb":
        edges = st.sampled_from(
            [
                0,
                1,
                -1,
                2**31 - 1,
                -(2**31),
                JS_SAFE_MAX,
                2**53,
                2**53 + 1,
                -(2**53 + 1),
                I64_MAX,
                I64_MIN,
                2**63,
                -(2**63) - 1,
                2**120,
                -(2**120),
            ]
        )
        return st.one_of(edges, st.integers(min_value=I64_MIN, max_value=I64_MAX))
    if engine == "age":
        edges = st.sampled_from(
            [
                0,
                1,
                -1,
                2**31 - 1,
                -(2**31),
                JS_SAFE_MAX,
                2**53,
                2**53 + 1,
                -(2**53 + 1),
                I64_MAX,
                I64_MIN,
            ]
        )
        return st.one_of(edges, st.integers(min_value=I64_MIN, max_value=I64_MAX))
    edges = st.sampled_from(
        [
            0,
            1,
            -1,
            2**31 - 1,
            -(2**31),
            JS_SAFE_MAX,
            2**53,
            2**53 + 1,
            -(2**53 + 1),
            I64_MAX,
            I64_MIN,
        ]
    )
    return st.one_of(edges, st.integers(min_value=I64_MIN, max_value=I64_MAX))


def _strat_float(engine: str, _path: str) -> st.SearchStrategy[float]:
    if engine in ("falkordb", "age"):
        edges = st.sampled_from([0.0, -0.0, 1.5, -1.5, 1e100, -1e100, 1e-100, -1e-100])
        return st.one_of(
            edges,
            st.floats(allow_nan=False, allow_infinity=False, width=64),
        )
    edges = st.sampled_from(
        [
            0.0,
            -0.0,
            1.5,
            -1.5,
            1e308,
            -1e308,
            1e-308,
            float("nan"),
            float("inf"),
            float("-inf"),
        ]
    )
    return st.one_of(edges, st.floats(allow_nan=True, allow_infinity=True, width=64))


def _strat_decimal(engine: str, _path: str) -> st.SearchStrategy[Decimal]:
    if engine == "duckdb":
        edges = st.sampled_from(
            [
                Decimal("0.00"),
                Decimal("1.50"),
                Decimal("-123.4500"),
                Decimal("99999999999999999999.123456"),
                Decimal("-0.000001"),
            ]
        )
        return st.one_of(
            edges,
            st.decimals(
                min_value=Decimal("-99999999999999999999.999999"),
                max_value=Decimal("99999999999999999999.999999"),
                places=6,
                allow_nan=False,
                allow_infinity=False,
            ),
        )
    edges = st.sampled_from(
        [
            Decimal("0.00"),
            Decimal("1.50"),
            Decimal("-123.4500"),
            Decimal("1234567890123456789012345678901234567890.123456"),
            Decimal("NaN"),
            Decimal("Infinity"),
            Decimal("-Infinity"),
        ]
    )
    return st.one_of(
        edges,
        st.decimals(
            min_value=Decimal("-1e20"),
            max_value=Decimal("1e20"),
            allow_nan=False,
            allow_infinity=False,
        ),
    )


def _strat_str(engine: str, path: str) -> st.SearchStrategy[str]:
    edges = [
        "",
        "hello",
        "2024-01-02",
        "12345678-1234-5678-1234-567812345678",
        "42",
        "true",
        "null",
        "NaN",
        "café 🚀 日本語 مرحبا e\u0301",
    ]
    if path in ("param", "stored") and engine != "age":
        edges.append("x" * 70_000)
    # PG/AGE/DuckDB/FalkorDB reject NUL bytes in text strings; Neo4j/Memgraph accept printable/Unicode.
    alphabet = st.characters(blacklist_categories=("Cs",), blacklist_characters="\x00")
    return st.one_of(st.sampled_from(edges), st.text(alphabet=alphabet, max_size=200))


def _strat_bytes(_engine: str, path: str) -> st.SearchStrategy[bytes]:
    edges = [b"", b"\x00\x01\x02\xfe\xff"]
    if path in ("param", "stored"):
        edges.append(b"\xab" * 70_000)
    return st.one_of(st.sampled_from(edges), st.binary(min_size=0, max_size=256))


def _strat_date(engine: str, _path: str) -> st.SearchStrategy[datetime.date]:
    if engine == "falkordb":
        # FalkorDB date() supports typical proleptic/Unix-era dates cleanly.
        edges = st.sampled_from(
            [
                datetime.date(1970, 1, 1),
                datetime.date(2000, 2, 29),
                datetime.date(2024, 1, 2),
                datetime.date(2099, 12, 31),
            ]
        )
        return st.one_of(
            edges,
            st.dates(min_value=datetime.date(1970, 1, 1), max_value=datetime.date(2100, 12, 31)),
        )
    edges = st.sampled_from(
        [
            datetime.date(1, 1, 1),
            datetime.date(1582, 10, 15),
            datetime.date(1969, 12, 31),
            datetime.date(1970, 1, 1),
            datetime.date(2024, 2, 29),
            datetime.date(9999, 12, 31),
        ]
    )
    return st.one_of(edges, st.dates())


def _strat_time_naive(engine: str, _path: str) -> st.SearchStrategy[datetime.time]:
    if engine == "falkordb":
        # FalkorDB localtime drops sub-second precision.
        return st.builds(
            lambda h, m, s: datetime.time(h, m, s),
            st.integers(0, 23),
            st.integers(0, 59),
            st.integers(0, 59),
        )
    edges = st.sampled_from(
        [
            datetime.time(0, 0, 0),
            datetime.time(14, 30, 15, 123456),
            datetime.time(23, 59, 59, 999999),
        ]
    )
    return st.one_of(edges, st.times())


def _strat_time_offset(_engine: str, _path: str) -> st.SearchStrategy[datetime.time]:
    offsets = [
        datetime.UTC,
        datetime.timezone(datetime.timedelta(hours=2)),
        datetime.timezone(datetime.timedelta(hours=5, minutes=45)),
        datetime.timezone(datetime.timedelta(hours=-9, minutes=-30)),
        datetime.timezone(datetime.timedelta(hours=14)),
        datetime.timezone(datetime.timedelta(hours=-12)),
    ]
    return st.builds(
        lambda t, tz: t.replace(tzinfo=tz),
        st.times(),
        st.sampled_from(offsets),
    )


def _strat_datetime_naive(engine: str, _path: str) -> st.SearchStrategy[datetime.datetime]:
    if engine == "falkordb":
        return st.builds(
            lambda d, t: datetime.datetime.combine(d, t),
            st.dates(min_value=datetime.date(1970, 1, 1), max_value=datetime.date(2100, 12, 31)),
            st.builds(
                lambda h, m, s: datetime.time(h, m, s),
                st.integers(0, 23),
                st.integers(0, 59),
                st.integers(0, 59),
            ),
        )
    edges = st.sampled_from(
        [
            datetime.datetime(1, 1, 1, 0, 0, 0),
            datetime.datetime(1600, 1, 1, 12, 30, 0, 123456),
            datetime.datetime(1970, 1, 1, 0, 0, 0),
            datetime.datetime(2024, 1, 2, 3, 4, 5, 123456),
            datetime.datetime(2300, 6, 15, 18, 0, 0, 654321),
            datetime.datetime(9999, 12, 31, 23, 59, 59, 999999),
        ]
    )
    return st.one_of(edges, st.datetimes())


def _strat_datetime_offset(engine: str, _path: str) -> st.SearchStrategy[datetime.datetime]:
    offsets = [
        datetime.UTC,
        datetime.timezone(datetime.timedelta(hours=2)),
        datetime.timezone(datetime.timedelta(hours=5, minutes=45)),
        datetime.timezone(datetime.timedelta(hours=-3, minutes=-30)),
        datetime.timezone(datetime.timedelta(hours=14)),
        datetime.timezone(datetime.timedelta(hours=-12)),
    ]
    # Keep away from 0001-01-01 / 9999-12-31 UTC shift overflow when offset pushes year out of [1..9999].
    dt_strat = st.datetimes(
        min_value=datetime.datetime(2, 1, 1),
        max_value=datetime.datetime(9998, 12, 31, 23, 59, 59, 999999),
    )
    if engine == "duckdb":
        dt_strat = st.datetimes(
            min_value=datetime.datetime(1900, 1, 1),
            max_value=datetime.datetime(2200, 12, 31, 23, 59, 59, 999999),
        )
    elif engine == "memgraph":
        dt_strat = st.datetimes(
            min_value=datetime.datetime(1970, 1, 1),
            max_value=datetime.datetime(9998, 12, 31, 23, 59, 59, 999999),
        )
    return st.builds(lambda dt, tz: dt.replace(tzinfo=tz), dt_strat, st.sampled_from(offsets))


def _strat_datetime_zone(_engine: str, _path: str) -> st.SearchStrategy[datetime.datetime]:
    samples = [
        datetime.datetime(2024, 1, 2, 3, 4, 5, 123456, tzinfo=zoneinfo.ZoneInfo("Europe/Berlin")),
        # Spring-forward / fall-back DST edge instants in Europe/Berlin and America/New_York
        datetime.datetime(2024, 3, 31, 3, 30, 0, tzinfo=zoneinfo.ZoneInfo("Europe/Berlin")),
        datetime.datetime(
            2024, 10, 27, 2, 30, 0, fold=0, tzinfo=zoneinfo.ZoneInfo("Europe/Berlin")
        ),
        datetime.datetime(
            2024, 10, 27, 2, 30, 0, fold=1, tzinfo=zoneinfo.ZoneInfo("Europe/Berlin")
        ),
        datetime.datetime(
            2024, 11, 3, 1, 30, 0, fold=1, tzinfo=zoneinfo.ZoneInfo("America/New_York")
        ),
        datetime.datetime(2024, 6, 1, 12, 0, 0, 500000, tzinfo=zoneinfo.ZoneInfo("Asia/Kathmandu")),
        datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=zoneinfo.ZoneInfo("UTC")),
    ]
    return st.sampled_from(samples)


def _strat_duration(engine: str, _path: str) -> st.SearchStrategy[Duration]:
    if engine == "memgraph":
        # Memgraph forbids months != 0 and folds seconds >= 86400 into days.
        return st.builds(
            lambda d, s, us: Duration(months=0, days=d, seconds=s, nanoseconds=us * 1_000),
            st.integers(-30, 30),
            st.integers(-86_399, 86_399),
            st.integers(-999_999, 999_999),
        )
    if engine == "falkordb":
        return st.builds(
            lambda m, d, s: Duration(months=m, days=d, seconds=s, nanoseconds=0),
            st.integers(-24, 24),
            st.integers(-30, 30),
            st.integers(-86_399, 86_399),
        )
    if engine in ("pg19", "duckdb"):
        # PostgreSQL & DuckDB intervals have microsecond resolution.
        return st.one_of(
            st.sampled_from(
                [
                    Duration(0, 0, 0, 0),
                    Duration(14, 3, 3661, 500_000_000),
                    Duration(0, 0, 90_000, 500_000_000),
                    Duration(1, 0, -1, 500_000_000),
                    Duration(-14, -2, -5, -250_000_000),
                ]
            ),
            st.builds(
                lambda m, d, s, us: Duration(months=m, days=d, seconds=s, nanoseconds=us * 1_000),
                st.integers(-24, 24),
                st.integers(-30, 30),
                st.integers(-100_000, 100_000),
                st.integers(-999_999, 999_999),
            ),
        )
    # Neo4j supports full months, days, seconds, and nanoseconds (including sub-microsecond ns).
    return st.one_of(
        st.sampled_from(
            [
                Duration(0, 0, 0, 0),
                Duration(14, 3, 3661, 500_000_000),
                Duration(0, 0, 90_000, 500_000_000),
                Duration(1, 0, 0, -500_000_000),
                Duration(0, 0, 1, 123_456_789),
                Duration(-14, -2, -5, -500),
            ]
        ),
        st.builds(
            lambda m, d, s, ns: Duration(months=m, days=d, seconds=s, nanoseconds=ns),
            st.integers(-24, 24),
            st.integers(-30, 30),
            st.integers(-100_000, 100_000),
            st.integers(-999_999_999, 999_999_999),
        ),
    )


def _strat_point(_engine: str, _path: str) -> st.SearchStrategy[Point]:
    return st.one_of(
        st.builds(
            lambda lat, lon: Point(latitude=lat, longitude=lon),
            st.floats(-89.9, 89.9, allow_nan=False, allow_infinity=False),
            st.floats(-179.9, 179.9, allow_nan=False, allow_infinity=False),
        ),
        st.builds(
            lambda lat, lon, h: Point(latitude=lat, longitude=lon, height=h),
            st.floats(-89.9, 89.9, allow_nan=False, allow_infinity=False),
            st.floats(-179.9, 179.9, allow_nan=False, allow_infinity=False),
            st.floats(-1000.0, 10000.0, allow_nan=False, allow_infinity=False),
        ),
        st.builds(
            lambda x, y: Point(x=x, y=y, crs="cartesian"),
            st.floats(-1000.0, 1000.0, allow_nan=False, allow_infinity=False),
            st.floats(-1000.0, 1000.0, allow_nan=False, allow_infinity=False),
        ),
        st.builds(
            lambda x, y, z: Point(x=x, y=y, z=z, crs="cartesian-3d"),
            st.floats(-1000.0, 1000.0, allow_nan=False, allow_infinity=False),
            st.floats(-1000.0, 1000.0, allow_nan=False, allow_infinity=False),
            st.floats(-1000.0, 1000.0, allow_nan=False, allow_infinity=False),
        ),
    )


def _strat_uuid(_engine: str, _path: str) -> st.SearchStrategy[uuid.UUID]:
    return st.one_of(
        st.sampled_from(
            [
                uuid.UUID("00000000-0000-0000-0000-000000000000"),
                uuid.UUID("12345678-1234-5678-1234-567812345678"),
                uuid.UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"),
            ]
        ),
        st.uuids(),
    )


def _json_scalar_strat() -> st.SearchStrategy[Any]:
    alphabet = st.characters(blacklist_categories=("Cs",), blacklist_characters="\x00")
    return st.one_of(
        st.none(),
        st.booleans(),
        st.integers(-1_000_000, 1_000_000),
        st.floats(-1e6, 1e6, allow_nan=False, allow_infinity=False),
        st.text(alphabet=alphabet, max_size=32),
    )


def _strat_json_doc(_engine: str, _path: str) -> st.SearchStrategy[Any]:
    leaf = _json_scalar_strat()
    key = st.text(
        alphabet=st.characters(min_codepoint=97, max_codepoint=122),
        min_size=1,
        max_size=8,
    )
    doc = st.recursive(
        leaf,
        lambda children: st.one_of(
            st.lists(children, max_size=4),
            st.dictionaries(key, children, max_size=4),
        ),
        max_leaves=8,
    )
    return st.one_of(
        st.sampled_from([{}, [], {"a": [1, True, None], "nested": {"k": "v"}}]),
        st.dictionaries(key, doc, max_size=4),
    )


def _strat_list(engine: str, path: str) -> st.SearchStrategy[list[Any]]:
    if engine == "pg19":
        # PG typed arrays are homogeneous 1-D or rectangular multidimensional lists.
        nested_ints = st.integers(0, 3).flatmap(
            lambda rows: st.integers(0, 3).flatmap(
                lambda cols: st.lists(
                    st.lists(
                        st.one_of(st.none(), st.integers(-100, 100)),
                        min_size=cols,
                        max_size=cols,
                    ),
                    min_size=rows,
                    max_size=rows,
                )
            )
        )
        nested_texts = st.lists(
            st.lists(
                st.one_of(
                    st.none(),
                    st.text(
                        alphabet=st.characters(
                            blacklist_categories=("Cs",), blacklist_characters="\x00"
                        ),
                        max_size=12,
                    ),
                ),
                min_size=2,
                max_size=2,
            ),
            min_size=1,
            max_size=2,
        )
        return st.one_of(
            st.lists(st.integers(I64_MIN, I64_MAX), min_size=0, max_size=5),
            st.lists(st.one_of(st.none(), st.integers(I64_MIN, I64_MAX)), min_size=1, max_size=5),
            st.lists(
                st.text(
                    alphabet=st.characters(
                        blacklist_categories=("Cs",), blacklist_characters="\x00"
                    ),
                    max_size=24,
                ),
                min_size=0,
                max_size=4,
            ),
            st.lists(st.dates(), min_size=0, max_size=4),
            st.lists(st.uuids(), min_size=0, max_size=4),
            nested_ints,
            nested_texts,
            st.just([[[1, 2], [3, 4]], [[5, 6], [7, None]]]),
        )
    if engine == "neo4j" and path in ("stored", "node_prop", "rel_prop"):
        # Neo4j properties only allow homogeneous non-null primitive arrays.
        return st.one_of(
            st.lists(st.integers(I64_MIN, I64_MAX), min_size=0, max_size=5),
            st.lists(st.floats(allow_nan=False, allow_infinity=False, width=64), max_size=4),
            st.lists(st.booleans(), min_size=0, max_size=4),
            st.lists(st.dates(), min_size=0, max_size=4),
            st.lists(st.text(max_size=24), min_size=0, max_size=4),
        )
    if engine == "falkordb":
        # FalkorDB lists hold scalars (int, float, bool, str) or nested int lists;
        # stored properties forbid nulls and maps inside lists.
        if path in ("stored", "node_prop", "rel_prop"):
            return st.one_of(
                st.lists(st.integers(I64_MIN, I64_MAX), min_size=0, max_size=5),
                st.lists(st.floats(allow_nan=False, allow_infinity=False, width=64), max_size=4),
                st.lists(st.booleans(), min_size=0, max_size=4),
                st.lists(st.lists(st.integers(-100, 100), max_size=3), min_size=0, max_size=3),
                st.just([1, 2.5, "x", True]),
            )
        return st.one_of(
            st.just([]),
            st.just([1, None, 3]),
            st.just([[1, 2], [3]]),
            st.lists(st.integers(I64_MIN, I64_MAX), max_size=4),
            st.lists(_json_scalar_strat(), max_size=4),
        )
    if engine == "duckdb":
        return st.one_of(
            st.lists(st.integers(I64_MIN, I64_MAX), min_size=0, max_size=5),
            st.lists(st.one_of(st.none(), st.integers(-1000, 1000)), min_size=1, max_size=5),
            st.lists(st.lists(st.integers(-100, 100), max_size=3), min_size=0, max_size=3),
            st.lists(st.dates(), min_size=0, max_size=4),
        )
    if engine == "age":
        return st.one_of(
            st.just([]),
            st.lists(st.integers(-100_000, 100_000), max_size=4),
            st.lists(_json_scalar_strat(), max_size=4),
            st.lists(st.lists(st.integers(-100, 100), max_size=3), max_size=3),
        )
    # Neo4j / Memgraph RETURN $v and Memgraph properties support mixed/nested/null lists.
    return st.one_of(
        st.just([]),
        st.just([1, None, 3]),
        st.just([[1, 2], [3]]),
        st.just([1, 2.5, 9_007_199_254_740_993]),
        st.lists(st.integers(I64_MIN, I64_MAX), max_size=4),
        st.lists(st.dates(), max_size=3),
        st.lists(_json_scalar_strat(), max_size=4),
    )


def _strat_map(engine: str, _path: str) -> st.SearchStrategy[dict[str, Any]]:
    key = st.text(
        alphabet=st.characters(min_codepoint=97, max_codepoint=122),
        min_size=1,
        max_size=8,
    )
    if engine in ("pg19", "age", "duckdb"):
        return st.one_of(
            st.just({}),
            st.dictionaries(key, _json_scalar_strat(), max_size=4),
        )
    # Neo4j / Memgraph / FalkorDB RETURN $v maps can hold scalars and dates too.
    val = st.one_of(_json_scalar_strat(), st.dates())
    if engine == "falkordb":
        val = _json_scalar_strat()
    return st.one_of(
        st.just({}),
        st.dictionaries(key, val, max_size=4),
    )


_STRATEGY_BUILDERS: dict[str, Callable[[str, str], st.SearchStrategy[Any]]] = {
    "null": _strat_null,
    "bool": _strat_bool,
    "int": _strat_int,
    "float": _strat_float,
    "decimal": _strat_decimal,
    "str": _strat_str,
    "bytes": _strat_bytes,
    "date": _strat_date,
    "time_naive": _strat_time_naive,
    "time_offset": _strat_time_offset,
    "datetime_naive": _strat_datetime_naive,
    "datetime_offset": _strat_datetime_offset,
    "datetime_zone": _strat_datetime_zone,
    "duration": _strat_duration,
    "point": _strat_point,
    "uuid": _strat_uuid,
    "json_doc": _strat_json_doc,
    "list": _strat_list,
    "map": _strat_map,
}
