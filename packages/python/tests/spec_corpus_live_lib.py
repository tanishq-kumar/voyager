"""Fixtures, engine loaders and result comparison for the Query Spec live corpus (issue #254).

The setup graph of each scenario is an engine-neutral JSON fixture under
``spec/corpus/fixtures``.  A loader writes that fixture into a dedicated graph,
schema or namespace with the engine's own client, never through Voyager's write
compilation.  The compiled corpus statement is then executed and the rows are
compared with ``rows.json``.

Graph engines that only expose one default database (Neo4j Community and
Memgraph) cannot create a throw-away database.  Those loaders tag every loaded
node with a unique ``__corpus_254`` marker and refuse to run when the database
already contains unmarked nodes, so they never delete or read another suite's
data.  The marker is stripped from every result value before comparison.
"""

from __future__ import annotations

import decimal
import json
import os
import re
import socket
import uuid
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from conftest import get_test_host as test_host
from jsonschema import Draft202012Validator
from voyager_ogm.agtype import decode_agtype
from voyager_ogm.entities import GraphNode, GraphPath, GraphRelationship

REPO_ROOT = Path(__file__).resolve().parents[3]
CORPUS_DIR = REPO_ROOT / "spec" / "corpus"
FIXTURES_DIR = CORPUS_DIR / "fixtures"
FIXTURE_SCHEMA_PATH = CORPUS_DIR / "fixture.schema.json"

MARKER_KEY = "__corpus_254"
MARKER_ID_KEY = "__corpus_254_id"

#: The engine-neutral fixture schema used by loaders and tests.
FIXTURE_SCHEMA = json.loads(FIXTURE_SCHEMA_PATH.read_text(encoding="utf-8"))

#: Target dialect per live engine.
ENGINE_DIALECT: dict[str, str] = {
    "neo4j": "cypher",
    "memgraph": "cypher",
    "falkordb": "cypher",
    "age": "age",
    "pg19": "sql_pgq",
}

#: scenario.json target key per live engine.
ENGINE_TARGET: dict[str, str] = {
    "neo4j": "cypher",
    "memgraph": "cypher",
    "falkordb": "cypher",
    "age": "age",
    "pg19": "sqlpgq",
}

ENGINES: tuple[str, ...] = ("neo4j", "memgraph", "falkordb", "age", "pg19")

#: Engines that name unaliased columns after the Cypher expression text verbatim.
CYPHER_COLUMN_ENGINES: frozenset[str] = frozenset({"neo4j", "memgraph", "falkordb"})


class EngineUnavailableError(RuntimeError):
    """Raised when a live engine or one of its Python clients is unavailable."""


class FixtureUnsupportedError(RuntimeError):
    """Raised when a fixture cannot be represented by an engine's data model."""


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw else default


def engine_endpoint(engine: str) -> tuple[str, int]:
    """Return the ``(host, port)`` for a live engine, honouring per-engine env overrides."""
    host = test_host()
    defaults = {"neo4j": 7687, "memgraph": 7688, "falkordb": 6379, "age": 5455, "pg19": 5456}
    env_names = {
        "neo4j": "VOYAGER_TEST_NEO4J_PORT",
        "memgraph": "VOYAGER_TEST_MEMGRAPH_PORT",
        "falkordb": "VOYAGER_TEST_FALKORDB_PORT",
        "age": "VOYAGER_TEST_AGE_PORT",
        "pg19": "VOYAGER_TEST_PG19_PORT",
    }
    return host, _env_int(env_names[engine], defaults[engine])


def is_port_open(host: str, port: int, timeout: float = 2.0) -> bool:
    """Return whether a TCP port accepts connections."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def ensure_engine_available(engine: str) -> None:
    """Raise :class:`EngineUnavailableError` when the engine's port is closed."""
    host, port = engine_endpoint(engine)
    if not is_port_open(host, port):
        raise EngineUnavailableError(f"{engine} is not reachable on {host}:{port}")


def load_json(path: Path) -> dict[str, Any]:
    """Load a JSON document."""
    return json.loads(path.read_text(encoding="utf-8"))


def discover_scenarios() -> list[tuple[str, Path]]:
    """Discover corpus scenarios as ``(scenario_id, directory)`` pairs."""
    if not CORPUS_DIR.is_dir():
        return []
    scenarios: list[tuple[str, Path]] = []
    for pack_dir in sorted(CORPUS_DIR.iterdir()):
        if not pack_dir.is_dir() or pack_dir.name.startswith(".") or pack_dir.name == "fixtures":
            continue
        for scenario_dir in sorted(pack_dir.iterdir()):
            if not scenario_dir.is_dir() or scenario_dir.name.startswith("."):
                continue
            scenarios.append((f"{pack_dir.name}/{scenario_dir.name}", scenario_dir))
    return scenarios


def load_fixture(name: str | None) -> dict[str, Any]:
    """Load and validate a fixture by base name."""
    if name is None:
        return {"nodes": [], "relationships": []}
    path = FIXTURES_DIR / f"{name}.json"
    if not path.is_file():
        raise FileNotFoundError(f"Fixture '{name}' not found at {path}")
    fixture = load_json(path)
    Draft202012Validator(FIXTURE_SCHEMA).validate(fixture)
    return fixture


def engine_status(scenario: dict[str, Any], engine: str) -> dict[str, Any]:
    """Return the scenario's status for a live engine.

    ``scenario.json`` may carry an ``engines`` map with per-engine evidence.
    Without it the status of the engine's compilation target is used, so
    scenarios written before the live harness keep working.
    """
    engines = scenario.get("engines") or {}
    if engine in engines:
        return engines[engine]
    return scenario["targets"][ENGINE_TARGET[engine]]


def supported_engines(scenario: dict[str, Any]) -> list[str]:
    """Return the engines whose live status is ``supported``."""
    return [
        engine for engine in ENGINES if engine_status(scenario, engine)["status"] == "supported"
    ]


def scenario_parameters(scenario: dict[str, Any], compiled: dict[str, Any]) -> dict[str, Any]:
    """Merge compiler-extracted parameters with the scenario's user parameters."""
    return {**compiled.get("parameters", {}), **scenario.get("params", {})}


def spec_projection_aliases(spec: dict[str, Any]) -> list[str | None]:
    """Extract explicit projection aliases in column order from a Query Spec document."""
    aliases: list[str | None] = []
    for item in spec.get("projections", []):
        if not isinstance(item, list) or not item:
            aliases.append(None)
            continue
        tag = item[0]
        if tag == "field" and len(item) >= 4:
            alias = item[3]
        elif tag == "expr" and len(item) >= 3:
            alias = item[2]
        elif tag == "agg" and len(item) >= 5:
            alias = item[4]
        else:
            alias = None
        aliases.append(alias if isinstance(alias, str) else None)
    return aliases


def compare_columns(
    expected: Sequence[str] | None,
    actual: Sequence[str],
    *,
    engine: str,
    aliases: Sequence[str | None] | None = None,
) -> list[str]:
    """Compare returned column names against ``rows.json`` according to per-engine rules.

    - When ``expected`` is ``None``, column checking is skipped for backward
      compatibility with legacy ``rows.json`` files without a ``columns`` key.
    - Explicit aliases (from ``aliases``) are checked verbatim on every engine.
    - Unaliased columns are checked verbatim on Cypher engines (``neo4j``, ``memgraph``,
      ``falkordb``) and treated as informational on ``age`` and ``pg19``.
    """
    if expected is None:
        return []

    diffs: list[str] = []
    if len(expected) != len(actual):
        diffs.append(
            f"column count: expected {len(expected)} ({list(expected)}), "
            f"got {len(actual)} ({list(actual)})"
        )

    check_all = engine in CYPHER_COLUMN_ENGINES
    for index, (exp_col, act_col) in enumerate(zip(expected, actual, strict=False)):
        alias = aliases[index] if aliases is not None and index < len(aliases) else None
        if not check_all and alias is None:
            continue
        if act_col != exp_col:
            diffs.append(f"column {index}: expected {exp_col!r}, got {act_col!r}")
    return diffs


# ---------------------------------------------------------------------------
# Canonical result values and comparison
# ---------------------------------------------------------------------------


def _strip_markers(properties: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in properties.items() if k not in (MARKER_KEY, MARKER_ID_KEY)}


def canonicalize(value: Any, engine: str, *, parse_age: bool = True) -> Any:
    """Convert an engine value to the canonical JSON shape used by ``rows.json``.

    Canonical values are JSON scalars, lists, maps, and entity objects::

        {"kind": "node", "labels": ["A"], "properties": {"name": "x"}}
        {"kind": "relationship", "type": "LOOP", "properties": {}}

    Node labels are sorted.  Node and relationship identity is intentionally
    dropped: only labels/type and properties are compared.  ``parse_age`` is
    cleared inside an already-decoded agtype value so a string property such as
    ``"2020"`` is not mistaken for an agtype number.
    """
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, decimal.Decimal):
        return int(value) if value % 1 == 0 else float(value)
    if isinstance(value, str):
        if engine == "age" and parse_age:
            return _parse_age_scalar(value)
        return value
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, GraphNode):
        return {
            "kind": "node",
            "labels": sorted(str(label) for label in value.labels),
            "properties": _strip_markers(
                {
                    str(k): canonicalize(v, engine, parse_age=False)
                    for k, v in value.properties.items()
                }
            ),
        }
    if isinstance(value, GraphRelationship):
        return {
            "kind": "relationship",
            "type": str(value.type),
            "properties": _strip_markers(
                {
                    str(k): canonicalize(v, engine, parse_age=False)
                    for k, v in value.properties.items()
                }
            ),
        }
    if isinstance(value, GraphPath):
        return {
            "kind": "path",
            "nodes": [canonicalize(item, engine, parse_age=False) for item in value.nodes],
            "relationships": [
                canonicalize(item, engine, parse_age=False) for item in value.relationships
            ],
        }
    if isinstance(value, dict):
        kind = value.get("kind") if engine == "age" else None
        if kind == "node":
            properties = value.get("properties") or {}
            return {
                "kind": "node",
                "labels": sorted(str(label) for label in value.get("labels") or []),
                "properties": _strip_markers(
                    {
                        str(k): canonicalize(v, engine, parse_age=False)
                        for k, v in dict(properties).items()
                    }
                ),
            }
        if kind == "relationship":
            properties = value.get("properties") or {}
            return {
                "kind": "relationship",
                "type": str(value.get("type", "")),
                "properties": _strip_markers(
                    {
                        str(k): canonicalize(v, engine, parse_age=False)
                        for k, v in dict(properties).items()
                    }
                ),
            }
        if kind == "path":
            return {
                "kind": "path",
                "nodes": [
                    canonicalize(item, engine, parse_age=False) for item in value.get("nodes") or []
                ],
                "relationships": [
                    canonicalize(item, engine, parse_age=False)
                    for item in value.get("relationships") or []
                ],
            }
        return _strip_markers(
            {str(k): canonicalize(v, engine, parse_age=False) for k, v in value.items()}
        )
    if isinstance(value, (list, tuple)):
        return [canonicalize(item, engine, parse_age=False) for item in value]

    # FalkorDB edge (falkordb.Edge).
    if hasattr(value, "relation"):
        return {
            "kind": "relationship",
            "type": str(value.relation),
            "properties": _strip_markers(
                {
                    str(k): canonicalize(v, engine)
                    for k, v in dict(getattr(value, "properties", {})).items()
                }
            ),
        }
    # Neo4j / Memgraph path (neo4j.Path).
    if hasattr(value, "nodes") and hasattr(value, "relationships"):
        return {
            "kind": "path",
            "nodes": [canonicalize(item, engine) for item in value.nodes],
            "relationships": [canonicalize(item, engine) for item in value.relationships],
        }
    # Neo4j / Memgraph relationship (neo4j.Relationship).
    if hasattr(value, "start_node") and hasattr(value, "type"):
        return {
            "kind": "relationship",
            "type": str(value.type),
            "properties": _strip_markers(
                {str(k): canonicalize(v, engine) for k, v in dict(value).items()}
            ),
        }
    # Neo4j / Memgraph and FalkorDB nodes.  The Neo4j 6 Node is a Mapping and
    # exposes labels without a ``properties`` attribute.
    if hasattr(value, "labels") or hasattr(value, "properties"):
        labels = getattr(value, "labels", None) or []
        properties = getattr(value, "properties", None)
        if properties is None:
            properties = dict(value)
        return {
            "kind": "node",
            "labels": sorted(str(label) for label in labels),
            "properties": _strip_markers(
                {str(k): canonicalize(v, engine) for k, v in dict(properties).items()}
            ),
        }
    return str(value)


def _parse_age_scalar(value: Any) -> Any:
    """Decodes agtype text, leaving a value that is already decoded untouched."""
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not text:
        return value
    try:
        return decode_agtype(text)
    except Exception:
        return value


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def values_equal(expected: Any, actual: Any) -> bool:
    """Compare canonical values with the documented corpus rules.

    Numbers compare numerically (``1`` equals ``1.0``), booleans only equal
    booleans, null only equals null, lists are order-sensitive, maps compare by
    key set, and entities compare labels/type plus properties without identity.
    """
    if expected is None or actual is None:
        return expected is None and actual is None
    if isinstance(expected, bool) or isinstance(actual, bool):
        return isinstance(expected, bool) and isinstance(actual, bool) and expected == actual
    if _is_number(expected) or _is_number(actual):
        return _is_number(expected) and _is_number(actual) and expected == actual
    if isinstance(expected, str) or isinstance(actual, str):
        return isinstance(expected, str) and isinstance(actual, str) and expected == actual
    if isinstance(expected, list) or isinstance(actual, list):
        return (
            isinstance(expected, list)
            and isinstance(actual, list)
            and len(expected) == len(actual)
            and all(values_equal(e, a) for e, a in zip(expected, actual, strict=True))
        )
    if isinstance(expected, dict) or isinstance(actual, dict):
        return (
            isinstance(expected, dict)
            and isinstance(actual, dict)
            and set(expected) == set(actual)
            and all(values_equal(expected[k], actual[k]) for k in expected)
        )
    return expected == actual


def _row_key(value: Any) -> Any:
    """Build a hashable key where numeric types are interchangeable."""
    if isinstance(value, bool) or value is None or isinstance(value, str):
        return (type(value).__name__, value)
    if _is_number(value):
        return ("number", float(value))
    if isinstance(value, list):
        return ("list", tuple(_row_key(item) for item in value))
    if isinstance(value, dict):
        return ("dict", tuple(sorted((k, _row_key(v)) for k, v in value.items())))
    return (type(value).__name__, repr(value))


def _render_row(row: Sequence[Any]) -> str:
    return json.dumps(row, ensure_ascii=False, sort_keys=True)


def compare_rows(
    expected: Sequence[Sequence[Any]],
    actual: Sequence[Sequence[Any]],
    *,
    ordered: bool,
    scenario_id: str = "",
    engine: str = "",
) -> list[str]:
    """Return a list of human-readable differences; an empty list means a match."""
    diffs: list[str] = []
    if len(expected) != len(actual):
        diffs.append(f"row count: expected {len(expected)}, got {len(actual)}")

    if ordered:
        for index, (exp_row, act_row) in enumerate(zip(expected, actual, strict=False)):
            if len(exp_row) != len(act_row):
                diffs.append(f"row {index}: expected {len(exp_row)} columns, got {len(act_row)}")
            elif not all(values_equal(e, a) for e, a in zip(exp_row, act_row, strict=True)):
                diffs.append(
                    f"row {index}: expected {_render_row(exp_row)}, got {_render_row(act_row)}"
                )
        return diffs

    expected_keys = Counter(tuple(_row_key(v) for v in row) for row in expected)
    actual_keys = Counter(tuple(_row_key(v) for v in row) for row in actual)
    missing = expected_keys - actual_keys
    extra = actual_keys - expected_keys
    if missing:
        for key, count in missing.items():
            diffs.append(f"missing row x{count}: {_render_key(key)}")
    if extra:
        for key, count in extra.items():
            diffs.append(f"unexpected row x{count}: {_render_key(key)}")
    return diffs


def _render_key(key: tuple[Any, ...]) -> str:
    return json.dumps([_key_to_json(value) for value in key], sort_keys=True)


def _key_to_json(value: Any) -> Any:
    kind, payload = value
    if kind == "list":
        return [_key_to_json(item) for item in payload]
    if kind == "dict":
        return {name: _key_to_json(item) for name, item in payload}
    if kind == "number":
        return payload
    if kind == "NoneType":
        return None
    return payload


def canonical_rows(
    columns: Sequence[str], rows: Iterable[Sequence[Any]], engine: str
) -> tuple[list[str], list[list[Any]]]:
    """Canonicalize a result set, preserving column order."""
    return list(columns), [[canonicalize(value, engine) for value in row] for row in rows]


# ---------------------------------------------------------------------------
# Engine adapters
# ---------------------------------------------------------------------------


@dataclass
class Execution:
    """One live statement execution."""

    statement: str
    parameters: dict[str, Any]
    columns: list[str]
    rows: list[list[Any]]
    engine: str


@dataclass
class LoadResult:
    """Bookkeeping for one fixture load."""

    engine: str
    namespace: str
    nodes: int = 0
    relationships: int = 0
    extra: dict[str, Any] = field(default_factory=dict)


class BaseEngine:
    """Common adapter surface used by the live tests and the row recorder."""

    name = ""

    def load(self, fixture: dict[str, Any], token: str) -> LoadResult:
        """Load the fixture into a dedicated namespace and return its bookkeeping."""
        raise NotImplementedError

    def cleanup(self, result: LoadResult) -> None:
        """Remove everything the loader created."""
        raise NotImplementedError

    def execute(
        self, statement: str, parameters: dict[str, Any]
    ) -> tuple[list[str], list[list[Any]]]:
        """Execute a compiled statement and return columns and canonical rows."""
        raise NotImplementedError

    def close(self) -> None:
        """Release client resources."""


class BoltEngine(BaseEngine):
    """Neo4j or Memgraph loaded and queried through the official Bolt driver."""

    name = "bolt"

    def __init__(self, engine: str) -> None:
        ensure_engine_available(engine)
        try:
            from neo4j import GraphDatabase
        except ImportError as err:  # pragma: no cover - optional test dependency
            raise EngineUnavailableError("neo4j driver is not installed") from err
        self.name = engine
        host, port = engine_endpoint(engine)
        auth = (
            ("neo4j", os.environ.get("VOYAGER_TEST_NEO4J_PASSWORD", "voyagerpass123"))
            if engine == "neo4j"
            else None
        )
        try:
            self.driver = GraphDatabase.driver(
                f"bolt://{host}:{port}", auth=auth, connection_timeout=5.0
            )
            self.driver.verify_connectivity()
        except Exception as err:  # pragma: no cover - depends on the environment
            raise EngineUnavailableError(f"{engine} is not reachable: {err}") from err

    def load(self, fixture: dict[str, Any], token: str) -> LoadResult:
        marker = f"{self.name}-{token}"
        with self.driver.session() as session:
            unmarked = session.run(
                f"MATCH (n) WHERE n.{MARKER_KEY} IS NULL RETURN count(n) AS c"
            ).single()["c"]
            if unmarked:
                raise EngineUnavailableError(
                    f"{self.name} already contains {unmarked} unmarked node(s); "
                    "refusing to mix corpus data with another suite's data"
                )
            try:
                session.run(
                    f"MATCH (n) WHERE n.{MARKER_KEY} = $token DETACH DELETE n",
                    {"token": marker},
                ).consume()
                for node in fixture["nodes"]:
                    labels = "".join(f":{_cypher_ident(label)}" for label in node["labels"])
                    properties = {
                        **node["properties"],
                        MARKER_KEY: marker,
                        MARKER_ID_KEY: node["id"],
                    }
                    session.run(
                        f"CREATE (n{labels}) SET n = $properties", {"properties": properties}
                    ).consume()
                for relationship in fixture["relationships"]:
                    props = dict(relationship["properties"])
                    session.run(
                        "MATCH (a), (b) "
                        f"WHERE a.{MARKER_KEY} = $token AND b.{MARKER_KEY} = $token "
                        f"AND a.{MARKER_ID_KEY} = $start AND b.{MARKER_ID_KEY} = $end "
                        f"CREATE (a)-[r:{_cypher_ident(relationship['type'])}]->(b) "
                        "SET r = $properties",
                        {
                            "token": marker,
                            "start": relationship["start"],
                            "end": relationship["end"],
                            "properties": props,
                        },
                    ).consume()
            except Exception:
                session.run(
                    f"MATCH (n) WHERE n.{MARKER_KEY} = $token DETACH DELETE n",
                    {"token": marker},
                ).consume()
                raise
        return LoadResult(
            engine=self.name,
            namespace=marker,
            nodes=len(fixture["nodes"]),
            relationships=len(fixture["relationships"]),
        )

    def cleanup(self, result: LoadResult) -> None:
        with self.driver.session() as session:
            session.run(
                f"MATCH (n) WHERE n.{MARKER_KEY} = $token DETACH DELETE n",
                {"token": result.namespace},
            ).consume()

    def execute(
        self, statement: str, parameters: dict[str, Any]
    ) -> tuple[list[str], list[list[Any]]]:
        with self.driver.session() as session:
            result = session.run(statement, parameters)
            columns = list(result.keys())
            records = list(result)
        rows = [[canonicalize(value, self.name) for value in record.values()] for record in records]
        return columns, rows

    def close(self) -> None:
        self.driver.close()


class FalkorDBEngine(BaseEngine):
    """FalkorDB loaded and queried through its Python client."""

    name = "falkordb"

    def __init__(self) -> None:
        ensure_engine_available("falkordb")
        try:
            from falkordb import FalkorDB
        except ImportError as err:  # pragma: no cover - optional test dependency
            raise EngineUnavailableError("falkordb client is not installed") from err
        host, port = engine_endpoint("falkordb")
        try:
            self.client = FalkorDB(host=host, port=port)
            self.client.connection.ping()
        except Exception as err:  # pragma: no cover - depends on the environment
            raise EngineUnavailableError(f"falkordb is not reachable: {err}") from err
        self.graph = None
        self.graph_name = ""

    def load(self, fixture: dict[str, Any], token: str) -> LoadResult:
        self.graph_name = f"voyager_corpus_{token}"
        self.graph = self.client.select_graph(self.graph_name)
        self.graph.query("MATCH (n) DETACH DELETE n")
        marker = f"falkordb-{token}"
        try:
            for node in fixture["nodes"]:
                labels = "".join(f":{_cypher_ident(label)}" for label in node["labels"])
                props = {**node["properties"], MARKER_KEY: marker, MARKER_ID_KEY: node["id"]}
                self.graph.query(f"CREATE (n{labels}) SET n = $properties", {"properties": props})
            for relationship in fixture["relationships"]:
                self.graph.query(
                    "MATCH (a), (b) "
                    f"WHERE a.{MARKER_KEY} = $token AND b.{MARKER_KEY} = $token "
                    f"AND a.{MARKER_ID_KEY} = $start AND b.{MARKER_ID_KEY} = $end "
                    f"CREATE (a)-[r:{_cypher_ident(relationship['type'])}]->(b) "
                    "SET r = $properties",
                    {
                        "token": marker,
                        "start": relationship["start"],
                        "end": relationship["end"],
                        "properties": dict(relationship["properties"]),
                    },
                )
        except Exception:
            self.graph.delete()
            raise
        return LoadResult(
            engine=self.name,
            namespace=self.graph_name,
            nodes=len(fixture["nodes"]),
            relationships=len(fixture["relationships"]),
        )

    def cleanup(self, result: LoadResult) -> None:
        self.client.select_graph(result.namespace).delete()

    def execute(
        self, statement: str, parameters: dict[str, Any]
    ) -> tuple[list[str], list[list[Any]]]:
        if self.graph is None:  # pragma: no cover - guarded by load()
            raise RuntimeError("FalkorDB fixture has not been loaded")
        response = self.graph.query(statement, parameters)
        header = getattr(response, "header", None) or []
        columns = [
            str(item[1]) if isinstance(item, (list, tuple)) and len(item) > 1 else str(item)
            for item in header
        ]
        rows = [
            [canonicalize(value, self.name) for value in row]
            for row in (getattr(response, "result_set", None) or [])
        ]
        return columns, rows


def _cypher_ident(name: str) -> str:
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        return name
    escaped = name.replace("`", "``")
    return f"`{escaped}`"


def _cypher_literal(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return json.dumps(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        return "[" + ", ".join(_cypher_literal(item) for item in value) + "]"
    if isinstance(value, dict):
        entries = ", ".join(
            f"{_cypher_ident(str(key))}: {_cypher_literal(item)}" for key, item in value.items()
        )
        return "{" + entries + "}"
    raise FixtureUnsupportedError(f"Cannot express fixture value {value!r} as a Cypher literal")


def _cypher_properties(properties: dict[str, Any]) -> str:
    entries = ", ".join(
        f"{_cypher_ident(str(key))}: {_cypher_literal(value)}" for key, value in properties.items()
    )
    return "{" + entries + "}"


class AgeEngine(BaseEngine):
    """Apache AGE loaded with literal Cypher and queried through Voyager's Postgres bridge."""

    name = "age"

    def __init__(self) -> None:
        ensure_engine_available("age")
        try:
            import psycopg
        except ImportError as err:  # pragma: no cover - optional test dependency
            raise EngineUnavailableError("psycopg is not installed") from err
        host, port = engine_endpoint("age")
        try:
            self.conn = psycopg.connect(
                host=host,
                port=port,
                user=os.environ.get("VOYAGER_TEST_PG_USER", "postgres"),
                password=os.environ.get("VOYAGER_TEST_PG_PASSWORD", "voyagerpass123"),
                dbname=os.environ.get("VOYAGER_TEST_PG_DB", "voyager_graph"),
                connect_timeout=5,
            )
            self.conn.autocommit = True
            with self.conn.cursor() as cur:
                cur.execute("LOAD 'age'; SET search_path = ag_catalog, \"$user\", public;")
        except Exception as err:  # pragma: no cover - depends on the environment
            raise EngineUnavailableError(f"Apache AGE is not reachable: {err}") from err
        self.graph_name = ""

    def _sql(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[tuple[Any, ...]]:
        with self.conn.cursor() as cur:
            cur.execute(statement, parameters or ())
            return list(cur.fetchall()) if cur.description else []

    def load(self, fixture: dict[str, Any], token: str) -> LoadResult:
        for node in fixture["nodes"]:
            if len(node["labels"]) > 1:
                raise FixtureUnsupportedError(
                    "Apache AGE stores at most one label per vertex; "
                    f"node {node['id']!r} has labels {node['labels']}"
                )
        self.graph_name = f"voyager_corpus_{token}"
        existing = self._sql(
            "SELECT count(*) FROM ag_catalog.ag_graph WHERE name = %s", (self.graph_name,)
        )[0][0]
        if existing:
            self._sql(f"SELECT drop_graph('{self.graph_name}', true)")
        self._sql(f"SELECT create_graph('{self.graph_name}')")
        marker = f"age-{token}"
        try:
            for node in fixture["nodes"]:
                labels = "".join(f":{_cypher_ident(label)}" for label in node["labels"])
                props = {**node["properties"], MARKER_KEY: marker, MARKER_ID_KEY: node["id"]}
                self._sql(
                    f"SELECT * FROM cypher('{self.graph_name}', "
                    f"$$ CREATE (n{labels} {_cypher_properties(props)}) RETURN n $$) AS (n agtype)"
                )
            for relationship in fixture["relationships"]:
                self._sql(
                    f"SELECT * FROM cypher('{self.graph_name}', $$ "
                    f"MATCH (a {_cypher_properties({MARKER_ID_KEY: relationship['start']})}), "
                    f"(b {_cypher_properties({MARKER_ID_KEY: relationship['end']})}) "
                    f"CREATE (a)-[r:{_cypher_ident(relationship['type'])} "
                    f"{_cypher_properties(dict(relationship['properties']))}]->(b) RETURN r $$ "
                    ") AS (r agtype)"
                )
        except Exception:
            self._sql(f"SELECT drop_graph('{self.graph_name}', true)")
            raise
        return LoadResult(
            engine=self.name,
            namespace=self.graph_name,
            nodes=len(fixture["nodes"]),
            relationships=len(fixture["relationships"]),
        )

    def cleanup(self, result: LoadResult) -> None:
        existing = self._sql(
            "SELECT count(*) FROM ag_catalog.ag_graph WHERE name = %s", (result.namespace,)
        )[0][0]
        if existing:
            self._sql(f"SELECT drop_graph('{result.namespace}', true)")

    def execute(
        self, statement: str, parameters: dict[str, Any]
    ) -> tuple[list[str], list[list[Any]]]:
        return _postgres_session_execute(
            self.conn,
            statement,
            parameters,
            dialect="age",
            graph_name=self.graph_name,
            engine=self.name,
            parse_age=False,
        )

    def close(self) -> None:
        self.conn.close()


class PG19Engine(BaseEngine):
    """PostgreSQL SQL/PGQ (experimental: PostgreSQL 19 Beta 1-3 and 20 development builds) loaded into plain tables in a dedicated schema."""

    name = "pg19"

    def __init__(self) -> None:
        ensure_engine_available("pg19")
        try:
            import psycopg
        except ImportError as err:  # pragma: no cover - optional test dependency
            raise EngineUnavailableError("psycopg is not installed") from err
        host, port = engine_endpoint("pg19")
        try:
            self.conn = psycopg.connect(
                host=host,
                port=port,
                user=os.environ.get("VOYAGER_TEST_PG_USER", "postgres"),
                password=os.environ.get("VOYAGER_TEST_PG_PASSWORD", "voyagerpass123"),
                dbname=os.environ.get("VOYAGER_TEST_PG_DB", "voyager_graph"),
                connect_timeout=5,
            )
            self.conn.autocommit = True
        except Exception as err:  # pragma: no cover - depends on the environment
            raise EngineUnavailableError(f"PostgreSQL 19 is not reachable: {err}") from err
        self.schema = ""
        self.graph_name = ""

    def load(self, fixture: dict[str, Any], token: str) -> LoadResult:
        self.schema = f"voyager_corpus_{token}"
        self.graph_name = f"voyager_graph_{token}"
        with self.conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {_sql_ident(self.schema)} CASCADE")
            cur.execute(f"CREATE SCHEMA {_sql_ident(self.schema)}")
            cur.execute(f"SET search_path TO {_sql_ident(self.schema)}, public")
            try:
                vertex_tables, node_ids = self._create_vertex_tables(cur, fixture)
                edge_tables = self._create_edge_tables(cur, fixture, node_ids)
                self._create_property_graph(cur, vertex_tables, edge_tables)
            except Exception:
                cur.execute(f"DROP SCHEMA IF EXISTS {_sql_ident(self.schema)} CASCADE")
                raise
        return LoadResult(
            engine=self.name,
            namespace=self.schema,
            nodes=len(fixture["nodes"]),
            relationships=len(fixture["relationships"]),
            extra={"graph_name": self.graph_name},
        )

    def _create_vertex_tables(
        self, cur: Any, fixture: dict[str, Any]
    ) -> tuple[list[tuple[str, list[str]]], dict[str, tuple[str, int]]]:
        groups: dict[str, list[dict[str, Any]]] = {}
        for node in fixture["nodes"]:
            if len(node["labels"]) > 1:
                raise FixtureUnsupportedError(
                    "PostgreSQL SQL/PGQ needs one vertex table per label and cannot load a "
                    f"multi-label node; node {node['id']!r} has labels {node['labels']}"
                )
            table = _table_name(node["labels"][0]) if node["labels"] else "corpus_node"
            groups.setdefault(table, []).append(node)

        vertex_tables: list[tuple[str, list[str]]] = []
        node_ids: dict[str, tuple[str, int]] = {}
        for table, nodes in groups.items():
            properties: dict[str, str] = {}
            for node in nodes:
                for key, value in node["properties"].items():
                    inferred = _pg_type(value)
                    if key in properties and properties[key] != inferred:
                        raise FixtureUnsupportedError(
                            f"Property {key!r} has mixed SQL types in table {table!r}"
                        )
                    properties[key] = inferred
            columns = ["__id bigint PRIMARY KEY"]
            columns.extend(f"{_sql_ident(key)} {sql_type}" for key, sql_type in properties.items())
            cur.execute(f"CREATE TABLE {_sql_ident(table)} ({', '.join(columns)})")
            property_names = list(properties)
            rows = []
            for index, node in enumerate(nodes, start=1):
                node_ids[node["id"]] = (table, index)
                rows.append([index] + [node["properties"].get(key) for key in property_names])
            if rows:
                placeholders = ", ".join(["%s"] * (1 + len(property_names)))
                cur.executemany(f"INSERT INTO {_sql_ident(table)} VALUES ({placeholders})", rows)
            vertex_tables.append((table, [node["labels"][0]] if node["labels"] else []))
        return vertex_tables, node_ids

    def _create_edge_tables(
        self,
        cur: Any,
        fixture: dict[str, Any],
        node_ids: dict[str, tuple[str, int]],
    ) -> list[tuple[str, str, str, list[str]]]:
        groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
        for relationship in fixture["relationships"]:
            try:
                start_table, _ = node_ids[relationship["start"]]
                end_table, _ = node_ids[relationship["end"]]
            except KeyError as err:
                raise FixtureUnsupportedError(
                    f"Relationship {relationship['id']!r} references unknown node {err}"
                ) from err
            key = (relationship["type"], start_table, end_table)
            groups.setdefault(key, []).append(relationship)

        edge_tables: list[tuple[str, str, str, list[str]]] = []
        for index, ((rel_type, start_table, end_table), relationships) in enumerate(groups.items()):
            table = f"corpus_edge_{index}"
            properties: dict[str, str] = {}
            for relationship in relationships:
                for key, value in relationship["properties"].items():
                    inferred = _pg_type(value)
                    if key in properties and properties[key] != inferred:
                        raise FixtureUnsupportedError(
                            f"Property {key!r} has mixed SQL types in edge table {table!r}"
                        )
                    properties[key] = inferred
            columns = [
                "__id bigint PRIMARY KEY",
                "__source bigint NOT NULL",
                "__target bigint NOT NULL",
            ]
            columns.extend(f"{_sql_ident(key)} {sql_type}" for key, sql_type in properties.items())
            cur.execute(f"CREATE TABLE {_sql_ident(table)} ({', '.join(columns)})")
            property_names = list(properties)
            rows = []
            for edge_index, relationship in enumerate(relationships, start=1):
                _, source_id = node_ids[relationship["start"]]
                _, target_id = node_ids[relationship["end"]]
                rows.append(
                    [edge_index, source_id, target_id]
                    + [relationship["properties"].get(key) for key in property_names]
                )
            if rows:
                placeholders = ", ".join(["%s"] * (3 + len(property_names)))
                cur.executemany(f"INSERT INTO {_sql_ident(table)} VALUES ({placeholders})", rows)
            edge_tables.append((table, start_table, end_table, [rel_type]))
        return edge_tables

    def _create_property_graph(
        self,
        cur: Any,
        vertex_tables: list[tuple[str, list[str]]],
        edge_tables: list[tuple[str, list[str], str, list[str]]],
    ) -> None:
        vertex_parts = []
        for table, labels in vertex_tables:
            label_sql = f" LABEL {_sql_label(labels[0])}" if labels else ""
            vertex_parts.append(f"{_sql_ident(table)} KEY (__id){label_sql}")
        edge_parts = []
        for table, start_table, end_table, labels in edge_tables:
            edge_parts.append(
                f"{_sql_ident(table)} "
                f"SOURCE KEY (__source) REFERENCES {_sql_ident(start_table)} (__id) "
                f"DESTINATION KEY (__target) REFERENCES {_sql_ident(end_table)} (__id) "
                f"LABEL {_sql_label(labels[0])}"
            )
        vertex_sql = ",\n      ".join(vertex_parts) if vertex_parts else ""
        ddl = (
            f"CREATE PROPERTY GRAPH {_sql_ident(self.graph_name)} "
            f"VERTEX TABLES (\n      {vertex_sql}\n    )"
        )
        if edge_parts:
            ddl += " EDGE TABLES (\n      " + ",\n      ".join(edge_parts) + "\n    )"
        cur.execute(ddl)

    def cleanup(self, result: LoadResult) -> None:
        with self.conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {_sql_ident(result.namespace)} CASCADE")

    def execute(
        self, statement: str, parameters: dict[str, Any]
    ) -> tuple[list[str], list[list[Any]]]:
        return _postgres_session_execute(
            self.conn,
            statement,
            parameters,
            dialect="sql_pgq",
            graph_name=None,
            engine=self.name,
            parse_age=True,
        )


def _postgres_session_execute(
    conn: Any,
    statement: str,
    parameters: dict[str, Any],
    *,
    dialect: str,
    graph_name: str | None,
    engine: str,
    parse_age: bool,
) -> tuple[list[str], list[list[Any]]]:
    """Execute via Session/PostgresBridge and return columns and canonical rows."""
    from voyager_ogm import Session
    from voyager_ogm.bridge import PostgresBridge

    bridge = PostgresBridge(conn, graph_name=graph_name)
    session = Session(bridge=bridge, dialect=dialect)
    result = session.execute(statement, parameters)
    columns = list(result.columns)
    rows = [
        [canonicalize(value, engine, parse_age=parse_age) for value in record.values()]
        for record in result.all()
    ]
    return columns, rows

    def close(self) -> None:
        self.conn.close()


def _table_name(label: str) -> str:
    return "corpus_v_" + re.sub(r"[^A-Za-z0-9_]", "_", label).lower()


def _sql_ident(name: str) -> str:
    # The SQL/PGQ emitter writes property and table names unquoted, so
    # PostgreSQL folds them to lower case.  Emit safe identifiers unquoted too
    # so the loader and the compiled statement agree; only quote when needed.
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        return name
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _sql_label(name: str) -> str:
    # The SQL/PGQ emitter double-quotes labels in IS patterns (for example
    # IS "Person"), so the property graph must declare the label with the same
    # exact case.  Always quote labels instead of relying on lower-case folding.
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _pg_type(value: Any) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "bigint"
    if isinstance(value, float):
        return "double precision"
    if isinstance(value, str):
        return "text"
    if isinstance(value, list):
        if not value:
            raise FixtureUnsupportedError("Empty list properties have no inferable SQL type")
        element_types = {_pg_type(item) for item in value}
        if len(element_types) != 1:
            raise FixtureUnsupportedError(f"Mixed list element types: {element_types}")
        return f"{element_types.pop()}[]"
    if isinstance(value, dict):
        return "jsonb"
    if value is None:
        raise FixtureUnsupportedError("null-only properties have no inferable SQL type")
    raise FixtureUnsupportedError(f"Unsupported fixture value {value!r}")


def make_engine(engine: str) -> BaseEngine:
    """Create the adapter for a live engine."""
    if engine in ("neo4j", "memgraph"):
        return BoltEngine(engine)
    if engine == "falkordb":
        return FalkorDBEngine()
    if engine == "age":
        return AgeEngine()
    if engine == "pg19":
        return PG19Engine()
    raise ValueError(f"Unknown engine {engine!r}")


def new_token() -> str:
    """Return a short unique token used for dedicated graph/schema names."""
    return uuid.uuid4().hex[:12]


# ---------------------------------------------------------------------------
# Scenario execution
# ---------------------------------------------------------------------------


def execute_statement(
    engine: str,
    statement: str,
    parameters: dict[str, Any],
    fixture: dict[str, Any],
    token: str,
    *,
    adapter: BaseEngine | None = None,
) -> Execution:
    """Load the fixture, execute a statement on ``engine``, and canonicalize rows."""
    owned = adapter is None
    active_adapter = make_engine(engine) if adapter is None else adapter
    load_result: LoadResult | None = None
    try:
        load_result = active_adapter.load(fixture, token)
        columns, rows = active_adapter.execute(statement, parameters)
        return Execution(
            statement=statement,
            parameters=parameters,
            columns=columns,
            rows=rows,
            engine=engine,
        )
    finally:
        if load_result is not None:
            active_adapter.cleanup(load_result)
        if owned:
            active_adapter.close()


def execute_scenario(
    engine: str,
    scenario: dict[str, Any],
    spec: dict[str, Any],
    fixture: dict[str, Any],
    token: str,
    *,
    adapter: BaseEngine | None = None,
) -> Execution:
    """Load the fixture, compile for the engine's dialect, execute and canonicalize rows."""
    owned = adapter is None
    active_adapter = make_engine(engine) if adapter is None else adapter
    load_result: LoadResult | None = None
    try:
        load_result = active_adapter.load(fixture, token)
        graph_name = getattr(active_adapter, "graph_name", None) or None
        from voyager_ogm._voyager_rs import compile_query_from_spec

        compiled = compile_query_from_spec(spec, ENGINE_DIALECT[engine], graph_name=graph_name)
        parameters = scenario_parameters(scenario, compiled)
        columns, rows = active_adapter.execute(compiled["statement"], parameters)
        return Execution(
            statement=compiled["statement"],
            parameters=parameters,
            columns=columns,
            rows=rows,
            engine=engine,
        )
    finally:
        if load_result is not None:
            active_adapter.cleanup(load_result)
        if owned:
            active_adapter.close()


def resolve_reference_query(
    scenario: dict[str, Any], spec: dict[str, Any]
) -> tuple[str, dict[str, Any]] | None:
    """Select the openCypher reference statement and parameters for recording ``rows.json``.

    - When ``targets.cypher.status`` is ``"supported"``, compiles ``spec`` for
      ``cypher`` (rejecting a stray ``reference_cypher`` override).
    - When ``targets.cypher.status`` is ``"known-diff"`` or ``"unsupported"``,
      returns the scenario's ``reference_cypher`` statement if present, or
      ``None`` so the recorder never silently uses a known-diff compiled query.
    """
    cypher_status = scenario["targets"]["cypher"]["status"]
    reference = scenario.get("reference_cypher")
    if cypher_status == "supported":
        if reference is not None:
            raise ValueError(
                "reference_cypher must not be set when targets.cypher.status is 'supported'"
            )
        from voyager_ogm._voyager_rs import compile_query_from_spec

        compiled = compile_query_from_spec(spec, "cypher")
        return compiled["statement"], scenario_parameters(scenario, compiled)

    if reference is None:
        return None
    if not isinstance(reference, dict) or not isinstance(reference.get("statement"), str):
        raise ValueError("reference_cypher must be an object with a string 'statement'")
    statement = reference["statement"].strip()
    if not statement:
        raise ValueError("reference_cypher.statement must be non-empty")

    extracted: dict[str, Any] = {}
    if cypher_status == "known-diff":
        from voyager_ogm._voyager_rs import compile_query_from_spec

        try:
            extracted = compile_query_from_spec(spec, "cypher").get("parameters", {})
        except Exception:
            extracted = {}
    params = {**extracted, **scenario.get("params", {}), **(reference.get("params") or {})}
    return statement, params


def record_scenario_rows(
    scenario_id: str,
    scenario: dict[str, Any],
    spec: dict[str, Any],
    token: str,
    *,
    adapter: BaseEngine | None = None,
) -> dict[str, Any]:
    """Execute one scenario on the reference engine (Neo4j) and return rows.json content."""
    resolved = resolve_reference_query(scenario, spec)
    if resolved is None:
        cypher_status = scenario["targets"]["cypher"]["status"]
        raise ValueError(
            f"{scenario_id}: cypher target is {cypher_status!r} and no reference_cypher is configured"
        )
    statement, parameters = resolved
    fixture = load_fixture(scenario.get("fixture"))
    execution = execute_statement("neo4j", statement, parameters, fixture, token, adapter=adapter)
    rows = execution.rows
    if scenario.get("rows") != "ordered":
        rows = sorted(rows, key=_render_row)
    return {"columns": execution.columns, "rows": rows}
