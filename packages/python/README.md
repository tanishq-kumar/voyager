# Voyager OGM for Python

**Multi-dialect Object-Graph Mapper (OGM) and query compiler for Python.**

[![PyPI](https://img.shields.io/pypi/v/voyager-ogm.svg?label=PyPI)](https://pypi.org/project/voyager-ogm/)
[![PyPI - Python Versions](https://img.shields.io/pypi/pyversions/voyager-ogm.svg)](https://pypi.org/project/voyager-ogm/)
[![CI](https://github.com/tanishq-kumar/voyager/actions/workflows/ci.yml/badge.svg)](https://github.com/tanishq-kumar/voyager/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-MIT%20OR%20Apache--2.0-blue.svg)](https://github.com/tanishq-kumar/voyager/blob/main/LICENSE)

Voyager OGM lets you define graph models with Python type annotations and build a query once as an abstract syntax tree, then compile it to the dialect your database speaks: openCypher (Neo4j, Memgraph, FalkorDB), SQL:2023 PGQ `GRAPH_TABLE` (DuckDB/DuckPGQ; PostgreSQL SQL/PGQ (experimental: PostgreSQL 19 Beta 1-3 and 20 development builds)), ISO GQL, or Apache AGE. Results stream into Polars DataFrames through the Apache Arrow C Data Interface, and optional extras add Bolt, PostgreSQL, embedded DuckDB and Relate, SQLAlchemy, NetworkX, and notebook integrations.

## Installation

With pip:

```bash
pip install voyager-ogm
```

In a uv project (adds it to `pyproject.toml`):

```bash
uv add voyager-ogm
```

Into an existing virtual environment with uv:

```bash
uv pip install voyager-ogm
```

A base install pulls in only `polars` and `pyarrow` (`tzdata` is added on Windows). Prebuilt wheels are available for **Python 3.11 through 3.14** on:

- **Linux**: `manylinux_2_28` on x86_64 and aarch64
- **macOS**: x86_64 and arm64
- **Windows**: x86_64

The wheels are abi3 (`cp311-abi3`), so one wheel per platform covers every supported Python version.

### Optional extras

Some integrations live behind extras so the base install stays small:

| Extra | Adds | What it enables | Install |
| :--- | :--- | :--- | :--- |
| `neo4j` | `neo4j` | Bolt bridge to Neo4j and Memgraph (`bolt://`, `neo4j://`) | `pip install "voyager-ogm[neo4j]"` |
| `age` | `psycopg[binary]` | PostgreSQL and Apache AGE bridge (`postgresql://`, `age://`) | `pip install "voyager-ogm[age]"` |
| `duckdb` | `duckdb` | Embedded DuckDB / DuckPGQ bridge (`duckdb://`) | `pip install "voyager-ogm[duckdb]"` |
| `relate` | `relate-db` | Embedded Relate bridge (`relate://`, `relatedb://`) | `pip install "voyager-ogm[relate]"` |
| `sqlalchemy` | `sqlalchemy` | SQLAlchemy interop and SQLAlchemy-backed query compilation (`HybridSession`) | `pip install "voyager-ogm[sqlalchemy]"` |
| `analytics` | `networkx` | NetworkX graph export via `to_networkx()` | `pip install "voyager-ogm[analytics]"` |
| `notebook` | `marimo`, `anywidget`, `traitlets` | Interactive `show()` / `explore()` viewer widget in Marimo and Jupyter/VS Code notebooks | `pip install "voyager-ogm[notebook]"` |

In a uv project, swap `pip install` for `uv add` (or use `uv pip install` for an existing environment). Combine extras in one command, with pip:

```bash
pip install "voyager-ogm[neo4j,sqlalchemy]"
```

or with uv:

```bash
uv add "voyager-ogm[neo4j,sqlalchemy]"
```

> The `duckdb` and `age` extras install their drivers on every supported Python version, including 3.14. The FalkorDB/Redis bridge uses the `falkordb` package, which has no extra: `pip install falkordb`.

> **DuckPGQ extension:** SQL/PGQ queries (`CREATE PROPERTY GRAPH`, `GRAPH_TABLE`) on DuckDB need DuckDB 2.0.1 or newer and the [DuckPGQ](https://duckdb.org/community_extensions/extensions/duckpgq) extension. Voyager loads it with `LOAD duckpgq;` before the first SQL/PGQ statement on a connection, including a `duckdb://` URI session and a connection that has never loaded it, and then opts that connection into DuckDB 2.0's grammar-extension API with `SET active_grammar_extensions = ['duckpgq'];`; a session that only runs plain SQL never needs it. If the extension is not installed, that first SQL/PGQ statement raises an error naming DuckPGQ and the `INSTALL duckpgq FROM community;` command; pass `install_duckpgq=True` to `Session`/`AsyncSession` to let Voyager install it first (that downloads the extension). A connection the caller already prepared with `LOAD duckpgq;` is detected, kept loaded, and gets the grammar opt-in.

### Pre-releases

Pre-releases (`X.Y.ZaN`) are published to [TestPyPI](https://test.pypi.org/project/voyager-ogm/), not PyPI. Install one with PyPI as the fallback index for the dependencies, with pip:

```bash
pip install --pre -i https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ voyager-ogm
```

or with uv:

```bash
uv pip install --prerelease allow --index https://test.pypi.org/simple/ --default-index https://pypi.org/simple/ --index-strategy unsafe-best-match voyager-ogm
```

## Quick start

Define models, build one query, and compile it to each dialect:

```python
from voyager_ogm import Node, Relationship, Query, node, relationship


@node(label="Person")
class Person:
    name: str
    age: int
    city: str = "London"


@relationship(type_name="WORKS_AT")
class WorksAt:
    since: int = 2024


@node(label="Company")
class Company:
    name: str


p = Person()
c = Company()
w = WorksAt()

query = (
    Query.match(p)
    .to(w)
    .node(c)
    .where(p.age >= 21, c.name == "Acme Corp")
    .return_(p.name, p.age, company_name=c.name)
    .order_by(p.name)
    .limit(10)
)

# 1. Compile to openCypher (Neo4j, Memgraph, FalkorDB)
cypher = query.compile("cypher")
print(cypher.statement)
# MATCH (_person_0:Person)-[_works_at_0:WORKS_AT]->(_company_0:Company)
# WHERE (_person_0.age >= $p0) AND (_company_0.name = $p1)
# RETURN _person_0.name, _person_0.age, _company_0.name AS company_name
# ORDER BY _person_0.name ASC LIMIT 10

# 2. Compile to SQL:2023 PGQ (DuckDB/DuckPGQ; PostgreSQL SQL/PGQ experimental on 19 Beta 1-3 / 20 dev)
pgq = query.compile("sql_pgq", graph_name="corp_graph")
print(pgq.statement)

# 3. Compile to ISO GQL
gql = query.compile("iso_gql")
print(gql.statement)
```

## Query Result Entities

When a query projects whole graph entities (`RETURN a, r, p`, `collect(n)`, or `{node: a}`), `Session.execute()` returns read-only `GraphNode`, `GraphRelationship`, and `GraphPath` values on every engine and backend (Neo4j, Memgraph, FalkorDB, Apache AGE, and Relate):

```python
from voyager_ogm import GraphNode, GraphRelationship, GraphPath, json_default
import json

row = session.execute("MATCH p=(a:Person)-[r:KNOWS]->(b:Person) RETURN a, r, p").first()

node: GraphNode = row["a"]
node.id  # engine integer ID (when present)
node.element_id  # engine string element_id (Neo4j, Relate) or None
node.labels  # frozenset[str], e.g. frozenset({"Person"})
node.properties  # MappingProxyType read-only view of properties
node["name"]  # subscript, "in", len(), .get(), .keys() read properties directly

rel: GraphRelationship = row["r"]
rel.type  # "KNOWS"
rel.start_id  # start node ID
rel.end_id  # end node ID
rel["since"]  # relationship property access

path: GraphPath = row["p"]
path.nodes  # tuple[GraphNode, ...]
path.relationships  # tuple[GraphRelationship, ...]
len(path)  # number of relationships (hops)
list(path)  # iterates over path.relationships

# Equality and hashing use identity (element_id when not None, else id).
# Convert to kind-tagged dicts or JSON with .to_dict() / json_default:
payload = json.dumps(node, default=json_default)
```

In `execute_to_polars()` and `execute_to_arrow()`, entity columns contain the same `.to_dict()` shape serialized as JSON text (and native Arrow columns carry `voyager.encoding="json"` and, when homogeneous, `voyager.kind` metadata). SQL:2023 PGQ (PostgreSQL 19 and DuckPGQ) projects scalar columns only.

## Features

- **Multi-dialect query compilation** for openCypher, SQL:2023 PGQ (`GRAPH_TABLE`), ISO GQL, and Apache AGE ([overview](https://github.com/tanishq-kumar/voyager/blob/main/README.md#1-multi-dialect-query-compilation), [example](https://github.com/tanishq-kumar/voyager/blob/main/examples/python/08_multi_dialect_compilation.py)).
- **Zero-copy Apache Arrow and Polars streaming** through the Arrow C Data Interface, without materializing Python objects ([overview](https://github.com/tanishq-kumar/voyager/blob/main/README.md#2-zero-copy-apache-arrow--polars-streaming), [example](https://github.com/tanishq-kumar/voyager/blob/main/examples/python/21_data_science_polars_streaming.py)).
- **Rule-based query optimizer** with predicate pushdown, constant folding, and dead-clause elimination ([overview](https://github.com/tanishq-kumar/voyager/blob/main/README.md#3-rule-based-query-optimizer), [example](https://github.com/tanishq-kumar/voyager/blob/main/examples/python/10_query_optimizer_constant_folding.py)).
- **Complex expressions and graph functions** for arithmetic trees, string operations, and built-in graph functions ([overview](https://github.com/tanishq-kumar/voyager/blob/main/README.md#4-complex-expressions--graph-functions), [example](https://github.com/tanishq-kumar/voyager/blob/main/examples/python/12_vector_and_fulltext_search.py)).
- **Real transactions, identity map, and unit of work** for batched writes and consistent in-memory objects ([overview](https://github.com/tanishq-kumar/voyager/blob/main/README.md#5-real-database-transactions-in-memory-identity-map--unit-of-work), [example](https://github.com/tanishq-kumar/voyager/blob/main/examples/python/18_transactions_and_identity_map.py)).
- **Interactive graph viewer** with force-directed physics, schema legend chips, and an inspector drawer ([overview](https://github.com/tanishq-kumar/voyager/blob/main/README.md#6-interactive-graph-viewer), [example](https://github.com/tanishq-kumar/voyager/blob/main/examples/python/27_marimo_graphrag_demo.py)).
- **Database compatibility** with Neo4j, Memgraph, Apache AGE, DuckDB/DuckPGQ, FalkorDB, and PostgreSQL ([overview](https://github.com/tanishq-kumar/voyager/blob/main/README.md#database-compatibility), [example](https://github.com/tanishq-kumar/voyager/blob/main/examples/python/31_live_multi_engine_execution.py)).

## Links

- **Repository**: <https://github.com/tanishq-kumar/voyager>
- **Changelog**: <https://github.com/tanishq-kumar/voyager/blob/main/CHANGELOG.md>
- **Contributing**: <https://github.com/tanishq-kumar/voyager/blob/main/CONTRIBUTING.md>
- **Issue tracker**: <https://github.com/tanishq-kumar/voyager/issues>
- **Rust crates**: [`voyager-ogm`](https://crates.io/crates/voyager-ogm), [`voyager-drivers`](https://crates.io/crates/voyager-drivers), and [`voyager-cli`](https://crates.io/crates/voyager-cli) on crates.io.
- **License**: [MIT](https://github.com/tanishq-kumar/voyager/blob/main/LICENSE-MIT) OR [Apache-2.0](https://github.com/tanishq-kumar/voyager/blob/main/LICENSE-APACHE), at your option.

## License

Voyager OGM is dual-licensed under either the [MIT License](https://github.com/tanishq-kumar/voyager/blob/main/LICENSE-MIT) or the [Apache License, Version 2.0](https://github.com/tanishq-kumar/voyager/blob/main/LICENSE-APACHE), at your option.
