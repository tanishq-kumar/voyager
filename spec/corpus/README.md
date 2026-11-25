# Query Spec Conformance Corpus

This directory contains the versioned conformance corpus for Voyager OGM's Query Spec v1.
The corpus verifies cross-dialect compilation correctness, fluent SDK code generation, and live engine execution across all supported query engines.

## Layout

Scenarios are organized into packs by behavioural milestone:

```text
spec/corpus/
├── scenario.schema.json             # Draft 2020-12 schema for scenario.json
├── fixture.schema.json              # Draft 2020-12 schema for fixture files
├── fixtures/                        # Engine-neutral setup graphs shared by scenarios
│   └── <name>.json
├── NOTICE                           # Attribution notice for openCypher TCK
├── README.md                        # Format documentation and developer guide
└── <pack>/                          # e.g. p0-match-return
    └── <scenario>/                  # short kebab-case directory name
        ├── scenario.json            # Scenario metadata, parameters, fixture reference, target statuses
        ├── query.spec.json          # Query Spec v1 AST document
        ├── expected.cypher          # Compiled openCypher statement
        ├── expected.age.sql         # Compiled Apache AGE SQL statement
        ├── expected.gql             # Compiled ISO GQL statement
        ├── expected.sqlpgq.sql      # Compiled SQL:2023 PGQ statement
        ├── expected.py              # (Reserved, #234) Fluent Python builder code
        ├── expected.rs              # (Reserved, #234) Fluent Rust builder code
        ├── rows.json                # Expected live engine result rows (#254)
        └── post_state.json          # (Reserved, #260) Post-execution graph state for writes
```

## `scenario.json` Fields

Every scenario is described by a `scenario.json` file conforming to `scenario.schema.json`:

| Field | Type | Description |
| --- | --- | --- |
| `id` | `string` | Scenario identifier matching `<pack>/<scenario>` (e.g. `p0-match-return/match-all-nodes`). |
| `pack` | `string` | Pack folder name (e.g. `p0-match-return`). |
| `description` | `string` | Brief description of the behaviour being exercised. |
| `source` | `object` | Provenance reference: `kind` (`"opencypher-tck"`, `"postgresql-regress"`, or `"voyager"`), `feature` (relative path under `test_data/tck/opencypher` for TCK), and exact `scenario` title. |
| `params` | `object` | Parameter mapping for query parameters (e.g. `{"name": "Alice"}`). May be `{}` if no parameters are used. |
| `fixture` | `string` | Base name of the engine-neutral setup graph under `spec/corpus/fixtures/`; never `null` for a live scenario. |
| `rows` | `"ordered"` or `"unordered"` | Expected row ordering semantics for result comparison. |
| `targets` | `object` | Compilation targets: `cypher`, `age`, `gql`, `sqlpgq`. |

### Target Statuses

Each target in `targets` has the shape:
```json
{
  "status": "supported" | "unsupported" | "known-diff",
  "reason": "Explanation string (required unless status is 'supported')"
}
```

- **`supported`**: The compiler emits this construct for this dialect and the live engine evidence matches `rows.json` on every engine the dialect covers. The compilation runner also asserts that `compile_query_from_spec` succeeds, statement text matches the `expected.*` file exactly, and all statement parameter placeholders are satisfied by `params` and compiler-extracted parameters.
- **`unsupported`**: The construct cannot be expressed in this dialect or is not yet implemented. The compilation runner asserts that `compile_query_from_spec` raises `VoyagerError` with the error code recorded in `reason`.
- **`known-diff`**: The dialect compiles the query, but current compiler output differs from the ideal standard query or is rejected by live engine behaviour. The `expected.*` file captures today's compiler output to guard against unintentional regressions, and `reason` documents why it differs from the ideal or live engine behaviour.

Live engine coverage per target:

| Target | Compilation dialect | Live engine(s) |
| --- | --- | --- |
| `cypher` | `cypher` | Neo4j (reference), Memgraph, FalkorDB |
| `age` | `age` | Apache AGE |
| `sqlpgq` | `sql_pgq` | PostgreSQL SQL/PGQ (experimental: PostgreSQL 19 Beta 1-3 and 20 development builds) |
| `gql` | `iso_gql` | Compile check only; no GQL engine is available |

## Fixtures

Fixtures are engine-neutral setup graphs shared between scenarios. They live in
`spec/corpus/fixtures/<name>.json`, conform to `fixture.schema.json` (Draft
2020-12), and describe nodes and relationships:

```json
{
  "description": "Match1 [2]: one A, one B with a name, and one unlabeled named node.",
  "nodes": [
    {"id": "a0", "labels": ["A"], "properties": {}},
    {"id": "b0", "labels": ["B"], "properties": {"name": "b"}},
    {"id": "n0", "labels": [], "properties": {"name": "c"}}
  ],
  "relationships": [
    {"id": "r0", "type": "LOOP", "start": "a0", "end": "b0", "properties": {}}
  ]
}
```

`id` values are fixture-local: they link relationships to endpoints and are not
loaded as properties. Node and relationship identifiers are not part of the
result contract (see "Comparison rules"). A TCK fixture reproduces the `Given a
graph` / `And having executed:` blocks of the cited feature file; a
Voyager-specific scenario uses the smallest graph that exercises its behaviour.

Fixtures are loaded by `packages/python/tests/spec_corpus_live_lib.py`, never
through Voyager's own write compilation. Each engine adapter owns its namespace
and leaves the engine clean:

| Engine | Loader | Namespace and cleanup | Mapping notes |
| --- | --- | --- | --- |
| Neo4j | `neo4j` Python driver, literal `CREATE` statements with properties as parameters | Neo4j Community has one database. Loaded nodes carry a unique `__corpus_254` marker; cleanup deletes exactly that marker. The loader refuses to run when the database already contains unmarked nodes, so it never deletes or reads another suite's data. | One node per fixture node; all labels are kept; relationships use the fixture type. The marker is stripped from results. |
| Memgraph | `neo4j` Python driver, same statements as Neo4j | Same marker namespace and cleanup as Neo4j. | Same as Neo4j. |
| FalkorDB | `falkordb` Python client (`GRAPH.QUERY`) | A dedicated graph key `voyager_corpus_<token>`; the graph is deleted after the scenario. | One node per fixture node; labels are kept; unlabeled nodes report an empty label set. |
| Apache AGE | SQL `cypher('<graph>', $$ ... $$)` in a dedicated AGE graph | A dedicated graph `voyager_corpus_<token>` created with `create_graph` and dropped with `drop_graph(..., true)`. | AGE stores at most one label per vertex, so the loader rejects multi-label fixture nodes (those targets are `unsupported`). Property maps are inlined into the `CREATE` statement because AGE does not accept a parameter as a `CREATE` property map; the marker is stripped from results. |
| PostgreSQL 19 Beta 3 (experimental SQL/PGQ) | Plain tables in a dedicated schema plus `CREATE PROPERTY GRAPH` | A dedicated schema `voyager_corpus_<token>` (and property graph in it) created for each scenario and dropped with `DROP SCHEMA ... CASCADE`. | One vertex table per label, plus one unlabeled vertex table; one edge table per (type, start table, end table). `PROPERTIES` exposes the fixture property names. Node properties become typed columns (booleans, integers, floats, text, arrays). Multi-label nodes and duplicate labels across tables cannot be represented, so the loader rejects them (those targets are `unsupported`). SQL/PGQ labels and relationship types are double-quoted so their declared case is preserved; property names stay unquoted and PostgreSQL folds them to lower case. |

Batches of live tests are short and each scenario cleans up in a `finally`
block. If a process dies mid-run, only namespace names carrying the
`voyager_corpus_` prefix can remain; those belong to the corpus and can be
dropped (Neo4j/Memgraph: delete nodes with `__corpus_254`; FalkorDB:
`GRAPH.DELETE voyager_corpus_<token>`; AGE: `drop_graph`; PostgreSQL: `DROP
SCHEMA ... CASCADE`).

## Execution checks

`packages/python/tests/test_spec_corpus_live.py` parameterizes every scenario
against every engine whose target status is `supported`. For each case it:

1. loads the scenario fixture into the engine's dedicated namespace;
2. compiles `query.spec.json` for the engine's dialect (with the dedicated graph
   name for SQL/PGQ and AGE, so the checked-in `expected.*` files are untouched);
3. merges compiler-extracted parameters with `scenario.json` `params`;
4. executes the compiled statement through Voyager's execution path (the
   `Session`/`PostgresBridge` path for Apache AGE and PostgreSQL 19 Beta 3) or the
   engine's own client where entity labels/types must survive (Neo4j, Memgraph,
   FalkorDB, whose Voyager bridge normalizes entities to property maps);
5. canonicalizes the rows and compares them with `rows.json` under the rules
   below.

A mismatch fails with the scenario id, the engine, the statement, the
parameters, the expected and actual columns, and a row-level diff.

Run one engine or all engines (the host defaults to `127.0.0.1`):

```bash
VOYAGER_TEST_HOST=192.168.1.12 just corpus-live neo4j
VOYAGER_TEST_HOST=192.168.1.12 just corpus-live "age or pg19"
VOYAGER_TEST_HOST=192.168.1.12 just corpus-live
```

`just corpus-live` prints a per-engine pass/fail/skip summary. Engine ports can
be overridden with `VOYAGER_TEST_NEO4J_PORT`, `VOYAGER_TEST_MEMGRAPH_PORT`,
`VOYAGER_TEST_FALKORDB_PORT`, `VOYAGER_TEST_AGE_PORT` and
`VOYAGER_TEST_PG19_PORT`; the PostgreSQL user, password and database with
`VOYAGER_TEST_PG_USER`, `VOYAGER_TEST_PG_PASSWORD` and `VOYAGER_TEST_PG_DB`.

## Comparison rules

`rows.json` records the canonical values returned by the reference engine
(Neo4j):

```json
{
  "columns": ["a", "r", "b"],
  "rows": [
    [
      {"kind": "node", "labels": ["A"], "properties": {}},
      {"kind": "relationship", "type": "LOOP", "properties": {}},
      {"kind": "node", "labels": ["B"], "properties": {}}
    ]
  ]
}
```

- **Ordering**: `rows: "ordered"` compares rows position by position;
  `rows: "unordered"` compares them as a multiset, so duplicate rows are
  significant but their order is not.
- **Columns**: column names are informational. Values are compared by position;
  AGE names whole-entity columns `entity`, and SQL/PGQ may name them differently.
- **Numbers**: integers and floats compare numerically, so `1` equals `1.0`.
  Booleans are not numbers.
- **Nulls**: `null` equals only `null`.
- **Lists**: order-sensitive, element by element.
- **Maps**: compare by key set and then value; key order is irrelevant.
- **Nodes**: compare the sorted label set and the property map. Node identity is
  not compared.
- **Relationships**: compare the type and the property map. Relationship
  identity and endpoint identity are not compared.
- **Property values**: compare recursively under the same rules. The loader's
  `__corpus_254` marker properties are removed before comparison.

## Recording rows

Neo4j is the reference engine. Record `rows.json` for every scenario, then read
the result and cross-check it against the TCK's expected result table for the
cited scenario:

```bash
VOYAGER_TEST_HOST=192.168.1.12 just corpus-record-rows
VOYAGER_TEST_HOST=192.168.1.12 just corpus-record-rows p0-match-return/match-all-nodes
```

The recorder compiles and executes the `cypher` target only, sorts rows for
`unordered` scenarios, strips loader markers, and writes `rows.json`. A target
may only be marked `supported` after its live check passes on every engine the
dialect covers; otherwise use `known-diff` (compiles but differs or is rejected
live) or `unsupported` (does not compile) with a reason that names the cause and
starts with the error code for `unsupported` targets. Cite an issue number only
while that issue is still open.

## Adding a Scenario

1. Identify the behavioural feature and cite an official source:
   - For openCypher behaviours: locate the matching feature file under `test_data/tck/opencypher/` and note its relative path and scenario title.
   - For PostgreSQL / PGQ behaviours: cite the PostgreSQL regression test file and test name.
   - For Voyager-specific extensions or when no TCK scenario exists: set `kind: "voyager"` and explain why in `description`.
2. Add or reuse a fixture under `spec/corpus/fixtures/`. Reproduce the TCK
   `Given a graph` / `And having executed:` setup exactly; keep fixtures small.
3. Check the fluent SDK output:
   - Write the query with the fluent Python SDK (`Query.match(...)...`).
   - Run `query.to_spec_dict()` to produce the canonical Query Spec v1 representation.
4. Create `spec/corpus/<pack>/<scenario>/`:
   - Create `scenario.json` with target statuses and the `fixture` name.
   - Create `query.spec.json` with the Query Spec v1 payload.
5. Generate and inspect expected compilation files:
   - Run `just corpus-update` to compile all supported and known-diff targets into `expected.*`.
   - Read every generated `expected.*` file. If an emitter produces invalid or sub-optimal SQL/GQL, mark that target as `known-diff` with an explanation.
6. Record live rows with `just corpus-record-rows <pack>/<scenario>` and read them
   against the cited expected result table.
7. Run verification:
   - Run `just corpus` to ensure all scenarios pass schema validation, fixture validation and compilation checks.
   - Run `just corpus-live` with the remote or local engine host and confirm the
     new scenario passes or is recorded as `known-diff` / `unsupported`.

## Updating Expected Files

When making an intended compiler or emitter change:

```bash
just corpus-update
```

This runs `scripts/update_corpus.py` to re-emit all `expected.*` files for `supported` and `known-diff` targets and prints `git diff --stat spec/corpus`. Review the git diff carefully to confirm that only intended statements changed.

## Roadmap & Milestone Checks

- **#253**: Scenario format, JSON Schema, compilation runner (`just corpus`), update tool (`just corpus-update`), and Pack P0 (MATCH / RETURN basics).
- **#254 (Current)**: Engine-neutral fixtures (`spec/corpus/fixtures`, `fixture.schema.json`), per-engine loaders, live execution checks against `rows.json`, `just corpus-live`, `just corpus-record-rows`, and Pack P0 live evidence on Neo4j, Memgraph, FalkorDB, Apache AGE and PostgreSQL 19 Beta 3.
- **#234**: Fluent code generation from Query Spec for Python (`expected.py`) and Rust (`expected.rs`).
- **Packs P1–P7**: Filtering (P1, #255), Ordering & Pagination (P2, #256), Aggregation (P3, #257), Paths & Variable-Length (P4, #258), Optional Match & UNWIND (P5, #259), Writes & DML (P6, #260), Subqueries & CALL (P7, #261).
