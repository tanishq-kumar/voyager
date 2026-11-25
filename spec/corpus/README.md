# Query Spec Conformance Corpus

This directory contains the versioned conformance corpus for Voyager OGM's Query Spec v1.
The corpus verifies cross-dialect compilation correctness, fluent SDK code generation, and live engine execution across all supported query engines.

## Layout

Scenarios are organized into packs by behavioural milestone:

```text
spec/corpus/
├── scenario.schema.json             # Draft 2020-12 schema for scenario.json
├── NOTICE                           # Attribution notice for openCypher TCK
├── README.md                        # Format documentation and developer guide
└── <pack>/                          # e.g. p0-match-return
    └── <scenario>/                  # short kebab-case directory name
        ├── scenario.json            # Scenario metadata, parameters, and target statuses
        ├── query.spec.json          # Query Spec v1 AST document
        ├── expected.cypher          # Compiled openCypher statement
        ├── expected.age.sql         # Compiled Apache AGE SQL statement
        ├── expected.gql             # Compiled ISO GQL statement
        ├── expected.sqlpgq.sql      # Compiled SQL:2023 PGQ statement
        ├── expected.py              # (Reserved, #234) Fluent Python builder code
        ├── expected.rs              # (Reserved, #234) Fluent Rust builder code
        ├── rows.json                # (Reserved, #254) Expected live engine result rows
        └── post_state.json          # (Reserved, #260 / #254) Post-execution graph state for writes
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
| `fixture` | `string` or `null` | Reference to graph dataset fixture (reserved for #254; currently `null`). |
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

- **`supported`**: The compiler fully supports this construct for this dialect. The compilation runner asserts that `compile_query_from_spec` succeeds, statement text matches the `expected.*` file exactly, and all statement parameter placeholders are satisfied by `params` and compiler-extracted parameters. Note that `supported` means "compiles and is believed to run"; issue #254 replaces belief with live evidence.
- **`unsupported`**: The construct cannot be expressed in this dialect or is not yet implemented. The compilation runner asserts that `compile_query_from_spec` raises `VoyagerError` with the error code recorded in `reason`.
- **`known-diff`**: The dialect compiles the query, but current compiler output differs from the ideal standard query or is rejected by live engine behaviour (e.g. issues #285 and #286). The `expected.*` file captures today's compiler output to guard against unintentional regressions, and `reason` documents why it differs from the ideal or live engine behaviour.

## Adding a Scenario

1. Identify the behavioural feature and cite an official source:
   - For openCypher behaviours: locate the matching feature file under `test_data/tck/opencypher/` and note its relative path and scenario title.
   - For PostgreSQL / PGQ behaviours: cite the PostgreSQL regression test file and test name.
   - For Voyager-specific extensions or when no TCK scenario exists: set `kind: "voyager"` and explain why in `description`.
2. Check the fluent SDK output:
   - Write the query with the fluent Python SDK (`Query.match(...)...`).
   - Run `query.to_spec_dict()` to produce the canonical Query Spec v1 representation.
3. Create `spec/corpus/<pack>/<scenario>/`:
   - Create `scenario.json` with target statuses.
   - Create `query.spec.json` with the Query Spec v1 payload.
4. Generate and inspect expected compilation files:
   - Run `just corpus-update` to compile all supported and known-diff targets into `expected.*`.
   - Read every generated `expected.*` file. If an emitter produces invalid or sub-optimal SQL/GQL, mark that target as `known-diff` with an explanation.
5. Run verification:
   - Run `just corpus` to ensure all scenarios pass schema validation and compilation checks.

## Updating Expected Files

When making an intended compiler or emitter change:

```bash
just corpus-update
```

This runs `scripts/update_corpus.py` to re-emit all `expected.*` files for `supported` and `known-diff` targets and prints `git diff --stat spec/corpus`. Review the git diff carefully to confirm that only intended statements changed.

## Roadmap & Milestone Checks

- **#253 (Current)**: Scenario format, JSON Schema, compilation runner (`just corpus`), update tool (`just corpus-update`), and Pack P0 (MATCH / RETURN basics).
- **#254**: Database engine fixtures and live execution harness verifying query results against `rows.json`.
- **#234**: Fluent code generation from Query Spec for Python (`expected.py`) and Rust (`expected.rs`).
- **Packs P1–P7**: Filtering (P1, #255), Ordering & Pagination (P2, #256), Aggregation (P3, #257), Paths & Variable-Length (P4, #258), Optional Match & UNWIND (P5, #259), Writes & DML (P6, #260), Subqueries & CALL (P7, #261).
