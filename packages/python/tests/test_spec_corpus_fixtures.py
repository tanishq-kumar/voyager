"""Schema and fixture checks for the Query Spec conformance corpus (issue #254).

These checks need no database.  They validate the engine-neutral fixture format,
every scenario's fixture reference, and the shape of the recorded ``rows.json``
files so the live runner can rely on them.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import spec_corpus_live_lib as corpus
from jsonschema import Draft202012Validator

SCENARIOS = corpus.discover_scenarios()
FIXTURE_PATHS = sorted(corpus.FIXTURES_DIR.glob("*.json")) if corpus.FIXTURES_DIR.is_dir() else []


def load_json(path: Path) -> dict[str, Any]:
    """Load one JSON file."""
    return json.loads(path.read_text(encoding="utf-8"))


def test_fixture_schema_is_valid_draft_2020_12() -> None:
    """The fixture schema itself must be a valid JSON Schema 2020-12 document."""
    assert corpus.FIXTURE_SCHEMA, "fixture.schema.json must exist and be non-empty"
    Draft202012Validator.check_schema(corpus.FIXTURE_SCHEMA)


def test_all_fixtures_validate() -> None:
    """Every fixture must validate against the fixture schema."""
    assert FIXTURE_PATHS, "spec/corpus/fixtures must contain at least one fixture"
    for path in FIXTURE_PATHS:
        Draft202012Validator(corpus.FIXTURE_SCHEMA).validate(load_json(path))


@pytest.mark.parametrize("fixture_path", FIXTURE_PATHS, ids=[p.stem for p in FIXTURE_PATHS])
def test_fixture_references_are_internally_consistent(fixture_path: Path) -> None:
    """Fixture node ids are unique and every relationship endpoint exists."""
    fixture = load_json(fixture_path)
    node_ids = [node["id"] for node in fixture["nodes"]]
    assert len(node_ids) == len(set(node_ids)), f"Duplicate node id in {fixture_path.name}"
    known = set(node_ids)
    relationship_ids = [relationship["id"] for relationship in fixture["relationships"]]
    assert len(relationship_ids) == len(set(relationship_ids)), (
        f"Duplicate relationship id in {fixture_path.name}"
    )
    for relationship in fixture["relationships"]:
        assert relationship["start"] in known, (
            f"{fixture_path.name}: relationship {relationship['id']} has unknown start "
            f"{relationship['start']!r}"
        )
        assert relationship["end"] in known, (
            f"{fixture_path.name}: relationship {relationship['id']} has unknown end "
            f"{relationship['end']!r}"
        )


def test_comparison_rules() -> None:
    """Comparison follows the documented rules for numbers, nulls, lists, maps and nodes."""
    first = {"kind": "node", "labels": ["A"], "properties": {"name": "x"}}
    second = {"kind": "node", "labels": ["A"], "properties": {"name": "x"}}
    assert corpus.values_equal(first, second)
    assert corpus.values_equal(1, 1.0)
    assert not corpus.values_equal(True, 1)
    assert not corpus.values_equal(None, 0)
    assert corpus.values_equal([1, 2], [1, 2])
    assert not corpus.values_equal([1, 2], [2, 1])
    assert corpus.values_equal({"a": 1, "b": 2}, {"b": 2, "a": 1})


def test_unordered_comparison_preserves_duplicate_rows() -> None:
    """Unordered comparison is a multiset comparison."""
    expected = [[1], [1], [2]]
    assert corpus.compare_rows(expected, [[2], [1], [1]], ordered=False) == []
    assert corpus.compare_rows(expected, [[1], [2], [2]], ordered=False)


def test_ordered_comparison_reports_row_diff() -> None:
    """Ordered comparison reports the failing row."""
    diffs = corpus.compare_rows([[1]], [[2]], ordered=True, scenario_id="x", engine="e")
    assert diffs
    assert "row 0" in diffs[0]


@pytest.mark.parametrize(("scenario_id", "scenario_dir"), SCENARIOS, ids=[s[0] for s in SCENARIOS])
def test_scenario_fixture_and_rows(scenario_id: str, scenario_dir: Path) -> None:
    """Every scenario references an existing fixture and has canonical recorded rows."""
    scenario = load_json(scenario_dir / "scenario.json")
    fixture_name = scenario["fixture"]
    assert fixture_name, f"{scenario_id} must reference a fixture (issue #254)"
    fixture_path = corpus.FIXTURES_DIR / f"{fixture_name}.json"
    assert fixture_path.is_file(), f"{scenario_id} references missing fixture {fixture_name!r}"

    rows_path = scenario_dir / "rows.json"
    assert rows_path.is_file(), f"{scenario_id} is missing rows.json"
    rows_doc = load_json(rows_path)
    assert isinstance(rows_doc.get("rows"), list), f"{scenario_id} rows.json needs a rows list"
    spec = load_json(scenario_dir / "query.spec.json")
    columns = rows_doc.get("columns")
    assert isinstance(columns, list), f"{scenario_id} rows.json needs a columns list"
    assert all(isinstance(name, str) for name in columns), (
        f"{scenario_id} rows.json columns must all be strings"
    )
    projections = spec.get("projections", [])
    assert len(columns) == len(projections), (
        f"{scenario_id} rows.json has {len(columns)} column(s) for {len(projections)} projection(s)"
    )
    for index, row in enumerate(rows_doc["rows"]):
        assert isinstance(row, list), f"{scenario_id} row {index} is not a list"
        assert len(row) == len(columns), (
            f"{scenario_id} row {index} has {len(row)} values for {len(columns)} columns"
        )
        for value in row:
            _check_canonical_value(value, scenario_id, index)
    assert scenario["rows"] in ("ordered", "unordered")


def test_spec_projection_aliases_extracts_all_projection_forms() -> None:
    """Aliases are read from Query Spec projection items without parsing query text."""
    spec = {
        "projections": [
            ["field", "n", "name", None],
            ["field", "n", "age", "person_age"],
            ["expr", ["prop", "a", ""], "ColumnName"],
            ["expr", ["lit", 1], None],
            ["agg", "n", "age", "count", "cnt"],
            ["agg", "n", "*", "count", None],
        ]
    }
    assert corpus.spec_projection_aliases(spec) == [
        None,
        "person_age",
        "ColumnName",
        None,
        "cnt",
        None,
    ]


def test_column_comparison_alias_mismatch_fails_and_names_column() -> None:
    """Dropped or renamed aliases fail on every engine and report position and column name."""
    for engine in corpus.ENGINES:
        diffs = corpus.compare_columns(
            ["n.name", "person_age"],
            ["n.name", "wrong_alias"],
            engine=engine,
            aliases=[None, "person_age"],
        )
        assert diffs, f"Expected mismatch on {engine}"
        assert "column 1" in diffs[0]
        assert "'person_age'" in diffs[0]
        assert "'wrong_alias'" in diffs[0]

        dropped = corpus.compare_columns(
            ["person_age"],
            ["n.age"],
            engine=engine,
            aliases=["person_age"],
        )
        assert dropped, f"Expected dropped alias mismatch on {engine}"
        assert "column 0" in dropped[0]
        assert "'person_age'" in dropped[0]
        assert "'n.age'" in dropped[0]


def test_column_comparison_unaliased_rules_per_engine() -> None:
    """Cypher engines check all columns; AGE and SQL/PGQ ignore unaliased column names."""
    for engine in ("age", "pg19"):
        # Unaliased columns differ (e.g. 'name' vs 'n.name', 'entity' vs 'a'), which must pass.
        assert (
            corpus.compare_columns(
                ["n.name", "a", "person_age"],
                ["name", "entity", "person_age"],
                engine=engine,
                aliases=[None, None, "person_age"],
            )
            == []
        )
        # Voyager quotes result aliases, so exact case is required on AGE and SQL/PGQ.
        assert (
            corpus.compare_columns(
                ["ColumnName"],
                ["ColumnName"],
                engine=engine,
                aliases=["ColumnName"],
            )
            == []
        )
        folded = corpus.compare_columns(
            ["ColumnName"],
            ["columnname"],
            engine=engine,
            aliases=["ColumnName"],
        )
        assert folded, f"Expected an alias case mismatch on {engine}"
        assert "'ColumnName'" in folded[0]
        assert "'columnname'" in folded[0]

    for engine in ("neo4j", "memgraph", "falkordb"):
        diffs = corpus.compare_columns(
            ["n.name"],
            ["name"],
            engine=engine,
            aliases=[None],
        )
        assert diffs
        assert "column 0" in diffs[0]
        assert "'n.name'" in diffs[0]
        assert "'name'" in diffs[0]


def test_column_comparison_backward_compatible_without_columns() -> None:
    """Legacy rows.json documents omitting 'columns' skip column-name checks."""
    for engine in corpus.ENGINES:
        assert corpus.compare_columns(None, ["anything"], engine=engine, aliases=["alias"]) == []


def test_column_comparison_reports_column_count_mismatch() -> None:
    """Mismatched column count is reported clearly."""
    diffs = corpus.compare_columns(
        ["a", "b"],
        ["a"],
        engine="neo4j",
        aliases=["a", "b"],
    )
    assert any("column count" in d for d in diffs)
    assert any("column 1" in d for d in diffs) or len(diffs) == 1


def test_recorder_reference_statement_selection() -> None:
    """Recorder selects compiled Cypher when supported and reference_cypher when known-diff."""
    supported_scenario = {
        "params": {},
        "fixture": None,
        "rows": "unordered",
        "targets": {"cypher": {"status": "supported"}},
    }
    spec = {
        "spec_version": 1,
        "matches": [{"paths": [[["node", "n", ["Person"]]]]}],
        "projections": [["field", "n", "name", None]],
    }
    resolved = corpus.resolve_reference_query(supported_scenario, spec)
    assert resolved == ("MATCH (n:Person) RETURN n.name", {})

    # Stray reference_cypher on a supported cypher target is rejected.
    with pytest.raises(ValueError, match="supported"):
        corpus.resolve_reference_query(
            {
                **supported_scenario,
                "reference_cypher": {"statement": "RETURN 1", "reason": "invalid"},
            },
            spec,
        )

    # known-diff without reference_cypher must never silently use the compiled statement.
    known_diff_no_ref = {
        "params": {},
        "fixture": None,
        "rows": "unordered",
        "targets": {"cypher": {"status": "known-diff", "reason": "compiler bug"}},
    }
    assert corpus.resolve_reference_query(known_diff_no_ref, spec) is None
    with pytest.raises(ValueError, match="no reference_cypher"):
        corpus.record_scenario_rows("pack/s", known_diff_no_ref, spec, "tok")

    # known-diff with reference_cypher executes the corrected statement, not the compiled one.
    known_diff_with_ref = {
        **known_diff_no_ref,
        "reference_cypher": {
            "statement": "MATCH (n:Person) WHERE n.age <> $min_age RETURN n.name",
            "reason": "current compiler emits != instead of <>",
            "params": {"min_age": 30},
        },
    }
    ref_stmt, ref_params = corpus.resolve_reference_query(known_diff_with_ref, spec)
    assert ref_stmt == "MATCH (n:Person) WHERE n.age <> $min_age RETURN n.name"
    assert ref_params["min_age"] == 30

    # unsupported cypher target with reference_cypher also works without compiling Cypher.
    unsupported_with_ref = {
        "params": {"limit": 5},
        "fixture": None,
        "rows": "ordered",
        "targets": {"cypher": {"status": "unsupported", "reason": "UNSUPPORTED_FEATURE: x"}},
        "reference_cypher": {
            "statement": "MATCH (n:Person) RETURN n.name LIMIT $limit",
            "reason": "manual reference Cypher",
        },
    }
    unsup_stmt, unsup_params = corpus.resolve_reference_query(unsupported_with_ref, spec)
    assert unsup_stmt == "MATCH (n:Person) RETURN n.name LIMIT $limit"
    assert unsup_params == {"limit": 5}

    # Verify record_scenario_rows passes the resolved statement to the engine adapter.
    class StubAdapter(corpus.BaseEngine):
        name = "neo4j"

        def __init__(self) -> None:
            self.executed: list[tuple[str, dict[str, Any]]] = []

        def load(self, fixture: dict[str, Any], token: str) -> corpus.LoadResult:
            return corpus.LoadResult(engine="neo4j", namespace=token)

        def cleanup(self, result: corpus.LoadResult) -> None:
            pass

        def execute(
            self, statement: str, parameters: dict[str, Any]
        ) -> tuple[list[str], list[list[Any]]]:
            self.executed.append((statement, parameters))
            return ["n.name"], [["Alice"]]

    stub = StubAdapter()
    recorded = corpus.record_scenario_rows("pack/s", known_diff_with_ref, spec, "tok", adapter=stub)
    assert stub.executed == [
        ("MATCH (n:Person) WHERE n.age <> $min_age RETURN n.name", {"min_age": 30})
    ]
    assert recorded == {"columns": ["n.name"], "rows": [["Alice"]]}


def test_scenario_schema_validates_reference_cypher() -> None:
    """scenario.schema.json accepts valid reference_cypher on non-supported cypher targets only."""
    schema = load_json(corpus.CORPUS_DIR / "scenario.schema.json")
    validator = Draft202012Validator(schema)
    base = {
        "id": "p0-match-return/sample",
        "pack": "p0-match-return",
        "description": "Sample scenario",
        "source": {"kind": "voyager", "feature": "", "scenario": "sample"},
        "params": {},
        "fixture": "match-all",
        "rows": "unordered",
        "targets": {
            "cypher": {"status": "known-diff", "reason": "diff"},
            "age": {"status": "supported"},
            "gql": {"status": "supported"},
            "sqlpgq": {"status": "supported"},
        },
        "reference_cypher": {
            "statement": "MATCH (n) RETURN n",
            "reason": "Use reference Cypher syntax",
        },
    }
    validator.validate(base)

    # Reject when targets.cypher.status is supported.
    bad_supported = {
        **base,
        "targets": {**base["targets"], "cypher": {"status": "supported"}},
    }
    assert list(validator.iter_errors(bad_supported))

    # Reject empty statement or missing reason.
    bad_empty = {**base, "reference_cypher": {"statement": "", "reason": "x"}}
    assert list(validator.iter_errors(bad_empty))
    bad_no_reason = {**base, "reference_cypher": {"statement": "RETURN 1"}}
    assert list(validator.iter_errors(bad_no_reason))


def _check_canonical_value(value: Any, scenario_id: str, row_index: int) -> None:
    if value is None or isinstance(value, (bool, int, float, str)):
        return
    if isinstance(value, list):
        for item in value:
            _check_canonical_value(item, scenario_id, row_index)
        return
    if isinstance(value, dict):
        if value.get("kind") == "node":
            assert set(value) == {"kind", "labels", "properties"}, (
                f"{scenario_id} row {row_index} has a malformed node value: {value}"
            )
            assert isinstance(value["labels"], list)
            assert isinstance(value["properties"], dict)
        elif value.get("kind") == "relationship":
            assert set(value) == {"kind", "type", "properties"}, (
                f"{scenario_id} row {row_index} has a malformed relationship value: {value}"
            )
            assert isinstance(value["type"], str)
            assert isinstance(value["properties"], dict)
        else:
            raise AssertionError(
                f"{scenario_id} row {row_index} has an uncanonical map value: {value}"
            )
        return
    raise AssertionError(f"{scenario_id} row {row_index} has an unsupported value: {value!r}")
