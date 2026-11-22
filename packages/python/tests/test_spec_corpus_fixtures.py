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
    columns = rows_doc.get("columns")
    if columns is not None:
        assert isinstance(columns, list)
        assert all(isinstance(name, str) for name in columns)
    for index, row in enumerate(rows_doc["rows"]):
        assert isinstance(row, list), f"{scenario_id} row {index} is not a list"
        if columns is not None:
            assert len(row) == len(columns), (
                f"{scenario_id} row {index} has {len(row)} values for {len(columns)} columns"
            )
        for value in row:
            _check_canonical_value(value, scenario_id, index)
    assert scenario["rows"] in ("ordered", "unordered")


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
