"""Query Spec conformance corpus compile runner (issue #253).

Validates every scenario.json against scenario.schema.json, every query.spec.json
against spec/query_spec.v1.schema.json, and verifies multi-dialect compilation
against expected files for every supported and known-diff target.
"""

from __future__ import annotations

import difflib
import json
import re
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator
from voyager_ogm import VoyagerError
from voyager_ogm._voyager_rs import compile_query_from_spec

REPO_ROOT = Path(__file__).resolve().parents[3]
CORPUS_DIR = REPO_ROOT / "spec" / "corpus"
SCENARIO_SCHEMA_PATH = CORPUS_DIR / "scenario.schema.json"
QUERY_SPEC_SCHEMA_PATH = REPO_ROOT / "spec" / "query_spec.v1.schema.json"

TARGET_DIALECT_MAP: dict[str, str] = {
    "cypher": "cypher",
    "age": "age",
    "gql": "iso_gql",
    "sqlpgq": "sql_pgq",
}

TARGET_FILE_MAP: dict[str, str] = {
    "cypher": "expected.cypher",
    "age": "expected.age.sql",
    "gql": "expected.gql",
    "sqlpgq": "expected.sqlpgq.sql",
}


def load_json(path: Path) -> dict[str, Any]:
    """Load one JSON file."""
    return json.loads(path.read_text(encoding="utf-8"))


SCENARIO_SCHEMA = load_json(SCENARIO_SCHEMA_PATH) if SCENARIO_SCHEMA_PATH.is_file() else {}
QUERY_SPEC_SCHEMA = load_json(QUERY_SPEC_SCHEMA_PATH) if QUERY_SPEC_SCHEMA_PATH.is_file() else {}


def discover_scenarios() -> list[tuple[str, Path]]:
    """Discover all scenario directories as (scenario_id, path)."""
    if not CORPUS_DIR.is_dir():
        return []
    scenarios = []
    for pack_dir in sorted(CORPUS_DIR.iterdir()):
        if not pack_dir.is_dir() or pack_dir.name.startswith("."):
            continue
        for scenario_dir in sorted(pack_dir.iterdir()):
            if not scenario_dir.is_dir() or scenario_dir.name.startswith("."):
                continue
            scenario_id = f"{pack_dir.name}/{scenario_dir.name}"
            scenarios.append((scenario_id, scenario_dir))
    return scenarios


ALL_SCENARIOS = discover_scenarios()


def discover_compile_cases() -> list[tuple[str, str, Path]]:
    """Discover all (scenario_id, target_name, scenario_dir) cases."""
    cases = []
    for scenario_id, scenario_dir in ALL_SCENARIOS:
        for target_name in ("cypher", "age", "gql", "sqlpgq"):
            cases.append((scenario_id, target_name, scenario_dir))
    return cases


ALL_COMPILE_CASES = discover_compile_cases()


def test_scenario_schema_is_valid_draft_2020_12() -> None:
    """The scenario schema itself must be a valid JSON Schema 2020-12 document."""
    assert SCENARIO_SCHEMA, "scenario.schema.json must exist and be non-empty"
    Draft202012Validator.check_schema(SCENARIO_SCHEMA)


def test_corpus_scenarios_exist() -> None:
    """The corpus must contain scenarios."""
    assert ALL_SCENARIOS, "No corpus scenarios found under spec/corpus/"


@pytest.mark.parametrize(
    ("scenario_id", "scenario_dir"),
    ALL_SCENARIOS,
    ids=[s[0] for s in ALL_SCENARIOS],
)
def test_scenario_metadata_and_spec_schema(scenario_id: str, scenario_dir: Path) -> None:
    """Validate scenario.json and query.spec.json against their schemas."""
    scenario_path = scenario_dir / "scenario.json"
    spec_path = scenario_dir / "query.spec.json"

    assert scenario_path.is_file(), f"Missing scenario.json in {scenario_dir}"
    assert spec_path.is_file(), f"Missing query.spec.json in {scenario_dir}"

    scenario_doc = load_json(scenario_path)
    spec_doc = load_json(spec_path)

    # Validate against scenario.schema.json
    Draft202012Validator(SCENARIO_SCHEMA).validate(scenario_doc)

    # Validate scenario.json ID consistency with directory structure
    pack_name, scenario_name = scenario_id.split("/", 1)
    assert scenario_doc["id"] == scenario_id, (
        f"scenario.json id '{scenario_doc['id']}' does not match path '{scenario_id}'"
    )
    assert scenario_doc["pack"] == pack_name, (
        f"scenario.json pack '{scenario_doc['pack']}' does not match directory '{pack_name}'"
    )

    # Validate against query_spec.v1.schema.json
    Draft202012Validator(QUERY_SPEC_SCHEMA).validate(spec_doc)


@pytest.mark.parametrize(
    ("scenario_id", "target_name", "scenario_dir"),
    ALL_COMPILE_CASES,
    ids=[f"{c[0]}-{c[1]}" for c in ALL_COMPILE_CASES],
)
def test_scenario_compile(scenario_id: str, target_name: str, scenario_dir: Path) -> None:
    """Compile query.spec.json and verify supported, unsupported, or known-diff behavior."""
    scenario_doc = load_json(scenario_dir / "scenario.json")
    spec_doc = load_json(scenario_dir / "query.spec.json")

    target_info = scenario_doc["targets"][target_name]
    status = target_info["status"]
    dialect = TARGET_DIALECT_MAP[target_name]
    expected_filename = TARGET_FILE_MAP[target_name]
    expected_path = scenario_dir / expected_filename

    if status == "supported":
        assert expected_path.is_file(), (
            f"Missing expected output file {expected_filename} for supported scenario {scenario_id}"
        )
        expected_text = expected_path.read_text(encoding="utf-8")

        compiled = compile_query_from_spec(spec_doc, dialect)
        actual_statement = compiled["statement"]
        actual_text = f"{actual_statement}\n"

        if actual_text != expected_text:
            diff = "".join(
                difflib.unified_diff(
                    expected_text.splitlines(keepends=True),
                    actual_text.splitlines(keepends=True),
                    fromfile=f"expected/{expected_filename}",
                    tofile=f"actual/{expected_filename}",
                )
            )
            pytest.fail(f"Mismatch for scenario '{scenario_id}' target '{target_name}':\n{diff}")

        # Check parameter coverage: all parameters used in the statement must be covered
        statement_params = set(re.findall(r"\$([A-Za-z_][A-Za-z0-9_]*)", actual_statement))
        extracted_params = set(compiled.get("parameters", {}).keys())
        user_params = statement_params - extracted_params
        scenario_params = set(scenario_doc.get("params", {}).keys())
        missing_params = user_params - scenario_params
        assert not missing_params, (
            f"Statement uses parameter(s) {missing_params} not defined in scenario params for {scenario_id}"
        )

    elif status == "unsupported":
        reason = target_info.get("reason", "")
        assert reason, (
            f"Target '{target_name}' is unsupported but missing required reason in {scenario_id}"
        )
        with pytest.raises(VoyagerError) as exc_info:
            compile_query_from_spec(spec_doc, dialect)
        # Ensure an error code is recorded
        assert exc_info.value.code, (
            f"VoyagerError code missing for unsupported target {target_name}"
        )

    elif status == "known-diff":
        reason = target_info.get("reason", "")
        assert reason, (
            f"Target '{target_name}' is known-diff but missing required reason in {scenario_id}"
        )
        assert expected_path.is_file(), (
            f"Missing expected output file {expected_filename} for known-diff scenario {scenario_id}"
        )
        expected_text = expected_path.read_text(encoding="utf-8")

        compiled = compile_query_from_spec(spec_doc, dialect)
        actual_statement = compiled["statement"]
        actual_text = f"{actual_statement}\n"

        if actual_text != expected_text:
            diff = "".join(
                difflib.unified_diff(
                    expected_text.splitlines(keepends=True),
                    actual_text.splitlines(keepends=True),
                    fromfile=f"expected/{expected_filename}",
                    tofile=f"actual/{expected_filename}",
                )
            )
            pytest.fail(
                f"Mismatch for known-diff scenario '{scenario_id}' target '{target_name}':\n{diff}"
            )
    else:
        pytest.fail(f"Unknown target status '{status}' in {scenario_id}")
