#!/usr/bin/env python3
"""Update expected output files in the Query Spec conformance corpus.

Iterates over every scenario under `spec/corpus/<pack>/<scenario>/`, compiles its
`query.spec.json` for supported and known-diff targets, and writes the output to
the respective `expected.*` file.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from voyager_ogm._voyager_rs import compile_query_from_spec

REPO_ROOT = Path(__file__).resolve().parents[1]
CORPUS_DIR = REPO_ROOT / "spec" / "corpus"

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
    """Load and parse a JSON file."""
    return json.loads(path.read_text(encoding="utf-8"))


def update_scenario(scenario_dir: Path) -> int:
    """Update expected files for a single scenario. Returns count of files written."""
    scenario_path = scenario_dir / "scenario.json"
    spec_path = scenario_dir / "query.spec.json"

    if not scenario_path.is_file() or not spec_path.is_file():
        return 0

    scenario = load_json(scenario_path)
    spec = load_json(spec_path)
    updated = 0

    targets = scenario.get("targets", {})
    for target_name, target_info in targets.items():
        status = target_info.get("status")
        if status not in ("supported", "known-diff"):
            continue

        dialect = TARGET_DIALECT_MAP.get(target_name)
        file_name = TARGET_FILE_MAP.get(target_name)
        if not dialect or not file_name:
            continue

        try:
            compiled = compile_query_from_spec(spec, dialect)
            statement = compiled["statement"]
            output_file = scenario_dir / file_name
            output_file.write_text(f"{statement}\n", encoding="utf-8", newline="\n")
            updated += 1
        except Exception as err:
            print(
                f"[ERROR] Failed to compile {scenario.get('id', scenario_dir.name)} for target '{target_name}': {err}"
            )

    return updated


def main() -> None:
    """Discover all scenarios and update their expected compilation files."""
    if not CORPUS_DIR.is_dir():
        print(f"Corpus directory not found: {CORPUS_DIR}")
        return

    total_scenarios = 0
    total_files = 0

    # Scenarios are directories matching spec/corpus/<pack>/<scenario>/
    for pack_dir in sorted(CORPUS_DIR.iterdir()):
        if not pack_dir.is_dir() or pack_dir.name.startswith("."):
            continue
        for scenario_dir in sorted(pack_dir.iterdir()):
            if not scenario_dir.is_dir() or scenario_dir.name.startswith("."):
                continue
            count = update_scenario(scenario_dir)
            if count > 0:
                total_scenarios += 1
                total_files += count

    print(
        f"Corpus update complete: updated {total_files} file(s) across {total_scenarios} scenario(s)."
    )


if __name__ == "__main__":
    main()
