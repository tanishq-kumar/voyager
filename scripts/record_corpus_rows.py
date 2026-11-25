#!/usr/bin/env python3
"""Record ``rows.json`` for corpus scenarios from the reference engine (Neo4j).

Usage::

    just corpus-record-rows
    just corpus-record-rows p0-match-return/match-all-nodes

Each scenario is loaded into Neo4j from its engine-neutral fixture with the
loader in ``packages/python/tests/spec_corpus_live_lib.py``, the compiled
openCypher statement is executed, and the canonical rows are written to
``rows.json``.  Review the diff before committing the recorded rows.
"""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "packages" / "python" / "tests"))

corpus = importlib.import_module("spec_corpus_live_lib")


def main(argv: list[str]) -> int:
    requested = list(argv)
    scenarios = corpus.discover_scenarios()
    if requested:
        wanted = set(requested)
        scenarios = [item for item in scenarios if item[0] in wanted]
        missing = wanted - {scenario_id for scenario_id, _ in scenarios}
        if missing:
            print(f"[ERROR] Unknown scenario(s): {', '.join(sorted(missing))}")
            return 1
    if not scenarios:
        print("[ERROR] No scenarios found")
        return 1

    engine = corpus.make_engine("neo4j")
    written = 0
    try:
        for scenario_id, scenario_dir in scenarios:
            scenario = corpus.load_json(scenario_dir / "scenario.json")
            spec = corpus.load_json(scenario_dir / "query.spec.json")
            resolved = corpus.resolve_reference_query(scenario, spec)
            if resolved is None:
                status = scenario["targets"]["cypher"]["status"]
                print(f"[SKIP] {scenario_id}: cypher target is {status} without reference_cypher")
                continue
            rows_doc = corpus.record_scenario_rows(
                scenario_id, scenario, spec, corpus.new_token(), adapter=engine
            )
            output = scenario_dir / "rows.json"
            output.write_text(
                json.dumps(rows_doc, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
                newline="\n",
            )
            written += 1
            via = " (via reference_cypher)" if scenario.get("reference_cypher") else ""
            print(
                f"[OK] {scenario_id}: {len(rows_doc['rows'])} row(s){via} -> "
                f"{output.relative_to(REPO_ROOT)}"
            )
    except corpus.EngineUnavailableError as err:
        print(f"[ERROR] Neo4j reference engine is unavailable: {err}")
        return 1
    finally:
        engine.close()

    print(f"Recorded rows.json for {written} scenario(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
