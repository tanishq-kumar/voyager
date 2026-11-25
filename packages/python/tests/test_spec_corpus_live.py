"""Live execution checks for the Query Spec conformance corpus (issue #254).

Every scenario whose live status is ``supported`` for an engine is loaded into a
dedicated graph, schema or namespace from its engine-neutral fixture, executed
with the compiled statement, and compared with ``rows.json``.  Run with::

    VOYAGER_TEST_HOST=192.168.1.12 just corpus-live
    VOYAGER_TEST_HOST=192.168.1.12 just corpus-live neo4j
"""

from __future__ import annotations

from pathlib import Path

import pytest
import spec_corpus_live_lib as corpus

pytestmark = pytest.mark.live

SCENARIOS = corpus.discover_scenarios()


def _cases() -> list[pytest.ParameterSet]:
    cases: list[pytest.ParameterSet] = []
    for scenario_id, scenario_dir in SCENARIOS:
        scenario = corpus.load_json(scenario_dir / "scenario.json")
        for engine in corpus.supported_engines(scenario):
            cases.append(
                pytest.param(scenario_id, engine, scenario_dir, id=f"{engine}-{scenario_id}")
            )
    return cases


CASES = _cases()


@pytest.mark.parametrize(("scenario_id", "engine", "scenario_dir"), CASES)
def test_live_corpus_scenario(scenario_id: str, engine: str, scenario_dir: Path) -> None:
    """Execute a corpus scenario on a live engine and compare its rows."""
    scenario = corpus.load_json(scenario_dir / "scenario.json")
    spec = corpus.load_json(scenario_dir / "query.spec.json")
    rows_path = scenario_dir / "rows.json"
    assert rows_path.is_file(), f"Missing rows.json for live scenario {scenario_id}"
    expected_doc = corpus.load_json(rows_path)
    fixture = corpus.load_fixture(scenario.get("fixture"))

    token = corpus.new_token()
    try:
        execution = corpus.execute_scenario(engine, scenario, spec, fixture, token)
    except corpus.EngineUnavailableError as err:
        pytest.skip(str(err))
    except corpus.FixtureUnsupportedError as err:
        pytest.fail(f"{scenario_id} cannot be loaded on {engine}: {err}")

    col_diffs = corpus.compare_columns(
        expected_doc.get("columns"),
        execution.columns,
        engine=engine,
        aliases=corpus.spec_projection_aliases(spec),
    )
    row_diffs = corpus.compare_rows(
        expected_doc["rows"],
        execution.rows,
        ordered=scenario.get("rows") == "ordered",
        scenario_id=scenario_id,
        engine=engine,
    )
    diffs = [*col_diffs, *row_diffs]
    if diffs:
        detail = "\n".join(f"  {line}" for line in diffs)
        pytest.fail(
            f"Corpus mismatch\n"
            f"  scenario: {scenario_id}\n"
            f"  engine: {engine}\n"
            f"  fixture: {scenario.get('fixture')}\n"
            f"  statement: {execution.statement}\n"
            f"  parameters: {execution.parameters}\n"
            f"  expected columns: {expected_doc.get('columns')}\n"
            f"  actual columns: {execution.columns}\n"
            f"  diff:\n{detail}"
        )
