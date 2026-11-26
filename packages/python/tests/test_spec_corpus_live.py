"""Live execution checks for the Query Spec conformance corpus (issues #254, #297).

Every scenario whose live status is ``supported`` for an engine is loaded into a
dedicated graph, schema or namespace from its engine-neutral fixture, executed
through one lane, and compared with ``rows.json``.  The ``official`` lane uses
the engine's own Python client (the reference path); ``bridge`` and ``native``
run the same compiled statement through Voyager's Python bridge and native Rust
client.  Run with::

    VOYAGER_TEST_HOST=192.168.1.12 just corpus-live
    VOYAGER_TEST_HOST=192.168.1.12 just corpus-live neo4j bridge
    VOYAGER_TEST_HOST=192.168.1.12 just corpus-live "age or pg19" all
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest
import spec_corpus_live_lib as corpus

pytestmark = pytest.mark.live

SCENARIOS = corpus.discover_scenarios()
SELECTED_LANES = corpus.select_lanes(os.environ.get("VOYAGER_CORPUS_LANE") or corpus.DEFAULT_LANE)
KNOWN_DIFFERENCES = corpus.load_known_lane_differences()


@pytest.fixture(scope="module", autouse=True)
def _live_corpus_run_lock() -> Iterator[None]:
    """Prevent overlapping local live corpus runs against shared databases."""
    with corpus.live_corpus_lock():
        yield


def _cases() -> list[pytest.ParameterSet]:
    cases: list[pytest.ParameterSet] = []
    for scenario_id, scenario_dir in SCENARIOS:
        scenario = corpus.load_json(scenario_dir / "scenario.json")
        for engine in corpus.supported_engines(scenario):
            for lane in SELECTED_LANES:
                if not corpus.lane_applies(engine, lane):
                    continue
                cases.append(
                    pytest.param(
                        scenario_id,
                        engine,
                        lane,
                        scenario_dir,
                        id=f"{lane}-{engine}-{scenario_id}",
                    )
                )
    return cases


CASES = _cases()


@pytest.mark.parametrize(("scenario_id", "engine", "lane", "scenario_dir"), CASES)
def test_live_corpus_scenario(scenario_id: str, engine: str, lane: str, scenario_dir: Path) -> None:
    """Execute a corpus scenario on a live engine and compare its rows."""
    scenario = corpus.load_json(scenario_dir / "scenario.json")
    spec = corpus.load_json(scenario_dir / "query.spec.json")
    rows_path = scenario_dir / "rows.json"
    assert rows_path.is_file(), f"Missing rows.json for live scenario {scenario_id}"
    expected_doc = corpus.load_json(rows_path)
    fixture = corpus.load_fixture(scenario.get("fixture"))

    token = corpus.new_token()
    execution: corpus.Execution | None = None
    error_difference: corpus.ResultDifference | None = None
    try:
        execution = corpus.execute_scenario(engine, scenario, spec, fixture, token, lane=lane)
    except corpus.EngineUnavailableError as err:
        pytest.skip(str(err))
    except corpus.FixtureUnsupportedError as err:
        pytest.fail(f"{scenario_id} cannot be loaded on {engine}: {err}")
    except Exception as err:
        error_difference = corpus.execution_error_difference(
            scenario_id=scenario_id,
            engine=engine,
            lane=lane,
            error=err,
        )

    if error_difference is not None:
        differences = [error_difference]
    else:
        assert execution is not None
        differences = corpus.compare_execution(
            expected_doc,
            execution,
            scenario_id=scenario_id,
            engine=engine,
            lane=lane,
            ordered=scenario.get("rows") == "ordered",
            aliases=corpus.spec_projection_aliases(spec),
            unordered_list_columns=corpus.spec_unordered_list_columns(spec),
        )
    _, unexplained = corpus.partition_known_differences(differences, KNOWN_DIFFERENCES)
    stale = corpus.stale_known_differences(
        KNOWN_DIFFERENCES,
        differences,
        engine=engine,
        lane=lane,
        scenario=scenario_id,
    )

    if not unexplained and not stale:
        return

    lines: list[str] = [f"Corpus mismatch for scenario {scenario_id}, lane {lane}"]
    if unexplained:
        lines.extend(f"  {difference.describe()}" for difference in unexplained)
    if stale:
        for entry in stale:
            column = entry.get("column")
            lines.append(
                f"  known lane difference is fixed, remove from "
                f"{corpus.KNOWN_LANE_DIFFERENCES_PATH.name}: "
                f"engine={entry.get('engine')!r} lane={entry.get('lane')!r} "
                f"scenario={entry.get('scenario')!r} column={column!r} "
                f"reason={entry.get('reason')!r}"
            )
    lines.append(f"  fixture: {scenario.get('fixture')}")
    if execution is not None:
        lines.extend(
            [
                f"  statement: {execution.statement}",
                f"  parameters: {execution.parameters}",
                f"  expected columns: {expected_doc.get('columns')}",
                f"  actual columns: {execution.columns}",
            ]
        )
    pytest.fail("\n".join(lines))
