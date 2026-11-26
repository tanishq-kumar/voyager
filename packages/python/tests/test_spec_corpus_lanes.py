"""Unit tests for live corpus lane selection and known lane differences (#297).

These checks need no database; they cover the selector, the structured diff
message, and the known-difference matcher including stale entries.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import spec_corpus_live_lib as corpus

SCENARIOS = {
    scenario_id: corpus.load_json(scenario_dir / "scenario.json")
    for scenario_id, scenario_dir in corpus.discover_scenarios()
}


def _execution(
    columns: list[str],
    rows: list[list[Any]],
    *,
    engine: str = "neo4j",
) -> corpus.Execution:
    return corpus.Execution(
        statement="MATCH (n) RETURN n",
        parameters={},
        columns=columns,
        rows=rows,
        engine=engine,
    )


def _difference(
    *,
    engine: str = "neo4j",
    lane: str = "native",
    scenario: str = "p0-match-return/match-all-nodes",
    column: str | None = "n.age",
) -> corpus.ResultDifference:
    return corpus.ResultDifference(
        scenario=scenario,
        engine=engine,
        lane=lane,
        column=column,
        expected=1,
        actual="1",
        message="row 0 column 'n.age': expected 1, got \"1\"",
    )


def test_select_lanes_defaults_to_official() -> None:
    """The default selector keeps today's official-only behaviour."""
    assert corpus.DEFAULT_LANE == "official"
    assert corpus.select_lanes(None) == ("official",)
    assert corpus.select_lanes("") == ("official",)


def test_select_lanes_named_and_all() -> None:
    """A named lane selects one lane; 'all' selects every lane in order."""
    assert corpus.select_lanes("bridge") == ("bridge",)
    assert corpus.select_lanes(" Native ") == ("native",)
    assert corpus.select_lanes("all") == corpus.LANES
    assert corpus.select_lanes("official") == ("official",)
    with pytest.raises(ValueError, match="Unknown lane"):
        corpus.select_lanes("rust")


@pytest.mark.parametrize("engine", corpus.ENGINES)
def test_every_engine_has_all_lanes(engine: str) -> None:
    """Every live engine has an official, bridge and native execution path."""
    for lane in corpus.LANES:
        assert corpus.lane_applies(engine, lane)
    assert not corpus.lane_applies(engine, "unknown")


def test_known_difference_matches_engine_lane_scenario_and_optional_column() -> None:
    """A known difference covers its exact engine/lane/scenario, and a column when set."""
    difference = _difference(column="n.age")
    entry = {
        "engine": "neo4j",
        "lane": "native",
        "scenario": "p0-match-return/match-all-nodes",
        "column": "n.age",
        "reason": "native Arrow stores mixed columns as strings",
    }
    assert corpus.known_difference_matches(entry, difference)
    assert not corpus.known_difference_matches({**entry, "engine": "memgraph"}, difference)
    assert not corpus.known_difference_matches({**entry, "lane": "bridge"}, difference)
    assert not corpus.known_difference_matches({**entry, "column": "n.name"}, difference)

    scenario_entry = {key: value for key, value in entry.items() if key != "column"}
    assert corpus.known_difference_matches(scenario_entry, difference)


def test_known_difference_partition_listed_and_unlisted() -> None:
    """Listed differences are separated from unexplained ones."""
    difference = _difference()
    entry = {
        "engine": "neo4j",
        "lane": "native",
        "scenario": difference.scenario,
        "column": "n.age",
        "reason": "mixed Arrow column",
    }
    known, unexplained = corpus.partition_known_differences([difference], [entry])
    assert known == [(difference, entry)]
    assert unexplained == []

    known, unexplained = corpus.partition_known_differences([difference], [])
    assert known == []
    assert unexplained == [difference]

    wrong_column = {**entry, "column": "n.name"}
    known, unexplained = corpus.partition_known_differences([difference], [wrong_column])
    assert known == []
    assert unexplained == [difference]


def test_stale_known_difference_is_reported() -> None:
    """A listed difference that no longer occurs must be reported as fixed."""
    entry = {
        "engine": "neo4j",
        "lane": "native",
        "scenario": "p0-match-return/match-all-nodes",
        "column": "n.age",
        "reason": "mixed Arrow column",
    }
    assert (
        corpus.stale_known_differences(
            [entry],
            [_difference()],
            engine="neo4j",
            lane="native",
            scenario="p0-match-return/match-all-nodes",
        )
        == []
    )
    assert corpus.stale_known_differences(
        [entry],
        [],
        engine="neo4j",
        lane="native",
        scenario="p0-match-return/match-all-nodes",
    ) == [entry]
    # The same entry is not stale while another engine/lane/scenario runs.
    assert (
        corpus.stale_known_differences(
            [entry],
            [],
            engine="memgraph",
            lane="native",
            scenario="p0-match-return/match-all-nodes",
        )
        == []
    )
    assert (
        corpus.stale_known_differences(
            [entry],
            [],
            engine="neo4j",
            lane="bridge",
            scenario="p0-match-return/match-all-nodes",
        )
        == []
    )


def test_diff_message_names_scenario_lane_column_expected_and_actual() -> None:
    """A structured row diff carries all the context needed to act on it."""
    expected = {"columns": ["n.name", "n.age"], "rows": [["Alice", 30]]}
    actual = _execution(["n.name", "n.age"], [["Alice", 31]])
    differences = corpus.compare_execution(
        expected,
        actual,
        scenario_id="p1-filtering/where-gt",
        engine="neo4j",
        lane="native",
        ordered=True,
        aliases=[None, None],
    )
    assert len(differences) == 1
    difference = differences[0]
    assert difference.column == "n.age"
    assert difference.expected == 30
    assert difference.actual == 31
    message = difference.describe()
    assert "p1-filtering/where-gt" in message
    assert "native" in message
    assert "'n.age'" in message
    assert "expected 30" in message
    assert "actual 31" in message


def test_diff_message_names_a_bad_column() -> None:
    """A column-name difference names the expected and actual column."""
    expected = {"columns": ["n.name"], "rows": [["Alice"]]}
    actual = _execution(["name"], [["Alice"]])
    differences = corpus.compare_execution(
        expected,
        actual,
        scenario_id="p0-match-return/return-node-property",
        engine="neo4j",
        lane="bridge",
        ordered=True,
        aliases=[None],
    )
    assert len(differences) == 1
    difference = differences[0]
    assert difference.column == "n.name"
    assert difference.expected == "n.name"
    assert difference.actual == "name"
    message = difference.describe()
    assert "bridge" in message
    assert "'n.name'" in message
    assert "actual 'name'" in message


def test_known_lane_differences_file_is_valid() -> None:
    """The known-difference file only names real scenarios, engines and lanes."""
    entries = corpus.load_known_lane_differences()
    assert isinstance(entries, list)
    seen: set[tuple[Any, ...]] = set()
    for entry in entries:
        assert set(entry) <= {"engine", "lane", "scenario", "column", "reason"}
        assert entry.get("engine") in corpus.ENGINES
        assert entry.get("lane") in corpus.LANES
        scenario_id = entry.get("scenario")
        assert scenario_id in SCENARIOS, f"unknown scenario {scenario_id!r}"
        assert entry["engine"] in corpus.supported_engines(SCENARIOS[scenario_id])
        assert corpus.lane_applies(entry["engine"], entry["lane"])
        reason = entry.get("reason")
        assert isinstance(reason, str) and reason.strip(), "reason is required"
        column = entry.get("column")
        assert column is None or isinstance(column, str)
        key = (entry["engine"], entry["lane"], scenario_id, column)
        assert key not in seen, f"duplicate known difference {key}"
        seen.add(key)


def test_known_lane_differences_missing_file_is_empty(tmp_path: Path) -> None:
    """A missing list behaves like an empty list."""
    assert corpus.load_known_lane_differences(tmp_path / "missing.json") == []


def test_known_lane_differences_rejects_non_array(tmp_path: Path) -> None:
    """A malformed list is rejected instead of being ignored."""
    path = tmp_path / "known.json"
    path.write_text(json.dumps({"engine": "neo4j"}), encoding="utf-8")
    with pytest.raises(ValueError, match="JSON array"):
        corpus.load_known_lane_differences(path)


def test_known_lane_differences_rejects_non_object_entry(tmp_path: Path) -> None:
    """Every list item must be an object, not a bare string."""
    path = tmp_path / "known.json"
    path.write_text(json.dumps([{"engine": "neo4j"}, "not-an-entry"]), encoding="utf-8")
    with pytest.raises(ValueError, match="objects"):
        corpus.load_known_lane_differences(path)
