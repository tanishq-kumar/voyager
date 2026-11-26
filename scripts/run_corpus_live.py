#!/usr/bin/env python3
"""Run the live corpus checks and print a per-lane, per-engine summary.

Usage::

    just corpus-live
    just corpus-live neo4j
    just corpus-live neo4j bridge
    just corpus-live "age or pg19" all

The first argument is a pytest ``-k`` expression (normally an engine name).  The
second is the lane: ``official`` (default), ``bridge``, ``native`` or ``all``.
Test ids look like
``[bridge-neo4j-p0-match-return/match-all-nodes]``.
"""

from __future__ import annotations

import argparse
import importlib
import os
import re
import sys
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
TEST_FILE = REPO_ROOT / "packages" / "python" / "tests" / "test_spec_corpus_live.py"
TESTS_DIR = TEST_FILE.parent

sys.path.insert(0, str(TESTS_DIR))
corpus = importlib.import_module("spec_corpus_live_lib")

LANE_ENV = "VOYAGER_CORPUS_LANE"
TEST_ID_PATTERN = re.compile(r"\[(?P<lane>[a-z]+)-(?P<engine>[a-z0-9]+)-[a-z0-9-]+/")


class SummaryPlugin:
    """Collect one outcome per test id and group it by lane and engine."""

    def __init__(self) -> None:
        self.outcomes: dict[str, str] = {}

    def pytest_runtest_logreport(self, report) -> None:
        if report.when == "call":
            self.outcomes[report.nodeid] = report.outcome
        elif report.when == "setup" and report.outcome == "skipped":
            self.outcomes.setdefault(report.nodeid, "skipped")
        elif report.failed:
            self.outcomes.setdefault(report.nodeid, "failed")

    def summary(self) -> dict[tuple[str, str], Counter]:
        counts: dict[tuple[str, str], Counter] = {}
        for nodeid, outcome in self.outcomes.items():
            match = TEST_ID_PATTERN.search(nodeid)
            lane = match.group("lane") if match else "unknown"
            engine = match.group("engine") if match else "unknown"
            counts.setdefault((lane, engine), Counter())[outcome] += 1
        return counts


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "expression",
        nargs="*",
        help="pytest -k expression selecting engines (default: all engines)",
    )
    parser.add_argument(
        "--lane",
        default=os.environ.get(LANE_ENV) or corpus.DEFAULT_LANE,
        choices=(*corpus.LANES, "all"),
        help="execution lane (default: official)",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    import pytest

    options = _parse_args(argv)
    expression = " or ".join(arg for arg in options.expression if arg and arg not in (".", "all"))
    args = [str(TEST_FILE), "-v", "-m", "live", "-p", "no:cacheprovider"]
    if expression:
        args.extend(["-k", expression])

    os.environ[LANE_ENV] = options.lane

    plugin = SummaryPlugin()
    try:
        with corpus.live_corpus_lock():
            exit_code = pytest.main(args, plugins=[plugin])
    except RuntimeError as err:
        print(f"[ERROR] {err}", file=sys.stderr)
        return 1

    print(f"\nLive corpus summary (lane {options.lane})")
    print(f"{'lane':<10} {'engine':<12} {'passed':>7} {'failed':>7} {'skipped':>8}")
    for lane, engine in sorted(plugin.summary()):
        counts = plugin.summary()[(lane, engine)]
        print(
            f"{lane:<10} {engine:<12} {counts.get('passed', 0):>7} "
            f"{counts.get('failed', 0):>7} {counts.get('skipped', 0):>8}"
        )
    return int(exit_code)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
