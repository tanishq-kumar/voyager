#!/usr/bin/env python3
"""Run the live corpus checks and print a per-engine pass/fail/skip summary.

Usage::

    just corpus-live
    just corpus-live neo4j
    just corpus-live "age or pg19"

Arguments are pytest ``-k`` expressions.  The engine is taken from the test id
suffix, for example ``[neo4j-p0-match-return/match-all-nodes]``.
"""

from __future__ import annotations

import re
import sys
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
TEST_FILE = REPO_ROOT / "packages" / "python" / "tests" / "test_spec_corpus_live.py"

ENGINE_PATTERN = re.compile(r"\[(?P<engine>[a-z0-9]+)-[a-z0-9-]+/")


class SummaryPlugin:
    """Collect one outcome per test id and group it by engine."""

    def __init__(self) -> None:
        self.outcomes: dict[str, str] = {}

    def pytest_runtest_logreport(self, report) -> None:
        if report.when == "call":
            self.outcomes[report.nodeid] = report.outcome
        elif report.when == "setup" and report.outcome == "skipped":
            self.outcomes.setdefault(report.nodeid, "skipped")
        elif report.failed:
            self.outcomes.setdefault(report.nodeid, "failed")

    def summary(self) -> dict[str, Counter]:
        counts: dict[str, Counter] = {}
        for nodeid, outcome in self.outcomes.items():
            match = ENGINE_PATTERN.search(nodeid)
            engine = match.group("engine") if match else "unknown"
            counts.setdefault(engine, Counter())[outcome] += 1
        return counts


def main(argv: list[str]) -> int:
    import pytest

    expression = " or ".join(arg for arg in argv if arg and arg != ".")
    args = [str(TEST_FILE), "-v", "-m", "live", "-p", "no:cacheprovider"]
    if expression:
        args.extend(["-k", expression])

    plugin = SummaryPlugin()
    exit_code = pytest.main(args, plugins=[plugin])

    print("\nLive corpus summary")
    print(f"{'engine':<12} {'passed':>7} {'failed':>7} {'skipped':>8}")
    for engine in sorted(plugin.summary()):
        counts = plugin.summary()[engine]
        print(
            f"{engine:<12} {counts.get('passed', 0):>7} "
            f"{counts.get('failed', 0):>7} {counts.get('skipped', 0):>8}"
        )
    return int(exit_code)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
