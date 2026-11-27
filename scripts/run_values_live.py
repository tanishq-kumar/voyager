#!/usr/bin/env python3
"""Run the live value conformance pack and emit the conformance matrix (#299).

Usage::

    just values-live
    just values-live neo4j
    just values-live pg19 --seed 42 --examples 20
    just values-live all --exploratory
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
VALUES_TESTS_DIR = REPO_ROOT / "packages" / "python" / "tests" / "values"


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "engine",
        nargs="?",
        default="all",
        help="Target engine (neo4j, memgraph, falkordb, age, pg19, duckdb) or 'all'",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=int(os.environ.get("VOYAGER_VALUES_SEED", "299")),
        help="Fixed Hypothesis seed (default: 299)",
    )
    parser.add_argument(
        "--examples",
        type=int,
        default=None,
        help="Hypothesis max_examples per cell (default: 5 in CI, 50 in exploratory)",
    )
    parser.add_argument(
        "--exploratory",
        action="store_true",
        help="Enable longer exploratory Hypothesis profile",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    import pytest

    options = _parse_args(argv)
    profile = "exploratory" if options.exploratory else "ci"
    os.environ["VOYAGER_VALUES_PROFILE"] = profile
    os.environ["VOYAGER_VALUES_SEED"] = str(options.seed)
    if options.examples is not None:
        os.environ["VOYAGER_VALUES_EXAMPLES"] = str(options.examples)
    elif profile == "exploratory":
        os.environ.setdefault("VOYAGER_VALUES_EXAMPLES", "50")
    else:
        os.environ.setdefault("VOYAGER_VALUES_EXAMPLES", "5")

    args = [
        str(VALUES_TESTS_DIR / "test_values_conformance.py"),
        "-v",
        "-s",
        "-m",
        "live",
        "-p",
        "no:cacheprovider",
    ]
    engine = options.engine.strip()
    if engine and engine not in (".", "all"):
        args.extend(["-k", f"[{engine}-"])

    return int(pytest.main(args))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
