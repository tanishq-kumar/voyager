#!/usr/bin/env python3
"""Verify that the Cargo workspace version and Python CLI package versions stay in lockstep.

Usage:
    python3 scripts/check_version_sync.py
    python3 scripts/check_version_sync.py --update
"""

from __future__ import annotations

import argparse
import re
import sys
import tomllib
from pathlib import Path

CARGO_SEMVER_RE = re.compile(
    r"^(?P<core>[0-9]+\.[0-9]+\.[0-9]+)"
    r"(?:-(?P<tag>alpha|beta|rc)\.(?P<num>[0-9]+))?$"
)
PEP440_TAG_MAP = {
    "alpha": "a",
    "beta": "b",
    "rc": "rc",
}
CLI_PIN_LINE_RE = re.compile(r'("voyager-cli==)([^"]+)(")')


def cargo_to_pep440(cargo_version: str) -> str:
    """Convert a Cargo workspace SemVer string to the PEP 440 version maturin emits."""
    match = CARGO_SEMVER_RE.fullmatch(cargo_version)
    if match is None:
        raise ValueError(
            f"Unsupported Cargo version format '{cargo_version}'. "
            "Expected X.Y.Z or X.Y.Z-(alpha|beta|rc).N."
        )
    core = match.group("core")
    tag = match.group("tag")
    num = match.group("num")
    if tag is None:
        return core
    return f"{core}{PEP440_TAG_MAP[tag]}{num}"


def check_sync(repo_root: Path, update: bool = False) -> int:
    """Check (or update) lockstep versions across Cargo.toml, pyproject.toml, and packages/cli/pyproject.toml."""
    cargo_path = repo_root / "Cargo.toml"
    root_pyproject_path = repo_root / "pyproject.toml"
    cli_pyproject_path = repo_root / "packages" / "cli" / "pyproject.toml"

    cargo_data = tomllib.loads(cargo_path.read_text(encoding="utf-8"))
    cargo_version = cargo_data["workspace"]["package"]["version"]
    expected_pep440 = cargo_to_pep440(cargo_version)

    cli_data = tomllib.loads(cli_pyproject_path.read_text(encoding="utf-8"))
    cli_project = cli_data.get("project", {})
    if cli_project.get("name") != "voyager-cli":
        print(
            f"[FAIL] {cli_pyproject_path}: expected project.name == 'voyager-cli', "
            f"got {cli_project.get('name')!r}",
            file=sys.stderr,
        )
        return 1
    if "version" not in cli_project.get("dynamic", []):
        print(
            f"[FAIL] {cli_pyproject_path}: project.dynamic must include 'version'",
            file=sys.stderr,
        )
        return 1
    maturin_cfg = cli_data.get("tool", {}).get("maturin", {})
    if maturin_cfg.get("bindings") != "bin":
        print(
            f"[FAIL] {cli_pyproject_path}: tool.maturin.bindings must be 'bin'",
            file=sys.stderr,
        )
        return 1

    root_text = root_pyproject_path.read_text(encoding="utf-8")
    root_data = tomllib.loads(root_text)
    cli_extra = root_data.get("project", {}).get("optional-dependencies", {}).get("cli")
    expected_req = f"voyager-cli=={expected_pep440}"

    if cli_extra == [expected_req]:
        print(
            f"[PASS] Version sync OK: Cargo.toml={cargo_version} -> voyager-ogm[cli]={expected_req}"
        )
        return 0

    if not update:
        print(
            f"[FAIL] Version mismatch: Cargo.toml has '{cargo_version}' "
            f"(PEP 440 '{expected_pep440}'), but pyproject.toml optional-dependencies.cli "
            f"is {cli_extra!r} instead of {[expected_req]!r}.",
            file=sys.stderr,
        )
        return 1

    updated_text, replacements = CLI_PIN_LINE_RE.subn(
        rf"\g<1>{expected_pep440}\g<3>",
        root_text,
        count=1,
    )
    if replacements != 1:
        print(
            f"[FAIL] Could not locate 'voyager-cli==...' pin in {root_pyproject_path}",
            file=sys.stderr,
        )
        return 1

    root_pyproject_path.write_text(updated_text, encoding="utf-8")
    print(f"[OK] Updated voyager-ogm[cli] pin in pyproject.toml to {expected_req}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--update",
        action="store_true",
        help="Update voyager-cli==<version> in pyproject.toml to match Cargo.toml",
    )
    args = parser.parse_args()
    repo_root = Path(__file__).resolve().parent.parent
    return check_sync(repo_root, update=args.update)


if __name__ == "__main__":
    raise SystemExit(main())
