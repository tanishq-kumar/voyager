"""Conformance matrix recorder, artifact writer, and terminal summary (#299)."""

from __future__ import annotations

import json
import os
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_ARTIFACT_DIR = REPO_ROOT / "target" / "values-conformance"


def get_artifact_dir() -> Path:
    """Returns the output directory for conformance matrix run artifacts."""
    override = os.environ.get("VOYAGER_VALUES_ARTIFACT_DIR", "").strip()
    if override:
        return Path(override)
    return DEFAULT_ARTIFACT_DIR


@dataclass
class MatrixCell:
    """Recorded outcome for one ``(engine, backend, path, value_kind)`` cell."""

    engine: str
    backend: str
    path: str
    value_kind: str
    status: str  # pass | known_diff | fail | restricted | skipped
    python_type: str = ""
    arrow_type: str = ""
    detail: str = ""


class MatrixRecorder:
    """Accumulates conformance cells and writes JSON/Markdown run artifacts."""

    def __init__(self, *, seed: int, examples: int, profile: str) -> None:
        self.seed = seed
        self.examples = examples
        self.profile = profile
        self.cells: dict[tuple[str, str, str, str], MatrixCell] = {}
        self.executed_cells: set[tuple[str, str, str, str]] = set()
        self.matched_known_diffs: set[int] = set()

    def record(
        self,
        *,
        engine: str,
        backend: str,
        path: str,
        value_kind: str,
        status: str,
        python_type: str = "",
        arrow_type: str = "",
        detail: str = "",
        executed: bool = True,
    ) -> None:
        """Records or updates one matrix cell (preserving higher-severity statuses)."""
        key = (engine, backend, path, value_kind)
        if executed:
            self.executed_cells.add(key)

        priority = {"fail": 4, "known_diff": 3, "pass": 2, "restricted": 1, "skipped": 0}
        existing = self.cells.get(key)
        if existing is not None and priority.get(existing.status, 0) > priority.get(status, 0):
            return
        self.cells[key] = MatrixCell(
            engine=engine,
            backend=backend,
            path=path,
            value_kind=value_kind,
            status=status,
            python_type=python_type or (existing.python_type if existing else ""),
            arrow_type=arrow_type or (existing.arrow_type if existing else ""),
            detail=detail or (existing.detail if existing else ""),
        )

    def mark_known_diff_matched(self, entry_index: int) -> None:
        """Marks a known-difference entry as actively observed in this run."""
        self.matched_known_diffs.add(entry_index)

    def summary_counts(self) -> dict[tuple[str, str], Counter[str]]:
        """Groups cell counts by ``(engine, backend)``."""
        counts: dict[tuple[str, str], Counter[str]] = {}
        for cell in self.cells.values():
            counts.setdefault((cell.engine, cell.backend), Counter())[cell.status] += 1
        return counts

    def write_artifacts(self, stale_descriptions: list[str] | None = None) -> tuple[Path, Path]:
        """Writes ``matrix.json`` and ``matrix.md`` artifacts and returns both paths."""
        out_dir = get_artifact_dir()
        out_dir.mkdir(parents=True, exist_ok=True)
        json_path = out_dir / "matrix.json"
        md_path = out_dir / "matrix.md"

        sorted_cells = sorted(
            self.cells.values(),
            key=lambda c: (c.engine, c.backend, c.value_kind, c.path),
        )
        payload: dict[str, Any] = {
            "generated_at": datetime.now(UTC).isoformat(),
            "seed": self.seed,
            "examples": self.examples,
            "profile": self.profile,
            "stale_known_differences": stale_descriptions or [],
            "summary": [
                {
                    "engine": engine,
                    "backend": backend,
                    "passed": cnt.get("pass", 0),
                    "known_diff": cnt.get("known_diff", 0),
                    "failed": cnt.get("fail", 0),
                    "restricted": cnt.get("restricted", 0),
                    "skipped": cnt.get("skipped", 0),
                }
                for (engine, backend), cnt in sorted(self.summary_counts().items())
            ],
            "cells": [asdict(c) for c in sorted_cells],
        }
        json_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

        lines = [
            "# Value Conformance Matrix",
            "",
            f"- Generated: {payload['generated_at']}",
            f"- Profile: `{self.profile}` (seed={self.seed}, max_examples={self.examples})",
            "",
            "## Summary",
            "",
            "| Engine | Backend | Passed | Known Diff | Failed | Restricted | Skipped |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
        for row in payload["summary"]:
            lines.append(
                f"| {row['engine']} | {row['backend']} | {row['passed']} | "
                f"{row['known_diff']} | {row['failed']} | {row['restricted']} | {row['skipped']} |"
            )
        lines.extend(
            [
                "",
                "## Matrix (engine x backend x value_kind x path)",
                "",
                "| Engine | Backend | Value Kind | Path | Python Type | Arrow/Polars Type | Status | Notes |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for cell in sorted_cells:
            note = cell.detail.replace("\n", " ").replace("|", "/")[:140]
            lines.append(
                f"| {cell.engine} | {cell.backend} | {cell.value_kind} | {cell.path} | "
                f"{cell.python_type or '-'} | {cell.arrow_type or '-'} | {cell.status} | {note} |"
            )
        if stale_descriptions:
            lines.extend(["", "## Stale Known Differences", ""])
            lines.extend(f"- {desc}" for desc in stale_descriptions)

        md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return json_path, md_path

    def format_terminal_summary(self) -> str:
        """Formats a compact terminal table matching the live corpus summary style."""
        lines = [
            "",
            f"Value conformance summary (profile={self.profile}, seed={self.seed}, examples={self.examples})",
            f"{'engine':<10} {'backend':<16} {'passed':>7} {'kdiff':>7} {'failed':>7} {'restr':>7} {'skip':>7}",
        ]
        for (engine, backend), cnt in sorted(self.summary_counts().items()):
            lines.append(
                f"{engine:<10} {backend:<16} {cnt.get('pass', 0):>7} "
                f"{cnt.get('known_diff', 0):>7} {cnt.get('fail', 0):>7} "
                f"{cnt.get('restricted', 0):>7} {cnt.get('skipped', 0):>7}"
            )
        return "\n".join(lines)
