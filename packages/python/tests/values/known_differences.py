"""Known differences registry and stale-entry verification (#299)."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

KNOWN_DIFFERENCES_PATH = Path(__file__).resolve().parent / "known_differences.json"

VALID_CATEGORIES = frozenset({"limitation", "fallback", "bug"})


@dataclass(frozen=True, slots=True)
class KnownDifferenceEntry:
    """One reviewed known difference between reference/input and a Voyager backend path."""

    index: int
    engine: str
    backend: str
    path: str
    value_kind: str
    category: str
    reason: str
    issue: str | None = None
    repro: str | None = None
    match: str | None = None

    def covers(self, engine: str, backend: str, path: str, value_kind: str) -> bool:
        """Returns whether this entry applies to the given matrix cell."""
        if self.engine != engine:
            return False
        if self.backend != "*" and self.backend != backend:
            return False
        if self.path != "*" and self.path != path:
            return False
        return self.value_kind == "*" or self.value_kind == value_kind

    def matches_message(self, message: str) -> bool:
        """Returns whether ``message`` matches the entry's optional ``match`` pattern."""
        if not self.match:
            return True
        return bool(re.search(self.match, message))


def load_known_differences(
    path: Path = KNOWN_DIFFERENCES_PATH,
) -> list[KnownDifferenceEntry]:
    """Loads and validates ``known_differences.json``."""
    if not path.is_file():
        return []
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError(f"{path.name} must contain a JSON array")
    entries: list[KnownDifferenceEntry] = []
    for idx, item in enumerate(raw):
        if not isinstance(item, dict):
            raise ValueError(f"{path.name}[{idx}] must be an object")
        category = str(item.get("category", ""))
        if category not in VALID_CATEGORIES:
            raise ValueError(
                f"{path.name}[{idx}].category must be one of {sorted(VALID_CATEGORIES)}, got {category!r}"
            )
        reason = str(item.get("reason", "")).strip()
        if not reason:
            raise ValueError(f"{path.name}[{idx}].reason must be non-empty")
        repro = item.get("repro")
        if category == "bug" and not (isinstance(repro, str) and repro.strip()):
            raise ValueError(f"{path.name}[{idx}] with category='bug' requires a non-empty 'repro'")
        entries.append(
            KnownDifferenceEntry(
                index=idx,
                engine=str(item["engine"]),
                backend=str(item["backend"]),
                path=str(item["path"]),
                value_kind=str(item["value_kind"]),
                category=category,
                reason=reason,
                issue=item.get("issue"),
                repro=repro,
                match=item.get("match"),
            )
        )
    return entries


def find_matching_entry(
    entries: list[KnownDifferenceEntry],
    *,
    engine: str,
    backend: str,
    path: str,
    value_kind: str,
    message: str,
) -> KnownDifferenceEntry | None:
    """Returns the first known-difference entry matching the observed failure."""
    for entry in entries:
        if entry.covers(engine, backend, path, value_kind) and entry.matches_message(message):
            return entry
    return None


def find_stale_entries(
    entries: list[KnownDifferenceEntry],
    executed_cells: set[tuple[str, str, str, str]],
    matched_indices: set[int],
) -> list[KnownDifferenceEntry]:
    """Returns entries whose covered cells were executed yet never triggered a match."""
    stale: list[KnownDifferenceEntry] = []
    for entry in entries:
        was_exercised = any(
            entry.covers(engine, backend, path, value_kind)
            for (engine, backend, path, value_kind) in executed_cells
        )
        if was_exercised and entry.index not in matched_indices:
            stale.append(entry)
    return stale
