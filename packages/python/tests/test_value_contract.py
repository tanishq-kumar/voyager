"""The Python exports of the Arrow metadata registry match the written contract."""

from __future__ import annotations

from pathlib import Path

from voyager_ogm import _voyager_rs

CONTRACT_PATH = Path(__file__).resolve().parents[3] / "spec" / "values" / "contract.md"

METADATA_REGISTRY = (
    ("VOYAGER_META_TYPE", "voyager.type"),
    ("VOYAGER_META_ENCODING", "voyager.encoding"),
    ("VOYAGER_META_KIND", "voyager.kind"),
    ("ARROW_EXTENSION_NAME", "ARROW:extension:name"),
    ("ARROW_UUID_EXTENSION", "arrow.uuid"),
    ("META_TYPE_DATE", "date"),
    ("META_TYPE_TIME", "time"),
    ("META_TYPE_TIME_TZ", "time_tz"),
    ("META_TYPE_LOCAL_DATETIME", "local_datetime"),
    ("META_TYPE_DATETIME_TZ", "datetime_tz"),
    ("META_TYPE_DURATION", "duration"),
    ("META_TYPE_DECIMAL", "decimal"),
    ("META_TYPE_UUID", "uuid"),
    ("META_TYPE_HUGEINT", "hugeint"),
    ("META_TYPE_POINT", "point"),
    ("META_TYPE_JSON", "json"),
    ("META_ENCODING_JSON", "json"),
    ("META_KIND_NODE", "node"),
    ("META_KIND_RELATIONSHIP", "relationship"),
    ("META_KIND_PATH", "path"),
)


def test_metadata_constants_match_contract() -> None:
    contract = CONTRACT_PATH.read_text(encoding="utf-8")
    for name, value in METADATA_REGISTRY:
        assert getattr(_voyager_rs, name) == value
        assert f"| `{name}` | `{value}` |" in contract, (
            f"spec/values/contract.md is missing the registry row for {name}"
        )


def test_metadata_constants_are_strings() -> None:
    for name, _ in METADATA_REGISTRY:
        assert isinstance(getattr(_voyager_rs, name), str)
