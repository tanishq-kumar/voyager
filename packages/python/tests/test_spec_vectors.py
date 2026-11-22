"""Query Spec v1 schema and reference-vector conformance (issue #188).

The JSON Schema must be a valid draft 2020-12 document, every valid vector must
pass it and compile to the recorded statement and parameters in every dialect,
and every invalid vector must match its recorded schema result and raise the
consumer's ``INVALID_SPEC`` error at the recorded JSON Pointer.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator
from voyager_ogm import VoyagerError
from voyager_ogm._voyager_rs import compile_query_from_spec

REPO_ROOT = Path(__file__).resolve().parents[3]
VECTORS = REPO_ROOT / "spec" / "vectors"
SCHEMA_PATH = REPO_ROOT / "spec" / "query_spec.v1.schema.json"

# Vector dialect key -> the dialect alias accepted by the consumer.
DIALECTS = {
    "cypher": "cypher",
    "age": "age",
    "iso_gql": "iso_gql",
    "sql_pgq": "sql_pgq",
    "duck_pgq": "duckpgq",
}


def load_json(path: Path) -> dict[str, Any]:
    """Load one schema or vector JSON document."""
    return json.loads(path.read_text(encoding="utf-8"))


SCHEMA = load_json(SCHEMA_PATH)
VALID_VECTORS = sorted((VECTORS / "valid").glob("*.json"))
INVALID_VECTORS = sorted((VECTORS / "invalid").glob("*.json"))


def test_schema_is_valid_draft_2020_12() -> None:
    """The schema itself must be a valid JSON Schema 2020-12 document."""
    Draft202012Validator.check_schema(SCHEMA)


def test_vectors_are_present() -> None:
    """Both vector directories must carry files, so a glob mistake cannot pass."""
    assert VALID_VECTORS
    assert INVALID_VECTORS


@pytest.mark.parametrize("path", VALID_VECTORS, ids=lambda path: path.stem)
def test_valid_vector(path: Path) -> None:
    """A valid vector passes the schema and compiles as recorded in every dialect."""
    document = load_json(path)
    Draft202012Validator(SCHEMA).validate(document["spec"])

    assert set(document["expected"]) == set(DIALECTS), path.name
    for key, dialect in DIALECTS.items():
        expected = document["expected"][key]
        if "error" in expected:
            with pytest.raises(VoyagerError) as exc_info:
                compile_query_from_spec(document["spec"], dialect)
            assert exc_info.value.code == expected["error"], (path.name, key)
            continue

        compiled = compile_query_from_spec(document["spec"], dialect)
        assert compiled["statement"] == expected["statement"], (path.name, key)
        assert compiled["parameters"] == expected["parameters"], (path.name, key)


@pytest.mark.parametrize("path", INVALID_VECTORS, ids=lambda path: path.stem)
def test_invalid_vector(path: Path) -> None:
    """An invalid vector matches its schema result and raises the recorded error."""
    document = load_json(path)
    schema_valid = Draft202012Validator(SCHEMA).is_valid(document["spec"])
    assert schema_valid is document["schema_valid"], path.name

    with pytest.raises(VoyagerError) as exc_info:
        compile_query_from_spec(document["spec"], "cypher")

    error = exc_info.value
    assert error.code == document["expected"]["error"], path.name
    assert error.code == "INVALID_SPEC", path.name
    assert error.path == document["expected"]["path"], path.name
