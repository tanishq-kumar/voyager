"""Query Spec v1 revision constants and revision-aware validation (issue #292)."""

from __future__ import annotations

import json

import pytest
from voyager_ogm import (
    QUERY_SPEC_REVISION,
    QUERY_SPEC_VERSION,
    Field,
    Node,
    Query,
    VoyagerError,
    _voyager_rs,
    col,
    compile_query_from_spec,
)

BASE_SPEC = {
    "spec_version": 1,
    "matches": [{"paths": [[("node", "p", ["Person"], None)]]}],
    "projections": [("field", "p", "name", None)],
}


class RevisionPerson(Node):
    """A minimal model for the fluent round trip."""

    __labels__ = ["RevisionPerson"]

    name: str = Field()


def test_spec_version_and_revision_constants_are_exposed() -> None:
    """The Python package and the extension expose the Rust constants."""
    assert QUERY_SPEC_VERSION == 1
    assert QUERY_SPEC_REVISION == 1
    assert _voyager_rs.QUERY_SPEC_VERSION == QUERY_SPEC_VERSION
    assert _voyager_rs.QUERY_SPEC_REVISION == QUERY_SPEC_REVISION


def test_missing_revision_is_revision_one() -> None:
    """A document without spec_revision still compiles."""
    compiled = compile_query_from_spec(BASE_SPEC, "cypher")
    assert compiled["statement"] == "MATCH (p:Person) RETURN p.name"


def test_current_revision_is_accepted() -> None:
    """The revision this build supports is accepted."""
    compiled = compile_query_from_spec({**BASE_SPEC, "spec_revision": 1}, "cypher")
    assert compiled["statement"] == "MATCH (p:Person) RETURN p.name"


@pytest.mark.parametrize("revision", [2, 99])
def test_newer_revision_is_rejected(revision: int) -> None:
    """A newer revision is rejected with the same pointer and a clear message."""
    with pytest.raises(VoyagerError) as exc_info:
        compile_query_from_spec({**BASE_SPEC, "spec_revision": revision}, "cypher")
    error = exc_info.value
    assert error.code == "INVALID_SPEC"
    assert error.path == "/spec_revision"
    assert f"requires Query Spec revision {revision}" in str(error)
    assert f"supports up to revision {QUERY_SPEC_REVISION}" in str(error)


@pytest.mark.parametrize("revision", [0, -1, 1.5, "1", True])
def test_invalid_revision_values_are_rejected(revision: object) -> None:
    """Non-integer and smaller-than-one values are rejected at /spec_revision."""
    with pytest.raises(VoyagerError) as exc_info:
        compile_query_from_spec({**BASE_SPEC, "spec_revision": revision}, "cypher")
    assert exc_info.value.code == "INVALID_SPEC"
    assert exc_info.value.path == "/spec_revision"


def test_nested_newer_revision_is_rejected_at_the_nested_pointer() -> None:
    """Nested match and with clauses validate their own spec_revision."""
    spec = {
        "spec_version": 1,
        "matches": [
            {
                "spec_revision": 2,
                "paths": [[("node", "p", ["Person"], None)]],
            }
        ],
        "projections": [("field", "p", "name", None)],
    }
    with pytest.raises(VoyagerError) as exc_info:
        compile_query_from_spec(spec, "cypher")
    assert exc_info.value.code == "INVALID_SPEC"
    assert exc_info.value.path == "/matches/0/spec_revision"

    spec = {
        "spec_version": 1,
        "matches": [{"paths": [[("node", "p", ["Person"], None)]]}],
        "with_clauses": [
            {
                "spec_revision": 2,
                "projections": [("field", "p", "name", "n")],
            }
        ],
        "projections": [("expr", ("ident", "n"), None)],
    }
    with pytest.raises(VoyagerError) as exc_info:
        compile_query_from_spec(spec, "cypher")
    assert exc_info.value.code == "INVALID_SPEC"
    assert exc_info.value.path == "/with_clauses/0/spec_revision"


def test_expression_to_spec_dict_emits_the_current_revision() -> None:
    """Expression documents carry both version facts."""
    spec = (col("score") > 10).to_spec_dict()
    assert spec["spec_version"] == QUERY_SPEC_VERSION
    assert spec["spec_revision"] == QUERY_SPEC_REVISION


def test_to_spec_dict_round_trips_through_compile_query_from_spec() -> None:
    """The exported revision is accepted and compiles to the same statement."""
    p = RevisionPerson(alias="p")
    query = Query.match(p).where(p.name == "Alice").return_(p.name)
    spec = query.to_spec_dict()

    assert spec["spec_version"] == QUERY_SPEC_VERSION
    assert spec["spec_revision"] == QUERY_SPEC_REVISION
    assert json.loads(query.to_json()) == spec

    expected = query.compile("cypher")
    compiled = compile_query_from_spec(spec, "cypher")
    assert compiled["statement"] == expected.statement
    assert compiled["parameters"] == expected.parameters


def test_rejected_spellings_surface_the_same_code_and_path() -> None:
    """The non-canonical spellings fail through Python with the Rust pointers."""
    alias_direction = {
        "spec_version": 1,
        "matches": [
            {
                "paths": [
                    [
                        ("node", "p", ["Person"], None),
                        ("edge", "out", ["KNOWS"], None, None, None),
                        ("node", "f", ["Person"], None),
                    ]
                ]
            }
        ],
        "projections": [("field", "p", "name", None)],
    }
    with pytest.raises(VoyagerError) as exc_info:
        compile_query_from_spec(alias_direction, "cypher")
    assert exc_info.value.code == "INVALID_SPEC"
    assert exc_info.value.path == "/matches/0/paths/0/1/1"
    assert 'use "outgoing" instead of "out"' in str(exc_info.value)

    root_offset = {**BASE_SPEC, "offset": 2}
    with pytest.raises(VoyagerError) as exc_info:
        compile_query_from_spec(root_offset, "cypher")
    assert exc_info.value.code == "INVALID_SPEC"
    assert exc_info.value.path == "/offset"
    assert 'use "skip" instead of "offset"' in str(exc_info.value)

    alias_outside_projection = {
        "spec_version": 1,
        "matches": [
            {
                "paths": [[("node", "p", ["Person"], None)]],
                "where": ["bin", "eq", ["alias", ["lit", 1], "one"], ["lit", 1]],
            }
        ],
        "projections": [("field", "p", "name", None)],
    }
    with pytest.raises(VoyagerError) as exc_info:
        compile_query_from_spec(alias_outside_projection, "cypher")
    assert exc_info.value.code == "INVALID_SPEC"
    assert exc_info.value.path == "/matches/0/where/2"
