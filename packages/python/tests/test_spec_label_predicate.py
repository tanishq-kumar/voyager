"""Query Spec v1 label predicates (#306).

The consumer, the fluent API and the reference vectors must agree on the
``["label", variable, labels]`` expression: every listed label is required, and
the emitted statement is the label test native to each dialect (or
``UNSUPPORTED_FEATURE`` where the dialect has no label test in ``WHERE``).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator
from voyager_ogm import Query, VoyagerError, compile_query_from_spec, has_label, ident
from voyager_ogm.models import Field, Node

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA = json.loads((REPO_ROOT / "spec" / "query_spec.v1.schema.json").read_text(encoding="utf-8"))

MATCH_WHERE_5_SPEC: dict[str, Any] = {
    "spec_version": 1,
    "matches": [
        {
            "paths": [
                [
                    ["node", "root", ["Root"]],
                    ["edge", "undirected", [], None],
                    ["node", "i", []],
                ]
            ],
            "where": [
                "bin",
                "and",
                ["bin", "gt", ["prop", "i", "var"], ["lit", "te"]],
                ["label", "i", ["TextNode"]],
            ],
        }
    ],
    "projections": [["field", "i", "var", None]],
}


class LabelPredicateNode(Node):
    """Node model used by the fluent round-trip."""

    __labels__ = ["LabelPredicateNode"]

    name: str = Field()
    var: str = Field()


def test_schema_accepts_label_predicate() -> None:
    """The new expression is part of the v1 JSON Schema."""
    Draft202012Validator(SCHEMA).validate(MATCH_WHERE_5_SPEC)


def test_consumer_emits_native_label_test_per_dialect() -> None:
    """Each dialect gets the label-test form it accepts."""
    cypher = compile_query_from_spec(MATCH_WHERE_5_SPEC, "cypher")
    assert (
        cypher["statement"]
        == "MATCH (root:Root)-[]-(i) WHERE (i.var > $p0) AND i:TextNode RETURN i.var"
    )
    assert cypher["parameters"] == {"p0": "te"}

    age = compile_query_from_spec(MATCH_WHERE_5_SPEC, "age")
    assert (
        age["statement"] == "SELECT * FROM cypher('age_graph', $$ MATCH (root:Root)-[]-(i) "
        "WHERE (i.var > $p0) AND label(i) = 'TextNode' RETURN i.var $$, %s) "
        "AS (var agtype)"
    )
    assert age["parameters"] == {"p0": "te"}

    gql = compile_query_from_spec(MATCH_WHERE_5_SPEC, "iso_gql")
    assert (
        gql["statement"]
        == "MATCH (root:Root)-[]-(i) WHERE (i.var > $p0) AND i IS TextNode RETURN i.var"
    )
    assert gql["parameters"] == {"p0": "te"}


@pytest.mark.parametrize("dialect", ["sql_pgq", "duckpgq"])
def test_sql_pgq_raises_unsupported_feature(dialect: str) -> None:
    """SQL:PGQ has no label test in a WHERE clause."""
    with pytest.raises(VoyagerError) as exc_info:
        compile_query_from_spec(MATCH_WHERE_5_SPEC, dialect)
    assert exc_info.value.code == "UNSUPPORTED_FEATURE"
    assert "Label predicate expressions" in str(exc_info.value)


def test_multiple_labels_mean_all_of_them() -> None:
    """``["label", "i", ["A", "B"]]`` is the conjunction ``i:A:B``."""
    spec = {
        "spec_version": 1,
        "matches": [{"paths": [[["node", "i", ["Node"]]]], "where": ["label", "i", ["A", "B"]]}],
        "projections": [["field", "i", "name", None]],
    }
    assert (
        compile_query_from_spec(spec, "cypher")["statement"]
        == "MATCH (i:Node) WHERE i:A:B RETURN i.name"
    )
    age = compile_query_from_spec(spec, "age")
    assert "WHERE label(i) = 'A' AND label(i) = 'B'" in age["statement"]
    assert compile_query_from_spec(spec, "iso_gql")["statement"] == (
        "MATCH (i:Node) WHERE i IS A&B RETURN i.name"
    )


def test_fluent_has_label_round_trips_through_the_spec() -> None:
    """The fluent label predicate serialises and recompiles to the same statement."""
    p = LabelPredicateNode(alias="p")
    query = Query.match(p).where((p.var > "te") & has_label("p", "TextNode")).return_(p.var)
    spec = query.to_spec_dict()
    Draft202012Validator(SCHEMA).validate(spec)
    assert spec["matches"][0]["where"][3] == ["label", "p", ["TextNode"]]

    for dialect in ("cypher", "age", "iso_gql"):
        fluent = query.compile(dialect)
        from_spec = compile_query_from_spec(json.loads(query.to_json()), dialect)
        assert from_spec["statement"] == fluent.statement
        assert from_spec["parameters"] == fluent.parameters


def test_ident_has_label_is_the_same_expression() -> None:
    """``ident("p").has_label(...)`` matches the module-level helper."""
    assert ident("p").has_label("A", "B").to_spec() == has_label("p", "A", "B").to_spec()


@pytest.mark.parametrize(
    ("where_expression", "path"),
    [
        (["label", "i", []], "/matches/0/where/2"),
        (["label", "i", [""]], "/matches/0/where/2/0"),
        (["label", "i", ["A", 1]], "/matches/0/where/2/1"),
        (["label", "i", "TextNode"], "/matches/0/where/2"),
        (["label", "i"], "/matches/0/where"),
        (["label", "", ["TextNode"]], "/matches/0/where/1"),
    ],
)
def test_malformed_label_arrays_are_invalid_spec(where_expression: Any, path: str) -> None:
    """Malformed label arrays fail with INVALID_SPEC at the offending element."""
    spec = {
        "spec_version": 1,
        "matches": [{"paths": [[["node", "i", ["Node"]]]], "where": where_expression}],
        "projections": [["field", "i", "name", None]],
    }
    with pytest.raises(VoyagerError) as exc_info:
        compile_query_from_spec(spec, "cypher")
    assert exc_info.value.code == "INVALID_SPEC"
    assert exc_info.value.path == path


def test_label_predicate_variable_follows_scope() -> None:
    """An unbound label-predicate variable is INVALID_SPEC at the variable."""
    spec = {
        "spec_version": 1,
        "matches": [
            {
                "paths": [[["node", "i", ["Node"]]]],
                "where": ["label", "unknown", ["TextNode"]],
            }
        ],
        "projections": [["field", "i", "name", None]],
    }
    with pytest.raises(VoyagerError) as exc_info:
        compile_query_from_spec(spec, "cypher")
    assert exc_info.value.code == "INVALID_SPEC"
    assert exc_info.value.path == "/matches/0/where/1"
    assert "variable 'unknown' is not in scope" in str(exc_info.value)


def test_has_label_requires_an_identifier_or_variable_name() -> None:
    """A non-identifier expression cannot name the label-predicate variable."""
    with pytest.raises(VoyagerError) as exc_info:
        ident("p").concat("x").has_label("A")
    assert exc_info.value.code == "INVALID_CLAUSE"


def test_has_label_requires_at_least_one_label() -> None:
    """The fluent API rejects an empty label list."""
    with pytest.raises(VoyagerError) as exc_info:
        has_label("p")
    assert exc_info.value.code == "INVALID_CLAUSE"
