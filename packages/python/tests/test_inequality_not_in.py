"""Regression tests for inequality spelling (#301) and ``NOT IN`` lowering (#302)."""

from __future__ import annotations

import pytest
from voyager_ogm import (
    Field,
    Node,
    Query,
    compile_query_from_spec,
    node,
    reset_alias_counters,
)

DIALECTS = ["cypher", "apache_age", "iso_gql", "sql_pgq", "duckpgq"]


@node(label="Person")
class Person(Node):
    name = Field()
    age = Field()


@pytest.fixture(autouse=True)
def _reset():
    reset_alias_counters()


@pytest.mark.parametrize("dialect", DIALECTS)
def test_inequality_compiles_to_angle_brackets(dialect: str) -> None:
    p = Person(alias="p")
    compiled = Query.match(p).where(p.age != 30).return_(p.name).compile(dialect, graph_name="g")
    assert "p.age <> $p0" in compiled.statement
    assert "!=" not in compiled.statement
    assert compiled.parameters == {"p0": 30}


@pytest.mark.parametrize("dialect", DIALECTS)
def test_not_in_compiles_to_negated_in(dialect: str) -> None:
    p = Person(alias="p")
    compiled = (
        Query.match(p)
        .where(p.name.not_in(["Mallory", "Eve"]))
        .return_(p.name)
        .compile(dialect, graph_name="g")
    )
    if dialect in ("sql_pgq", "duckpgq"):
        expected = "p.name NOT IN $p0"
    else:
        expected = "NOT (p.name IN $p0)"
    assert expected in compiled.statement
    assert compiled.parameters == {"p0": ["Mallory", "Eve"]}


def _where_spec(operator: object) -> dict[str, object]:
    return {
        "spec_version": 1,
        "matches": [
            {
                "paths": [[["node", "p", ["Person"]]]],
                "where": ["bin", operator, ["prop", "p", "age"], ["lit", 30]],
            }
        ],
        "projections": [["field", "p", "name", None]],
    }


@pytest.mark.parametrize("spelling", ["neq", "ne", "!=", "<>"])
@pytest.mark.parametrize("dialect", DIALECTS)
def test_spec_inequality_spellings_still_compile(dialect: str, spelling: str) -> None:
    compiled = compile_query_from_spec(_where_spec(spelling), dialect, graph_name="g")
    assert "p.age <> $p0" in compiled["statement"]
    assert compiled["parameters"] == {"p0": 30}


def _not_in_spec(operator: str) -> dict[str, object]:
    return {
        "spec_version": 1,
        "matches": [
            {
                "paths": [[["node", "p", ["Person"]]]],
                "where": [
                    "bin",
                    operator,
                    ["prop", "p", "name"],
                    ["lit", ["Mallory", "Eve"]],
                ],
            }
        ],
        "projections": [["field", "p", "name", None]],
    }


@pytest.mark.parametrize("spelling", ["not_in", "not in"])
@pytest.mark.parametrize("dialect", DIALECTS)
def test_spec_not_in_spellings_still_compile(dialect: str, spelling: str) -> None:
    compiled = compile_query_from_spec(_not_in_spec(spelling), dialect, graph_name="g")
    if dialect in ("sql_pgq", "duckpgq"):
        expected = "p.name NOT IN $p0"
    else:
        expected = "NOT (p.name IN $p0)"
    assert expected in compiled["statement"]
    assert compiled["parameters"] == {"p0": ["Mallory", "Eve"]}
