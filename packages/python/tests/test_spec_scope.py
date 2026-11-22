"""Query Spec variable scope in the consumer's fixed emission order (issue #271).

The consumer emits `load_csv`, `unwinds`, `matches`, `linear_clauses`,
`with_clauses`, `mutations` and the final `RETURN` in that order, regardless of
the order of the spec's keys. A reference that the emission order cannot honour
is rejected with `INVALID_SPEC` at the identifier's JSON Pointer.
"""

from __future__ import annotations

from typing import Any

import pytest
from voyager_ogm import VoyagerError, compile_query_from_spec

PERSON_PATH = [[["node", "p", ["Person"]]]]
PERSON_AND_FRIEND_PATH = [[["node", "p", ["Person"]]], [["node", "q", ["Person"]]]]


def compile_cypher(spec: dict[str, Any]) -> dict[str, Any]:
    """Compile a spec for openCypher, where every construct in this file works."""
    return compile_query_from_spec(spec, "cypher")


def assert_invalid(spec: dict[str, Any], path: str) -> VoyagerError:
    """Assert the spec is rejected with `INVALID_SPEC` at `path` and return the error."""
    with pytest.raises(VoyagerError) as exc_info:
        compile_cypher(spec)
    error = exc_info.value
    assert error.code == "INVALID_SPEC", str(error)
    assert error.path == path, str(error)
    return error


def test_issue_271_reproduction_is_rejected() -> None:
    """A linear filter cannot read an alias a later WITH defines."""
    spec = {
        "spec_version": 1,
        "matches": [{"paths": PERSON_PATH}],
        "with_clauses": [{"projections": [["field", "p", "name", "n"]]}],
        "linear_clauses": [["filter", ["bin", "gt", ["ident", "n"], ["lit", 0]]]],
        "projections": [["expr", ["ident", "n"], None]],
    }
    error = assert_invalid(spec, "/linear_clauses/0/1/2/1")
    message = str(error)
    assert "variable 'n' is not in scope here" in message
    assert "linear clauses are emitted before WITH clauses" in message
    assert "with_clauses[0].where" in message


def test_issue_271_correct_form_puts_the_filter_in_the_with_clause() -> None:
    """Moving the predicate into `with_clauses[0].where` compiles as intended."""
    spec = {
        "spec_version": 1,
        "matches": [{"paths": PERSON_PATH}],
        "with_clauses": [
            {
                "projections": [["field", "p", "name", "n"]],
                "where": ["bin", "gt", ["ident", "n"], ["lit", 0]],
            }
        ],
        "projections": [["expr", ["ident", "n"], None]],
    }
    compiled = compile_cypher(spec)
    assert compiled["statement"] == ("MATCH (p:Person) WITH p.name AS n WHERE n > $p0 RETURN n")


def test_mutation_after_with_rejects_a_dropped_match_variable() -> None:
    """A mutation sees only what the last WITH projects."""
    spec = {
        "spec_version": 1,
        "matches": [{"paths": PERSON_AND_FRIEND_PATH}],
        "with_clauses": [{"projections": [["field", "p", "name", "n"]]}],
        "mutations": [["set", "q", "age", ["lit", 30]]],
        "projections": [["expr", ["ident", "n"], None]],
    }
    error = assert_invalid(spec, "/mutations/0/1")
    message = str(error)
    assert "variable 'q' is not in scope here" in message
    assert "with_clauses[0]" in message


def test_mutation_after_with_accepts_a_projected_name() -> None:
    """A variable the WITH projects is available to a following mutation."""
    spec = {
        "spec_version": 1,
        "matches": [{"paths": PERSON_PATH}],
        "with_clauses": [{"projections": [["expr", ["ident", "p"], "p"]]}],
        "mutations": [["set", "p", "age", ["lit", 30]]],
        "projections": [["field", "p", "name", None]],
    }
    compiled = compile_cypher(spec)
    assert compiled["statement"] == ("MATCH (p:Person) WITH p AS p SET p.age = $p0 RETURN p.name")


def test_with_replaces_scope_with_the_names_it_projects() -> None:
    """A WITH drops match variables that it does not project.

    A bare variable projection keeps that variable. A field projection keeps
    only its alias: the source variable does not stay in scope.
    """
    ident_spec = {
        "spec_version": 1,
        "matches": [{"paths": PERSON_PATH}],
        "with_clauses": [{"projections": [["expr", ["ident", "p"], None]]}],
        "projections": [["field", "p", "name", None]],
    }
    assert compile_cypher(ident_spec)["statement"] == ("MATCH (p:Person) WITH p RETURN p.name")

    field_spec = {
        "spec_version": 1,
        "matches": [{"paths": PERSON_PATH}],
        "with_clauses": [{"projections": [["field", "p", "name", "n"]]}],
        "projections": [["field", "p", "name", None]],
    }
    error = assert_invalid(field_spec, "/projections/0/1")
    assert "variable 'p' is not in scope here" in str(error)
    assert "with_clauses[0]" in str(error)


def test_with_drops_load_csv_and_unwind_names_unless_projected() -> None:
    """LOAD CSV and UNWIND aliases do not survive a WITH on their own."""
    spec = {
        "spec_version": 1,
        "load_csv": ["file:///tmp/people.csv", True, "row"],
        "unwinds": [["$batch", "item"]],
        "matches": [{"paths": PERSON_PATH}],
        "with_clauses": [{"projections": [["expr", ["ident", "p"], None]]}],
        "projections": [["field", "row", "name", None]],
    }
    error = assert_invalid(spec, "/projections/0/1")
    assert "variable 'row' is not in scope here" in str(error)

    valid = {
        "spec_version": 1,
        "load_csv": ["file:///tmp/people.csv", True, "row"],
        "matches": [{"paths": PERSON_PATH}],
        "with_clauses": [
            {
                "projections": [
                    ["expr", ["ident", "p"], None],
                    ["expr", ["ident", "row"], None],
                ]
            }
        ],
        "projections": [["field", "row", "name", None]],
    }
    assert compile_cypher(valid)["statement"].endswith("WITH p, row RETURN row.name")


@pytest.mark.parametrize(
    ("projection", "rendered"),
    [
        (["field", "p", "name", None], "p.name"),
        (["agg", "p", "age", "avg", None], "avg(p.age)"),
        (["expr", ["prop", "p", "name"], None], "p.name"),
        (["expr", ["lit", 1], None], "1"),
    ],
    ids=["field", "aggregation", "property-expression", "literal-expression"],
)
def test_with_projection_that_is_not_a_bare_variable_needs_an_alias(
    projection: list[Any], rendered: str
) -> None:
    """Only a bare variable may be projected by WITH without `AS`."""
    spec = {
        "spec_version": 1,
        "matches": [{"paths": PERSON_PATH}],
        "with_clauses": [{"projections": [projection]}],
        "projections": [["expr", ["ident", "p"], None]],
    }
    error = assert_invalid(spec, "/with_clauses/0/projections/0")
    message = str(error)
    assert "needs an alias" in message
    assert "only a bare variable may be projected without one" in message
    if rendered != "1":
        assert rendered in message


def test_with_projection_accepts_a_bare_variable_or_an_alias() -> None:
    """Bare variables and aliased expressions both satisfy the alias rule."""
    bare = {
        "spec_version": 1,
        "matches": [{"paths": PERSON_PATH}],
        "with_clauses": [{"projections": [["expr", ["ident", "p"], None]]}],
        "projections": [["field", "p", "name", None]],
    }
    assert compile_cypher(bare)["statement"] == ("MATCH (p:Person) WITH p RETURN p.name")

    aliased = {
        "spec_version": 1,
        "matches": [{"paths": PERSON_PATH}],
        "with_clauses": [{"projections": [["field", "p", "name", "name"]]}],
        "projections": [["expr", ["ident", "name"], None]],
    }
    assert compile_cypher(aliased)["statement"] == (
        "MATCH (p:Person) WITH p.name AS name RETURN name"
    )

    # A field with an empty property is the bare variable, so it needs no alias
    # and keeps the variable in scope.
    empty_property = {
        "spec_version": 1,
        "matches": [{"paths": PERSON_PATH}],
        "with_clauses": [{"projections": [["field", "p", "", None]]}],
        "projections": [["field", "p", "name", None]],
    }
    assert compile_cypher(empty_property)["statement"] == ("MATCH (p:Person) WITH p RETURN p.name")


def test_with_order_by_may_still_read_a_pre_with_name() -> None:
    """WITH ORDER BY is evaluated with the projected and pre-WITH names."""
    spec = {
        "spec_version": 1,
        "matches": [{"paths": PERSON_PATH}],
        "with_clauses": [
            {
                "projections": [["field", "p", "name", "n"]],
                "order_by": [["p", "age", True]],
            }
        ],
        "projections": [["expr", ["ident", "n"], None]],
    }
    compiled = compile_cypher(spec)
    assert compiled["statement"] == (
        "MATCH (p:Person) WITH p.name AS n ORDER BY p.age ASC RETURN n"
    )


def test_with_where_can_read_a_projected_alias_but_not_an_unknown_name() -> None:
    """A WITH's WHERE sees the names that clause projects."""
    valid = {
        "spec_version": 1,
        "matches": [{"paths": PERSON_PATH}],
        "with_clauses": [
            {
                "projections": [["field", "p", "name", "n"]],
                "where": [["bin", "=", ["ident", "n"], ["lit", "Alice"]]],
            }
        ],
        "projections": [["expr", ["ident", "n"], None]],
    }
    assert compile_cypher(valid)["statement"].startswith(
        "MATCH (p:Person) WITH p.name AS n WHERE n = $p0"
    )

    invalid = {
        "spec_version": 1,
        "matches": [{"paths": PERSON_PATH}],
        "with_clauses": [
            {
                "projections": [["field", "p", "name", "n"]],
                "where": ["bin", ">", ["ident", "missing"], ["lit", 0]],
            }
        ],
        "projections": [["expr", ["ident", "n"], None]],
    }
    error = assert_invalid(invalid, "/with_clauses/0/where/2/1")
    assert "with_clauses[0].where" in str(error)


def test_linear_let_binds_from_its_own_entry_onward() -> None:
    """A let is visible to later linear clauses, not to earlier ones."""
    valid = {
        "spec_version": 1,
        "matches": [{"paths": PERSON_PATH}],
        "linear_clauses": [
            ["let", "limit", ["lit", 1]],
            ["filter", ["bin", ">", ["ident", "limit"], ["lit", 0]]],
        ],
        "projections": [["field", "p", "name", None]],
    }
    assert compile_cypher(valid)["statement"].startswith(
        "MATCH (p:Person) WITH *, $p0 AS limit WHERE limit > $p1"
    )

    invalid = {
        "spec_version": 1,
        "matches": [{"paths": PERSON_PATH}],
        "linear_clauses": [
            ["filter", ["bin", ">", ["ident", "limit"], ["lit", 0]]],
            ["let", "limit", ["lit", 1]],
        ],
        "projections": [["field", "p", "name", None]],
    }
    error = assert_invalid(invalid, "/linear_clauses/0/1/2/1")
    assert "linear 'let'" in str(error)


def test_list_comprehension_variable_is_local_to_the_comprehension() -> None:
    """The iteration variable is not in scope after the comprehension."""
    valid = {
        "spec_version": 1,
        "matches": [{"paths": PERSON_PATH}],
        "projections": [
            ["expr", ["list_comp", "x", ["ident", "p"], None, ["ident", "x"]], "items"]
        ],
    }
    assert compile_cypher(valid)["statement"] == ("MATCH (p:Person) RETURN [x IN p | x] AS items")

    invalid = {
        "spec_version": 1,
        "matches": [{"paths": PERSON_PATH}],
        "projections": [
            ["expr", ["list_comp", "x", ["ident", "p"], None, ["ident", "x"]], "items"]
        ],
        "order_by": [[["ident", "x"], True]],
    }
    assert_invalid(invalid, "/order_by/0/0/1")


def test_pattern_comprehension_variables_are_local() -> None:
    """Variables bound by a pattern comprehension do not leak out."""
    valid = {
        "spec_version": 1,
        "matches": [{"paths": PERSON_PATH}],
        "projections": [
            [
                "expr",
                ["pattern_comp", [["node", "friend", ["Person"]]], None, ["ident", "friend"]],
                "friends",
            ]
        ],
    }
    assert compile_cypher(valid)["statement"] == (
        "MATCH (p:Person) RETURN [(friend:Person) | friend] AS friends"
    )

    invalid = {
        "spec_version": 1,
        "matches": [{"paths": PERSON_PATH}],
        "projections": [
            [
                "expr",
                ["pattern_comp", [["node", "friend", ["Person"]]], None, ["ident", "friend"]],
                "friends",
            ]
        ],
        "order_by": [[["ident", "friend"], True]],
    }
    assert_invalid(invalid, "/order_by/0/0/1")


def test_exists_subquery_variables_are_local_but_outer_names_are_visible() -> None:
    """A subquery can read outer variables; its own variables stay inside."""
    subquery = {
        "matches": [{"paths": [[["node", "q", ["Person"]]]]}],
        "projections": [["field", "q", "name", None]],
    }
    correlated = {
        "spec_version": 1,
        "matches": [
            {
                "paths": PERSON_PATH,
                "where": [
                    [
                        "exists",
                        {
                            "matches": [
                                {
                                    "paths": [[["node", "q", ["Person"]]]],
                                    "where": [
                                        ["bin", "=", ["prop", "q", "name"], ["prop", "p", "name"]]
                                    ],
                                }
                            ],
                            "projections": [["field", "q", "name", None]],
                        },
                    ]
                ],
            }
        ],
        "projections": [["field", "p", "name", None]],
    }
    compiled = compile_cypher(correlated)
    assert "q.name = p.name" in compiled["statement"]

    leaked = {
        "spec_version": 1,
        "matches": [
            {"paths": PERSON_PATH},
            {
                "paths": [[["node", "r", ["Person"]]]],
                "where": [["bin", "=", ["exists", subquery], ["lit", True]]],
            },
            {
                "paths": [[["node", "s", ["Person"]]]],
                "where": [["bin", "=", ["ident", "q"], ["lit", 1]]],
            },
        ],
        "projections": [["field", "p", "name", None]],
    }
    assert_invalid(leaked, "/matches/2/where/0/2/1")


@pytest.mark.parametrize(
    ("spec", "path"),
    [
        (
            {
                "spec_version": 1,
                "matches": [{"paths": PERSON_PATH}],
                "projections": [["field", "unknown", "name", None]],
            },
            "/projections/0/1",
        ),
        (
            {
                "spec_version": 1,
                "matches": [{"paths": PERSON_PATH}],
                "projections": [["agg", "unknown", "age", "count", None]],
            },
            "/projections/0/1",
        ),
        (
            {
                "spec_version": 1,
                "matches": [
                    {
                        "paths": PERSON_PATH,
                        "where": ["bin", ">", ["prop", "unknown", "age"], ["lit", 0]],
                    }
                ],
                "projections": [["field", "p", "name", None]],
            },
            "/matches/0/where/2/1",
        ),
        (
            {
                "spec_version": 1,
                "matches": [{"paths": PERSON_PATH}],
                "projections": [["field", "p", "name", None]],
                "order_by": [["unknown", "age", True]],
            },
            "/order_by/0/0",
        ),
        (
            {
                "spec_version": 1,
                "matches": [{"paths": PERSON_PATH}],
                "mutations": [["set", "unknown", "age", ["lit", 1]]],
                "projections": [["field", "p", "name", None]],
            },
            "/mutations/0/1",
        ),
        (
            {
                "spec_version": 1,
                "matches": [{"paths": PERSON_PATH}],
                "mutations": [["delete", False, ["unknown"]]],
            },
            "/mutations/0/2/0",
        ),
        (
            {
                "spec_version": 1,
                "matches": [{"paths": PERSON_PATH}],
                "mutations": [["remove", "unknown", "age"]],
            },
            "/mutations/0/1",
        ),
    ],
    ids=[
        "field-projection",
        "aggregation-target",
        "match-where",
        "order-by-variable",
        "set-target",
        "delete-target",
        "remove-target",
    ],
)
def test_unknown_variable_is_rejected_in_every_position(spec: dict[str, Any], path: str) -> None:
    """Every variable-reading position resolves against the current scope."""
    error = assert_invalid(spec, path)
    assert "is not in scope" in str(error)


def test_parameters_and_literals_are_always_in_scope() -> None:
    """Parameters and literals are not variables and never need a binding."""
    spec = {
        "spec_version": 1,
        "matches": [
            {
                "paths": PERSON_PATH,
                "where": ["bin", ">", ["prop", "p", "age"], ["param", "min_age"]],
            }
        ],
        "projections": [["expr", ["lit", 1], "one"]],
    }
    compiled = compile_cypher(spec)
    assert "p.age > $min_age" in compiled["statement"]


def test_with_where_may_read_the_scope_before_the_with() -> None:
    """A non-DISTINCT, non-aggregating WITH's WHERE sees pre-WITH names."""
    spec = {
        "spec_version": 1,
        "matches": [{"paths": PERSON_PATH}],
        "with_clauses": [
            {
                "projections": [["field", "p", "name", "name"]],
                "where": ["bin", ">", ["prop", "p", "age"], ["lit", 18]],
            }
        ],
        "projections": [["expr", ["ident", "name"], None]],
    }
    assert compile_cypher(spec)["statement"] == (
        "MATCH (p:Person) WITH p.name AS name WHERE p.age > $p0 RETURN name"
    )


def test_with_distinct_where_sees_only_the_projected_names() -> None:
    """DISTINCT collapses the WITH's WHERE scope to its projected names."""
    spec = {
        "spec_version": 1,
        "matches": [{"paths": PERSON_PATH}],
        "with_clauses": [
            {
                "distinct": True,
                "projections": [["field", "p", "name", "name"]],
                "where": ["bin", ">", ["prop", "p", "age"], ["lit", 18]],
            }
        ],
        "projections": [["expr", ["ident", "name"], None]],
    }
    error = assert_invalid(spec, "/with_clauses/0/where/2/1")
    assert "a WITH with DISTINCT or an aggregation only exposes the names it projects" in str(error)


def test_with_aggregation_where_sees_only_the_projected_names() -> None:
    """An aggregation collapses the WITH's WHERE scope to its projected names."""
    spec = {
        "spec_version": 1,
        "matches": [{"paths": PERSON_PATH}],
        "with_clauses": [
            {
                "projections": [
                    ["field", "p", "name", "name"],
                    ["agg", "p", "*", "count", "c"],
                ],
                "where": ["bin", ">", ["prop", "p", "age"], ["lit", 18]],
            }
        ],
        "projections": [["expr", ["ident", "name"], None]],
    }
    error = assert_invalid(spec, "/with_clauses/0/where/2/1")
    assert "a WITH with DISTINCT or an aggregation only exposes the names it projects" in str(error)


def test_with_order_by_may_read_the_scope_before_the_with() -> None:
    """A non-DISTINCT, non-aggregating WITH's ORDER BY sees pre-WITH names."""
    spec = {
        "spec_version": 1,
        "matches": [{"paths": PERSON_PATH}],
        "with_clauses": [
            {
                "projections": [["field", "p", "name", "name"]],
                "order_by": [["p", "age", True]],
            }
        ],
        "projections": [["expr", ["ident", "name"], None]],
    }
    assert compile_cypher(spec)["statement"] == (
        "MATCH (p:Person) WITH p.name AS name ORDER BY p.age ASC RETURN name"
    )


def test_with_distinct_order_by_sees_only_the_projected_names() -> None:
    """DISTINCT collapses the WITH's ORDER BY scope to its projected names."""
    spec = {
        "spec_version": 1,
        "matches": [{"paths": PERSON_PATH}],
        "with_clauses": [
            {
                "distinct": True,
                "projections": [["field", "p", "name", "name"]],
                "order_by": [["p", "age", True]],
            }
        ],
        "projections": [["expr", ["ident", "name"], None]],
    }
    error = assert_invalid(spec, "/with_clauses/0/order_by/0/0")
    assert "a WITH with DISTINCT or an aggregation only exposes the names it projects" in str(error)


def test_with_aggregation_order_by_sees_only_the_projected_names() -> None:
    """An aggregation collapses the WITH's ORDER BY scope to its projected names."""
    spec = {
        "spec_version": 1,
        "matches": [{"paths": PERSON_PATH}],
        "with_clauses": [
            {
                "projections": [
                    ["field", "p", "name", "name"],
                    ["agg", "p", "*", "count", "c"],
                ],
                "order_by": [["p", "age", True]],
            }
        ],
        "projections": [["expr", ["ident", "name"], None]],
    }
    error = assert_invalid(spec, "/with_clauses/0/order_by/0/0")
    assert "a WITH with DISTINCT or an aggregation only exposes the names it projects" in str(error)


def test_clauses_after_a_with_see_only_its_projected_names() -> None:
    """A WITH's WHERE may read a pre-WITH name, but later clauses may not."""
    spec = {
        "spec_version": 1,
        "matches": [{"paths": PERSON_PATH}],
        "with_clauses": [
            {
                "projections": [["field", "p", "name", "name"]],
                "where": ["bin", ">", ["prop", "p", "age"], ["lit", 18]],
            }
        ],
        "projections": [["field", "p", "age", None]],
    }
    error = assert_invalid(spec, "/projections/0/1")
    assert "variable 'p' is not in scope here" in str(error)
    assert "with_clauses[0]" in str(error)


def test_root_order_by_may_read_pre_return_names_without_distinct_or_aggregation() -> None:
    """Root ORDER BY sees the RETURN projections plus the pre-RETURN scope."""
    spec = {
        "spec_version": 1,
        "matches": [{"paths": PERSON_PATH}],
        "projections": [["field", "p", "name", "name"]],
        "order_by": [["p", "age", True]],
    }
    assert compile_cypher(spec)["statement"] == (
        "MATCH (p:Person) RETURN p.name AS name ORDER BY p.age ASC"
    )


def test_root_distinct_order_by_sees_only_the_returned_names() -> None:
    """DISTINCT collapses root ORDER BY scope to the RETURN projections."""
    spec = {
        "spec_version": 1,
        "matches": [{"paths": PERSON_PATH}],
        "projections": [["field", "p", "name", "name"]],
        "distinct": True,
        "order_by": [["p", "age", True]],
    }
    error = assert_invalid(spec, "/order_by/0/0")
    message = str(error)
    assert "a RETURN with DISTINCT or an aggregation only exposes the names it projects" in message


def test_root_aggregation_order_by_sees_only_the_returned_names() -> None:
    """An aggregation collapses root ORDER BY scope to the RETURN projections."""
    spec = {
        "spec_version": 1,
        "matches": [{"paths": PERSON_PATH}],
        "projections": [["agg", "p", "*", "count", "c"]],
        "order_by": [["p", "age", True]],
    }
    error = assert_invalid(spec, "/order_by/0/0")
    message = str(error)
    assert "a RETURN with DISTINCT or an aggregation only exposes the names it projects" in message


# ---------------------------------------------------------------------------
# A DISTINCT/aggregating ORDER BY or WHERE may repeat a projected expression.
# This table mirrors DISTINCT_ORDER_BY_CASES in test_fluent_scope.py.
# ---------------------------------------------------------------------------

DISTINCT_ORDER_BY_CASES: list[tuple[str, dict[str, Any], tuple[Any, ...]]] = [
    (
        "distinct-return-order-by-projected-field",
        {
            "spec_version": 1,
            "matches": [{"paths": PERSON_PATH}],
            "projections": [["field", "p", "city", None]],
            "distinct": True,
            "order_by": [["p", "city", True]],
        },
        (
            "accept",
            "MATCH (p:Person) RETURN DISTINCT p.city ORDER BY p.city ASC",
            {},
        ),
    ),
    (
        "distinct-return-order-by-projected-subexpression",
        {
            "spec_version": 1,
            "matches": [{"paths": PERSON_PATH}],
            "projections": [["field", "p", "city", None]],
            "distinct": True,
            "order_by": [[["bin", "+", ["prop", "p", "city"], ["lit", 1]], True]],
        },
        (
            "accept",
            "MATCH (p:Person) RETURN DISTINCT p.city ORDER BY p.city + $p0 ASC",
            {"p0": 1},
        ),
    ),
    (
        "distinct-return-order-by-aliased-projected-field",
        {
            "spec_version": 1,
            "matches": [{"paths": PERSON_PATH}],
            "projections": [["field", "p", "city", "city"]],
            "distinct": True,
            "order_by": [["p", "city", True]],
        },
        (
            "accept",
            "MATCH (p:Person) RETURN DISTINCT p.city AS city ORDER BY p.city ASC",
            {},
        ),
    ),
    (
        "distinct-return-order-by-unprojected-field",
        {
            "spec_version": 1,
            "matches": [{"paths": PERSON_PATH}],
            "projections": [["field", "p", "city", None]],
            "distinct": True,
            "order_by": [["p", "age", True]],
        },
        ("reject", "/order_by/0/0"),
    ),
    (
        "distinct-return-order-by-bare-variable-when-only-property-projected",
        {
            "spec_version": 1,
            "matches": [{"paths": PERSON_PATH}],
            "projections": [["field", "p", "city", None]],
            "distinct": True,
            "order_by": [[["ident", "p"], True]],
        },
        ("reject", "/order_by/0/0/1"),
    ),
    (
        "distinct-return-order-by-partly-unprojected-subexpression",
        {
            "spec_version": 1,
            "matches": [{"paths": PERSON_PATH}],
            "projections": [["field", "p", "city", None]],
            "distinct": True,
            "order_by": [
                [
                    [
                        "bin",
                        "+",
                        ["prop", "p", "city"],
                        ["prop", "p", "age"],
                    ],
                    True,
                ]
            ],
        },
        ("reject", "/order_by/0/0/2/1"),
    ),
    (
        "aggregating-return-order-by-projected-field",
        {
            "spec_version": 1,
            "matches": [{"paths": PERSON_PATH}],
            "projections": [
                ["field", "p", "city", None],
                ["agg", "p", "*", "count", "c"],
            ],
            "order_by": [["p", "city", True]],
        },
        (
            "accept",
            "MATCH (p:Person) RETURN p.city, COUNT(p) AS c ORDER BY p.city ASC",
            {},
        ),
    ),
    (
        "aggregating-return-order-by-repeated-aggregation",
        {
            "spec_version": 1,
            "matches": [{"paths": PERSON_PATH}],
            "projections": [["agg", "p", "*", "count", "c"]],
            "order_by": [[["fn", "count", [["ident", "p"]]], True]],
        },
        (
            "accept",
            "MATCH (p:Person) RETURN COUNT(p) AS c ORDER BY count(p) ASC",
            {},
        ),
    ),
    (
        "aggregating-return-order-by-unprojected-field",
        {
            "spec_version": 1,
            "matches": [{"paths": PERSON_PATH}],
            "projections": [
                ["field", "p", "city", None],
                ["agg", "p", "*", "count", "c"],
            ],
            "order_by": [["p", "age", True]],
        },
        ("reject", "/order_by/0/0"),
    ),
    (
        "distinct-with-order-by-projected-field",
        {
            "spec_version": 1,
            "matches": [{"paths": PERSON_PATH}],
            "with_clauses": [
                {
                    "distinct": True,
                    "projections": [["field", "p", "city", "city"]],
                    "order_by": [["p", "city", True]],
                }
            ],
            "projections": [["expr", ["ident", "city"], None]],
        },
        (
            "accept",
            "MATCH (p:Person) WITH DISTINCT p.city AS city ORDER BY p.city ASC RETURN city",
            {},
        ),
    ),
    (
        "aggregating-with-order-by-projected-field",
        {
            "spec_version": 1,
            "matches": [{"paths": PERSON_PATH}],
            "with_clauses": [
                {
                    "projections": [
                        ["field", "p", "city", "city"],
                        ["agg", "p", "*", "count", "c"],
                    ],
                    "order_by": [["p", "city", True]],
                }
            ],
            "projections": [
                ["expr", ["ident", "city"], None],
                ["expr", ["ident", "c"], None],
            ],
        },
        (
            "accept",
            "MATCH (p:Person) WITH p.city AS city, COUNT(p) AS c "
            "ORDER BY p.city ASC RETURN city, c",
            {},
        ),
    ),
    (
        "distinct-with-where-projected-field",
        {
            "spec_version": 1,
            "matches": [{"paths": PERSON_PATH}],
            "with_clauses": [
                {
                    "distinct": True,
                    "projections": [["field", "p", "city", "city"]],
                    "where": [["unary", "is_not_null", ["prop", "p", "city"]]],
                }
            ],
            "projections": [["expr", ["ident", "city"], None]],
        },
        (
            "accept",
            "MATCH (p:Person) WITH DISTINCT p.city AS city WHERE p.city IS NOT NULL RETURN city",
            {},
        ),
    ),
    (
        "distinct-with-order-by-unprojected-field",
        {
            "spec_version": 1,
            "matches": [{"paths": PERSON_PATH}],
            "with_clauses": [
                {
                    "distinct": True,
                    "projections": [["field", "p", "city", "city"]],
                    "order_by": [["p", "age", True]],
                }
            ],
            "projections": [["expr", ["ident", "city"], None]],
        },
        ("reject", "/with_clauses/0/order_by/0/0"),
    ),
    (
        "distinct-with-where-unprojected-field",
        {
            "spec_version": 1,
            "matches": [{"paths": PERSON_PATH}],
            "with_clauses": [
                {
                    "distinct": True,
                    "projections": [["field", "p", "city", "city"]],
                    "where": [["unary", "is_not_null", ["prop", "p", "age"]]],
                }
            ],
            "projections": [["expr", ["ident", "city"], None]],
        },
        ("reject", "/with_clauses/0/where/0/2/1"),
    ),
]


@pytest.mark.parametrize(
    ("spec", "expected"),
    [case[1:] for case in DISTINCT_ORDER_BY_CASES],
    ids=[case[0] for case in DISTINCT_ORDER_BY_CASES],
)
def test_distinct_order_by_table(spec: dict[str, Any], expected: tuple[Any, ...]) -> None:
    """A DISTINCT/aggregating clause may repeat a projected expression."""
    outcome = expected[0]
    if outcome == "reject":
        assert_invalid(spec, expected[1])
        return
    compiled = compile_cypher(spec)
    assert compiled["statement"] == expected[1]
    assert compiled["parameters"] == expected[2]
