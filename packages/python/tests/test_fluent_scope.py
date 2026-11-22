"""Fluent-query variable scope check (issue #287).

The fluent builder compiles its AST directly, so it now runs the same variable
scope rules as the Query Spec consumer. Each case here mirrors a case in
``test_spec_scope.py``: a reference must resolve at the point its clause is
emitted, and a violation raises ``VoyagerError`` with code ``INVALID_CLAUSE``
naming the variable and the clause.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest
from voyager_ogm import (
    Field,
    Node,
    Query,
    VoyagerError,
    col,
    fn,
    ident,
    lit,
    node,
    param,
    reset_alias_counters,
)
from voyager_ogm.expressions import list_comprehension, pattern_comprehension


@pytest.fixture(autouse=True)
def reset_aliases() -> None:
    """Keep generated aliases stable between cases."""
    reset_alias_counters()


@node(label="Person")
class Person(Node):
    name = Field()
    age = Field()
    city = Field()


@node(label="Company")
class Company(Node):
    name = Field()


def compile_cypher(query: Query) -> Any:
    """Compile for openCypher, where every construct in this file works."""
    return query.compile("cypher")


def assert_invalid(query: Query, variable: str, clause: str) -> VoyagerError:
    """Assert compilation raises `INVALID_CLAUSE` naming the variable and clause."""
    with pytest.raises(VoyagerError) as exc_info:
        compile_cypher(query)
    error = exc_info.value
    assert error.code == "INVALID_CLAUSE", str(error)
    message = str(error)
    assert f"variable '{variable}'" in message, message
    assert clause in message, message
    return error


def assert_statement(query: Query, expected: str) -> None:
    """Assert compilation succeeds with exactly `expected`."""
    assert compile_cypher(query).statement == expected


# ---------------------------------------------------------------------------
# Invalid fluent cases: (id, query builder, variable, clause)
# ---------------------------------------------------------------------------

INVALID_CASES: list[tuple[str, Callable[[], Query], str, str]] = [
    (
        "issue-287-with-then-return-dropped-variable",
        lambda: (
            Query.match(Person(alias="p")).with_(name=Person("p").name).return_(Person("p").name)
        ),
        "p",
        "RETURN",
    ),
    (
        "issue-287-return-unbound-variable",
        lambda: Query.match(Person(alias="p")).return_(Person(alias="q").name),
        "q",
        "RETURN",
    ),
    (
        "issue-287-distinct-with-order-by",
        lambda: (
            Query.match(Person(alias="p"))
            .with_(name=Person("p").name, distinct=True)
            .order_by(Person("p").age)
            .return_("name")
        ),
        "p",
        "WITH clause 1 ORDER BY",
    ),
    (
        "mutation-after-with-dropped-match-variable",
        lambda: (
            Query.match(Person(alias="p"))
            .match(Person(alias="q"))
            .with_(name=Person("p").name)
            .set(Person("q").age == 30)
            .return_("name")
        ),
        "q",
        "mutation",
    ),
    (
        "with-replaces-scope-with-aliases-only",
        lambda: Query.match(Person(alias="p")).with_(n=Person("p").name).return_(Person("p").name),
        "p",
        "RETURN",
    ),
    (
        "load-csv-name-dropped-by-with",
        lambda: (
            Query.load_csv("file:///tmp/people.csv", True, "row")
            .match(Person(alias="p"))
            .with_(Person(alias="p"))
            .return_("row.name")
        ),
        "row",
        "RETURN",
    ),
    (
        "unwind-name-dropped-by-with",
        lambda: (
            Query.match(Person(alias="p"))
            .unwind("$batch", "item")
            .with_(Person(alias="p"))
            .return_("item.name")
        ),
        "item",
        "RETURN",
    ),
    (
        "linear-filter-cannot-read-a-later-with-alias",
        lambda: (
            Query.match(Person(alias="p"))
            .linear_filter(col("n") > 0)
            .with_(n=Person("p").name)
            .return_(col("n"))
        ),
        "n",
        "linear FILTER",
    ),
    (
        "linear-filter-cannot-read-a-later-let",
        lambda: (
            Query.match(Person(alias="p"))
            .linear_filter(col("limit") > 0)
            .let_(limit=lit(1))
            .return_(Person("p").name)
        ),
        "limit",
        "linear FILTER",
    ),
    (
        "list-comprehension-variable-does-not-leak",
        lambda: (
            Query.match(Person(alias="p"))
            .return_(items=list_comprehension("x", Person("p").city, None, ident("x")))
            .order_by(ident("x"))
        ),
        "x",
        "root ORDER BY",
    ),
    (
        "pattern-comprehension-variable-does-not-leak",
        lambda: (
            Query.match(Person(alias="p"))
            .return_(
                friends=pattern_comprehension(
                    path=Query.match(Person(alias="friend")),
                    proj=Person("friend").name,
                )
            )
            .order_by(ident("friend"))
        ),
        "friend",
        "root ORDER BY",
    ),
    (
        "exists-subquery-variable-does-not-leak",
        lambda: (
            Query.match(Person(alias="p"))
            .where(Query.exists(Query.match(Person(alias="q")).return_(Person("q").name)))
            .match(Person(alias="r"))
            .where(col("q") == 1)
            .return_(Person("p").name)
        ),
        "q",
        "MATCH",
    ),
    (
        "field-projection",
        lambda: Query.match(Person(alias="p")).return_(Person(alias="unknown").name),
        "unknown",
        "RETURN",
    ),
    (
        "aggregation-target",
        lambda: Query.match(Person(alias="p")).return_(count=Person(alias="unknown").count()),
        "unknown",
        "RETURN",
    ),
    (
        "match-where",
        lambda: Query.match(Person(alias="p")).where(col("unknown") > 0).return_(Person("p").name),
        "unknown",
        "MATCH",
    ),
    (
        "order-by-variable",
        lambda: Query.match(Person(alias="p")).return_(Person("p").name).order_by(ident("unknown")),
        "unknown",
        "root ORDER BY",
    ),
    (
        "set-target",
        lambda: Query.match(Person(alias="p")).set(**{"unknown.age": 1}).return_(Person("p").name),
        "unknown",
        "mutation",
    ),
    (
        "delete-target",
        lambda: Query.match(Person(alias="p")).delete("unknown").return_(Person("p").name),
        "unknown",
        "mutation",
    ),
    (
        "remove-target",
        lambda: Query.match(Person(alias="p")).remove("unknown.age").return_(Person("p").name),
        "unknown",
        "mutation",
    ),
    (
        "with-projection-cannot-read-an-unbound-name",
        lambda: Query.match(Person(alias="p")).with_(n=Person(alias="q").name).return_(col("n")),
        "q",
        "WITH clause 1",
    ),
    (
        "with-where-unknown-name",
        lambda: (
            Query.match(Person(alias="p"))
            .with_(n=Person("p").name)
            .where(col("missing") > 0)
            .return_(col("n"))
        ),
        "missing",
        "WITH clause 1 WHERE",
    ),
    (
        "distinct-with-where-sees-only-projected-names",
        lambda: (
            Query.match(Person(alias="p"))
            .with_(n=Person("p").name, distinct=True)
            .where(Person("p").age > 18)
            .return_(col("n"))
        ),
        "p",
        "WITH clause 1 WHERE",
    ),
    (
        "aggregating-with-where-sees-only-projected-names",
        lambda: (
            Query.match(Person(alias="p"))
            .with_(n=Person("p").name, total=Person("p").count())
            .where(Person("p").age > 18)
            .return_(col("n"))
        ),
        "p",
        "WITH clause 1 WHERE",
    ),
    (
        "aggregating-with-order-by-sees-only-projected-names",
        lambda: (
            Query.match(Person(alias="p"))
            .with_(n=Person("p").name, total=Person("p").count())
            .order_by(Person("p").age)
            .return_(col("n"))
        ),
        "p",
        "WITH clause 1 ORDER BY",
    ),
    (
        "clauses-after-with-see-only-projected-names",
        lambda: (
            Query.match(Person(alias="p"))
            .with_(n=Person("p").name)
            .where(Person("p").age > 18)
            .return_(Person("p").age)
        ),
        "p",
        "RETURN",
    ),
    (
        "distinct-return-order-by-non-projected-expression",
        lambda: (
            Query.match(Person(alias="p"))
            .return_(Person("p").name, distinct=True)
            .order_by(Person("p").age)
        ),
        "p",
        "root ORDER BY",
    ),
]


@pytest.mark.parametrize(
    ("query_factory", "variable", "clause"),
    [case[1:] for case in INVALID_CASES],
    ids=[case[0] for case in INVALID_CASES],
)
def test_invalid_fluent_scope(
    query_factory: Callable[[], Query], variable: str, clause: str
) -> None:
    """Every invalid case raises `INVALID_CLAUSE` naming the variable and clause."""
    assert_invalid(query_factory(), variable, clause)


# ---------------------------------------------------------------------------
# Valid fluent cases: (id, query builder, expected statement)
# ---------------------------------------------------------------------------

VALID_CASES: list[tuple[str, Callable[[], Query], str]] = [
    (
        "issue-287-with-where-on-an-earlier-variable",
        lambda: (
            Query.match(Person(alias="p"))
            .with_(name=Person("p").name)
            .where(Person("p").age > 1)
            .return_("name")
        ),
        "MATCH (p:Person) WITH p.name AS name WHERE p.age > $p0 RETURN name",
    ),
    (
        "with-where-on-a-projected-alias",
        lambda: (
            Query.match(Person(alias="p"))
            .with_(n=Person("p").name)
            .where(col("n") == "Alice")
            .return_(col("n"))
        ),
        "MATCH (p:Person) WITH p.name AS n WHERE n = $p0 RETURN n",
    ),
    (
        "with-order-by-on-an-earlier-variable",
        lambda: (
            Query.match(Person(alias="p"))
            .with_(n=Person("p").name)
            .order_by(Person("p").age)
            .return_(col("n"))
        ),
        "MATCH (p:Person) WITH p.name AS n ORDER BY p.age ASC RETURN n",
    ),
    (
        "bare-variable-with-keeps-the-variable",
        lambda: Query.match(Person(alias="p")).with_(Person("p")).return_(Person("p").name),
        "MATCH (p:Person) WITH p RETURN p.name",
    ),
    (
        "raw-bare-variable-with-keeps-the-variable",
        lambda: Query.match(Person(alias="p")).with_("p").return_(Person("p").name),
        "MATCH (p:Person) WITH p RETURN p.name",
    ),
    (
        "mutation-after-with-accepts-a-projected-name",
        lambda: (
            Query.match(Person(alias="p"))
            .with_(Person(alias="p"))
            .set(Person("p").age == 30)
            .return_(Person("p").name)
        ),
        "MATCH (p:Person) WITH p SET p.age = $p0 RETURN p.name",
    ),
    (
        "with-projects-load-csv-and-unwind-names",
        lambda: (
            Query.load_csv("file:///tmp/people.csv", True, "row")
            .match(Person(alias="p"))
            .unwind("$batch", "item")
            .with_(Person(alias="p"), "row", "item")
            .return_("row.name")
        ),
        "LOAD CSV WITH HEADERS FROM $p0 AS row UNWIND $batch AS item MATCH (p:Person) WITH p, row, item RETURN row.name",
    ),
    (
        "linear-let-binds-from-its-own-entry-onward",
        lambda: (
            Query.match(Person(alias="p"))
            .let_(limit=lit(1))
            .linear_filter(col("limit") > 0)
            .return_(Person("p").name)
        ),
        "MATCH (p:Person) WITH *, $p0 AS limit WHERE limit > $p1 RETURN p.name",
    ),
    (
        "list-comprehension-variable-is-local",
        lambda: Query.match(Person(alias="p")).return_(
            items=list_comprehension("x", Person("p").city, None, ident("x"))
        ),
        "MATCH (p:Person) RETURN [x IN p.city | x] AS items",
    ),
    (
        "pattern-comprehension-variable-is-local",
        lambda: Query.match(Person(alias="p")).return_(
            friends=pattern_comprehension(
                path=Query.match(Person(alias="friend")),
                proj=Person("friend").name,
            )
        ),
        "MATCH (p:Person) RETURN [(friend:Person) | friend.name] AS friends",
    ),
    (
        "exists-subquery-can-read-outer-variables",
        lambda: (
            Query.match(Person(alias="p"))
            .where(
                Query.exists(
                    Query.match(Person(alias="q")).where(Person("q").name == Person("p").name)
                )
            )
            .return_(Person("p").name)
        ),
        "MATCH (p:Person) WHERE EXISTS { MATCH (q:Person) WHERE q.name = p.name } RETURN p.name",
    ),
    (
        "parameters-and-literals-are-always-in-scope",
        lambda: (
            Query.match(Person(alias="p"))
            .where(Person("p").age > param("min_age"))
            .return_(one=lit(1))
        ),
        "MATCH (p:Person) WHERE p.age > $min_age RETURN $p0 AS one",
    ),
    (
        "root-order-by-sees-return-projection-aliases",
        lambda: (
            Query.match(Person(alias="p")).return_(name=Person("p").name).order_by(ident("name"))
        ),
        "MATCH (p:Person) RETURN p.name AS name ORDER BY name ASC",
    ),
    (
        "distinct-return-order-by-accepts-a-projected-expression",
        lambda: (
            Query.match(Person(alias="p"))
            .return_(Person("p").city, distinct=True)
            .order_by(Person("p").city)
        ),
        "MATCH (p:Person) RETURN DISTINCT p.city ORDER BY p.city ASC",
    ),
    (
        "aggregating-return-order-by-accepts-a-projected-expression",
        lambda: (
            Query.match(Person(alias="p"))
            .return_(city=Person("p").city, count=Person("p").count())
            .order_by(Person("p").city)
        ),
        "MATCH (p:Person) RETURN p.city AS city, COUNT(p) AS count ORDER BY p.city ASC",
    ),
    (
        "raw-projection-string-is-opaque",
        lambda: Query.match(Person(alias="p")).return_("name AS n"),
        "MATCH (p:Person) RETURN name AS n",
    ),
    (
        "raw-identifier-function-argument-is-opaque",
        lambda: Query.match(Person(alias="p")).return_(fn.nodes(ident("p_path")).as_("nodes")),
        "MATCH (p:Person) RETURN nodes(p_path) AS nodes",
    ),
    (
        "aggregation-alias-is-in-root-order-by-scope",
        lambda: (
            Query.match(Person(alias="p"))
            .return_(city=Person("p").city, count=Person("p").count())
            .order_by(ident("count"))
        ),
        "MATCH (p:Person) RETURN p.city AS city, COUNT(p) AS count ORDER BY count ASC",
    ),
]


@pytest.mark.parametrize(
    ("query_factory", "expected"),
    [case[1:] for case in VALID_CASES],
    ids=[case[0] for case in VALID_CASES],
)
def test_valid_fluent_scope(query_factory: Callable[[], Query], expected: str) -> None:
    """Every valid case compiles to the expected statement."""
    assert_statement(query_factory(), expected)


def test_with_projection_that_is_not_a_bare_variable_needs_an_alias() -> None:
    """The fluent alias rule is unchanged by the scope check."""
    with pytest.raises(VoyagerError) as exc_info:
        Query.match(Person(alias="p")).with_(Person("p").name).return_(Person("p").name).compile(
            "cypher"
        )
    error = exc_info.value
    assert error.code == "INVALID_CLAUSE"
    assert "needs an alias" in str(error)


def test_qualified_raw_strings_are_resolved() -> None:
    """A dotted raw string becomes an identifier reference and is checked."""
    assert_invalid(
        Query.match(Person(alias="p")).return_("q.name"),
        "q",
        "RETURN",
    )


def test_issue_287_full_reproduction() -> None:
    """The three statements from issue #287 raise at compile time."""
    p, q = Person(alias="p"), Person(alias="q")
    assert_invalid(Query.match(p).with_(name=p.name).return_(p.name), "p", "RETURN")
    assert_invalid(Query.match(p).return_(q.name), "q", "RETURN")
    assert_invalid(
        Query.match(p).with_(name=p.name, distinct=True).order_by(p.age).return_("name"),
        "p",
        "WITH clause 1 ORDER BY",
    )


def test_issue_287_corrected_with_where_still_compiles() -> None:
    """`WITH ... WHERE` on an earlier variable is valid Cypher and still compiles."""
    p = Person(alias="p")
    compiled = Query.match(p).with_(name=p.name).where(p.age > 1).return_("name").compile("cypher")
    assert compiled.statement == (
        "MATCH (p:Person) WITH p.name AS name WHERE p.age > $p0 RETURN name"
    )


# ---------------------------------------------------------------------------
# A DISTINCT/aggregating ORDER BY or WHERE may repeat a projected expression.
# This table mirrors DISTINCT_ORDER_BY_CASES in test_spec_scope.py.
# ---------------------------------------------------------------------------

DISTINCT_ORDER_BY_CASES: list[tuple[str, Callable[[], Query], Any]] = [
    (
        "distinct-return-order-by-projected-field",
        lambda: (
            Query.match(Person(alias="p"))
            .return_(Person("p").city, distinct=True)
            .order_by(Person("p").city)
        ),
        "MATCH (p:Person) RETURN DISTINCT p.city ORDER BY p.city ASC",
    ),
    (
        "distinct-return-order-by-projected-subexpression",
        lambda: (
            Query.match(Person(alias="p"))
            .return_(Person("p").city, distinct=True)
            .order_by(Person("p").city + 1)
        ),
        "MATCH (p:Person) RETURN DISTINCT p.city ORDER BY p.city + $p0 ASC",
    ),
    (
        "distinct-return-order-by-aliased-projected-field",
        lambda: (
            Query.match(Person(alias="p"))
            .return_(city=Person("p").city, distinct=True)
            .order_by(Person("p").city)
        ),
        "MATCH (p:Person) RETURN DISTINCT p.city AS city ORDER BY p.city ASC",
    ),
    (
        "distinct-return-order-by-unprojected-field",
        lambda: (
            Query.match(Person(alias="p"))
            .return_(Person("p").city, distinct=True)
            .order_by(Person("p").age)
        ),
        ("p", "root ORDER BY"),
    ),
    (
        "distinct-return-order-by-bare-variable-when-only-property-projected",
        lambda: (
            Query.match(Person(alias="p"))
            .return_(Person("p").city, distinct=True)
            .order_by(ident("p"))
        ),
        ("p", "root ORDER BY"),
    ),
    (
        "distinct-return-order-by-partly-unprojected-subexpression",
        lambda: (
            Query.match(Person(alias="p"))
            .return_(Person("p").city, distinct=True)
            .order_by(Person("p").city + Person("p").age)
        ),
        ("p", "root ORDER BY"),
    ),
    (
        "aggregating-return-order-by-projected-field",
        lambda: (
            Query.match(Person(alias="p"))
            .return_(city=Person("p").city, count=Person("p").count())
            .order_by(Person("p").city)
        ),
        "MATCH (p:Person) RETURN p.city AS city, COUNT(p) AS count ORDER BY p.city ASC",
    ),
    (
        "aggregating-return-order-by-repeated-aggregation",
        lambda: (
            Query.match(Person(alias="p"))
            .return_(count=Person("p").count())
            .order_by(fn.count(Person("p")))
        ),
        "MATCH (p:Person) RETURN COUNT(p) AS count ORDER BY count(p) ASC",
    ),
    (
        "aggregating-return-order-by-unprojected-field",
        lambda: (
            Query.match(Person(alias="p"))
            .return_(city=Person("p").city, count=Person("p").count())
            .order_by(Person("p").age)
        ),
        ("p", "root ORDER BY"),
    ),
    (
        "distinct-with-order-by-projected-field",
        lambda: (
            Query.match(Person(alias="p"))
            .with_(city=Person("p").city, distinct=True)
            .order_by(Person("p").city)
            .return_("city")
        ),
        "MATCH (p:Person) WITH DISTINCT p.city AS city ORDER BY p.city ASC RETURN city",
    ),
    (
        "aggregating-with-order-by-projected-field",
        lambda: (
            Query.match(Person(alias="p"))
            .with_(city=Person("p").city, c=Person("p").count())
            .order_by(Person("p").city)
            .return_("city", "c")
        ),
        "MATCH (p:Person) WITH p.city AS city, COUNT(p) AS c ORDER BY p.city ASC RETURN city, c",
    ),
    (
        "distinct-with-where-projected-field",
        lambda: (
            Query.match(Person(alias="p"))
            .with_(city=Person("p").city, distinct=True)
            .where(Person("p").city.is_not_null())
            .return_("city")
        ),
        "MATCH (p:Person) WITH DISTINCT p.city AS city WHERE p.city IS NOT NULL RETURN city",
    ),
    (
        "distinct-with-order-by-unprojected-field",
        lambda: (
            Query.match(Person(alias="p"))
            .with_(city=Person("p").city, distinct=True)
            .order_by(Person("p").age)
            .return_("city")
        ),
        ("p", "WITH clause 1 ORDER BY"),
    ),
    (
        "distinct-with-where-unprojected-field",
        lambda: (
            Query.match(Person(alias="p"))
            .with_(city=Person("p").city, distinct=True)
            .where(Person("p").age.is_not_null())
            .return_("city")
        ),
        ("p", "WITH clause 1 WHERE"),
    ),
]


@pytest.mark.parametrize(
    ("query_factory", "expected"),
    [case[1:] for case in DISTINCT_ORDER_BY_CASES],
    ids=[case[0] for case in DISTINCT_ORDER_BY_CASES],
)
def test_distinct_order_by_table(query_factory: Callable[[], Query], expected: Any) -> None:
    """A DISTINCT/aggregating clause may repeat a projected expression."""
    if isinstance(expected, tuple):
        assert_invalid(query_factory(), *expected)
    else:
        assert_statement(query_factory(), expected)
