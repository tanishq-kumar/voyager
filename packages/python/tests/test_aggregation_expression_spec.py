"""Expression spec coverage and aggregate misuse (issue #272).

Every expression class in the Python SDK that produces a Query Spec value must
be accepted by the consumer in a valid position. Aggregations are projection
items only: the consumer rejects an `agg` array in an expression position with a
specific message, and the fluent API raises at the point of misuse.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any

import pytest
import voyager_ogm.expressions as expressions_module
import voyager_ogm.models as models_module
import voyager_ogm.types as types_module
from voyager_ogm import Query, VoyagerError, compile_query_from_spec, fn
from voyager_ogm.expressions import (
    AliasedExpr,
    BinaryExpr,
    CaseExpr,
    Expression,
    FunctionExpr,
    IdentExpr,
    LabelPredicateExpr,
    ListCompExpr,
    ListLiteralExpr,
    LiteralExpr,
    ParamExpr,
    PatternCompExpr,
    PropExpr,
    SubqueryExpr,
    UnaryExpr,
)
from voyager_ogm.models import AggregationExpr, BoundField, Field, Node, PredicateExpr
from voyager_ogm.types import Point

MISUSE_MESSAGE = "aggregations are only valid in WITH/RETURN projections"


class SpecExprPerson(Node):
    """Node model used by the expression-position tests."""

    __labels__ = ["SpecExprPerson"]

    name: str = Field()
    age: int = Field()


def _person(alias: str = "p") -> SpecExprPerson:
    return SpecExprPerson(alias=alias)


def _make_prop() -> Expression:
    return PropExpr("p", "name")


def _make_ident() -> Expression:
    return IdentExpr("p")


def _make_label_predicate() -> Expression:
    return LabelPredicateExpr("p", ["SpecExprPerson"])


def _make_param() -> Expression:
    return ParamExpr("batch")


def _make_literal() -> Expression:
    return LiteralExpr(42)


def _make_list_literal() -> Expression:
    return ListLiteralExpr([LiteralExpr(1), LiteralExpr(2)])


def _make_binary() -> Expression:
    return BinaryExpr(PropExpr("p", "age"), ">", LiteralExpr(30))


def _make_unary() -> Expression:
    return UnaryExpr("NOT", PropExpr("p", "name"))


def _make_function() -> Expression:
    return FunctionExpr("toLower", [PropExpr("p", "name")])


def _make_case() -> Expression:
    return CaseExpr(
        None,
        [(BinaryExpr(PropExpr("p", "age"), ">", LiteralExpr(18)), LiteralExpr("adult"))],
        LiteralExpr("minor"),
    )


def _make_list_comp() -> Expression:
    return ListCompExpr("x", IdentExpr("p"), None, None)


def _make_pattern_comp() -> Expression:
    return PatternCompExpr(Query.match(_person()), PropExpr("p", "name"))


def _make_exists() -> Expression:
    return SubqueryExpr("exists", Query.match(_person("f")))


def _make_count() -> Expression:
    return SubqueryExpr("count", Query.match(_person("f")))


def _make_alias() -> Expression:
    return AliasedExpr(PropExpr("p", "name"), "alias")


def _make_bound_field() -> Expression:
    return BoundField("p", "name")


def _make_aggregation() -> Expression:
    return AggregationExpr("p", "name", "count")


def _make_predicate() -> Expression:
    return PredicateExpr("p", "age", ">", 30)


def _make_point() -> Expression:
    return Point(1.0, 2.0)


EXPRESSION_FACTORIES: dict[type[Expression], Callable[[], Expression]] = {
    AggregationExpr: _make_aggregation,
    AliasedExpr: _make_alias,
    BinaryExpr: _make_binary,
    BoundField: _make_bound_field,
    CaseExpr: _make_case,
    FunctionExpr: _make_function,
    IdentExpr: _make_ident,
    LabelPredicateExpr: _make_label_predicate,
    ListCompExpr: _make_list_comp,
    ListLiteralExpr: _make_list_literal,
    LiteralExpr: _make_literal,
    ParamExpr: _make_param,
    PatternCompExpr: _make_pattern_comp,
    Point: _make_point,
    PredicateExpr: _make_predicate,
    PropExpr: _make_prop,
    SubqueryExpr: _make_exists,
    UnaryExpr: _make_unary,
}


def _expression_classes() -> set[type[Expression]]:
    """Collect every concrete Expression subclass exposed by the package."""
    found: set[type[Expression]] = set()
    for module in (expressions_module, models_module, types_module):
        for _, obj in inspect.getmembers(module, inspect.isclass):
            if obj is Expression:
                continue
            if issubclass(obj, Expression) and callable(getattr(obj, "to_spec", None)):
                found.add(obj)
    return found


def test_every_expression_class_has_a_factory() -> None:
    """A new expression class must be added to the coverage list below."""
    assert _expression_classes() == set(EXPRESSION_FACTORIES)


@pytest.mark.parametrize(
    ("expression_type", "factory"),
    sorted(EXPRESSION_FACTORIES.items(), key=lambda item: item[0].__name__),
    ids=lambda value: value.__name__ if inspect.isclass(value) else None,
)
def test_expression_spec_is_accepted(
    expression_type: type[Expression], factory: Callable[[], Expression]
) -> None:
    """Each expression's spec compiles from a valid projection position."""
    expression = factory()
    if expression_type is AggregationExpr:
        item = list(expression.to_spec())
        assert item[0] == "agg"
        item[-1] = "v"
        projection = item
    else:
        projection = ["expr", expression.to_spec(), "v"]

    spec: dict[str, Any] = {
        "spec_version": 1,
        "matches": [{"paths": [[["node", "p", ["SpecExprPerson"]]]]}],
        "projections": [projection],
    }
    compiled = compile_query_from_spec(spec, "cypher")
    assert compiled["statement"].startswith("MATCH (p:SpecExprPerson) RETURN ")


@pytest.mark.parametrize(
    "build",
    [
        lambda p: p.age.count() > 2,
        lambda p: -p.age.count(),
        lambda p: fn.size(p.age.count()),
        lambda p: Query.match(p).where(p.age.count()),
        lambda p: Query.match(p).where_not(p.age.count()),
        lambda p: Query.match(p).order_by(p.age.count()),
        lambda p: Query.match(p).set(total=p.age.count()),
        lambda p: Query.match(p).let_(total=p.age.count()),
        lambda p: Query.match(p).linear_filter(p.age.count()),
    ],
    ids=[
        "binary",
        "unary",
        "function-argument",
        "where",
        "where_not",
        "order_by",
        "set",
        "let",
        "linear_filter",
    ],
)
def test_aggregation_misuse_raises_immediately(
    build: Callable[[SpecExprPerson], Any],
) -> None:
    """An aggregation outside a projection raises the clear INVALID_CLAUSE error."""
    p = _person()
    with pytest.raises(VoyagerError) as exc_info:
        build(p)
    assert exc_info.value.code == "INVALID_CLAUSE"
    assert MISUSE_MESSAGE in str(exc_info.value)


def test_aggregation_is_valid_in_return_and_with() -> None:
    """The same AggregationExpr still compiles as a WITH/RETURN projection."""
    p = _person()

    returned = Query.match(p).return_(p.age.count().as_("total"))
    assert "COUNT(p.age) AS total" in returned.compile("cypher").statement

    with_clause = Query.match(p).with_(p.age.avg().as_("avg_age")).return_("avg_age")
    assert "AVG(p.age) AS avg_age" in with_clause.compile("cypher").statement


def test_consumer_rejects_agg_in_expression_with_specific_message() -> None:
    """An `agg` array in `where` names aggregate misuse, not a literal hint."""
    spec: dict[str, Any] = {
        "spec_version": 1,
        "matches": [
            {
                "paths": [[["node", "p", ["SpecExprPerson"]]]],
                "where": [
                    "bin",
                    "gt",
                    ["agg", "p", "age", "count", None],
                    ["lit", 2],
                ],
            }
        ],
        "projections": [["field", "p", "name", None]],
    }
    with pytest.raises(VoyagerError) as exc_info:
        compile_query_from_spec(spec, "cypher")

    error = exc_info.value
    assert error.code == "INVALID_SPEC"
    assert error.path == "/matches/0/where/2"
    assert "aggregations are only valid as projection items" in str(error)
    assert "write a scalar literal as" not in str(error)
