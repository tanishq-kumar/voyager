"""Fluent Query -> Query Spec v1 round-trip (issue #189).

Every construct the fluent API can serialise must compile from its spec to the
same statement and parameters as the fluent query, in every dialect where the
fluent query compiles. The serialised document must also validate against the
v1 JSON Schema and contain JSON lists, never Python tuples.
"""

from __future__ import annotations

import json
import warnings
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator
from voyager_ogm import Query, VoyagerError, compile_query_from_spec, fn, reset_config
from voyager_ogm.expressions import case, col, list_comprehension
from voyager_ogm.models import Field, Node, Relationship

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA = json.loads((REPO_ROOT / "spec" / "query_spec.v1.schema.json").read_text(encoding="utf-8"))
DIALECTS = ("cypher", "iso_gql", "age", "sql_pgq")


@pytest.fixture(autouse=True)
def _default_config() -> Any:
    """Keep the round-trip comparison on the default (unoptimised) build path."""
    reset_config()
    yield
    reset_config()


class RoundtripPerson(Node):
    """Node model used by the round-trip queries."""

    __labels__ = ["RoundtripPerson"]

    name: str = Field()
    age: int = Field()
    city: str = Field()


class RoundtripKnows(Relationship):
    """Relationship model used by the round-trip queries."""

    __type__ = "ROUNDTRIP_KNOWS"

    since: int = Field()


def _person(alias: str = "p") -> RoundtripPerson:
    return RoundtripPerson(alias=alias)


def _query_match_properties() -> Query:
    p = RoundtripPerson(alias="p", name="Alice", age=30)
    return Query.match(p).return_(p.name, p.age)


def _query_where_predicates() -> Query:
    p = _person()
    return (
        Query.match(p)
        .where(p.age > 30, p.name.startswith("A"), p.city.in_(["Berlin", "Paris"]))
        .return_(p.name)
    )


def _query_multi_hop_edges() -> Query:
    p, f = _person("p"), _person("f")
    return Query.match(p).to(RoundtripKnows(alias="r", since=2020)).node(f).return_(p.name, f.name)


def _query_variable_length_edges() -> Query:
    p, f = _person("p"), _person("f")
    return Query.match(p).to(RoundtripKnows(alias="r")).node(f).hops(1, 3).return_(f.name)


def _query_optional_match() -> Query:
    p, f = _person("p"), _person("f")
    return (
        Query.match(p)
        .optional_match(p)
        .to(RoundtripKnows(alias="r"))
        .node(f)
        .return_(p.name, f.name)
    )


def _query_path_mode() -> Query:
    p, f = _person("p"), _person("f")
    return (
        Query.match(p).trail("path").to(RoundtripKnows(alias="r")).node(f).return_(p.name, f.name)
    )


def _query_with_clause() -> Query:
    p = _person()
    return (
        Query.match(p)
        .with_(name=p.name, bucket=p.age)
        .where(col("bucket") >= 18)
        .return_("name")
        .order_by("name")
    )


def _query_with_pagination() -> Query:
    p = _person()
    return (
        Query.match(p)
        .with_(name=p.name, distinct=True)
        .order_by("name", ascending=False)
        .skip(1)
        .limit(5)
        .return_("name")
    )


def _query_root_pagination_distinct() -> Query:
    p = _person()
    return (
        Query.match(p)
        .return_(p.name.as_("name"), distinct=True)
        .order_by("name", ascending=False)
        .skip(2)
        .limit(10)
    )


def _query_with_aggregations() -> Query:
    p = _person()
    return (
        Query.match(p)
        .with_(name=p.name, count=p.name.count(), avg_age=p.age.avg())
        .return_("name", "count", "avg_age")
    )


def _query_aliased_aggregation() -> Query:
    p = _person()
    return Query.match(p).with_(p.age.avg().as_("avg_age")).return_("avg_age")


def _query_unwind() -> Query:
    return Query.unwind("$batch", "row").match(_person()).return_(_person().name)


def _query_case() -> Query:
    p = _person()
    return Query.match(p).return_(case().when(p.age > 18, "adult").else_("minor").as_("age_group"))


def _query_functions() -> Query:
    p = _person()
    return Query.match(p).return_(fn.to_lower(p.name).as_("lower_name"), fn.size(p.name))


def _query_list_comprehension() -> Query:
    p = _person()
    return Query.match(p).return_(
        list_comprehension(
            "x",
            fn.range_(1, 3),
            where=fn.size(p.name) > 0,
            map_expr=fn.to_lower(p.name),
        ).as_("items")
    )


def _query_exists_subquery() -> Query:
    p, f = _person("p"), _person("f")
    return Query.match(p).where(Query.exists(Query.match(f).where(f.age > 18))).return_(p.name)


def _query_count_subquery() -> Query:
    p, f = _person("p"), _person("f")
    return Query.match(p).where(Query.count(Query.match(f).where(f.age > 18)) > 2).return_(p.name)


def _query_create() -> Query:
    p = RoundtripPerson(alias="p", name="Alice", age=30)
    return Query.create(p).return_(p.name)


def _query_merge_with_sets() -> Query:
    p = RoundtripPerson(alias="p", name="Alice")
    p.age = 30
    return Query.merge(p).on_create_set(p.age == 30).on_match_set(p.age == 31).return_(p.name)


def _query_set_and_remove() -> Query:
    p = _person()
    return Query.match(p).where(p.name == "Alice").set(p.age == 31).remove(p.city).return_(p.name)


def _query_delete() -> Query:
    p = _person()
    return Query.match(p).where(p.name == "Alice").delete(p)


def _query_detach_delete() -> Query:
    p = _person()
    return Query.match(p).where(p.name == "Alice").detach_delete(p)


def _query_explain() -> Query:
    p = _person()
    return Query.match(p).return_(p.name).explain()


def _query_linear_clauses() -> Query:
    p = _person()
    return Query.match(p).let_(threshold=30).linear_filter(p.age > 30).return_(p.name)


def _query_load_csv() -> Query:
    row = RoundtripPerson(alias="row")
    return Query.load_csv("file:///tmp/roundtrip.csv").match(row).return_(row.name)


def _query_open_hops_min() -> Query:
    p, f = _person("p"), _person("f")
    return Query.match(p).to(RoundtripKnows(alias="r")).node(f).hops(min_hops=2).return_(f.name)


def _query_open_hops_max() -> Query:
    p, f = _person("p"), _person("f")
    return Query.match(p).to(RoundtripKnows(alias="r")).node(f).hops(max_hops=3).return_(f.name)


def _query_set_map_merge() -> Query:
    p = _person()
    return Query.match(p).where(p.name == "Alice").set_merge(p, {"city": "Berlin"}).return_(p.name)


QUERY_FACTORIES: dict[str, Callable[[], Query]] = {
    "match_properties": _query_match_properties,
    "where_predicates": _query_where_predicates,
    "multi_hop_edges": _query_multi_hop_edges,
    "variable_length_edges": _query_variable_length_edges,
    "open_hops_min": _query_open_hops_min,
    "open_hops_max": _query_open_hops_max,
    "optional_match": _query_optional_match,
    "path_mode": _query_path_mode,
    "with_clause": _query_with_clause,
    "with_pagination": _query_with_pagination,
    "root_pagination_distinct": _query_root_pagination_distinct,
    "with_aggregations": _query_with_aggregations,
    "aliased_aggregation": _query_aliased_aggregation,
    "unwind": _query_unwind,
    "case": _query_case,
    "functions": _query_functions,
    "list_comprehension": _query_list_comprehension,
    "exists_subquery": _query_exists_subquery,
    "count_subquery": _query_count_subquery,
    "create": _query_create,
    "merge_with_sets": _query_merge_with_sets,
    "set_and_remove": _query_set_and_remove,
    "set_map_merge": _query_set_map_merge,
    "delete": _query_delete,
    "detach_delete": _query_detach_delete,
    "linear_clauses": _query_linear_clauses,
    "explain": _query_explain,
    "load_csv": _query_load_csv,
}


def _contains_tuple(value: Any) -> bool:
    if isinstance(value, tuple):
        return True
    if isinstance(value, list):
        return any(_contains_tuple(item) for item in value)
    if isinstance(value, dict):
        return any(_contains_tuple(item) for item in value.values())
    return False


def _compile(query: Query, dialect: str) -> Any:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return query.compile(dialect)


@pytest.mark.parametrize("name", sorted(QUERY_FACTORIES))
def test_fluent_spec_roundtrip(name: str) -> None:
    """Each fluent query round-trips through its spec for every working dialect."""
    query = QUERY_FACTORIES[name]()
    spec = query.to_spec_dict()

    assert isinstance(spec, dict)
    assert spec["spec_version"] == 1
    assert not _contains_tuple(spec), f"{name}: spec still contains Python tuples"
    Draft202012Validator(SCHEMA).validate(spec)

    json_spec = json.loads(query.to_json())
    assert json_spec == spec

    compiled_any = False
    for dialect in DIALECTS:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            try:
                expected = _compile(query, dialect)
            except Exception:
                continue
            compiled = compile_query_from_spec(json_spec, dialect)
        assert compiled["statement"] == expected.statement, (
            f"{name}: {dialect} statement differs\n"
            f"fluent: {expected.statement}\nspec: {compiled['statement']}"
        )
        assert compiled["parameters"] == expected.parameters, (
            f"{name}: {dialect} parameters differ\n"
            f"fluent: {expected.parameters}\nspec: {compiled['parameters']}"
        )
        compiled_any = True

    assert compiled_any, f"{name}: the fluent query did not compile in any dialect"


def test_fluent_with_projection_requires_an_alias() -> None:
    """A non-bare WITH projection without `AS` raises INVALID_CLAUSE at compile."""
    p = _person()
    query = Query.match(p).with_(p.name).return_("name")
    with pytest.raises(VoyagerError) as exc_info:
        query.compile("cypher")
    assert exc_info.value.code == "INVALID_CLAUSE"
    assert "needs an alias" in str(exc_info.value)
    assert "only a bare variable may be projected without one" in str(exc_info.value)


def test_fluent_with_bare_variable_and_aliased_projection_compile() -> None:
    """A bare variable and an explicitly aliased projection still compile."""
    p = _person()
    bare = Query.match(p).with_(p).return_(p.name)
    assert "WITH p RETURN p.name" in bare.compile("cypher").statement

    aliased = Query.match(p).with_(name=p.name).return_("name")
    assert "WITH p.name AS name RETURN name" in aliased.compile("cypher").statement


def test_to_json_forwards_json_kwargs() -> None:
    """`to_json` forwards keyword arguments to `json.dumps`."""
    p = _person()
    query = Query.match(p).return_(p.name)
    text = query.to_json(indent=2, sort_keys=True)
    assert "\n" in text
    assert text == json.dumps(query.to_spec_dict(), indent=2, sort_keys=True)


def test_procedure_call_raises_unsupported_feature() -> None:
    """Constructs Query Spec cannot express raise `UNSUPPORTED_FEATURE`."""
    from voyager_ogm import VoyagerError

    query = Query.call("apoc.path.expandConfig", 1)
    with pytest.raises(VoyagerError) as exc_info:
        query.to_spec_dict()
    assert exc_info.value.code == "UNSUPPORTED_FEATURE"
    assert "procedure call" in str(exc_info.value)
