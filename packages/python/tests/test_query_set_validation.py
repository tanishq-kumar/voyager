"""Tests for Query.set, on_create_set, and on_match_set argument validation and compilation (#295)."""

from __future__ import annotations

from typing import Any

import pytest
from voyager_ogm import Field, Node, Query, VoyagerError


class Person(Node):
    name: str = Field()
    city: str = Field()
    age: int = Field()
    status: str = Field()


REJECTED_KWARGS_SET: list[tuple[dict[str, Any], str]] = [
    ({"city": "Berlin"}, "city='Berlin'"),
    ({"p": {"city": "Berlin"}}, "p={'city': 'Berlin'}"),
    ({"nam": "x"}, "nam='x'"),
    ({"p.": "Berlin"}, "p.='Berlin'"),
    ({".city": "Berlin"}, ".city='Berlin'"),
    ({".": "Berlin"}, ".='Berlin'"),
    ({"p..city": "Berlin"}, "p..city='Berlin'"),
    ({"p. ": "Berlin"}, "p. ='Berlin'"),
    ({" .city": "Berlin"}, " .city='Berlin'"),
    ({"p.city.extra": "Berlin"}, "p.city.extra='Berlin'"),
]


@pytest.mark.parametrize("kwargs,offending", REJECTED_KWARGS_SET)
def test_query_set_rejected_keyword_arguments(kwargs: dict[str, Any], offending: str) -> None:
    p = Person(alias="p")
    q = Query.match(p)
    with pytest.raises(VoyagerError) as exc_info:
        q.set(**kwargs)

    assert exc_info.value.code == "INVALID_CLAUSE"
    assert offending in str(exc_info.value)
    assert (
        'Supported forms: set(p.city == value), a Node with changed fields, set(**{"p.city": value})'
        in str(exc_info.value)
    )


p_clean = Person(alias="p")
REJECTED_POSITIONAL_SET: list[tuple[Any, str]] = [
    ("Berlin", "'Berlin'"),
    (123, "123"),
    ({"city": "Berlin"}, "{'city': 'Berlin'}"),
    (["city", "Berlin"], "['city', 'Berlin']"),
    (p_clean.city, "p.city"),
    (p_clean.city > "Berlin", "(p.city gt 'Berlin')"),
    (p_clean.city != "Berlin", "(p.city ne 'Berlin')"),
]


@pytest.mark.parametrize("arg,offending", REJECTED_POSITIONAL_SET)
def test_query_set_rejected_positional_arguments(arg: Any, offending: str) -> None:
    p = Person(alias="p")
    q = Query.match(p)
    with pytest.raises(VoyagerError) as exc_info:
        q.set(arg)

    assert exc_info.value.code == "INVALID_CLAUSE"
    assert offending in str(exc_info.value)
    assert (
        'Supported forms: set(p.city == value), a Node with changed fields, set(**{"p.city": value})'
        in str(exc_info.value)
    )


REJECTED_MERGE_SET_KWARGS: list[tuple[str, dict[str, Any], str]] = [
    ("on_create_set", {"city": "Berlin"}, "city='Berlin'"),
    ("on_create_set", {"p.": "Berlin"}, "p.='Berlin'"),
    ("on_create_set", {".city": "Berlin"}, ".city='Berlin'"),
    ("on_match_set", {"city": "Berlin"}, "city='Berlin'"),
    ("on_match_set", {"p.": "Berlin"}, "p.='Berlin'"),
    ("on_match_set", {".city": "Berlin"}, ".city='Berlin'"),
]


@pytest.mark.parametrize("method_name,kwargs,offending", REJECTED_MERGE_SET_KWARGS)
def test_merge_set_rejected_keyword_arguments(
    method_name: str, kwargs: dict[str, Any], offending: str
) -> None:
    p = Person(alias="p")
    q = Query.merge(p)
    method = getattr(q, method_name)
    with pytest.raises(VoyagerError) as exc_info:
        method(**kwargs)

    assert exc_info.value.code == "INVALID_CLAUSE"
    assert offending in str(exc_info.value)
    assert f"Supported forms: {method_name}(p.city == value)" in str(exc_info.value)


REJECTED_MERGE_SET_POSITIONAL: list[tuple[str, Any, str]] = [
    ("on_create_set", "Berlin", "'Berlin'"),
    ("on_create_set", 123, "123"),
    ("on_create_set", p_clean.city, "p.city"),
    ("on_create_set", p_clean.city > "Berlin", "(p.city gt 'Berlin')"),
    ("on_match_set", "Berlin", "'Berlin'"),
    ("on_match_set", 123, "123"),
    ("on_match_set", p_clean.city, "p.city"),
    ("on_match_set", p_clean.city > "Berlin", "(p.city gt 'Berlin')"),
]


@pytest.mark.parametrize("method_name,arg,offending", REJECTED_MERGE_SET_POSITIONAL)
def test_merge_set_rejected_positional_arguments(
    method_name: str, arg: Any, offending: str
) -> None:
    p = Person(alias="p")
    q = Query.merge(p)
    method = getattr(q, method_name)
    with pytest.raises(VoyagerError) as exc_info:
        method(arg)

    assert exc_info.value.code == "INVALID_CLAUSE"
    assert offending in str(exc_info.value)
    assert f"Supported forms: {method_name}(p.city == value)" in str(exc_info.value)


def test_supported_set_predicate_expression() -> None:
    p = Person(alias="p")
    q = Query.match(p).set(p.city == "Berlin").return_(p.name)
    compiled_cypher = q.compile("cypher")
    assert compiled_cypher.statement == "MATCH (p:Person) SET p.city = $p0 RETURN p.name"
    assert compiled_cypher.parameters == {"p0": "Berlin"}

    compiled_gql = q.compile("iso_gql")
    assert compiled_gql.statement == "MATCH (p:Person) SET p.city = $p0 RETURN p.name"
    assert compiled_gql.parameters == {"p0": "Berlin"}


def test_supported_set_multiple_predicate_expressions() -> None:
    p = Person(alias="p")
    q = Query.match(p).set(p.name == "Alice", p.city == "Berlin").return_(p.name)
    compiled = q.compile("cypher")
    assert compiled.statement == "MATCH (p:Person) SET p.name = $p0, p.city = $p1 RETURN p.name"
    assert compiled.parameters == {"p0": "Alice", "p1": "Berlin"}


def test_supported_set_node_with_dirty_fields() -> None:
    p = Person(alias="p")
    p.city = "Berlin"
    assert p.dirty_fields == {"city": "Berlin"}
    q = Query.match(p).set(p).return_(p.name)
    compiled = q.compile("cypher")
    assert compiled.statement == "MATCH (p:Person) SET p.city = $p0 RETURN p.name"
    assert compiled.parameters == {"p0": "Berlin"}


def test_supported_set_node_with_multiple_dirty_fields() -> None:
    p = Person(alias="p")
    p.name = "Alice"
    p.city = "Berlin"
    assert p.dirty_fields == {"name": "Alice", "city": "Berlin"}
    q = Query.match(p).set(p).return_(p.name)
    compiled = q.compile("cypher")
    assert compiled.statement == "MATCH (p:Person) SET p.name = $p0, p.city = $p1 RETURN p.name"
    assert compiled.parameters == {"p0": "Alice", "p1": "Berlin"}


def test_supported_set_dotted_kwargs() -> None:
    p = Person(alias="p")
    q = Query.match(p).set(**{"p.city": "Berlin"}).return_(p.name)
    compiled = q.compile("cypher")
    assert compiled.statement == "MATCH (p:Person) SET p.city = $p0 RETURN p.name"
    assert compiled.parameters == {"p0": "Berlin"}


def test_supported_set_mixed_forms() -> None:
    p = Person(alias="p")
    q = Query.match(p).set(p.name == "Alice", **{"p.city": "Berlin"}).return_(p.name)
    compiled = q.compile("cypher")
    assert compiled.statement == "MATCH (p:Person) SET p.name = $p0, p.city = $p1 RETURN p.name"
    assert compiled.parameters == {"p0": "Alice", "p1": "Berlin"}


def test_supported_set_class_level_resolution() -> None:
    q = Query.match(Person).set(Person.city == "Berlin")
    compiled = q.compile("cypher")
    assert compiled.statement == "MATCH (_person_0:Person) SET _person_0.city = $p0"
    assert compiled.parameters == {"p0": "Berlin"}


def test_supported_on_create_and_on_match_set_forms() -> None:
    p = Person(alias="p")
    q = Query.merge(p).on_create_set(p.city == "Berlin").on_match_set(p.name == "Alice")
    compiled = q.compile("cypher")
    assert (
        compiled.statement
        == "MERGE (p:Person) ON CREATE SET p.city = $p0 ON MATCH SET p.name = $p1"
    )
    assert compiled.parameters == {"p0": "Berlin", "p1": "Alice"}


def test_supported_merge_set_dirty_node_and_dotted_kwargs() -> None:
    p = Person(alias="p")
    p_dirty = Person(alias="p")
    p_dirty.city = "Berlin"
    q = Query.merge(p).on_create_set(p_dirty).on_match_set(**{"p.name": "Alice"})
    compiled = q.compile("cypher")
    assert (
        compiled.statement
        == "MERGE (p:Person) ON CREATE SET p.city = $p0 ON MATCH SET p.name = $p1"
    )
    assert compiled.parameters == {"p0": "Berlin", "p1": "Alice"}


@pytest.mark.parametrize("method_name", ["set", "on_create_set", "on_match_set"])
def test_node_without_changed_fields_is_a_no_op(method_name: str) -> None:
    """A Node with no changed fields writes nothing and does not raise, as before."""
    p = Person(alias="p")
    q = Query.match(p) if method_name == "set" else Query.merge(p)
    getattr(q, method_name)(p)
    assert "SET" not in q.return_(p).compile("cypher").statement
