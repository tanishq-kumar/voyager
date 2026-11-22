"""Tests for DML Mutations (CREATE, MERGE, SET, DELETE, REMOVE) and Model Dirty Tracking."""

import pytest
from voyager_ogm import Query, node, relationship
from voyager_ogm.models import reset_alias_counters


@node(label="Person")
class Person:
    name: str
    age: int
    status: str = "PENDING"


@relationship(type_name="FOLLOWS")
class Follows:
    since: int = 2026


@pytest.fixture(autouse=True)
def _reset_aliases():
    reset_alias_counters()


def test_python_create_single_node():
    p = Person()
    query = Query.create(p)
    compiled = query.compile("cypher")

    assert compiled.statement == "CREATE (_person_0:Person {status: $p0})"
    assert compiled.parameters == {"p0": "PENDING"}


def test_python_create_relationship_path():
    a = Person()
    b = Person()
    f = Follows()

    query = Query.create(a).to(f).node(b)
    compiled = query.compile("cypher")

    assert (
        compiled.statement
        == "CREATE (_person_0:Person {status: $p0})-[_follows_0:FOLLOWS {since: $p1}]->"
        "(_person_1:Person {status: $p2})"
    )


def test_python_merge_with_on_create_and_on_match():
    p = Person()
    query = (
        Query.merge(p)
        .on_create_set(p.name == "Alice", p.age == 30)
        .on_match_set(p.status == "ACTIVE")
    )
    compiled = query.compile("cypher")

    expected_stmt = (
        "MERGE (_person_0:Person) "
        "ON CREATE SET _person_0.status = $p0, _person_0.name = $p1, _person_0.age = $p2 "
        "ON MATCH SET _person_0.status = $p3"
    )
    assert compiled.statement == expected_stmt
    assert compiled.parameters["p0"] == "PENDING"
    assert compiled.parameters["p1"] == "Alice"
    assert compiled.parameters["p2"] == 30
    assert compiled.parameters["p3"] == "ACTIVE"


def test_python_match_then_set_properties():
    p = Person()
    query = (
        Query.match(p).where(p.age > 18).set(p.status == "VERIFIED", p.age == 21).return_(p.name)
    )
    compiled = query.compile("cypher")

    expected_stmt = (
        "MATCH (_person_0:Person) "
        "WHERE _person_0.age > $p0 "
        "SET _person_0.status = $p1, _person_0.age = $p2 "
        "RETURN _person_0.name"
    )
    assert compiled.statement == expected_stmt
    assert compiled.parameters["p0"] == 18
    assert compiled.parameters["p1"] == "VERIFIED"
    assert compiled.parameters["p2"] == 21


def test_python_detach_delete():
    p = Person()
    query = Query.match(p).where(p.status == "BANNED").detach_delete(p)
    compiled = query.compile("cypher")

    assert (
        compiled.statement
        == "MATCH (_person_0:Person) WHERE _person_0.status = $p0 DETACH DELETE _person_0"
    )
    assert compiled.parameters["p0"] == "BANNED"


def test_python_remove_property():
    p = Person()
    query = Query.match(p).where(p.name == "TempUser").remove(p.status)
    compiled = query.compile("cypher")

    assert (
        compiled.statement
        == "MATCH (_person_0:Person) WHERE _person_0.name = $p0 REMOVE _person_0.status"
    )


def test_python_model_dirty_tracking():
    p = Person(name="Alice", age=25)
    assert p.dirty_fields == {"name": "Alice", "age": 25, "status": "PENDING"}

    p.clear_dirty()
    assert p.dirty_fields == {}

    p.age = 26
    p.status = "VERIFIED"
    assert p.dirty_fields == {"age": 26, "status": "VERIFIED"}

    # Using dirty node in Query.set
    query = Query.match(p).where(p.name == "Alice").set(p)
    compiled = query.compile("cypher")

    assert "SET _person_0.age = $p1, _person_0.status = $p2" in compiled.statement
    assert compiled.parameters["p1"] == 26
    assert compiled.parameters["p2"] == "VERIFIED"


def test_set_kwargs_preserves_main_behavior():
    p = Person()
    # On main, key without dot is ignored, does not do map merge
    q_bare = Query.match(p).set(p={"city": "Berlin"}).return_(p)
    compiled_bare = q_bare.compile("cypher")
    assert "SET" not in compiled_bare.statement

    # Key with dot assigns property
    q_dot = Query.match(p).set(**{"_person_0.city": "Berlin"}).return_(p)
    compiled_dot = q_dot.compile("cypher")
    assert "SET _person_0.city = $p0" in compiled_dot.statement


def test_set_merge_explicit_method():
    p = Person()
    query = Query.match(p).set_merge(p, {"city": "Berlin"}).return_(p)
    compiled = query.compile("cypher")
    assert "SET _person_0 += $p0" in compiled.statement
    assert compiled.parameters["p0"] == {"city": "Berlin"}


def test_open_hops_fluent_compilation():
    p = Person()
    f = Person()
    rel = Follows()

    # max_hops=3 emits *1..3 (main-compatible)
    q_max = Query.match(p).to(rel).node(f).hops(max_hops=3).return_(f)
    assert "*1..3" in q_max.compile("cypher").statement
    assert "*1..3" in q_max.compile("age").statement
    assert "{1,3}" in q_max.compile("iso_gql").statement

    # min_hops=2 emits *2..
    q_min = Query.match(p).to(rel).node(f).hops(min_hops=2).return_(f)
    assert "*2.." in q_min.compile("cypher").statement
    assert "*2.." in q_min.compile("age").statement
    assert "{2,}" in q_min.compile("iso_gql").statement

    # inf is rejected with TypeError
    with pytest.raises(TypeError, match="expected an int, 'unbounded', or None for max_hops"):
        Query.match(p).to(rel).node(f).hops(max_hops="inf")
