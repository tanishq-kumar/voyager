"""Regression tests for ``= None`` as a declared field default (#333).

A class-body ``None`` on an annotated attribute is a real default, identical to
``Field(default=None)``. A property left at that default is absent rather than an
explicit null on create paths, never participates in a MERGE key, and is never
overwritten on ``ON MATCH SET``. All tests are offline.
"""

from __future__ import annotations

import json

import pytest
from voyager_ogm import (
    Field,
    Node,
    Query,
    Relationship,
    Session,
    export_schema,
    node,
    relationship,
    reset_alias_counters,
)
from voyager_ogm.bridge import MockBridge


@node("Zz333NoneDefault")
class Zz333NoneDefault(Node):
    id: str = Field(primary_key=True)
    bare_none: str | None = None
    field_none: str | None = Field(default=None)
    non_optional_none: str = None
    kept: str = "kept"


@relationship("ZZ333_NONE_DEFAULT_REL")
class Zz333NoneRel(Relationship):
    score: int | None = None
    explicit_none: str | None = Field(default=None)
    label: str = "rel"


@node("Zz333NoneBase")
class Zz333NoneBase(Node):
    id: str = Field(primary_key=True)
    inherited_none: str | None = None
    inherited_value: str = "base"


@node("Zz333NoneChild")
class Zz333NoneChild(Zz333NoneBase):
    extra: str | None = None


@node("Zz333NoneOnly")
class Zz333NoneOnly(Node):
    note: str | None = None


@node("Zz333NoneOverride")
class Zz333NoneOverride(Zz333NoneBase):
    inherited_value: str | None = None


NONE_FIELDS = ("bare_none", "field_none", "non_optional_none")


@pytest.fixture(autouse=True)
def _reset_aliases() -> None:
    reset_alias_counters()


def test_bare_none_and_field_default_none_are_equivalent() -> None:
    fields = Zz333NoneDefault._schema_fields

    for name in NONE_FIELDS:
        assert fields[name].has_default is True
        assert fields[name].default is None
        assert fields[name].resolve_default() is None

    assert fields["bare_none"].default_factory is None
    assert fields["field_none"].default_factory is None

    instance = Zz333NoneDefault(id="n1")
    for name in NONE_FIELDS:
        assert instance.get(name) is None
    assert instance.get("kept") == "kept"
    assert instance._defaulted_fields == {*NONE_FIELDS, "kept"}


def test_none_default_appears_in_export_schema_and_json_schema() -> None:
    props = export_schema([Zz333NoneDefault])["nodes"][0]["properties"]
    for name in NONE_FIELDS:
        assert props[name]["has_default"] is True
        assert "default" in props[name]
        assert props[name]["default"] is None

    schema = Zz333NoneDefault.to_json_schema()
    for name in NONE_FIELDS:
        assert schema["properties"][name]["default"] is None
    assert "bare_none" not in schema["required"]
    assert "field_none" not in schema["required"]
    assert "non_optional_none" not in schema["required"]
    assert "id" in schema["required"]

    # The default survives JSON serialisation as a real JSON null.
    roundtrip = json.loads(json.dumps(schema))
    assert roundtrip["properties"]["bare_none"]["default"] is None


def test_relationship_none_default_is_a_real_default() -> None:
    fields = Zz333NoneRel._schema_fields
    assert fields["score"].has_default is True
    assert fields["score"].default is None
    assert fields["explicit_none"].has_default is True
    assert fields["explicit_none"].default is None

    rel = Zz333NoneRel()
    assert rel.get("score") is None
    assert rel.get("explicit_none") is None
    assert rel.get("label") == "rel"
    assert {"score", "explicit_none", "label"} <= rel._defaulted_fields

    schema = Zz333NoneRel.to_json_schema()
    assert schema["properties"]["score"]["default"] is None
    assert schema["properties"]["explicit_none"]["default"] is None
    assert "score" not in schema.get("required", [])
    assert "explicit_none" not in schema.get("required", [])


def test_inheritance_keeps_none_defaults_and_allows_an_override() -> None:
    child = Zz333NoneChild(id="c1")
    assert Zz333NoneChild._schema_fields["inherited_none"].has_default is True
    assert Zz333NoneChild._schema_fields["inherited_none"].default is None
    assert child.get("inherited_none") is None
    assert child.get("extra") is None
    assert child.get("inherited_value") == "base"
    assert {"inherited_none", "extra"} <= child._defaulted_fields

    override_field = Zz333NoneOverride._schema_fields["inherited_value"]
    override = Zz333NoneOverride(id="o1")
    assert override_field.has_default is True
    assert override_field.default is None
    assert override.get("inherited_value") is None


def test_create_skips_none_defaults_in_statement_and_parameters() -> None:
    compiled = Query.create(Zz333NoneDefault(id="n1")).compile("cypher")

    for name in NONE_FIELDS:
        assert name not in compiled.statement
        assert name not in compiled.parameters.values()
    assert "kept" in compiled.statement
    assert "kept" in compiled.parameters.values()


def test_merge_key_and_match_never_include_a_none_default() -> None:
    compiled = Query.merge(Zz333NoneDefault(id="n1")).compile("cypher")

    assert "MERGE (_zz333nonedefault_0:Zz333NoneDefault {id: $p0})" in compiled.statement
    for name in NONE_FIELDS:
        assert name not in compiled.statement
        assert name not in compiled.parameters.values()
    assert "_zz333nonedefault_0.kept = $p1" in compiled.statement
    assert "ON MATCH SET" not in compiled.statement


def test_merge_without_a_key_does_not_key_on_a_none_default() -> None:
    compiled = Query.merge(Zz333NoneOnly()).compile("cypher")

    assert compiled.statement == "MERGE (_zz333noneonly_0:Zz333NoneOnly)"
    assert compiled.parameters == {}


def test_merge_writes_an_explicitly_supplied_value_on_create_and_match() -> None:
    compiled = Query.merge(Zz333NoneDefault(id="n1", bare_none="set")).compile("cypher")

    assert "_zz333nonedefault_0.bare_none = $p1" in compiled.statement
    assert compiled.statement.count("_zz333nonedefault_0.bare_none = $p1") == 2
    assert compiled.parameters["p1"] == "set"


def test_relationship_merge_skips_none_defaults() -> None:
    source = Zz333NoneDefault(alias="a", id="a")
    target = Zz333NoneDefault(alias="b", id="b")
    rel = Zz333NoneRel(alias="r")

    compiled = (
        Query.match(source).match(target).merge_relationship(source, rel, target).compile("cypher")
    )

    assert "MERGE (a)-[r:ZZ333_NONE_DEFAULT_REL]->(b)" in compiled.statement
    assert "ON CREATE SET r.label = $p0" in compiled.statement
    assert "score" not in compiled.statement
    assert "explicit_none" not in compiled.statement
    assert "ON MATCH SET" not in compiled.statement


def test_bulk_create_skips_none_defaults() -> None:
    session = Session(dialect="cypher")
    plan = session.bulk_create(Zz333NoneDefault, [{"id": "b1"}])

    for name in NONE_FIELDS:
        assert name not in plan.statement
        assert name not in plan.batches_data[0][0]
    assert "_zz333nonedefault_0.kept = row.kept" in plan.statement
    assert plan.batches_data[0][0] == {"id": "b1", "kept": "kept"}

    explicit = session.bulk_create(Zz333NoneDefault, [{"id": "b2", "bare_none": "set"}])
    assert "_zz333nonedefault_0.bare_none = row.bare_none" in explicit.statement
    assert explicit.batches_data[0][0]["bare_none"] == "set"


def test_bulk_merge_skips_none_defaults_and_keeps_create_only_value() -> None:
    session = Session(dialect="cypher")
    plan = session.bulk_upsert(Zz333NoneDefault, [{"id": "m1"}], key_field="id")

    create_part, _, match_part = plan.statement.partition("ON MATCH SET")
    for name in NONE_FIELDS:
        assert name not in plan.statement
        assert name not in plan.batches_data[0][0]
        assert name not in plan.batches[0].parameters["batch"][0]
    assert "MERGE (_zz333nonedefault_0:Zz333NoneDefault {id: row.id})" in create_part
    assert "_zz333nonedefault_0.kept = row.kept" in create_part
    assert match_part == ""


def test_bulk_merge_mixed_batch_defaults_none_only_for_the_rows_that_leave_it() -> None:
    session = Session(dialect="cypher")
    plan = session.bulk_upsert(
        Zz333NoneDefault,
        [{"id": "row1", "bare_none": "set"}, {"id": "row2"}],
        key_field="id",
    )

    assert plan.num_batches == 2
    by_id = {batch.parameters["batch"][0]["id"]: batch for batch in plan.batches}

    row1_create, _, row1_match = by_id["row1"].statement.partition("ON MATCH SET")
    assert "_zz333nonedefault_0.bare_none = row.bare_none" in row1_create
    assert "_zz333nonedefault_0.bare_none = row.bare_none" in row1_match
    assert by_id["row1"].parameters["batch"][0]["bare_none"] == "set"

    row2_create, _, row2_match = by_id["row2"].statement.partition("ON MATCH SET")
    for name in NONE_FIELDS:
        assert name not in by_id["row2"].statement
        assert name not in by_id["row2"].parameters["batch"][0]
    assert "_zz333nonedefault_0.kept = row.kept" in row2_create
    assert row2_match == ""


def test_session_flush_skips_none_defaults_on_create_and_match() -> None:
    bridge = MockBridge()
    session = Session(bridge=bridge, dialect="cypher", enable_identity_map=True)
    node = Zz333NoneDefault(id="f1")
    session.register(node)

    session.flush()

    statement, parameters = bridge.executed_queries[0]
    create_part, _, match_part = statement.partition("ON MATCH SET")
    for name in NONE_FIELDS:
        assert name not in statement
        assert name not in parameters["batch"][0]
    assert "_zz333nonedefault_0.kept = row.kept" in create_part
    assert match_part == ""


def test_session_flush_update_never_writes_a_none_default_on_match() -> None:
    bridge = MockBridge()
    session = Session(bridge=bridge, dialect="cypher", enable_identity_map=True)
    tracked = Zz333NoneDefault(id="f2")
    session.register(tracked, is_clean=True)

    tracked.kept = "changed"
    session.flush()

    statement, parameters = bridge.executed_queries[0]
    create_part, _, match_part = statement.partition("ON MATCH SET")
    for name in NONE_FIELDS:
        assert name not in statement
        assert name not in parameters["batch"][0]
    assert "_zz333nonedefault_0.kept = row.kept" in create_part
    assert "_zz333nonedefault_0.kept = row.kept" in match_part


def test_explicit_assignment_to_none_keeps_the_existing_write_behaviour() -> None:
    """An explicit assignment is a user change and is not treated as a default."""
    instance = Zz333NoneDefault(id="e1")
    instance.bare_none = None

    assert "bare_none" not in instance._defaulted_fields
    assert instance._dirty_fields["bare_none"] is None

    # Query.create already omitted every None value before #333, and still does.
    compiled = Query.create(instance).compile("cypher")
    assert "bare_none" not in compiled.statement

    # The bulk path still sends the explicit None, as it did before #333.
    session = Session(dialect="cypher")
    plan = session.bulk_upsert(
        Zz333NoneDefault,
        [dict(instance.dirty_fields)],
        key_field="id",
        defaulted_fields=[set(instance._defaulted_fields)],
    )
    assert "_zz333nonedefault_0.bare_none = row.bare_none" in plan.statement
    assert plan.batches_data[0][0]["bare_none"] is None


def test_bulk_create_relationship_skips_none_defaults() -> None:
    session = Session(dialect="cypher")
    plan = session.bulk_create_relationships(
        Zz333NoneRel,
        [{"from_id": "a", "to_id": "b"}],
        from_label="Zz333NoneDefault",
        from_key="id",
        to_label="Zz333NoneDefault",
        to_key="id",
    )

    assert "score" not in plan.statement
    assert "explicit_none" not in plan.statement
    assert "_zz333_none_default_rel_0.label = row.label" in plan.statement
    assert plan.batches_data[0][0] == {
        "from_id": "a",
        "to_id": "b",
        "label": "rel",
    }
