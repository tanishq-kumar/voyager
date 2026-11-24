"""Field nullability inference from type annotations (#332)."""

from __future__ import annotations

import datetime
from typing import Annotated, Any, Literal, Optional

from jsonschema import Draft202012Validator
from voyager_ogm import (
    Field,
    NativeSchemaRegistry,
    Node,
    Relationship,
    SchemaManager,
    VectorProperty,
)


class Zz332NullabilityMatrix(Node):
    plain: str
    optional_pipe: str | None
    optional_typing: Optional[str]  # noqa: UP045 - exercise the typing.Optional spelling
    optional_default_none: str | None = None
    bare_field: str = Field()
    explicit_true: str = Field(nullable=True)
    explicit_false: str = Field(nullable=False)
    conflict: str | None = Field(nullable=False)
    pk: str = Field(primary_key=True)
    any_value: Any
    unannotated = Field()
    annotated_optional: Annotated[str | None, "meta"]
    annotated_plain: Annotated[str, "meta"]
    literal_value: Literal["a", "b"]
    day: datetime.date
    vector: list[float] = VectorProperty(dimensions=3)


class Zz332PkExplicitNullable(Node):
    pk: str = Field(primary_key=True, nullable=True)


class Zz332PoisonedAnnotations(Node):
    good: str
    bad_eval: NotARealType  # noqa: F821
    bad_string: "NotARealType"  # noqa: F821, UP037 - quoted forward reference


class Zz332NullabilityBase(Node):
    base: str


class Zz332NullabilityChild(Zz332NullabilityBase):
    extra: int


class Zz332NullabilityOverride(Zz332NullabilityBase):
    base: str | None


class Zz332NullabilityDdl(Node):
    pk: str = Field(primary_key=True)
    plain: str
    maybe: str | None
    explicit_false: str = Field(nullable=False)


class Zz332NullabilityRel(
    Relationship,
    type_name="ZZ332_NULLABILITY_REL",
    source_node=Zz332NullabilityDdl,
    target_node=Zz332NullabilityDdl,
):
    ref: str
    note: str | None
    explicit_false: int = Field(nullable=False)


def test_matrix_resolves_each_case() -> None:
    fields = Zz332NullabilityMatrix._schema_fields

    assert fields["plain"].nullable is False
    assert fields["plain"].nullable_explicit is None
    assert fields["optional_pipe"].nullable is True
    assert fields["optional_typing"].nullable is True
    assert fields["optional_default_none"].nullable is True
    assert fields["bare_field"].nullable is False
    assert fields["explicit_true"].nullable is True
    assert fields["explicit_true"].nullable_explicit is True
    assert fields["explicit_false"].nullable is False
    assert fields["explicit_false"].nullable_explicit is False
    # Explicit nullable=False wins over the Optional annotation, without raising.
    assert fields["conflict"].nullable is False
    assert fields["conflict"].nullable_explicit is False
    assert fields["pk"].nullable is False
    assert fields["pk"].nullable_explicit is None
    pk_nullable = Zz332PkExplicitNullable._schema_fields["pk"]
    assert pk_nullable.nullable is False
    assert pk_nullable.nullable_explicit is True
    assert fields["any_value"].nullable is True
    assert fields["unannotated"].nullable is True
    assert fields["annotated_optional"].nullable is True
    assert fields["annotated_plain"].nullable is False
    assert fields["literal_value"].nullable is False
    assert fields["day"].nullable is False
    assert fields["vector"].nullable is True
    assert fields["vector"].nullable_explicit is True


def test_unresolvable_annotations_fail_open_per_field() -> None:
    fields = Zz332PoisonedAnnotations._schema_fields
    assert fields["good"].nullable is False
    assert fields["bad_eval"].nullable is True
    assert fields["bad_string"].nullable is True


def test_inherited_fields_and_overrides() -> None:
    child = Zz332NullabilityChild._schema_fields
    assert child["base"].nullable is False
    assert child["extra"].nullable is False

    override = Zz332NullabilityOverride._schema_fields
    assert override["base"].nullable is True
    # Re-inference on the subclass must not mutate the base descriptor.
    assert Zz332NullabilityBase._schema_fields["base"].nullable is False


def test_relationship_model_resolves_the_same_way() -> None:
    fields = Zz332NullabilityRel._schema_fields
    assert fields["ref"].nullable is False
    assert fields["note"].nullable is True
    assert fields["explicit_false"].nullable is False


def test_json_schema_required_and_null_follow_resolved_nullability() -> None:
    schema = Zz332NullabilityMatrix.to_json_schema()
    Draft202012Validator.check_schema(schema)
    props = schema["properties"]

    assert schema["required"] == [
        "plain",
        "bare_field",
        "explicit_false",
        "conflict",
        "pk",
        "annotated_plain",
        "literal_value",
        "day",
    ]
    assert props["plain"] == {"type": "string"}
    assert props["optional_pipe"] == {"type": ["string", "null"]}
    assert props["conflict"] == {"type": "string"}
    assert props["explicit_true"] == {"type": ["string", "null"]}
    assert props["any_value"] == {}
    assert props["unannotated"] == {}
    assert props["annotated_optional"]["type"] == ["string", "null"]
    assert props["annotated_plain"] == {"type": "string"}
    assert props["vector"]["type"] == ["array", "null"]


def test_constraint_ddl_is_explicit_only() -> None:
    node_create = SchemaManager.generate_constraint_ddl(Zz332NullabilityDdl)
    assert any("pk_not_null" in stmt for stmt in node_create)
    assert any("explicit_false_not_null" in stmt for stmt in node_create)
    assert not any("plain_not_null" in stmt for stmt in node_create)
    assert not any("maybe_not_null" in stmt for stmt in node_create)

    node_drop = SchemaManager.generate_drop_constraint_ddl(Zz332NullabilityDdl)
    assert any("pk_not_null" in stmt for stmt in node_drop)
    assert any("explicit_false_not_null" in stmt for stmt in node_drop)
    assert not any("plain_not_null" in stmt for stmt in node_drop)

    rel_create = SchemaManager.generate_constraint_ddl(Zz332NullabilityRel)
    # Relationship fields follow the same explicit-only existence rule.
    assert any("explicit_false_not_null" in stmt for stmt in rel_create)
    assert not any("ref_not_null" in stmt for stmt in rel_create)
    assert not any("note_not_null" in stmt for stmt in rel_create)


def test_memgraph_constraint_ddl_is_explicit_only() -> None:
    create = SchemaManager.generate_constraint_ddl(Zz332NullabilityDdl, dialect="memgraph")
    assert any("ASSERT EXISTS (n.explicit_false)" in stmt for stmt in create)
    assert not any("ASSERT EXISTS (n.plain)" in stmt for stmt in create)
    assert not any("ASSERT EXISTS (n.maybe)" in stmt for stmt in create)

    drop = SchemaManager.generate_drop_constraint_ddl(Zz332NullabilityDdl, dialect="memgraph")
    assert any("ASSERT EXISTS (n.explicit_false)" in stmt for stmt in drop)
    assert not any("ASSERT EXISTS (n.plain)" in stmt for stmt in drop)


def test_graph_type_and_table_ddl_follow_resolved_nullability() -> None:
    gql = SchemaManager.generate_gql_graph_type_ddl("Zz332Graph", Zz332NullabilityDdl)
    assert "plain STRING NOT NULL" in gql
    assert "explicit_false STRING NOT NULL" in gql
    assert "maybe STRING" in gql
    assert "maybe STRING NOT NULL" not in gql

    cypher25 = SchemaManager.generate_cypher25_graph_type_ddl(Zz332NullabilityDdl)
    assert "plain :: STRING NOT NULL" in cypher25
    assert "maybe :: STRING" in cypher25
    assert "maybe :: STRING NOT NULL" not in cypher25

    tables = SchemaManager.generate_table_ddl(Zz332NullabilityDdl, dialect="postgres")
    assert '"plain" TEXT NOT NULL' in tables[0]
    assert '"explicit_false" TEXT NOT NULL' in tables[0]
    assert '"maybe" TEXT' in tables[0]
    assert '"maybe" TEXT NOT NULL' not in tables[0]


def test_snapshot_roundtrip_keeps_inferred_and_explicit_ddl() -> None:
    registry = NativeSchemaRegistry()
    Zz332NullabilityDdl.register_schema(registry)
    create = registry.generate_cypher_ddl(model_name="Zz332NullabilityDdl")
    drop = registry.generate_cypher_drop_ddl(model_name="Zz332NullabilityDdl")

    restored = NativeSchemaRegistry()
    restored.from_json(registry.to_json())
    assert restored.generate_cypher_ddl(model_name="Zz332NullabilityDdl") == create
    assert restored.generate_cypher_drop_ddl(model_name="Zz332NullabilityDdl") == drop


def test_pre_inference_snapshot_restores_identical_constraint_ddl() -> None:
    old_snapshot = """
    {
      "nodes": {
        "Zz332LegacySnapshot": {
          "name": "Zz332LegacySnapshot",
          "labels": ["Zz332LegacySnapshot"],
          "fields": {
            "pk": {
              "name": "pk",
              "field_type": "String",
              "nullable": false,
              "primary_key": true,
              "unique": true,
              "indexed": false,
              "index_type": null,
              "default_value": null,
              "vector_config": null
            },
            "legacy": {
              "name": "legacy",
              "field_type": "String",
              "nullable": false,
              "primary_key": false,
              "unique": false,
              "indexed": false,
              "index_type": null,
              "default_value": null,
              "vector_config": null
            }
          },
          "primary_key": "pk",
          "fulltext_indexes": []
        }
      },
      "relationships": {}
    }
    """
    registry = NativeSchemaRegistry()
    registry.from_json(old_snapshot)
    create = registry.generate_cypher_ddl(model_name="Zz332LegacySnapshot")
    drop = registry.generate_cypher_drop_ddl(model_name="Zz332LegacySnapshot")

    assert any("pk_not_null" in stmt for stmt in create)
    assert any("legacy_not_null" in stmt for stmt in create)
    assert any("pk_not_null" in stmt for stmt in drop)
    assert any("legacy_not_null" in stmt for stmt in drop)


def test_snapshot_without_explicit_nullability_keeps_old_bytes() -> None:
    class Zz332PlainSnapshot(Node):
        pk: str = Field(primary_key=True)
        name: str

    registry = NativeSchemaRegistry()
    Zz332PlainSnapshot.register_schema(registry)
    snapshot = registry.to_json()
    # Inferred non-nullability is marked so restore can distinguish it from an
    # explicit nullable=False, but the explicit key itself stays omitted.
    assert '"nullable_explicit"' not in snapshot
    assert '"nullable_inferred":true' in snapshot
