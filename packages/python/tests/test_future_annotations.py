"""Tests for per-field annotation evaluation under ``from __future__ import annotations`` (#334)."""

from __future__ import annotations

import datetime
import enum
import typing
import warnings
from typing import TYPE_CHECKING, Any

import pytest
from voyager_ogm import (
    Field,
    NativeSchemaRegistry,
    Node,
    Relationship,
    RelationshipTo,
    node,
    relationship,
)

if TYPE_CHECKING:
    from pathlib import PurePath as OnlyInTypeChecking

Opt = typing.Optional


class Status(enum.Enum):
    ACTIVE = "active"
    ARCHIVED = "archived"


class HealthyNode(Node):
    id: str = Field(primary_key=True)
    age: int | None
    tags: list[str]
    born: datetime.date
    status: Status
    nickname: Opt[str]


class HealthyRel(Relationship, type_name="HEALTHY_REL"):
    weight: int | None
    tags: list[str]
    since: datetime.date
    status: Status
    note: Opt[str]


class ForwardSourceNode(Node):
    id: str = Field(primary_key=True)
    knows: RelationshipTo[ForwardTargetNode] = RelationshipTo(
        target="ForwardTargetNode", rel_type="KNOWS_FORWARD"
    )


class ForwardTargetNode(Node):
    id: str = Field(primary_key=True)
    label: str


class SelfRefNode(Node):
    id: str = Field(primary_key=True)
    parent: SelfRefNode | None = None


class InnerScopedNode(Node):
    class Kind(enum.Enum):
        ALPHA = "alpha"

    kind: Kind


def test_healthy_models_resolve_all_fields_without_warning() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")

        class LocalHealthyNode(Node):
            age: int | None
            tags: list[str]
            born: datetime.date
            status: Status
            nickname: Opt[str]

        class LocalHealthyRel(Relationship, type_name="LOCAL_HEALTHY_REL"):
            weight: int | None
            tags: list[str]
            since: datetime.date
            status: Status
            note: Opt[str]

    assert caught == []

    for model, opt_field in (
        (HealthyNode, "nickname"),
        (LocalHealthyNode, "nickname"),
    ):
        fields = model._schema_fields
        assert fields["age"].type_annotation == int | None
        assert fields["tags"].type_annotation == list[str]
        assert fields["born"].type_annotation is datetime.date
        assert fields["status"].type_annotation is Status
        assert fields[opt_field].type_annotation == Opt[str]

    for rel_model in (HealthyRel, LocalHealthyRel):
        fields = rel_model._schema_fields
        assert fields["weight"].type_annotation == int | None
        assert fields["tags"].type_annotation == list[str]
        assert fields["since"].type_annotation is datetime.date
        assert fields["status"].type_annotation is Status
        assert fields["note"].type_annotation == Opt[str]


def test_node_with_one_unresolvable_annotation_warns_once_and_keeps_siblings() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")

        class NodeWithMissingImport(Node):
            age: int | None
            tags: list[str]
            born: datetime.date
            status: Status
            nickname: Opt[str]
            missing: OnlyInTypeChecking

    assert len(caught) == 1
    assert issubclass(caught[0].category, UserWarning)
    msg = str(caught[0].message)
    assert "NodeWithMissingImport" in msg
    assert "missing" in msg

    fields = NodeWithMissingImport._schema_fields
    assert fields["age"].type_annotation == int | None
    assert fields["tags"].type_annotation == list[str]
    assert fields["born"].type_annotation is datetime.date
    assert fields["status"].type_annotation is Status
    assert fields["nickname"].type_annotation == Opt[str]
    assert fields["missing"].type_annotation is Any
    assert fields["missing"].nullable is True


def test_relationship_with_one_unresolvable_annotation_warns_once_and_keeps_siblings() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")

        class RelWithMissingImport(Relationship, type_name="REL_MISSING"):
            weight: int | None
            tags: list[str]
            since: datetime.date
            status: Status
            note: Opt[str]
            missing: OnlyInTypeChecking

    assert len(caught) == 1
    assert issubclass(caught[0].category, UserWarning)
    msg = str(caught[0].message)
    assert "RelWithMissingImport" in msg
    assert "missing" in msg

    fields = RelWithMissingImport._schema_fields
    assert fields["weight"].type_annotation == int | None
    assert fields["tags"].type_annotation == list[str]
    assert fields["since"].type_annotation is datetime.date
    assert fields["status"].type_annotation is Status
    assert fields["note"].type_annotation == Opt[str]
    assert fields["missing"].type_annotation is Any
    assert fields["missing"].nullable is True


def test_decorated_models_warn_only_once_per_unresolvable_field() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")

        @node("DecoratedMissingNode")
        class DecoratedMissingNode:
            age: int | None
            bad_ref: OnlyInTypeChecking

        @relationship("DECORATED_MISSING_REL")
        class DecoratedMissingRel:
            since: datetime.date
            bad_ref: OnlyInTypeChecking

    assert len(caught) == 2
    assert all(issubclass(w.category, UserWarning) for w in caught)
    assert "DecoratedMissingNode" in str(caught[0].message)
    assert "bad_ref" in str(caught[0].message)
    assert "DecoratedMissingRel" in str(caught[1].message)
    assert "bad_ref" in str(caught[1].message)

    assert DecoratedMissingNode._schema_fields["age"].type_annotation == int | None
    assert DecoratedMissingNode._schema_fields["bad_ref"].type_annotation is Any
    assert DecoratedMissingRel._schema_fields["since"].type_annotation is datetime.date
    assert DecoratedMissingRel._schema_fields["bad_ref"].type_annotation is Any


def test_forward_references_in_same_module_and_class_namespace_resolve() -> None:
    assert ForwardSourceNode.knows.resolve() is ForwardTargetNode
    assert SelfRefNode._schema_fields["parent"].type_annotation == SelfRefNode | None
    assert InnerScopedNode._schema_fields["kind"].type_annotation is InnerScopedNode.Kind

    registry = NativeSchemaRegistry()
    ForwardSourceNode.register_schema(registry)
    ForwardTargetNode.register_schema(registry)
    node_schema = registry.get_node("ForwardSourceNode")
    assert node_schema is not None
    assert node_schema["relationship_declarations"][0]["target_model"] == "ForwardTargetNode"


def test_relationship_descriptor_forward_ref_does_not_warn() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")

        class LateNodeCheck(Node):
            id: str = Field(primary_key=True)
            rel: RelationshipTo[ForwardTargetNode] = RelationshipTo(
                target="ForwardTargetNode", rel_type="LATE_REL"
            )
            missing_rel: RelationshipTo[OnlyInTypeChecking] = RelationshipTo(
                target="ForwardTargetNode", rel_type="MISSING_ANN"
            )

    assert caught == []
    assert LateNodeCheck.rel.resolve() is ForwardTargetNode
    assert LateNodeCheck.missing_rel.resolve() is ForwardTargetNode


def test_unresolvable_field_registers_as_any_in_schema() -> None:
    with pytest.warns(UserWarning, match=r"SchemaFallbackNode\.bad"):

        class SchemaFallbackNode(Node):
            name: str
            bad: OnlyInTypeChecking

    registry = NativeSchemaRegistry()
    SchemaFallbackNode.register_schema(registry)
    schema = registry.get_node("SchemaFallbackNode")
    assert schema is not None
    assert schema["fields"]["name"]["type"] == "STRING"
    assert schema["fields"]["bad"]["type"] == "ANY"
    assert SchemaFallbackNode.to_json_schema()["properties"]["bad"] == {}
