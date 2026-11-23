"""Relationship endpoint keywords and Optional field types in the schema registry (#125)."""

from __future__ import annotations

from typing import Any, Optional

from voyager_ogm import Field, Node, Relationship, relationship
from voyager_ogm._voyager_rs import NativeSchemaRegistry


class EpPerson(Node):
    name: str = Field()


class EpCompany(Node):
    name: str = Field()


def _registry_entry(model: Any) -> dict[str, Any]:
    registry = NativeSchemaRegistry()
    model.register_schema(registry=registry)
    if issubclass(model, Relationship):
        return registry.relationship_schemas()[0]
    return registry.node_schemas()[0]


def test_class_keywords_declare_endpoints_and_direction() -> None:
    class EpWorksAt(
        Relationship, source_node=EpPerson, target_node=EpCompany, direction="incoming"
    ):
        since: int = Field()

    assert EpWorksAt.__source_labels__ == ["EpPerson"]
    assert EpWorksAt.__target_labels__ == ["EpCompany"]
    assert EpWorksAt.__direction__ == "incoming"


def test_label_keywords_declare_endpoints() -> None:
    class EpOwns(Relationship, source_labels=["EpPerson"], to_labels=["EpCompany"]):
        share: float = Field()

    assert EpOwns.__source_labels__ == ["EpPerson"]
    assert EpOwns.__target_labels__ == ["EpCompany"]
    assert EpOwns.__direction__ == "outgoing"


def test_class_body_declarations_are_kept() -> None:
    class EpManages(Relationship):
        __source_labels__ = ["EpPerson"]
        __target_labels__ = ["EpPerson"]
        __direction__ = "undirected"
        level: int = Field()

    assert EpManages.__source_labels__ == ["EpPerson"]
    assert EpManages.__target_labels__ == ["EpPerson"]
    assert EpManages.__direction__ == "undirected"


def test_decorator_endpoints_and_direction() -> None:
    @relationship(
        type_name="EP_KNOWS", source_node=EpPerson, target_node=EpPerson, direction="incoming"
    )
    class EpKnows(Relationship):
        since: int = Field()

    assert EpKnows.__type__ == "EP_KNOWS"
    assert EpKnows.__source_labels__ == ["EpPerson"]
    assert EpKnows.__target_labels__ == ["EpPerson"]
    assert EpKnows.__direction__ == "incoming"


def test_optional_annotations_register_the_inner_type() -> None:
    class EpOptional(Node):
        a: str | None = None
        b: Optional[int] = None  # noqa: UP045
        c: int | str | None = None
        d: list[str] | None = None

    fields = _registry_entry(EpOptional)["fields"]
    assert {name: fields[name]["type"] for name in ("a", "b", "c", "d")} == {
        "a": "STRING",
        "b": "INTEGER",
        "c": "ANY",
        "d": "LIST",
    }
