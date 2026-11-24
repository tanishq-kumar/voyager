"""Tests for model schema export: ``export_schema`` and ``Model.to_json_schema`` (#125)."""

from __future__ import annotations

import datetime
import enum
import json
import re
import uuid
from typing import Any, Literal, Optional

import jsonschema
import pytest
from jsonschema import Draft202012Validator
from voyager_ogm import (
    Field,
    NativeSchemaRegistry,
    Node,
    Point,
    Relationship,
    VectorProperty,
    export_schema,
    node,
    relationship,
)


class Color(enum.Enum):
    RED = "red"
    BLUE = "blue"


class ExpPerson(Node, label="ExpPerson"):
    __description__ = "A person in the graph."

    id: str = Field(primary_key=True, description="Stable identifier.")
    name: str = Field(nullable=False, min_length=1, max_length=50)
    email: str = Field(unique=True, pattern=r"[^@\s]+@[^@\s]+")
    age: int = Field(ge=0, le=150)
    score: float = Field(gt=0.0, lt=1.0, default=0.5)
    active: bool = True
    status: str = Field(choices=["active", "inactive"], default="active", index=True)
    nickname: str | None = None
    tags: list[str] = Field(default_factory=list, max_length=3)
    attrs: dict[str, int] = Field(default_factory=dict)
    born: datetime.date = Field(default=datetime.date(2000, 1, 2))
    seen_at: datetime.datetime | None = None
    wake: datetime.time | None = None
    tenure: datetime.timedelta | None = None
    home: Point | None = None
    token: str = Field(default_factory=lambda: uuid.uuid4().hex, name="api_token")


class ExpMovie(Node):
    __labels__ = ["ExpMovie", "ExpWork"]

    title: str = Field(primary_key=True)
    released: int | None = None
    color: Color | None = None
    rating: Literal["G", "PG", "R"] = "G"
    embedding: list[float] = VectorProperty(dimensions=3)


class ExpActedIn(
    Relationship,
    type_name="EXP_ACTED_IN",
    source_node=ExpPerson,
    target_node=ExpMovie,
):
    __description__ = "Who acted in which movie."

    role: str = Field(nullable=False, description="Character name.")
    billing: int = Field(default=1, ge=1)


class ExpKnows(Relationship, direction="undirected"):
    since: int | None = None


@node("ExpDecorated")
class ExpDecorated:
    code: str = Field(primary_key=True, pattern=r"^[A-Z]{3}$")


@relationship(
    "EXP_DECORATED_REL",
    source_labels=["ExpDecorated"],
    target_node=ExpMovie,
    direction="undirected",
)
class ExpDecoratedRel:
    weight: float = 1.0


ALL_MODELS: list[Any] = [
    ExpPerson,
    ExpMovie,
    ExpDecorated,
    ExpActedIn,
    ExpKnows,
    ExpDecoratedRel,
]


def _validator(model: Any) -> Draft202012Validator:
    schema = model.to_json_schema()
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=Draft202012Validator.FORMAT_CHECKER)


def _valid(model: Any, instance: dict[str, Any]) -> bool:
    return _validator(model).is_valid(instance)


# ---------------------------------------------------------------------------
# export_schema
# ---------------------------------------------------------------------------


class TestExportSchema:
    def test_is_json_serialisable_and_splits_kinds(self) -> None:
        schema = export_schema(ALL_MODELS)
        assert json.loads(json.dumps(schema)) == schema
        assert [n["name"] for n in schema["nodes"]] == ["ExpPerson", "ExpMovie", "ExpDecorated"]
        assert [r["name"] for r in schema["relationships"]] == [
            "ExpActedIn",
            "ExpKnows",
            "ExpDecoratedRel",
        ]

    def test_node_entry(self) -> None:
        person = export_schema([ExpPerson])["nodes"][0]
        assert person["labels"] == ["ExpPerson"]
        assert person["model"].endswith("test_schema_export.ExpPerson")
        assert person["description"] == "A person in the graph."
        assert person["primary_key"] == "id"

    def test_multi_label_node(self) -> None:
        movie = export_schema([ExpMovie])["nodes"][0]
        assert movie["labels"] == ["ExpMovie", "ExpWork"]
        assert movie["description"] is None
        assert movie["primary_key"] == "title"

    def test_decorated_models(self) -> None:
        schema = export_schema([ExpDecorated, ExpDecoratedRel])
        assert schema["nodes"][0]["labels"] == ["ExpDecorated"]
        rel = schema["relationships"][0]
        assert rel["type"] == "EXP_DECORATED_REL"
        assert rel["source_labels"] == ["ExpDecorated"]
        assert rel["target_labels"] == ["ExpMovie", "ExpWork"]
        assert rel["directed"] is False

    def test_relationship_without_declared_endpoints(self) -> None:
        class ExpLoose(Relationship):
            pass

        rel = export_schema([ExpLoose])["relationships"][0]
        assert rel["source_labels"] == []
        assert rel["target_labels"] == []
        assert rel["properties"] == {}

    def test_class_body_endpoints(self) -> None:
        class ExpBody(Relationship):
            __source_labels__ = ["ExpPerson"]
            __target_labels__ = ["ExpMovie"]
            __direction__ = "incoming"

        rel = export_schema([ExpBody])["relationships"][0]
        assert rel["source_labels"] == ["ExpPerson"]
        assert rel["target_labels"] == ["ExpMovie"]
        assert ExpBody.__direction__ == "incoming"

    def test_relationship_endpoints_and_direction(self) -> None:
        schema = export_schema([ExpActedIn, ExpKnows])
        acted, knows = schema["relationships"]
        assert acted["type"] == "EXP_ACTED_IN"
        assert acted["source_labels"] == ["ExpPerson"]
        assert acted["target_labels"] == ["ExpMovie", "ExpWork"]
        assert acted["directed"] is True
        assert acted["description"] == "Who acted in which movie."
        assert acted["properties"]["role"]["nullable"] is False
        assert acted["properties"]["role"]["description"] == "Character name."
        assert acted["properties"]["billing"]["default"] == 1
        assert acted["properties"]["billing"]["constraints"] == {"ge": 1}
        assert knows["type"] == "EXPKNOWS"
        assert knows["directed"] is False

    def test_property_flags(self) -> None:
        props = export_schema([ExpPerson])["nodes"][0]["properties"]
        pk = props["id"]
        assert pk["type"] == "STRING"
        assert (pk["primary_key"], pk["unique"], pk["indexed"], pk["nullable"]) == (
            True,
            True,
            True,
            False,
        )
        assert pk["index_type"] == "BTREE"
        assert pk["description"] == "Stable identifier."
        assert props["email"]["unique"] is True
        assert props["email"]["primary_key"] is False
        assert props["status"]["indexed"] is True
        assert props["age"]["indexed"] is False
        assert props["age"]["index_type"] is None
        assert props["age"]["type"] == "INTEGER"
        assert props["score"]["type"] == "FLOAT"
        assert props["active"]["type"] == "BOOLEAN"
        assert props["tags"]["type"] == "LIST"
        assert props["age"]["nullable"] is False

    def test_optional_annotations_use_inner_type(self) -> None:
        class ExpOptional(Node):
            a: str | None = None
            b: Optional[int] = None  # noqa: UP045
            c: int | str | None = None
            d: list[str] | None = None

        props = export_schema([ExpOptional])["nodes"][0]["properties"]
        assert {k: v["type"] for k, v in props.items()} == {
            "a": "STRING",
            "b": "INTEGER",
            "c": "ANY",
            "d": "LIST",
        }

    def test_property_keyed_by_database_name(self) -> None:
        props = export_schema([ExpPerson])["nodes"][0]["properties"]
        assert "token" not in props
        assert props["api_token"]["attribute"] == "token"
        assert props["id"]["attribute"] == "id"

    def test_defaults(self) -> None:
        props = export_schema([ExpPerson])["nodes"][0]["properties"]
        assert props["score"]["default"] == 0.5
        assert props["active"]["default"] is True
        assert props["born"]["default"] == "2000-01-02"
        assert props["status"]["has_default"] is True
        # default_factory is never called: has_default without a default value.
        assert props["tags"]["has_default"] is True
        assert "default" not in props["tags"]
        assert "default" not in props["api_token"]
        # No declared default.
        assert props["age"]["has_default"] is False
        assert "default" not in props["age"]

    def test_constraints(self) -> None:
        props = export_schema([ExpPerson])["nodes"][0]["properties"]
        assert props["name"]["constraints"] == {"min_length": 1, "max_length": 50}
        assert props["email"]["constraints"] == {"pattern": r"[^@\s]+@[^@\s]+"}
        assert props["age"]["constraints"] == {"ge": 0, "le": 150}
        assert props["score"]["constraints"] == {"gt": 0.0, "lt": 1.0}
        assert props["status"]["constraints"] == {"choices": ["active", "inactive"]}
        assert props["active"]["constraints"] == {}

    def test_compiled_pattern_constraint(self) -> None:
        class ExpCompiled(Node):
            code: str = Field(pattern=re.compile(r"\d+"))

        props = export_schema([ExpCompiled])["nodes"][0]["properties"]
        assert props["code"]["constraints"] == {"pattern": r"\d+"}

    def test_vector_property(self) -> None:
        props = export_schema([ExpMovie])["nodes"][0]["properties"]
        assert props["embedding"]["index_type"] == "VECTOR"
        assert props["embedding"]["vector"] == {
            "dimensions": 3,
            "similarity": "cosine",
            "index_name": None,
        }
        assert "vector" not in props["title"]

    def test_matches_registry_snapshot(self) -> None:
        reg = NativeSchemaRegistry()
        ExpPerson.register_schema(registry=reg)
        exported = export_schema([ExpPerson])["nodes"][0]
        reg_node = reg.node_schemas()[0]
        assert exported["labels"] == reg_node["labels"]
        assert exported["primary_key"] == reg_node["primary_key"]
        reg_fields = reg_node["fields"]
        assert set(exported["properties"]) == set(reg_fields)
        for name, prop in exported["properties"].items():
            for key in ("type", "nullable", "primary_key", "unique", "indexed", "index_type"):
                assert prop[key] == reg_fields[name][key], (name, key)

    def test_does_not_touch_global_registry(self) -> None:
        class ExpPrivate(Node):
            x: int = 1

        reg = NativeSchemaRegistry.global_registry()
        before = len(reg)
        export_schema([ExpPrivate, ExpPrivate])
        assert len(reg) == before

    def test_empty(self) -> None:
        assert export_schema([]) == {"nodes": [], "relationships": []}

    @pytest.mark.parametrize("bad", [ExpPerson, [object], [Node], ["ExpPerson"], [ExpPerson()]])
    def test_rejects_non_models(self, bad: Any) -> None:
        with pytest.raises(TypeError):
            export_schema(bad)


# ---------------------------------------------------------------------------
# to_json_schema
# ---------------------------------------------------------------------------


class TestToJsonSchema:
    @pytest.mark.parametrize("model", ALL_MODELS)
    def test_valid_against_metaschema(self, model: Any) -> None:
        schema = model.to_json_schema()
        Draft202012Validator.check_schema(schema)
        assert json.loads(json.dumps(schema)) == schema
        assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
        assert schema["title"] == model.__name__
        assert schema["type"] == "object"
        assert schema["additionalProperties"] is False

    def test_header_and_description(self) -> None:
        schema = ExpPerson.to_json_schema()
        assert schema["description"] == "A person in the graph."
        assert "description" not in ExpMovie.to_json_schema()
        assert ExpActedIn.to_json_schema()["description"] == "Who acted in which movie."

    def test_required_rule(self) -> None:
        # Required = resolved not nullable and no default (plain or factory).
        # `str`/`int` annotations are inferred non-nullable; `... | None` stays optional.
        assert ExpPerson.to_json_schema()["required"] == ["id", "name", "email", "age"]
        assert ExpMovie.to_json_schema()["required"] == ["title"]
        assert ExpActedIn.to_json_schema()["required"] == ["role"]
        assert "required" not in ExpKnows.to_json_schema()

    def test_type_mapping(self) -> None:
        props = ExpPerson.to_json_schema()["properties"]
        assert props["id"]["type"] == "string"
        assert props["name"]["type"] == "string"
        assert props["age"]["type"] == "integer"
        assert props["score"]["type"] == "number"
        assert props["active"]["type"] == "boolean"
        assert props["nickname"]["type"] == ["string", "null"]
        assert props["tags"]["type"] == "array"
        assert props["tags"]["items"] == {"type": "string"}
        assert props["attrs"]["type"] == "object"
        assert props["attrs"]["additionalProperties"] == {"type": "integer"}
        assert props["born"] == {
            "type": "string",
            "format": "date",
            "default": "2000-01-02",
        }
        assert props["seen_at"]["format"] == "date-time"
        assert props["wake"]["format"] == "time"
        assert props["tenure"]["format"] == "duration"
        assert props["home"]["type"] == ["object", "null"]
        assert props["home"]["required"] == ["x", "y"]
        # Keyed by Python attribute name, not the database property name.
        assert "token" in props
        assert "api_token" not in props

    def test_enum_literal_and_vector(self) -> None:
        props = ExpMovie.to_json_schema()["properties"]
        assert props["color"] == {"enum": ["red", "blue", None], "default": None}
        assert props["rating"] == {"enum": ["G", "PG", "R"], "default": "G"}
        assert props["embedding"]["items"] == {"type": "number"}
        assert props["embedding"]["minItems"] == 3
        assert props["embedding"]["maxItems"] == 3

    def test_descriptions_and_defaults(self) -> None:
        props = ExpPerson.to_json_schema()["properties"]
        assert props["id"]["description"] == "Stable identifier."
        assert "default" not in props["id"]
        assert props["score"]["default"] == 0.5
        assert props["active"]["default"] is True
        assert props["status"]["default"] == "active"
        assert "default" not in props["tags"]
        assert "default" not in props["token"]
        assert props["nickname"]["default"] is None

    def test_constraint_keywords(self) -> None:
        props = ExpPerson.to_json_schema()["properties"]
        assert props["name"]["minLength"] == 1
        assert props["name"]["maxLength"] == 50
        assert props["email"]["pattern"] == r"^(?:[^@\s]+@[^@\s]+)$"
        assert props["age"]["minimum"] == 0
        assert props["age"]["maximum"] == 150
        assert props["score"]["exclusiveMinimum"] == 0.0
        assert props["score"]["exclusiveMaximum"] == 1.0
        assert props["status"]["enum"] == ["active", "inactive"]
        assert props["tags"]["maxItems"] == 3
        assert "maxLength" not in props["tags"]
        assert ExpDecorated.to_json_schema()["properties"]["code"]["pattern"] == r"^[A-Z]{3}$"

    def test_valid_instances(self) -> None:
        assert _valid(ExpPerson, {"id": "p1", "name": "Ann", "email": "ann@example.com", "age": 1})
        full = {
            "id": "p1",
            "name": "Ann",
            "email": "ann@example.com",
            "age": 150,
            "score": 0.9,
            "active": False,
            "status": "inactive",
            "nickname": None,
            "tags": ["a", "b", "c"],
            "attrs": {"k": 1},
            "born": "1999-12-31",
            "seen_at": "2026-01-02T03:04:05Z",
            "wake": "07:30:00Z",
            "tenure": "P1DT2H",
            "home": {"x": 1.0, "y": 2.0, "srid": 7203},
            "token": "abc",
        }
        assert _valid(ExpPerson, full)
        assert _valid(ExpMovie, {"title": "Heat", "color": "red", "embedding": [0.1, 0.2, 0.3]})
        assert _valid(ExpActedIn, {"role": "Neil", "billing": 1})
        assert _valid(ExpKnows, {})
        assert _valid(ExpDecorated, {"code": "ABC"})

    @pytest.mark.parametrize(
        ("model", "instance"),
        [
            (ExpPerson, {"name": "Ann"}),  # missing primary key
            (ExpPerson, {"id": "p1"}),  # missing non-nullable field
            (ExpPerson, {"id": None, "name": "Ann"}),  # primary key not nullable
            (ExpPerson, {"id": "p1", "name": ""}),  # minLength
            (ExpPerson, {"id": "p1", "name": "x" * 51}),  # maxLength
            (ExpPerson, {"id": "p1", "name": "Ann", "email": "nope"}),  # pattern
            (ExpPerson, {"id": "p1", "name": "Ann", "email": "a@b and more x"}),  # anchored
            (ExpPerson, {"id": "p1", "name": "Ann", "age": -1}),  # minimum
            (ExpPerson, {"id": "p1", "name": "Ann", "age": 151}),  # maximum
            (ExpPerson, {"id": "p1", "name": "Ann", "age": 1.5}),  # integer type
            (ExpPerson, {"id": "p1", "name": "Ann", "score": 0}),  # exclusiveMinimum
            (ExpPerson, {"id": "p1", "name": "Ann", "score": 1.0}),  # exclusiveMaximum
            (ExpPerson, {"id": "p1", "name": "Ann", "status": "gone"}),  # enum
            (ExpPerson, {"id": "p1", "name": "Ann", "tags": ["a", "b", "c", "d"]}),  # maxItems
            (ExpPerson, {"id": "p1", "name": "Ann", "tags": [1]}),  # items type
            (ExpPerson, {"id": "p1", "name": "Ann", "attrs": {"k": "v"}}),  # map values
            (ExpPerson, {"id": "p1", "name": "Ann", "born": "02/01/2000"}),  # date format
            (ExpPerson, {"id": "p1", "name": "Ann", "home": {"x": 1.0}}),  # point needs y
            (ExpPerson, {"id": "p1", "name": "Ann", "unknown": 1}),  # additionalProperties
            (ExpMovie, {"title": "Heat", "color": "green"}),  # Enum
            (ExpMovie, {"title": "Heat", "rating": "X"}),  # Literal
            (ExpMovie, {"title": "Heat", "embedding": [0.1, 0.2]}),  # vector dimensions
            (ExpActedIn, {"billing": 1}),  # missing required relationship property
            (ExpActedIn, {"role": "Neil", "billing": 0}),  # relationship minimum
            (ExpDecorated, {"code": "abc"}),  # pattern
        ],
    )
    def test_invalid_instances(self, model: Any, instance: dict[str, Any]) -> None:
        assert not _valid(model, instance)
        with pytest.raises(jsonschema.ValidationError):
            _validator(model).validate(instance)

    def test_nullable_false_enum_excludes_null(self) -> None:
        class ExpStrict(Node):
            kind: str = Field(nullable=False, choices=["a", "b"])

        prop = ExpStrict.to_json_schema()["properties"]["kind"]
        assert prop == {"type": "string", "enum": ["a", "b"]}
        assert not _valid(ExpStrict, {"kind": None})

    def test_unknown_and_union_types(self) -> None:
        class ExpOdd(Node):
            anything: Any = None
            either: int | str | None = None
            pair: tuple[int, str] | None = None
            raw_list: list | None = None

        props = ExpOdd.to_json_schema()["properties"]
        assert props["anything"] == {"default": None}
        assert props["either"] == {
            "anyOf": [{"type": "integer"}, {"type": "string"}, {"type": "null"}],
            "default": None,
        }
        assert props["pair"]["prefixItems"] == [{"type": "integer"}, {"type": "string"}]
        assert props["pair"]["default"] is None
        assert props["raw_list"] == {"type": ["array", "null"], "default": None}
        Draft202012Validator.check_schema(ExpOdd.to_json_schema())

    def test_string_or_list_length_constraint(self) -> None:
        class ExpMixed(Node):
            value: str | list[str] = Field(min_length=2)

        prop = ExpMixed.to_json_schema()["properties"]["value"]
        assert prop["minItems"] == 2
        assert prop["minLength"] == 2
        assert not _valid(ExpMixed, {"value": "a"})
        assert not _valid(ExpMixed, {"value": ["a"]})
        assert _valid(ExpMixed, {"value": ["a", "b"]})

    def test_non_json_default_is_omitted(self) -> None:
        class ExpOpaque(Node):
            blob: Any = Field(default=object())
            when: datetime.timedelta = Field(default=datetime.timedelta(hours=1))

        props = ExpOpaque.to_json_schema()["properties"]
        assert "default" not in props["blob"]
        assert props["when"]["default"] == "PT1H"
        assert "default" not in export_schema([ExpOpaque])["nodes"][0]["properties"]["blob"]
