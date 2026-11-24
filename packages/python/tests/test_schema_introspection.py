"""Unit tests for ``introspect_schema`` that need no database."""

from __future__ import annotations

from typing import Any

import pytest
from voyager_ogm import Field, Node, VoyagerError, export_schema, introspect_schema, node
from voyager_ogm.schema_export import DEFAULT_SAMPLE_SIZE, _introspection_uri


@node("IntroUnitPerson")
class IntroUnitPerson(Node):
    pid: str = Field(primary_key=True)
    email: str = Field(unique=True)


class _HasUri:
    def __init__(self, uri: str) -> None:
        self.uri = uri


class _NativeSession:
    def __init__(self, uri: str) -> None:
        self._native_client = _HasUri(uri)


def test_default_sample_size_matches_the_rust_default() -> None:
    assert DEFAULT_SAMPLE_SIZE == 1000


@pytest.mark.parametrize(
    "source",
    [
        "bolt://localhost:7687",
        _HasUri("bolt://localhost:7687"),
        _NativeSession("bolt://localhost:7687"),
    ],
)
def test_sources_resolve_to_their_uri(source: Any) -> None:
    assert _introspection_uri(source) == "bolt://localhost:7687"


def test_sessions_without_a_native_client_are_rejected() -> None:
    with pytest.raises(TypeError, match="connection URI"):
        introspect_schema(object())


@pytest.mark.parametrize("sample_size", [-1, 1.5, True])
def test_invalid_sample_sizes_are_rejected(sample_size: Any) -> None:
    with pytest.raises(ValueError, match="sample_size"):
        introspect_schema("bolt://localhost:7687", sample_size=sample_size)


def test_duckdb_is_unsupported_without_connecting() -> None:
    with pytest.raises(VoyagerError) as raised:
        introspect_schema("duckdb:///tmp/voyager-introspect.db")
    assert raised.value.code == "UNSUPPORTED_FEATURE"
    assert "DuckDB" in str(raised.value)


def test_connection_failures_raise_voyager_error() -> None:
    with pytest.raises(VoyagerError):
        introspect_schema("bolt://127.0.0.1:1")


def test_export_keys_that_live_introspection_mirrors() -> None:
    """Pins the export keys the live schema reproduces (see the live tests)."""
    exported = export_schema([IntroUnitPerson])["nodes"][0]
    assert list(exported) == [
        "name",
        "model",
        "labels",
        "description",
        "primary_key",
        "properties",
        "relationship_attributes",
    ]
    prop = exported["properties"]["email"]
    assert set(prop) >= {
        "name",
        "attribute",
        "type",
        "nullable",
        "primary_key",
        "unique",
        "indexed",
        "index_type",
        "description",
        "has_default",
        "constraints",
    }
