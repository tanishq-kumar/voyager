"""Regression tests for DuckDB UNION exports to Polars (#446)."""

from __future__ import annotations

import warnings

import polars as pl
import pyarrow as pa
import pytest
from voyager_ogm import Session
from voyager_ogm.bridge import DuckDbBridge


@pytest.fixture
def duckdb_session():
    """Yields a Session over an in-memory DuckDB connection."""
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(":memory:")
    session = Session(bridge=DuckDbBridge(con), dialect="duckpgq")
    try:
        yield session
    finally:
        session.close()


def test_duckdb_polars_union_column_becomes_tagged_struct(duckdb_session: Session) -> None:
    """A top-level UNION is a struct per member plus ``tag``, with a warning naming the column."""
    statement = "SELECT union_value(num := 2)::UNION(num INT, str VARCHAR) AS u"

    with pytest.warns(UserWarning, match="UNION column 'u'"):
        frame = duckdb_session.execute_to_polars(statement)

    assert [(field.name, field.dtype) for field in frame.schema["u"].fields] == [
        ("num", pl.Int32),
        ("str", pl.String),
        ("tag", pl.String),
    ]
    assert frame["u"].to_list() == [{"num": 2, "str": None, "tag": "num"}]

    table = duckdb_session.execute_to_arrow(statement)
    assert pa.types.is_union(table.schema.field("u").type)


def test_duckdb_polars_nested_union_columns_are_converted_recursively(
    duckdb_session: Session,
) -> None:
    """Unions inside lists, structs, maps and other unions keep their member types and tags."""
    statement = """
        SELECT
            [union_value(num := 2)::UNION(num INT, str VARCHAR),
             union_value(str := 'x')::UNION(num INT, str VARCHAR)] AS list_col,
            {'a': union_value(num := 3)::UNION(num INT, str VARCHAR)} AS struct_col,
            MAP {'k': union_value(str := 'y')::UNION(num INT, str VARCHAR)} AS map_col,
            union_value(x := union_value(num := 4)::UNION(num INT, str VARCHAR))
                ::UNION(x UNION(num INT, str VARCHAR), y INT) AS nested_col
    """

    with pytest.warns(UserWarning, match="UNION column"):
        frame = duckdb_session.execute_to_polars(statement)

    assert frame["list_col"].to_list() == [
        [
            {"num": 2, "str": None, "tag": "num"},
            {"num": None, "str": "x", "tag": "str"},
        ]
    ]
    assert frame["struct_col"].to_list() == [{"a": {"num": 3, "str": None, "tag": "num"}}]
    assert frame["map_col"].to_list() == [
        [{"key": "k", "value": {"num": None, "str": "y", "tag": "str"}}]
    ]
    assert frame["nested_col"].to_list() == [
        {"x": {"num": 4, "str": None, "tag": "num"}, "y": None, "tag": "x"}
    ]


def test_duckdb_polars_union_empty_result_keeps_struct_schema(
    duckdb_session: Session,
) -> None:
    """A 0-row UNION result exposes the converted struct schema instead of raising."""
    statement = (
        "SELECT union_value(num := 2)::UNION(num INT, str VARCHAR) AS u, "
        "[union_value(num := 2)::UNION(num INT, str VARCHAR)] AS us WHERE 1 = 0"
    )

    with pytest.warns(UserWarning, match="UNION column 'u'"):
        with pytest.warns(UserWarning, match="UNION column 'us'"):
            frame = duckdb_session.execute_to_polars(statement)

    assert frame.height == 0
    assert [(field.name, field.dtype) for field in frame.schema["u"].fields] == [
        ("num", pl.Int32),
        ("str", pl.String),
        ("tag", pl.String),
    ]
    assert frame.schema["us"].inner == frame.schema["u"]


def test_duckdb_polars_union_statement_runs_once_and_warns_once(
    duckdb_session: Session,
) -> None:
    """Converting a UNION does not re-execute the statement and warns once per column."""
    duckdb_session.execute("CREATE SEQUENCE zz_union_polars_seq")
    statement = (
        "SELECT union_value(num := nextval('zz_union_polars_seq'))"
        "::UNION(num BIGINT, str VARCHAR) AS u"
    )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        frame = duckdb_session.execute_to_polars(statement)

    assert frame["u"].to_list() == [{"num": 1, "str": None, "tag": "num"}]
    assert len(caught) == 1
    assert "UNION column 'u'" in str(caught[0].message)
    assert duckdb_session.execute("SELECT nextval('zz_union_polars_seq')").scalars().first() == 2

    with duckdb_session.transaction():
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            frame = duckdb_session.execute_to_polars(
                "SELECT union_value(num := 5)::UNION(num INT, str VARCHAR) AS u"
            )
    assert frame["u"].to_list() == [{"num": 5, "str": None, "tag": "num"}]
    assert len(caught) == 1


def test_duckdb_polars_union_tag_field_avoids_member_name_collision(
    duckdb_session: Session,
) -> None:
    """A union member called ``tag`` keeps its name and the tag field is suffixed."""
    statement = "SELECT union_value(tag := 7)::UNION(tag INT, x VARCHAR) AS u"

    with pytest.warns(UserWarning, match="'tag_2'"):
        frame = duckdb_session.execute_to_polars(statement)

    assert frame["u"].to_list() == [{"tag": 7, "x": None, "tag_2": "tag"}]


def test_duckdb_polars_non_union_columns_do_not_warn(duckdb_session: Session) -> None:
    """Non-union results keep the direct Arrow path without a fallback warning."""
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        frame = duckdb_session.execute_to_polars("SELECT 1 AS n, 'x' AS s")

    assert frame.to_dicts() == [{"n": 1, "s": "x"}]
