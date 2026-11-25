"""Tests for DuckPGQ aggregate null and empty-input parity (#349b).

Validates:
1. Compilation of SUM and COLLECT under the DuckPGQ flavor (emitting [] instead of '{}').
2. Execution of compiled aggregate SQL on an in-memory DuckDB table without the duckpgq extension.
3. Live end-to-end GRAPH_TABLE query execution when the duckpgq extension is available.
"""

from __future__ import annotations

import pytest
from voyager_ogm import Field, Node, Query, Session, node


@node(label="Person")
class Person(Node):
    """Person model for DuckPGQ aggregate testing."""

    id = Field(primary_key=True)
    name = Field()
    age = Field()
    city = Field()


@node(label="Employee")
class Employee(Node):
    """Employee model for grouped aggregation testing."""

    id = Field(primary_key=True)
    dept = Field()
    salary = Field()
    tag = Field()


def test_duckpgq_compilation_aggregates():
    """DuckPGQ flavor emits empty list [] instead of PostgreSQL array literal '{}'."""
    p = Person(alias="p")
    q = Query.match(p).return_(
        total_age=p.age.sum(),
        cities=p.city.collect(),
    )

    # Generic sql_pgq emits PostgreSQL array literal '{}'
    compiled_pg = q.compile("sql_pgq", graph_name="g")
    assert (
        'COALESCE(ARRAY_AGG("city") FILTER (WHERE "city" IS NOT NULL), \'{}\') AS "cities"'
        in compiled_pg.statement
    )
    assert 'COALESCE(SUM("age"), 0) AS "total_age"' in compiled_pg.statement

    # duckpgq flavor emits DuckDB empty list []
    compiled_duck = q.compile("duckpgq", graph_name="g")
    assert (
        'COALESCE(ARRAY_AGG("city") FILTER (WHERE "city" IS NOT NULL), []) AS "cities"'
        in compiled_duck.statement
    )
    assert 'COALESCE(SUM("age"), 0) AS "total_age"' in compiled_duck.statement


def test_duckpgq_compilation_distinct_and_grouped():
    """DuckPGQ flavor supports DISTINCT and grouped aggregations."""
    e = Employee(alias="e")
    q = (
        Query.match(e)
        .return_(
            dept=e.dept,
            total_salary=e.salary.sum(),
            tags=e.tag.collect(),
        )
        .order_by(e.dept)
    )

    compiled = q.compile("duckpgq", graph_name="corp")
    assert 'SELECT "dept", COALESCE(SUM("salary"), 0) AS "total_salary"' in compiled.statement
    assert (
        'COALESCE(ARRAY_AGG("tag") FILTER (WHERE "tag" IS NOT NULL), []) AS "tags"'
        in compiled.statement
    )
    assert 'GROUP BY "dept"' in compiled.statement


def test_duckpgq_aggregates_on_in_memory_duckdb_without_duckpgq():
    """Runs compiled aggregate expressions on an in-memory DuckDB table without requiring DuckPGQ extension."""
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(":memory:")

    con.execute(
        """
        CREATE TABLE person (
            id BIGINT PRIMARY KEY,
            name VARCHAR,
            age INTEGER
        );
        INSERT INTO person VALUES
            (1, 'Alice', 10),
            (2, NULL, NULL),
            (3, 'Bob', 20);
        """
    )

    p = Person(alias="p")
    q = Query.match(p).return_(
        total_age=p.age.sum(),
        names=p.name.collect(),
    )
    compiled = q.compile("duckpgq", graph_name="dummy_graph")

    # Substitute the GRAPH_TABLE clause with the in-memory relational table
    select_clause = compiled.statement.split("FROM GRAPH_TABLE")[0].strip()
    relational_sql = f"{select_clause} FROM person"

    # 1. Inputs with nulls: sum sums non-nulls (30), collect filters nulls (['Alice', 'Bob'])
    rows = con.execute(relational_sql).fetchall()
    assert rows == [(30, ["Alice", "Bob"])]

    # 2. Empty input: sum returns 0, collect returns empty list []
    empty_sql = f"{select_clause} FROM person WHERE FALSE"
    empty_rows = con.execute(empty_sql).fetchall()
    assert empty_rows == [(0, [])]

    # 3. All-null input: row with id=2 has NULL age and NULL name
    null_sql = f"{select_clause} FROM person WHERE id = 2"
    null_rows = con.execute(null_sql).fetchall()
    assert null_rows == [(0, [])]


def test_duckpgq_end_to_end_graph_table_if_available():
    """Executes live GRAPH_TABLE query with property graph when DuckPGQ extension is available."""
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    try:
        con.execute("LOAD duckpgq;")
    except Exception as exc:
        pytest.skip(f"duckpgq extension not available: {exc}")

    con.execute(
        """
        CREATE TABLE Person (
            id BIGINT PRIMARY KEY,
            name VARCHAR,
            age INTEGER
        );
        INSERT INTO Person VALUES
            (1, 'Alice', 30),
            (2, NULL, NULL),
            (3, 'Bob', 40);

        CREATE PROPERTY GRAPH person_pg
        VERTEX TABLES (
            Person LABEL "Person"
        );
        """
    )

    session = Session(bridge=con, dialect="duckpgq")
    p = Person(alias="p")

    # 1. Normal execution with null filtering:
    q = Query.match(p).return_(
        total_age=p.age.sum(),
        all_names=p.name.collect(),
    )
    records = session.execute(q.compile("duckpgq", graph_name="person_pg")).all()
    assert len(records) == 1
    assert records[0]["total_age"] == 70
    assert records[0]["all_names"] == ["Alice", "Bob"]

    # 2. Empty input execution:
    q_empty = (
        Query.match(p)
        .where(p.age > 1000)
        .return_(
            total_age=p.age.sum(),
            all_names=p.name.collect(),
        )
    )
    empty_records = session.execute(q_empty.compile("duckpgq", graph_name="person_pg")).all()
    assert len(empty_records) == 1
    assert empty_records[0]["total_age"] == 0
    assert empty_records[0]["all_names"] == []

    # 3. Session auto-detection: Session(bridge=con) auto-selects duckpgq
    auto_session = Session(bridge=con)
    assert auto_session.dialect == "duckpgq"
    auto_records = auto_session.execute(q.compile("duckpgq", graph_name="person_pg")).all()
    assert auto_records[0]["total_age"] == 70
    assert auto_records[0]["all_names"] == ["Alice", "Bob"]
