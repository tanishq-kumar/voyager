"""Bulk ingestion dialect tests (#397).

SQL/PGQ and DuckPGQ bulk writes raise ``UNSUPPORTED_FEATURE`` instead of sending
Cypher to a SQL server; an unknown dialect raises ``UNSUPPORTED_DIALECT``.
"""

from __future__ import annotations

import pytest
from voyager_ogm import Field, Node, Session, VoyagerError, create_bulk_create_plan, node


@node("ZzBulkDialectPerson")
class Person(Node):
    id: Field[int] = Field()
    name: Field[str] = Field()


@pytest.fixture()
def duckdb_connection():
    # duckdb is not installed on every supported Python (see pyproject.toml), so only
    # the tests that need a DuckDB connection are skipped when it is missing.
    duckdb = pytest.importorskip("duckdb")
    connection = duckdb.connect(":memory:")
    try:
        yield connection
    finally:
        connection.close()


@pytest.mark.parametrize("dialect", ["sql_pgq", "pgq", "sql", "duckpgq", "duck_pgq"])
def test_sql_pgq_bulk_plan_raises_unsupported_feature(dialect: str) -> None:
    with pytest.raises(VoyagerError) as exc_info:
        create_bulk_create_plan(Person, [{"id": 1, "name": "Ada"}], dialect=dialect)

    error = exc_info.value
    assert error.code == "UNSUPPORTED_FEATURE"
    assert dialect in str(error)
    assert "Cypher" in str(error)


def test_unknown_bulk_dialect_raises_unsupported_dialect() -> None:
    with pytest.raises(VoyagerError) as exc_info:
        create_bulk_create_plan(Person, [{"id": 1, "name": "Ada"}], dialect="not_a_dialect")

    error = exc_info.value
    assert error.code == "UNSUPPORTED_DIALECT"
    assert "not_a_dialect" in str(error)


def test_cypher_bulk_plan_still_compiles() -> None:
    plan = create_bulk_create_plan(Person, [{"id": 1, "name": "Ada"}], dialect="cypher")
    assert plan.statement.startswith("UNWIND $batch AS row CREATE (_zzbulkdialectperson_0")
    assert plan.dialect == "cypher"


def test_session_bulk_create_on_postgres_sql_pgq_raises() -> None:
    session = Session(dialect="sql_pgq")
    try:
        with pytest.raises(VoyagerError) as exc_info:
            session.bulk_create(Person, [{"id": 1, "name": "Ada"}])
        assert exc_info.value.code == "UNSUPPORTED_FEATURE"
        assert "sql_pgq" in str(exc_info.value)
    finally:
        session.close()


def test_session_flush_on_duckdb_ingests_relationally(duckdb_connection) -> None:
    """DuckDB sessions keep flushing: the bridge ingests the openCypher plan as table rows."""
    session = Session(bridge=duckdb_connection, enable_identity_map=True)
    assert session.dialect == "duckpgq"
    tracked = session.register(Person(id=1, name="Ada"), is_clean=True)
    tracked.name = "Ada Lovelace"

    try:
        results = session.flush()
        assert len(results) == 1
        assert results[0].total_records == 1
        rows = duckdb_connection.execute("SELECT name FROM ZzBulkDialectPerson").fetchall()
        assert rows == [("Ada Lovelace",)]
    finally:
        session.close()


def test_session_bulk_create_on_duckdb_with_explicit_duckpgq_raises(duckdb_connection) -> None:
    """An explicitly requested DuckPGQ bulk plan still raises instead of emitting Cypher."""
    session = Session(bridge=duckdb_connection)
    try:
        with pytest.raises(VoyagerError) as exc_info:
            session.bulk_create(Person, [{"id": 1, "name": "Ada"}], dialect="duckpgq")
        assert exc_info.value.code == "UNSUPPORTED_FEATURE"
    finally:
        session.close()
