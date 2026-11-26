"""Live Session test for SQL/PGQ column name disambiguation (#364).

Verifies that unaliased projections of the same property name from different variables
(e.g. `RETURN a.name, b.name`) emit unique column names (e.g. `COLUMNS (a.name, b.name AS "name_1")`),
so that multi-column row records in `Session.execute` preserve all projected column values
under distinct dictionary keys instead of one overwriting the other.

Tested on:
- PostgreSQL 19 Beta 3 (if reachable on VOYAGER_TEST_HOST:5456)
- DuckDB / DuckPGQ (embedded in-memory)
"""

from __future__ import annotations

import os
import socket
import uuid

import pytest
from conftest import get_test_host
from voyager_ogm import Field, Node, Query, Relationship, Session, node, relationship

HOST = get_test_host()
PG19_PORT = int(os.environ.get("VOYAGER_TEST_PG19_PORT", "5456"))
PG19_URI = f"postgresql://postgres:voyagerpass123@{HOST}:{PG19_PORT}/voyager_graph"


def _is_port_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1.0):
            return True
    except OSError:
        return False


@node(label="Person")
class Person(Node):
    id = Field(primary_key=True)
    name = Field()


@relationship(type_name="KNOWS")
class Knows(Relationship):
    pass


def test_duckpgq_embedded_session_column_disambiguation():
    """Verifies in-memory DuckPGQ Session.execute preserves clashing column names under distinct keys."""
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    try:
        con.execute("LOAD duckpgq;")
    except Exception as exc:
        pytest.skip(f"duckpgq extension not available: {exc}")

    graph_name = f"zz364_duckpg_{uuid.uuid4().hex[:8]}"
    con.execute(
        f"""
        CREATE TABLE Person (id BIGINT PRIMARY KEY, name VARCHAR);
        CREATE TABLE Knows (src_id BIGINT, dst_id BIGINT);
        INSERT INTO Person VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');
        INSERT INTO Knows VALUES (1, 2), (2, 3);
        CREATE PROPERTY GRAPH {graph_name}
            VERTEX TABLES (Person LABEL "Person")
            EDGE TABLES (
                Knows
                SOURCE KEY (src_id) REFERENCES Person (id)
                DESTINATION KEY (dst_id) REFERENCES Person (id)
                LABEL "KNOWS"
            );
        """
    )

    session = Session(bridge=con, dialect="duckpgq")

    a = Person(alias="a")
    b = Person(alias="b")
    c = Person(alias="c")

    # 1. Two unaliased same-name properties:
    q2 = Query.match(a).to(Knows, var="r").node(b).where(a.id == 1).return_(a.name, b.name)
    compiled2 = q2.compile("duckpgq", graph_name=graph_name)
    assert 'COLUMNS (a.name, b.name AS "name_1")' in compiled2.statement
    records2 = session.execute(compiled2).all()
    assert len(records2) == 1
    assert "name" in records2[0]
    assert "name_1" in records2[0]
    assert records2[0]["name"] == "Alice"
    assert records2[0]["name_1"] == "Bob"

    # 2. Three unaliased same-name properties:
    q3 = (
        Query.match(a)
        .to(Knows, var="r1")
        .node(b)
        .to(Knows, var="r2")
        .node(c)
        .where(a.id == 1)
        .return_(a.name, b.name, c.name)
    )
    compiled3 = q3.compile("duckpgq", graph_name=graph_name)
    assert 'COLUMNS (a.name, b.name AS "name_1", c.name AS "name_2")' in compiled3.statement
    records3 = session.execute(compiled3).all()
    assert len(records3) == 1
    assert records3[0]["name"] == "Alice"
    assert records3[0]["name_1"] == "Bob"
    assert records3[0]["name_2"] == "Charlie"

    # 3. Explicit alias colliding with default name:
    q_coll = (
        Query.match(a)
        .to(Knows, var="r")
        .node(b)
        .where(a.id == 1)
        .return_("a.name AS name", "b.name")
    )
    compiled_coll = q_coll.compile("duckpgq", graph_name=graph_name)
    assert 'COLUMNS (a.name AS "name", c.name AS "name_1")' not in compiled_coll.statement
    assert 'COLUMNS (a.name AS "name", b.name AS "name_1")' in compiled_coll.statement
    records_coll = session.execute(compiled_coll).all()
    assert records_coll[0]["name"] == "Alice"
    assert records_coll[0]["name_1"] == "Bob"


@pytest.mark.live
def test_postgres19_live_session_column_disambiguation():
    """Verifies live PostgreSQL 19 Session.execute preserves clashing column names under distinct keys."""
    if not _is_port_open(HOST, PG19_PORT):
        pytest.skip(f"PostgreSQL 19 not reachable on {HOST}:{PG19_PORT}")

    psycopg = pytest.importorskip("psycopg")
    uid = uuid.uuid4().hex[:8]
    node_table = f"zz364_node_{uid}"
    edge_table = f"zz364_edge_{uid}"
    graph_name = f"zz364_graph_{uid}"

    conn = psycopg.connect(PG19_URI, autocommit=True, connect_timeout=3)
    try:
        conn.execute(f'DROP PROPERTY GRAPH IF EXISTS "{graph_name}"')
        conn.execute(f'DROP TABLE IF EXISTS "{edge_table}" CASCADE')
        conn.execute(f'DROP TABLE IF EXISTS "{node_table}" CASCADE')

        conn.execute(
            f"""
            CREATE TABLE "{node_table}" (
                id BIGINT PRIMARY KEY,
                name TEXT NOT NULL
            );
            CREATE TABLE "{edge_table}" (
                src_id BIGINT REFERENCES "{node_table}"(id),
                dst_id BIGINT REFERENCES "{node_table}"(id),
                PRIMARY KEY (src_id, dst_id)
            );
            INSERT INTO "{node_table}" VALUES (1, 'Alice'), (2, 'Bob');
            INSERT INTO "{edge_table}" VALUES (1, 2);

            CREATE PROPERTY GRAPH "{graph_name}"
                VERTEX TABLES (
                    "{node_table}" LABEL "Person"
                )
                EDGE TABLES (
                    "{edge_table}"
                    SOURCE KEY (src_id) REFERENCES "{node_table}" (id)
                    DESTINATION KEY (dst_id) REFERENCES "{node_table}" (id)
                    LABEL "KNOWS"
                );
            """
        )

        session = Session(PG19_URI, dialect="sql_pgq")
        a = Person(alias="a")
        b = Person(alias="b")

        q = Query.match(a).to(Knows, var="r").node(b).where(a.id == 1).return_(a.name, b.name)
        compiled = q.compile("sql_pgq", graph_name=graph_name)

        assert 'COLUMNS (a.name, b.name AS "name_1")' in compiled.statement

        result = session.execute(compiled)
        records = result.all()

        assert len(records) == 1
        record = records[0]
        assert "name" in record, f"Expected 'name' in record keys: {list(record.keys())}"
        assert "name_1" in record, f"Expected 'name_1' in record keys: {list(record.keys())}"
        assert record["name"] == "Alice"
        assert record["name_1"] == "Bob"

    finally:
        try:
            conn.execute(f'DROP PROPERTY GRAPH IF EXISTS "{graph_name}"')
            conn.execute(f'DROP TABLE IF EXISTS "{edge_table}" CASCADE')
            conn.execute(f'DROP TABLE IF EXISTS "{node_table}" CASCADE')
        except Exception:
            pass
        conn.close()
