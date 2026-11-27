"""Live cross-engine tests for result.columns, keys(), and empty Polars/Arrow exports (#378)."""

from __future__ import annotations

import os
import socket
import uuid

import polars as pl
import pyarrow as pa
import pytest
from voyager_ogm import AsyncSession, Session

pytestmark = pytest.mark.live

HOST = os.environ.get("VOYAGER_TEST_HOST", "127.0.0.1")
NEO4J_PORT = int(os.environ.get("VOYAGER_TEST_NEO4J_PORT", "7687"))
MEMGRAPH_PORT = int(os.environ.get("VOYAGER_TEST_MEMGRAPH_PORT", "7688"))
FALKORDB_PORT = int(os.environ.get("VOYAGER_TEST_FALKORDB_PORT", "6379"))
AGE_PORT = int(os.environ.get("VOYAGER_TEST_AGE_PORT", "5455"))
PG19_PORT = int(os.environ.get("VOYAGER_TEST_PG19_PORT", "5456"))


def _port_open(port: int) -> bool:
    try:
        with socket.create_connection((HOST, port), timeout=1.0):
            return True
    except OSError:
        return False


def _assert_result_columns_workflow(
    session: Session,
    *,
    non_empty_stmt: str,
    empty_stmt: str,
    write_stmt: str | None,
    expected_cols: tuple[str, ...],
) -> None:
    res_full = session.execute(non_empty_stmt)
    assert res_full.columns == expected_cols
    assert res_full.keys() == list(expected_cols)
    assert len(res_full.all()) >= 1
    assert res_full.mappings().columns == expected_cols
    assert res_full.mappings().keys() == list(expected_cols)

    res_empty = session.execute(empty_stmt)
    assert res_empty.columns == expected_cols
    assert res_empty.keys() == list(expected_cols)
    assert res_empty.all() == []

    df_from_res = res_empty.to_polars()
    assert isinstance(df_from_res, pl.DataFrame)
    assert df_from_res.shape == (0, len(expected_cols))
    assert df_from_res.columns == list(expected_cols)

    tbl_from_res = res_empty.to_arrow()
    assert isinstance(tbl_from_res, pa.Table)
    assert tbl_from_res.num_rows == 0
    assert tbl_from_res.column_names == list(expected_cols)

    df_direct = session.execute_to_polars(empty_stmt)
    assert isinstance(df_direct, pl.DataFrame)
    assert df_direct.shape == (0, len(expected_cols))
    assert df_direct.columns == list(expected_cols)

    tbl_direct = session.execute_to_arrow(empty_stmt)
    assert isinstance(tbl_direct, pa.Table)
    assert tbl_direct.num_rows == 0
    assert tbl_direct.column_names == list(expected_cols)

    if write_stmt is not None:
        res_write = session.execute(write_stmt)
        assert res_write.columns == ()
        assert res_write.keys() == []
        assert res_write.all() == []
        assert session.execute_to_polars(write_stmt).shape == (0, 0)
        assert session.execute_to_arrow(write_stmt).num_columns == 0


@pytest.mark.parametrize("backend", ("bridge", "native"))
def test_live_neo4j_result_columns(backend: str) -> None:
    """Neo4j bridge and native report projection columns on rows, 0 rows, and writes."""
    if not _port_open(NEO4J_PORT):
        pytest.skip(f"Neo4j not reachable on {HOST}:{NEO4J_PORT}")

    tag = uuid.uuid4().hex[:8]
    label = f"ZzColNeo_{tag}"
    uri = f"bolt://neo4j:voyagerpass123@{HOST}:{NEO4J_PORT}"

    with Session(uri, dialect="cypher", backend=backend) as session:
        try:
            session.execute(f"CREATE (:{label} {{name: 'Alice', age: 30}})")
            _assert_result_columns_workflow(
                session,
                non_empty_stmt=f"MATCH (n:{label}) RETURN n.name AS person_name, n.age AS person_age",
                empty_stmt=(
                    f"MATCH (n:{label}) WHERE n.name = 'nobody' "
                    "RETURN n.name AS person_name, n.age AS person_age"
                ),
                write_stmt=f"CREATE (:{label} {{name: 'Bob', age: 40}})",
                expected_cols=("person_name", "person_age"),
            )
        finally:
            session.execute(f"MATCH (n:{label}) DETACH DELETE n")


@pytest.mark.asyncio
async def test_live_neo4j_async_result_columns() -> None:
    """AsyncSession preserves columns on Neo4j for bridge and native backends."""
    if not _port_open(NEO4J_PORT):
        pytest.skip(f"Neo4j not reachable on {HOST}:{NEO4J_PORT}")

    tag = uuid.uuid4().hex[:8]
    label = f"ZzColNeoAsync_{tag}"
    uri = f"bolt://neo4j:voyagerpass123@{HOST}:{NEO4J_PORT}"

    for backend in ("bridge", "native"):
        async with AsyncSession(uri, dialect="cypher", backend=backend) as session:
            try:
                res_write = await session.execute(f"CREATE (:{label} {{name: 'Alice', age: 30}})")
                assert res_write.columns == ()
                res_empty = await session.execute(
                    f"MATCH (n:{label}) WHERE n.name = 'missing' "
                    "RETURN n.name AS person_name, n.age AS person_age"
                )
                assert res_empty.columns == ("person_name", "person_age")
                assert res_empty.keys() == ["person_name", "person_age"]
                assert (
                    await session.execute_to_polars(
                        f"MATCH (n:{label}) WHERE n.name = 'missing' "
                        "RETURN n.name AS person_name, n.age AS person_age"
                    )
                ).columns == ["person_name", "person_age"]
                assert (
                    await session.execute_to_arrow(
                        f"MATCH (n:{label}) WHERE n.name = 'missing' "
                        "RETURN n.name AS person_name, n.age AS person_age"
                    )
                ).column_names == ["person_name", "person_age"]
            finally:
                await session.execute(f"MATCH (n:{label}) DETACH DELETE n")


@pytest.mark.parametrize("backend", ("bridge", "native"))
def test_live_memgraph_result_columns(backend: str) -> None:
    """Memgraph bridge and native report projection columns on rows, 0 rows, and writes."""
    if not _port_open(MEMGRAPH_PORT):
        pytest.skip(f"Memgraph not reachable on {HOST}:{MEMGRAPH_PORT}")

    tag = uuid.uuid4().hex[:8]
    label = f"ZzColMg_{tag}"
    uri = f"bolt://{HOST}:{MEMGRAPH_PORT}"

    with Session(uri, dialect="cypher", backend=backend) as session:
        try:
            session.execute(f"CREATE (:{label} {{name: 'Alice', age: 30}})")
            _assert_result_columns_workflow(
                session,
                non_empty_stmt=f"MATCH (n:{label}) RETURN n.name AS person_name, n.age AS person_age",
                empty_stmt=(
                    f"MATCH (n:{label}) WHERE n.name = 'nobody' "
                    "RETURN n.name AS person_name, n.age AS person_age"
                ),
                write_stmt=f"CREATE (:{label} {{name: 'Bob', age: 40}})",
                expected_cols=("person_name", "person_age"),
            )
        finally:
            session.execute(f"MATCH (n:{label}) DETACH DELETE n")


@pytest.mark.parametrize("backend", ("bridge", "native"))
def test_live_falkordb_result_columns(backend: str) -> None:
    """FalkorDB bridge and native report projection columns on rows, 0 rows, and writes."""
    if not _port_open(FALKORDB_PORT):
        pytest.skip(f"FalkorDB not reachable on {HOST}:{FALKORDB_PORT}")

    falkordb = pytest.importorskip("falkordb")
    tag = uuid.uuid4().hex[:8]
    graph_name = f"zz_col_falkor_{tag}"
    uri = f"falkordb://{HOST}:{FALKORDB_PORT}/{graph_name}"

    with Session(uri, dialect="cypher", backend=backend, graph_name=graph_name) as session:
        try:
            session.execute("CREATE (:ZzColPerson {name: 'Alice', age: 30})")
            _assert_result_columns_workflow(
                session,
                non_empty_stmt=(
                    "MATCH (n:ZzColPerson) RETURN n.name AS person_name, n.age AS person_age"
                ),
                empty_stmt=(
                    "MATCH (n:ZzColPerson) WHERE n.name = 'nobody' "
                    "RETURN n.name AS person_name, n.age AS person_age"
                ),
                write_stmt="CREATE (:ZzColPerson {name: 'Bob', age: 40})",
                expected_cols=("person_name", "person_age"),
            )
        finally:
            try:
                client = falkordb.FalkorDB(host=HOST, port=FALKORDB_PORT)
                client.select_graph(graph_name).delete()
            except Exception:
                pass


@pytest.mark.parametrize("backend", ("bridge", "native"))
def test_live_age_result_columns(backend: str) -> None:
    """Apache AGE bridge and native report columns on rows, 0 rows, and non-returning SQL writes."""
    if not _port_open(AGE_PORT):
        pytest.skip(f"Apache AGE not reachable on {HOST}:{AGE_PORT}")

    psycopg = pytest.importorskip("psycopg")
    tag = uuid.uuid4().hex[:8]
    graph_name = f"zz_col_age_{tag}"
    aux_table = f"zz_col_age_tbl_{tag}"
    uri = f"postgresql://postgres:voyagerpass123@{HOST}:{AGE_PORT}/voyager_graph"

    conn = psycopg.connect(uri, autocommit=True, connect_timeout=3)
    try:
        with conn.cursor() as cur:
            cur.execute("LOAD 'age'; SET search_path = ag_catalog, \"$user\", public;")
            cur.execute("SELECT ag_catalog.create_graph(%s);", (graph_name,))
            cur.execute(
                f"SELECT * FROM ag_catalog.cypher('{graph_name}', "
                "$$ CREATE (:ZzColPerson {name: 'Alice', age: 30}) $$) AS (v agtype);"
            )
            cur.execute(f'CREATE TABLE "{aux_table}" (id INT)')

        with Session(uri, dialect="age", backend=backend, graph_name=graph_name) as session:
            _assert_result_columns_workflow(
                session,
                non_empty_stmt=(
                    f"SELECT * FROM ag_catalog.cypher('{graph_name}', "
                    "$$ MATCH (n:ZzColPerson) RETURN n.name, n.age $$) "
                    "AS (person_name agtype, person_age agtype)"
                ),
                empty_stmt=(
                    f"SELECT * FROM ag_catalog.cypher('{graph_name}', "
                    "$$ MATCH (n:ZzColPerson) WHERE n.name = 'nobody' RETURN n.name, n.age $$) "
                    "AS (person_name agtype, person_age agtype)"
                ),
                write_stmt=f'INSERT INTO "{aux_table}" VALUES (1)',
                expected_cols=("person_name", "person_age"),
            )
    finally:
        try:
            with conn.cursor() as cur:
                cur.execute("LOAD 'age'; SET search_path = ag_catalog, \"$user\", public;")
                cur.execute("SELECT ag_catalog.drop_graph(%s, true);", (graph_name,))
                cur.execute(f'DROP TABLE IF EXISTS "{aux_table}"')
        except Exception:
            pass
        conn.close()


@pytest.mark.parametrize("backend", ("bridge", "native"))
def test_live_pg19_sql_pgq_result_columns(backend: str) -> None:
    """PostgreSQL 19 SQL/PGQ bridge and native report columns on rows, 0 rows, and writes."""
    if not _port_open(PG19_PORT):
        pytest.skip(f"PostgreSQL 19 not reachable on {HOST}:{PG19_PORT}")

    psycopg = pytest.importorskip("psycopg")
    tag = uuid.uuid4().hex[:8]
    node_table = f"zz_col_pgq_person_{tag}"
    aux_table = f"zz_col_pgq_aux_{tag}"
    graph_name = f"zz_col_pgq_graph_{tag}"
    uri = f"postgresql://postgres:voyagerpass123@{HOST}:{PG19_PORT}/voyager_graph"

    conn = psycopg.connect(uri, autocommit=True, connect_timeout=3)
    try:
        conn.execute(
            f"""
            CREATE TABLE "{node_table}" (id BIGINT PRIMARY KEY, name TEXT NOT NULL, age BIGINT NOT NULL);
            CREATE TABLE "{aux_table}" (id BIGINT);
            INSERT INTO "{node_table}" VALUES (1, 'Alice', 30);
            CREATE PROPERTY GRAPH "{graph_name}"
                VERTEX TABLES ("{node_table}" LABEL "Person");
            """
        )
        with Session(uri, dialect="sql_pgq", backend=backend) as session:
            _assert_result_columns_workflow(
                session,
                non_empty_stmt=(
                    f'SELECT * FROM GRAPH_TABLE ("{graph_name}" '
                    'MATCH (p IS "Person") COLUMNS (p.name AS person_name, p.age AS person_age))'
                ),
                empty_stmt=(
                    f'SELECT * FROM GRAPH_TABLE ("{graph_name}" '
                    "MATCH (p IS \"Person\") WHERE p.name = 'nobody' "
                    "COLUMNS (p.name AS person_name, p.age AS person_age))"
                ),
                write_stmt=f'INSERT INTO "{aux_table}" VALUES (1)',
                expected_cols=("person_name", "person_age"),
            )
    finally:
        try:
            conn.execute(f'DROP PROPERTY GRAPH IF EXISTS "{graph_name}"')
            conn.execute(f'DROP TABLE IF EXISTS "{aux_table}" CASCADE')
            conn.execute(f'DROP TABLE IF EXISTS "{node_table}" CASCADE')
        except Exception:
            pass
        conn.close()


def test_live_duckpgq_embedded_result_columns() -> None:
    """Embedded DuckDB/DuckPGQ exposes columns on rows, 0 rows, and empty Polars/Arrow exports."""
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    try:
        con.execute("LOAD duckpgq;")
        con.execute("SET active_grammar_extensions = ['duckpgq'];")
    except Exception as exc:
        con.close()
        pytest.skip(f"duckpgq extension not available: {exc}")

    graph_name = f"zz_col_duck_{uuid.uuid4().hex[:8]}"
    try:
        con.execute(
            f"""
            CREATE TABLE zz_col_person (id BIGINT PRIMARY KEY, name VARCHAR, age BIGINT);
            INSERT INTO zz_col_person VALUES (1, 'Alice', 30);
            CREATE PROPERTY GRAPH {graph_name}
                VERTEX TABLES (zz_col_person LABEL "Person");
            """
        )
        session = Session(bridge=con, dialect="duckpgq")
        _assert_result_columns_workflow(
            session,
            non_empty_stmt=(
                f"SELECT * FROM GRAPH_TABLE ({graph_name} "
                'MATCH (p IS "Person") COLUMNS (p.name AS person_name, p.age AS person_age))'
            ),
            empty_stmt=(
                f"SELECT * FROM GRAPH_TABLE ({graph_name} "
                "MATCH (p IS \"Person\") WHERE p.name = 'nobody' "
                "COLUMNS (p.name AS person_name, p.age AS person_age))"
            ),
            write_stmt=None,
            expected_cols=("person_name", "person_age"),
        )
    finally:
        con.close()
