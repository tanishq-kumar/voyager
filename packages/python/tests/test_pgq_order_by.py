"""Regression tests for SQL/PGQ ORDER BY expressions and non-returned keys (#319, #320).

#319: ORDER BY on an expression evaluated where pattern variables were out of scope outside GRAPH_TABLE.
#320: ORDER BY on a non-returned property named a column missing from GRAPH_TABLE.

Both now expose non-returned order keys inside GRAPH_TABLE COLUMNS under an internal alias (_ord_N),
order by that alias, and select only the returned columns in the outer query.
"""

from __future__ import annotations

from typing import Any

import pytest
from conftest import get_test_host
from conftest import is_port_open as _is_port_open
from voyager_ogm import (
    Field,
    Node,
    Query,
    VoyagerError,
    compile_query_from_spec,
    node,
    reset_alias_counters,
)
from voyager_ogm.bridge import DuckDbBridge, PostgresBridge

psycopg: Any = None
try:
    import psycopg
except ImportError:
    pass

duckdb: Any = None
try:
    import duckdb
except ImportError:
    pass

neo4j: Any = None
try:
    import neo4j
except ImportError:
    pass

HOST = get_test_host()
POSTGRES19_URI = f"postgresql://postgres:voyagerpass123@{HOST}:5456/voyager_graph"
NEO4J_URI = f"bolt://{HOST}:7687"
GRAPH = "fix319_320_graph"
PERSON = "fix319_320_person"
LABEL = "Fix319320Person"

ROWS = [
    (1, "Alice", 30, "Paris"),
    (2, "Bob", 25, "London"),
    (3, "Charlie", 25, "Berlin"),
    (4, "David", None, "Tokyo"),
    (5, "Eve", 40, None),
]


@node(label=LABEL)
class Person(Node):
    id = Field()
    name = Field()
    age = Field()
    city = Field()


@pytest.fixture(autouse=True)
def _reset():
    reset_alias_counters()


def _has_duckpgq() -> bool:
    if duckdb is None:
        return False
    try:
        connection = duckdb.connect(":memory:")
        connection.execute("LOAD duckpgq;")
        connection.close()
        return True
    except Exception:
        return False


HAS_DUCKPGQ = _has_duckpgq()


# ===========================================================================
# 1. Compilation tests (sql_pgq and duckpgq)
# ===========================================================================


@pytest.mark.parametrize("dialect", ["sql_pgq", "duckpgq"])
def test_order_by_expression_compiles_inside_columns(dialect: str) -> None:
    p = Person(alias="p")
    compiled = (
        Query.match(p).return_(p.name, p.age).order_by(p.age + 1).compile(dialect, graph_name=GRAPH)
    )
    expected = (
        f'SELECT "name", "age" FROM GRAPH_TABLE ({GRAPH} MATCH (p IS "{LABEL}") '
        'COLUMNS (p.name, p.age, p.age + $p0 AS "_ord_0")) ORDER BY "_ord_0" ASC NULLS LAST'
    )
    assert compiled.statement == expected
    assert compiled.parameters == {"p0": 1}


@pytest.mark.parametrize("dialect", ["sql_pgq", "duckpgq"])
def test_order_by_non_returned_property_compiles_inside_columns(dialect: str) -> None:
    p = Person(alias="p")
    compiled = Query.match(p).return_(p.name).order_by(p.age).compile(dialect, graph_name=GRAPH)
    expected = (
        f'SELECT "name" FROM GRAPH_TABLE ({GRAPH} MATCH (p IS "{LABEL}") '
        'COLUMNS (p.name, p.age AS "_ord_0")) ORDER BY "_ord_0" ASC NULLS LAST'
    )
    assert compiled.statement == expected
    assert compiled.parameters == {}


@pytest.mark.parametrize("dialect", ["sql_pgq", "duckpgq"])
def test_order_by_returned_property_stays_select_star(dialect: str) -> None:
    p = Person(alias="p")
    compiled = (
        Query.match(p).return_(p.name, p.age).order_by(p.age).compile(dialect, graph_name=GRAPH)
    )
    expected = (
        f'SELECT * FROM GRAPH_TABLE ({GRAPH} MATCH (p IS "{LABEL}") '
        'COLUMNS (p.name, p.age)) ORDER BY "age" ASC NULLS LAST'
    )
    assert compiled.statement == expected
    assert compiled.parameters == {}


@pytest.mark.parametrize("dialect", ["sql_pgq", "duckpgq"])
def test_order_by_multiple_mixed_keys_compiles(dialect: str) -> None:
    p = Person(alias="p")
    compiled = (
        Query.match(p)
        .return_(p.name, p.city)
        .order_by(p.age)
        .order_by(p.city, ascending=False)
        .order_by(p.age + 1)
        .compile(dialect, graph_name=GRAPH)
    )
    expected = (
        f'SELECT "name", "city" FROM GRAPH_TABLE ({GRAPH} MATCH (p IS "{LABEL}") '
        'COLUMNS (p.name, p.city, p.age AS "_ord_0", p.age + $p0 AS "_ord_1")) '
        'ORDER BY "_ord_0" ASC NULLS LAST, "city" DESC NULLS FIRST, "_ord_1" ASC NULLS LAST'
    )
    assert compiled.statement == expected
    assert compiled.parameters == {"p0": 1}


@pytest.mark.parametrize("dialect", ["sql_pgq", "duckpgq"])
def test_order_by_duplicate_key_reuses_alias(dialect: str) -> None:
    p = Person(alias="p")
    compiled = (
        Query.match(p)
        .return_(p.name)
        .order_by(p.age)
        .order_by(p.age, ascending=False)
        .compile(dialect, graph_name=GRAPH)
    )
    expected = (
        f'SELECT "name" FROM GRAPH_TABLE ({GRAPH} MATCH (p IS "{LABEL}") '
        'COLUMNS (p.name, p.age AS "_ord_0")) ORDER BY "_ord_0" ASC NULLS LAST, "_ord_0" DESC NULLS FIRST'
    )
    assert compiled.statement == expected


@pytest.mark.parametrize("dialect", ["sql_pgq", "duckpgq"])
def test_order_by_distinct_with_non_returned_key_rejected(dialect: str) -> None:
    p = Person(alias="p")
    # The fluent scope check (#287) rejects this before the emitter where it is
    # present; without it the SQL/PGQ emitter raises UnsupportedFeature.
    with pytest.raises(
        ValueError,
        match="ORDER BY on non-projected expressions with DISTINCT"
        "|a RETURN with DISTINCT or an aggregation only exposes the names it projects",
    ):
        (
            Query.match(p)
            .distinct()
            .return_(p.name)
            .order_by(p.age)
            .compile(dialect, graph_name=GRAPH)
        )


@pytest.mark.parametrize("dialect", ["sql_pgq", "duckpgq"])
def test_order_by_aggregation_with_non_returned_key_rejected(dialect: str) -> None:
    spec = {
        "spec_version": 1,
        "matches": [
            {
                "paths": [[["node", "p", [LABEL]]]],
            }
        ],
        "projections": [
            ["agg", "p", "*", "count", "c"],
        ],
        "order_by": [["p", "age", True]],
    }
    with pytest.raises(VoyagerError) as exc_info:
        compile_query_from_spec(spec, dialect, graph_name=GRAPH)
    assert "only exposes the names it projects" in str(exc_info.value)


# ===========================================================================
# 2. DuckPGQ Execution Tests (Embedded DuckDB, non-live)
# ===========================================================================


@pytest.fixture
def duckpgq_bridge():
    if not HAS_DUCKPGQ:
        pytest.skip("DuckPGQ extension is not available")
    connection = duckdb.connect()
    connection.execute("LOAD duckpgq;")
    connection.execute("SET active_grammar_extensions = ['duckpgq'];")
    connection.execute(
        f"CREATE TABLE {PERSON} (id INT PRIMARY KEY, name TEXT, age INT, city TEXT);"
    )
    connection.executemany(f"INSERT INTO {PERSON} VALUES (?, ?, ?, ?);", ROWS)
    connection.execute(f'CREATE PROPERTY GRAPH {GRAPH} VERTEX TABLES ({PERSON} LABEL "{LABEL}");')
    yield DuckDbBridge(connection)
    connection.close()


@pytest.mark.skipif(not HAS_DUCKPGQ, reason="DuckPGQ extension is not available")
def test_duckpgq_order_by_expression(duckpgq_bridge) -> None:
    p = Person(alias="p")
    # ASC: 25 (Bob, Charlie), 30 (Alice), 40 (Eve), None (David)
    compiled_asc = (
        Query.match(p)
        .return_(p.name, p.age)
        .order_by(p.age + 1)
        .order_by(p.name)
        .compile("duckpgq", graph_name=GRAPH)
    )
    rows_asc = duckpgq_bridge.execute(compiled_asc.statement, compiled_asc.parameters)
    # Check result columns are strictly returned ones
    assert list(rows_asc[0].keys()) == ["name", "age"]
    assert [r["name"] for r in rows_asc] == ["Bob", "Charlie", "Alice", "Eve", "David"]
    assert [r["age"] for r in rows_asc] == [25, 25, 30, 40, None]

    # DESC: DuckDB with NULLS FIRST orders NULLs first: None (David), 40 (Eve), 30 (Alice), 25 (Bob, Charlie)
    compiled_desc = (
        Query.match(p)
        .return_(p.name, p.age)
        .order_by(p.age + 1, ascending=False)
        .order_by(p.name)
        .compile("duckpgq", graph_name=GRAPH)
    )
    rows_desc = duckpgq_bridge.execute(compiled_desc.statement, compiled_desc.parameters)
    assert list(rows_desc[0].keys()) == ["name", "age"]
    assert [r["name"] for r in rows_desc] == ["David", "Eve", "Alice", "Bob", "Charlie"]
    assert [r["age"] for r in rows_desc] == [None, 40, 30, 25, 25]


@pytest.mark.skipif(not HAS_DUCKPGQ, reason="DuckPGQ extension is not available")
def test_duckpgq_order_by_non_returned_property(duckpgq_bridge) -> None:
    p = Person(alias="p")
    # ASC: 25 (Bob, Charlie), 30 (Alice), 40 (Eve), None (David)
    compiled_asc = (
        Query.match(p)
        .return_(p.name)
        .order_by(p.age)
        .order_by(p.name)
        .compile("duckpgq", graph_name=GRAPH)
    )
    rows_asc = duckpgq_bridge.execute(compiled_asc.statement, compiled_asc.parameters)
    assert list(rows_asc[0].keys()) == ["name"]
    assert [r["name"] for r in rows_asc] == ["Bob", "Charlie", "Alice", "Eve", "David"]

    # DESC: DuckDB with NULLS FIRST orders NULLs first
    compiled_desc = (
        Query.match(p)
        .return_(p.name)
        .order_by(p.age, ascending=False)
        .order_by(p.name)
        .compile("duckpgq", graph_name=GRAPH)
    )
    rows_desc = duckpgq_bridge.execute(compiled_desc.statement, compiled_desc.parameters)
    assert list(rows_desc[0].keys()) == ["name"]
    assert [r["name"] for r in rows_desc] == ["David", "Eve", "Alice", "Bob", "Charlie"]


@pytest.mark.skipif(not HAS_DUCKPGQ, reason="DuckPGQ extension is not available")
def test_duckpgq_order_by_multiple_mixed_keys_and_pagination(duckpgq_bridge) -> None:
    p = Person(alias="p")
    compiled = (
        Query.match(p)
        .return_(p.name, p.city)
        .order_by(p.age)
        .order_by(p.city, ascending=False)
        .skip(1)
        .limit(3)
        .compile("duckpgq", graph_name=GRAPH)
    )
    rows = duckpgq_bridge.execute(compiled.statement, compiled.parameters)
    assert list(rows[0].keys()) == ["name", "city"]
    # All rows by (age ASC, city DESC):
    # Bob (25, London), Charlie (25, Berlin), Alice (30, Paris), Eve (40, None), David (None, Tokyo)
    # Skip 1, limit 3: Charlie, Alice, Eve
    assert [r["name"] for r in rows] == ["Charlie", "Alice", "Eve"]


# ===========================================================================
# 3. Live PostgreSQL 19 vs Neo4j Parity Tests (@pytest.mark.live)
# ===========================================================================


@pytest.mark.live
class TestLivePostgres19AndNeo4jParity:
    """Live verification of SQL/PGQ #319 and #320 against PostgreSQL 19 Beta 3 and Neo4j."""

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self):
        if psycopg is None:
            pytest.skip("psycopg is not installed")
        if neo4j is None:
            pytest.skip("neo4j is not installed")
        if not _is_port_open(HOST, 5456):
            pytest.skip(f"PostgreSQL 19 not reachable on {HOST}:5456")
        if not _is_port_open(HOST, 7687):
            pytest.skip(f"Neo4j not reachable on {HOST}:7687")

        # PG19 connection
        conn_str = (
            f"host={HOST} port=5456 user=postgres password=voyagerpass123 dbname=voyager_graph"
        )
        try:
            pg_conn = psycopg.connect(conn_str, autocommit=True)
        except Exception as exc:
            pytest.skip(f"Failed to connect to PG19: {exc}")

        # Neo4j driver
        try:
            neo_driver = neo4j.GraphDatabase.driver(NEO4J_URI, auth=("neo4j", "voyagerpass123"))
        except Exception as exc:
            pg_conn.close()
            pytest.skip(f"Failed to connect to Neo4j: {exc}")

        # Setup PG19
        with pg_conn.cursor() as cur:
            cur.execute(f"DROP PROPERTY GRAPH IF EXISTS {GRAPH} CASCADE;")
            cur.execute(f"DROP TABLE IF EXISTS {PERSON} CASCADE;")
            cur.execute(
                f"CREATE TABLE {PERSON} (id INT PRIMARY KEY, name TEXT, age INT, city TEXT);"
            )
            cur.executemany(f"INSERT INTO {PERSON} VALUES (%s, %s, %s, %s);", ROWS)
            cur.execute(f'CREATE PROPERTY GRAPH {GRAPH} VERTEX TABLES ({PERSON} LABEL "{LABEL}");')

        # Setup Neo4j
        with neo_driver.session() as s:
            s.run(f"MATCH (n:{LABEL}) DETACH DELETE n;")
            for _, name, age, city in ROWS:
                props = {}
                if name is not None:
                    props["name"] = name
                if age is not None:
                    props["age"] = age
                if city is not None:
                    props["city"] = city
                s.run(f"CREATE (:{LABEL} $props)", props=props)

        yield pg_conn, neo_driver

        # Cleanup PG19
        with pg_conn.cursor() as cur:
            cur.execute(f"DROP PROPERTY GRAPH IF EXISTS {GRAPH} CASCADE;")
            cur.execute(f"DROP TABLE IF EXISTS {PERSON} CASCADE;")
        pg_conn.close()

        # Cleanup Neo4j
        with neo_driver.session() as s:
            s.run(f"MATCH (n:{LABEL}) DETACH DELETE n;")
        neo_driver.close()

    def test_live_319_order_by_expression_parity(self, setup_and_teardown) -> None:
        pg_conn, neo_driver = setup_and_teardown
        bridge = PostgresBridge(pg_conn)
        p = Person(alias="p")

        # ASC
        compiled = (
            Query.match(p)
            .return_(p.name, p.age)
            .order_by(p.age + 1)
            .order_by(p.name)
            .compile("sql_pgq", graph_name=GRAPH)
        )
        pg_rows = bridge.execute(compiled.statement, compiled.parameters)
        assert list(pg_rows[0].keys()) == ["name", "age"]

        with neo_driver.session() as s:
            neo_res = s.run(
                f"MATCH (p:{LABEL}) RETURN p.name AS name, p.age AS age "
                "ORDER BY p.age + 1 ASC, p.name ASC"
            ).data()

        assert pg_rows == neo_res

        # DESC: PG19 and Neo4j both default to NULLS FIRST in DESC
        compiled_desc = (
            Query.match(p)
            .return_(p.name, p.age)
            .order_by(p.age + 1, ascending=False)
            .order_by(p.name)
            .compile("sql_pgq", graph_name=GRAPH)
        )
        pg_rows_desc = bridge.execute(compiled_desc.statement, compiled_desc.parameters)
        assert list(pg_rows_desc[0].keys()) == ["name", "age"]

        with neo_driver.session() as s:
            neo_res_desc = s.run(
                f"MATCH (p:{LABEL}) RETURN p.name AS name, p.age AS age "
                "ORDER BY p.age + 1 DESC, p.name ASC"
            ).data()

        assert pg_rows_desc == neo_res_desc

    def test_live_320_order_by_non_returned_property_parity(self, setup_and_teardown) -> None:
        pg_conn, neo_driver = setup_and_teardown
        bridge = PostgresBridge(pg_conn)
        p = Person(alias="p")

        # ASC
        compiled = (
            Query.match(p)
            .return_(p.name)
            .order_by(p.age)
            .order_by(p.name)
            .compile("sql_pgq", graph_name=GRAPH)
        )
        pg_rows = bridge.execute(compiled.statement, compiled.parameters)
        assert list(pg_rows[0].keys()) == ["name"]

        with neo_driver.session() as s:
            neo_res = s.run(
                f"MATCH (p:{LABEL}) RETURN p.name AS name ORDER BY p.age ASC, p.name ASC"
            ).data()

        assert pg_rows == neo_res

        # DESC
        compiled_desc = (
            Query.match(p)
            .return_(p.name)
            .order_by(p.age, ascending=False)
            .order_by(p.name)
            .compile("sql_pgq", graph_name=GRAPH)
        )
        pg_rows_desc = bridge.execute(compiled_desc.statement, compiled_desc.parameters)
        assert list(pg_rows_desc[0].keys()) == ["name"]

        with neo_driver.session() as s:
            neo_res_desc = s.run(
                f"MATCH (p:{LABEL}) RETURN p.name AS name ORDER BY p.age DESC, p.name ASC"
            ).data()

        assert pg_rows_desc == neo_res_desc

    def test_live_multiple_keys_and_skip_limit_parity(self, setup_and_teardown) -> None:
        pg_conn, neo_driver = setup_and_teardown
        bridge = PostgresBridge(pg_conn)
        p = Person(alias="p")

        compiled = (
            Query.match(p)
            .return_(p.name, p.city)
            .order_by(p.age)
            .order_by(p.city, ascending=False)
            .skip(1)
            .limit(3)
            .compile("sql_pgq", graph_name=GRAPH)
        )
        pg_rows = bridge.execute(compiled.statement, compiled.parameters)
        assert list(pg_rows[0].keys()) == ["name", "city"]

        with neo_driver.session() as s:
            neo_res = s.run(
                f"MATCH (p:{LABEL}) RETURN p.name AS name, p.city AS city "
                "ORDER BY p.age ASC, p.city DESC SKIP 1 LIMIT 3"
            ).data()

        assert pg_rows == neo_res

    @pytest.mark.skipif(not HAS_DUCKPGQ, reason="DuckPGQ extension is not available")
    def test_live_322_null_ordering_parity_neo4j_pg19_duckpgq(self, setup_and_teardown) -> None:
        """Issue #322: ORDER BY on a key with nulls matches Cypher across Neo4j, PG19, and DuckPGQ."""
        pg_conn, neo_driver = setup_and_teardown
        pg_bridge = PostgresBridge(pg_conn)

        duck_conn = duckdb.connect()
        duck_conn.execute("LOAD duckpgq;")
        duck_conn.execute("SET active_grammar_extensions = ['duckpgq'];")
        duck_conn.execute(
            f"CREATE TABLE {PERSON} (id INT PRIMARY KEY, name TEXT, age INT, city TEXT);"
        )
        duck_conn.executemany(f"INSERT INTO {PERSON} VALUES (?, ?, ?, ?);", ROWS)
        duck_conn.execute(
            f'CREATE PROPERTY GRAPH {GRAPH} VERTEX TABLES ({PERSON} LABEL "{LABEL}");'
        )
        duck_bridge = DuckDbBridge(duck_conn)

        try:
            p = Person(alias="p")

            # 1. ASC without LIMIT (nulls last)
            q_asc = Query.match(p).return_(p.name, p.age).order_by(p.age).order_by(p.name)
            pgq_asc = q_asc.compile("sql_pgq", graph_name=GRAPH)
            duck_asc = q_asc.compile("duckpgq", graph_name=GRAPH)

            pg_res_asc = pg_bridge.execute(pgq_asc.statement, pgq_asc.parameters)
            duck_res_asc = duck_bridge.execute(duck_asc.statement, duck_asc.parameters)
            with neo_driver.session() as s:
                neo_res_asc = s.run(
                    f"MATCH (p:{LABEL}) RETURN p.name AS name, p.age AS age ORDER BY p.age ASC, p.name ASC"
                ).data()

            assert pg_res_asc == neo_res_asc
            assert duck_res_asc == neo_res_asc
            assert [r["name"] for r in duck_res_asc] == ["Bob", "Charlie", "Alice", "Eve", "David"]

            # 2. ASC with LIMIT (nulls last)
            q_asc_lim = (
                Query.match(p).return_(p.name, p.age).order_by(p.age).order_by(p.name).limit(3)
            )
            pgq_asc_lim = q_asc_lim.compile("sql_pgq", graph_name=GRAPH)
            duck_asc_lim = q_asc_lim.compile("duckpgq", graph_name=GRAPH)

            pg_res_asc_lim = pg_bridge.execute(pgq_asc_lim.statement, pgq_asc_lim.parameters)
            duck_res_asc_lim = duck_bridge.execute(duck_asc_lim.statement, duck_asc_lim.parameters)
            with neo_driver.session() as s:
                neo_res_asc_lim = s.run(
                    f"MATCH (p:{LABEL}) RETURN p.name AS name, p.age AS age ORDER BY p.age ASC, p.name ASC LIMIT 3"
                ).data()

            assert pg_res_asc_lim == neo_res_asc_lim
            assert duck_res_asc_lim == neo_res_asc_lim
            assert [r["name"] for r in duck_res_asc_lim] == ["Bob", "Charlie", "Alice"]

            # 3. DESC without LIMIT (nulls first)
            q_desc = (
                Query.match(p)
                .return_(p.name, p.age)
                .order_by(p.age, ascending=False)
                .order_by(p.name)
            )
            pgq_desc = q_desc.compile("sql_pgq", graph_name=GRAPH)
            duck_desc = q_desc.compile("duckpgq", graph_name=GRAPH)

            pg_res_desc = pg_bridge.execute(pgq_desc.statement, pgq_desc.parameters)
            duck_res_desc = duck_bridge.execute(duck_desc.statement, duck_desc.parameters)
            with neo_driver.session() as s:
                neo_res_desc = s.run(
                    f"MATCH (p:{LABEL}) RETURN p.name AS name, p.age AS age ORDER BY p.age DESC, p.name ASC"
                ).data()

            assert pg_res_desc == neo_res_desc
            assert duck_res_desc == neo_res_desc
            assert [r["name"] for r in duck_res_desc] == ["David", "Eve", "Alice", "Bob", "Charlie"]

            # 4. DESC with LIMIT (nulls first)
            q_desc_lim = (
                Query.match(p)
                .return_(p.name, p.age)
                .order_by(p.age, ascending=False)
                .order_by(p.name)
                .limit(3)
            )
            pgq_desc_lim = q_desc_lim.compile("sql_pgq", graph_name=GRAPH)
            duck_desc_lim = q_desc_lim.compile("duckpgq", graph_name=GRAPH)

            pg_res_desc_lim = pg_bridge.execute(pgq_desc_lim.statement, pgq_desc_lim.parameters)
            duck_res_desc_lim = duck_bridge.execute(
                duck_desc_lim.statement, duck_desc_lim.parameters
            )
            with neo_driver.session() as s:
                neo_res_desc_lim = s.run(
                    f"MATCH (p:{LABEL}) RETURN p.name AS name, p.age AS age ORDER BY p.age DESC, p.name ASC"
                    " LIMIT 3"
                ).data()

            assert pg_res_desc_lim == neo_res_desc_lim
            assert duck_res_desc_lim == neo_res_desc_lim
            assert [r["name"] for r in duck_res_desc_lim] == ["David", "Eve", "Alice"]
        finally:
            duck_conn.close()
