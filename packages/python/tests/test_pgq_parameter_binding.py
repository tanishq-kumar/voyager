"""Tests for SQL/PGQ GRAPH_TABLE parameter binding and literal encoding (Issue #226).

Verifies:
1. PostgreSQL bridge: binds GRAPH_TABLE parameters positionally ($name -> %s)
   without string interpolation, supporting string, int, float, boolean, null,
   and list (array) parameters, duplicate parameter names, and hostile injection values.
2. DuckDB / DuckPGQ bridge: documents that DuckPGQ does not support parameter
   binding (?/$1/$param fail with parameter count mismatch and PREPARE segfaults),
   and safely encodes all supported Python types (str with quote-doubling, int, float
   incl. nan/inf, bool, None, date/datetime/time, list, dict/map) as SQL literals,
   raising TypeError for unsupported types.
3. Live execution parity on PostgreSQL 19 and DuckPGQ.
"""

from __future__ import annotations

import datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest
from conftest import is_port_open as _is_port_open
from voyager_ogm.bridge import (
    AsyncPostgresBridge,
    DuckDbBridge,
    PostgresBridge,
    _encode_duckdb_literal,
)
from voyager_ogm.session import AsyncSession, Session

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


# ===========================================================================
# 1. Unit Tests (Offline / Not Live): DuckDB Literal Encoding
# ===========================================================================


def test_duckdb_literal_encoding_scalars():
    """Verifies scalar encoding for DuckDB SQL literals."""
    assert _encode_duckdb_literal(None) == "NULL"
    assert _encode_duckdb_literal(True) == "TRUE"
    assert _encode_duckdb_literal(False) == "FALSE"
    assert _encode_duckdb_literal(0) == "0"
    assert _encode_duckdb_literal(42) == "42"
    assert _encode_duckdb_literal(-100) == "-100"
    assert _encode_duckdb_literal(1.25) == "1.25"
    assert _encode_duckdb_literal(0.0) == "0.0"


def test_duckdb_literal_encoding_floats_nan_inf():
    """Verifies special float values (nan, inf, -inf) encode to 'nan'::DOUBLE / 'inf'::DOUBLE."""
    assert _encode_duckdb_literal(float("nan")) == "'nan'::DOUBLE"
    assert _encode_duckdb_literal(float("inf")) == "'inf'::DOUBLE"
    assert _encode_duckdb_literal(float("-inf")) == "'-inf'::DOUBLE"

    # Decimal handling
    assert _encode_duckdb_literal(Decimal("42.50")) == "42.50"
    assert _encode_duckdb_literal(Decimal("NaN")) == "'nan'::DOUBLE"
    assert _encode_duckdb_literal(Decimal("Infinity")) == "'inf'::DOUBLE"
    assert _encode_duckdb_literal(Decimal("-Infinity")) == "'-inf'::DOUBLE"


def test_duckdb_literal_encoding_strings_and_hostile_input():
    """Verifies string quoting and SQL injection prevention via quote-doubling."""
    assert _encode_duckdb_literal("hello") == "'hello'"
    assert _encode_duckdb_literal("O'Connor") == "'O''Connor'"
    assert _encode_duckdb_literal("") == "''"

    # Hostile input: quote-closing followed by OR / comments
    hostile = "x') OR 1=1 --"
    encoded = _encode_duckdb_literal(hostile)
    assert encoded == "'x'') OR 1=1 --'"


def test_duckdb_literal_encoding_temporal():
    """Verifies date, datetime (naive and timezone-aware), and time literal encodings."""
    d = datetime.date(2026, 11, 21)
    assert _encode_duckdb_literal(d) == "DATE '2026-11-21'"

    dt_naive = datetime.datetime(2026, 11, 21, 14, 30, 45)
    assert _encode_duckdb_literal(dt_naive) == "TIMESTAMP '2026-11-21 14:30:45'"

    dt_tz = datetime.datetime(2026, 11, 21, 14, 30, 45, tzinfo=datetime.UTC)
    assert _encode_duckdb_literal(dt_tz) == "TIMESTAMPTZ '2026-11-21 14:30:45+00:00'"

    t = datetime.time(14, 30, 45)
    assert _encode_duckdb_literal(t) == "TIME '14:30:45'"


def test_duckdb_literal_encoding_collections():
    """Verifies list, tuple, and dict/map literal encodings including nesting."""
    assert _encode_duckdb_literal([]) == "[]"
    assert _encode_duckdb_literal(()) == "[]"
    assert _encode_duckdb_literal([1, 2, 3]) == "[1, 2, 3]"
    assert _encode_duckdb_literal(["a", "b"]) == "['a', 'b']"
    assert _encode_duckdb_literal([True, None, "c"]) == "[TRUE, NULL, 'c']"
    assert _encode_duckdb_literal([[1, 2], [3, 4]]) == "[[1, 2], [3, 4]]"

    # Map / Dict
    assert _encode_duckdb_literal({}) == "MAP {}"
    assert _encode_duckdb_literal({"name": "Alice"}) == "MAP {'name': 'Alice'}"
    assert _encode_duckdb_literal({1: "a", 2: "b"}) == "MAP {1: 'a', 2: 'b'}"
    assert _encode_duckdb_literal({"tags": ["admin", "staff"]}) == (
        "MAP {'tags': ['admin', 'staff']}"
    )


def test_duckdb_literal_encoding_unsupported_type_raises():
    """Verifies that unsupported parameter types raise TypeError."""

    class CustomObject:
        pass

    with pytest.raises(TypeError, match="Unsupported parameter type CustomObject"):
        _encode_duckdb_literal(CustomObject())

    with pytest.raises(TypeError, match="Unsupported parameter type object"):
        _encode_duckdb_literal(object())


def test_duckdb_bridge_format_pgq_statement():
    """Verifies DuckDbBridge._format_pgq_statement interpolates safely for GRAPH_TABLE queries."""
    bridge = DuckDbBridge(MagicMock())

    # GRAPH_TABLE with parameters
    stmt = "SELECT * FROM GRAPH_TABLE (g MATCH (p) WHERE p.name = $name AND p.age > $age COLUMNS (p.id))"
    params = {"name": "Alice", "age": 30}
    formatted, remaining = bridge._format_pgq_statement(stmt, params)
    assert formatted == (
        "SELECT * FROM GRAPH_TABLE (g MATCH (p) WHERE p.name = 'Alice' AND p.age > 30 COLUMNS (p.id))"
    )
    assert remaining == {}

    # Hostile string interpolation stays safely quoted
    hostile = "x') OR 1=1 --"
    formatted_h, remaining_h = bridge._format_pgq_statement(
        "SELECT * FROM GRAPH_TABLE (g MATCH (p) WHERE p.name = $p COLUMNS (p.id))",
        {"p": hostile},
    )
    assert formatted_h == (
        "SELECT * FROM GRAPH_TABLE (g MATCH (p) WHERE p.name = 'x'') OR 1=1 --' COLUMNS (p.id))"
    )
    assert remaining_h == {}

    # Plain SQL (non GRAPH_TABLE) leaves parameters untouched for driver binding
    plain_stmt = "SELECT * FROM person WHERE name = $name"
    plain_formatted, plain_params = bridge._format_pgq_statement(plain_stmt, {"name": "Alice"})
    assert plain_formatted == plain_stmt
    assert plain_params == {"name": "Alice"}


# ===========================================================================
# 2. Unit Tests (Offline / Not Live): PostgresBridge Parameter Binding
# ===========================================================================


def test_postgres_bridge_format_pgq_statement_binding():
    """Verifies PostgresBridge._format_pgq_statement binds GRAPH_TABLE parameters positionally (%s)."""
    bridge = PostgresBridge(MagicMock())

    # GRAPH_TABLE query with multiple parameters
    stmt = (
        "SELECT * FROM GRAPH_TABLE (g MATCH (p IS Person) "
        "WHERE p.name = $name AND p.age >= $min_age AND p.score >= $min_score COLUMNS (p.id))"
    )
    params = {"name": "Alice", "min_age": 21, "min_score": 90.5}
    formatted, ordered = bridge._format_pgq_statement(stmt, params)

    assert formatted == (
        "SELECT * FROM GRAPH_TABLE (g MATCH (p IS Person) "
        "WHERE p.name = %s AND p.age >= %s AND p.score >= %s COLUMNS (p.id))"
    )
    assert ordered == ["Alice", 21, 90.5]


def test_postgres_bridge_format_pgq_statement_duplicate_parameter():
    """Verifies repeated parameter names produce corresponding %s placeholders and repeated values."""
    bridge = PostgresBridge(MagicMock())

    stmt = (
        "SELECT * FROM GRAPH_TABLE (g MATCH (p IS Person) "
        "WHERE p.name = $target OR p.nickname = $target COLUMNS (p.id))"
    )
    params = {"target": "Alice"}
    formatted, ordered = bridge._format_pgq_statement(stmt, params)

    assert formatted == (
        "SELECT * FROM GRAPH_TABLE (g MATCH (p IS Person) "
        "WHERE p.name = %s OR p.nickname = %s COLUMNS (p.id))"
    )
    assert ordered == ["Alice", "Alice"]


def test_postgres_bridge_format_pgq_statement_hostile_value():
    """Verifies hostile injection strings are passed into ordered_params, not written into SQL text."""
    bridge = PostgresBridge(MagicMock())

    hostile = "x') OR 1=1 --"
    stmt = "SELECT * FROM GRAPH_TABLE (g MATCH (p) WHERE p.name = $name COLUMNS (p.id))"
    params = {"name": hostile}
    formatted, ordered = bridge._format_pgq_statement(stmt, params)

    assert formatted == "SELECT * FROM GRAPH_TABLE (g MATCH (p) WHERE p.name = %s COLUMNS (p.id))"
    assert ordered == [hostile]
    assert hostile not in formatted


def test_postgres_bridge_format_pgq_statement_empty_params():
    """Verifies empty parameters returns the statement unchanged with empty tuple."""
    bridge = PostgresBridge(MagicMock())
    stmt = "SELECT * FROM GRAPH_TABLE (g MATCH (p) COLUMNS (p.id))"
    formatted, ordered = bridge._format_pgq_statement(stmt, {})
    assert formatted == stmt
    assert ordered == ()


def test_postgres_bridge_format_pgq_statement_starts_with_contains_ends_with():
    """Verifies literal '%' in LIKE string predicates is escaped as '%%' when parameters are bound (#305)."""
    bridge = PostgresBridge(MagicMock())

    # 1. STARTS WITH: LIKE $p0 || '%'
    stmt_starts = "SELECT * FROM GRAPH_TABLE (g MATCH (p IS Person) WHERE p.name LIKE $p0 || '%' COLUMNS (p.id))"
    fmt_starts, ord_starts = bridge._format_pgq_statement(stmt_starts, {"p0": "Al"})
    assert fmt_starts == (
        "SELECT * FROM GRAPH_TABLE (g MATCH (p IS Person) WHERE p.name LIKE %s || '%%' COLUMNS (p.id))"
    )
    assert ord_starts == ["Al"]

    # 2. ENDS WITH: LIKE '%' || $p0
    stmt_ends = "SELECT * FROM GRAPH_TABLE (g MATCH (p IS Person) WHERE p.name LIKE '%' || $p0 COLUMNS (p.id))"
    fmt_ends, ord_ends = bridge._format_pgq_statement(stmt_ends, {"p0": "ce"})
    assert fmt_ends == (
        "SELECT * FROM GRAPH_TABLE (g MATCH (p IS Person) WHERE p.name LIKE '%%' || %s COLUMNS (p.id))"
    )
    assert ord_ends == ["ce"]

    # 3. CONTAINS: LIKE '%' || $p0 || '%'
    stmt_contains = (
        "SELECT * FROM GRAPH_TABLE (g MATCH (p IS Person) "
        "WHERE p.city LIKE '%' || $p0 || '%' COLUMNS (p.id))"
    )
    fmt_contains, ord_contains = bridge._format_pgq_statement(stmt_contains, {"p0": "York"})
    assert fmt_contains == (
        "SELECT * FROM GRAPH_TABLE (g MATCH (p IS Person) "
        "WHERE p.city LIKE '%%' || %s || '%%' COLUMNS (p.id))"
    )
    assert ord_contains == ["York"]


def test_postgres_bridge_format_pgq_statement_percent_in_literals_and_identifiers():
    """Verifies literal '%' in string literals, identifiers, and format strings are properly escaped (#305)."""
    bridge = PostgresBridge(MagicMock())

    # % inside string literal with parameters
    stmt_lit = (
        "SELECT * FROM GRAPH_TABLE (g MATCH (p) "
        "WHERE p.name = '100%' AND p.age > $min_age COLUMNS (p.id))"
    )
    fmt_lit, ord_lit = bridge._format_pgq_statement(stmt_lit, {"min_age": 18})
    assert fmt_lit == (
        "SELECT * FROM GRAPH_TABLE (g MATCH (p) "
        "WHERE p.name = '100%%' AND p.age > %s COLUMNS (p.id))"
    )
    assert ord_lit == [18]

    # % inside identifier with parameters
    stmt_ident = (
        'SELECT * FROM GRAPH_TABLE (g MATCH (p) WHERE p."pct%col" = $target COLUMNS (p.id))'
    )
    fmt_ident, ord_ident = bridge._format_pgq_statement(stmt_ident, {"target": "foo"})
    assert fmt_ident == (
        'SELECT * FROM GRAPH_TABLE (g MATCH (p) WHERE p."pct%%col" = %s COLUMNS (p.id))'
    )
    assert ord_ident == ["foo"]

    # '%s' string literal with parameters does not collide with placeholder conversion
    stmt_s = (
        "SELECT * FROM GRAPH_TABLE (g MATCH (p) "
        "WHERE p.format = '%s' AND p.val = $val COLUMNS (p.id))"
    )
    fmt_s, ord_s = bridge._format_pgq_statement(stmt_s, {"val": 42})
    assert fmt_s == (
        "SELECT * FROM GRAPH_TABLE (g MATCH (p) "
        "WHERE p.format = '%%s' AND p.val = %s COLUMNS (p.id))"
    )
    assert ord_s == [42]


def test_postgres_bridge_format_pgq_statement_percent_without_parameters():
    """Verifies statements containing '%' without bound parameters leave '%' untouched (#305)."""
    bridge = PostgresBridge(MagicMock())

    # Raw statement with % and empty parameters dict
    stmt = "SELECT * FROM GRAPH_TABLE (g MATCH (p) WHERE p.name LIKE 'Al%' COLUMNS (p.id))"
    fmt, ordered = bridge._format_pgq_statement(stmt, {})
    assert fmt == stmt
    assert ordered == ()

    # Statement with parameters dict that has no matching keys in the statement
    fmt_unmatched, ord_unmatched = bridge._format_pgq_statement(stmt, {"unrelated": 123})
    assert fmt_unmatched == stmt
    assert ord_unmatched == ()


# ===========================================================================
# 3. Live DuckDB / DuckPGQ Tests
# ===========================================================================


@pytest.mark.live
def test_duckdb_duckpgq_graph_table_literal_encoding_live():
    """Verifies live DuckPGQ execution with safely encoded literals and documents why binding is unsupported.

    Evidence:
    DuckPGQ's table function parser does not extract parameter placeholders from
    inside GRAPH_TABLE(...). Passing positional or named parameters (? or $1 or $param)
    raises: InvalidInputException: Parameter argument/count mismatch, identifiers of
    the excess parameters: ...
    Additionally, attempting PREPARE with GRAPH_TABLE causes DuckPGQ to segfault (SIGSEGV).
    Therefore DuckDbBridge safely encodes all Python values as SQL literals.
    """
    if duckdb is None:
        pytest.skip("duckdb is not installed in this environment")

    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    try:
        con.execute("LOAD duckpgq;")
    except Exception:
        try:
            con.execute(
                "SET custom_extension_repository = 'http://duckpgq.s3.eu-north-1.amazonaws.com';"
            )
            con.execute("INSTALL duckpgq;")
            con.execute("LOAD duckpgq;")
        except Exception as exc:
            pytest.skip(f"duckpgq extension could not be loaded: {exc}")

    # Set up schema and property graph
    con.execute("DROP TABLE IF EXISTS duck_person CASCADE;")
    con.execute(
        """
        CREATE TABLE duck_person (
            id BIGINT PRIMARY KEY,
            name VARCHAR,
            age INT,
            score DOUBLE,
            active BOOLEAN,
            created DATE
        );
        INSERT INTO duck_person VALUES
            (1, 'Alice', 30, 95.5, true, DATE '2026-01-15'),
            (2, 'Bob', 25, 80.0, false, DATE '2026-02-20'),
            (3, 'x'') OR 1=1 --', 40, 50.0, true, DATE '2026-03-25');
        CREATE PROPERTY GRAPH duck_test_pg VERTEX TABLES (duck_person LABEL Person);
        """
    )

    session = Session(bridge=con, dialect="sql_pgq")

    # 1. Query with string, int, float, boolean, and date parameters
    query = """
    SELECT * FROM GRAPH_TABLE (duck_test_pg
        MATCH (p IS Person)
        WHERE p.name = $name AND p.age >= $age AND p.score >= $score AND p.active = $active AND p.created = $created
        COLUMNS (p.id, p.name)
    )
    """
    params = {
        "name": "Alice",
        "age": 20,
        "score": 90.0,
        "active": True,
        "created": datetime.date(2026, 1, 15),
    }
    rows = session.execute(query, params)
    assert len(rows) == 1
    assert rows[0]["id"] == 1
    assert rows[0]["name"] == "Alice"

    # 2. Hostile string injection attempt must be treated as exact data
    hostile = "x') OR 1=1 --"
    hostile_query = """
    SELECT * FROM GRAPH_TABLE (duck_test_pg
        MATCH (p IS Person)
        WHERE p.name = $name
        COLUMNS (p.id, p.name)
    )
    """
    rows_h = session.execute(hostile_query, {"name": hostile})
    assert len(rows_h) == 1
    assert rows_h[0]["id"] == 3
    assert rows_h[0]["name"] == hostile

    # 3. Hostile value not in DB returns 0 rows (does not bypass WHERE with 1=1)
    rows_none = session.execute(hostile_query, {"name": "nonexistent') OR 1=1 --"})
    assert len(rows_none) == 0


# ===========================================================================
# 4. Live PostgreSQL 19 Tests (Port 5456, Container postgres19)
# ===========================================================================


@pytest.mark.live
class TestPostgres19PgqParameterBindingLive:
    """Verifies PostgreSQL 19 GRAPH_TABLE positional parameter binding through PostgresBridge."""

    @pytest.fixture
    def pg19_conn(self):
        if psycopg is None:
            pytest.skip("psycopg is not installed in this environment")
        if not _is_port_open("127.0.0.1", 5456):
            pytest.skip("PostgreSQL 19 Beta container not available on port 5456")
        conn_str = (
            "host=127.0.0.1 port=5456 user=postgres password=voyagerpass123 "
            "dbname=voyager_graph connect_timeout=3"
        )
        try:
            conn = psycopg.connect(conn_str, autocommit=True)
        except Exception as e:
            pytest.skip(f"PostgreSQL 19 Beta container not available on port 5456: {e}")

        # Setup test schema and Property Graph
        with conn.cursor() as cur:
            cur.execute("DROP PROPERTY GRAPH IF EXISTS pg19_param_graph CASCADE;")
            cur.execute("DROP TABLE IF EXISTS pg19_param_person CASCADE;")
            cur.execute(
                """
                CREATE TABLE pg19_param_person (
                    id INT PRIMARY KEY,
                    name TEXT NOT NULL,
                    age INT NOT NULL,
                    score DOUBLE PRECISION NOT NULL,
                    active BOOLEAN NOT NULL,
                    tags TEXT[]
                );
                INSERT INTO pg19_param_person VALUES
                    (1, 'Alice', 30, 95.5, TRUE, ARRAY['admin', 'staff']),
                    (2, 'Bob', 25, 80.0, FALSE, ARRAY['user']),
                    (3, 'Charlie', 35, 70.0, TRUE, NULL),
                    (4, 'x'') OR 1=1 --', 40, 50.0, FALSE, ARRAY['test']);
                CREATE PROPERTY GRAPH pg19_param_graph
                    VERTEX TABLES (pg19_param_person LABEL Person);
                """
            )

        yield conn

        # Cleanup
        try:
            with conn.cursor() as cur:
                cur.execute("DROP PROPERTY GRAPH IF EXISTS pg19_param_graph CASCADE;")
                cur.execute("DROP TABLE IF EXISTS pg19_param_person CASCADE;")
            conn.close()
        except Exception:
            pass

    def test_pg19_bridge_bound_parameters_all_types(self, pg19_conn):
        """Verifies GRAPH_TABLE parameters with string, int, float, boolean, null, and list types."""
        bridge = PostgresBridge(pg19_conn)
        session = Session(bridge=bridge, dialect="sql_pgq")

        # 1. String, int, float, boolean, list binding
        query = """
        SELECT * FROM GRAPH_TABLE (pg19_param_graph
            MATCH (p IS Person)
            WHERE p.name = $name AND p.age >= $min_age AND p.score >= $min_score
                  AND p.active = $active AND p.tags = $tags
            COLUMNS (p.id, p.name)
        )
        """
        params = {
            "name": "Alice",
            "min_age": 20,
            "min_score": 90.0,
            "active": True,
            "tags": ["admin", "staff"],
        }
        rows = session.execute(query, params)
        assert len(rows) == 1
        assert rows[0] == {"id": 1, "name": "Alice"}

        # 2. Null parameter binding (IS NOT DISTINCT FROM)
        null_query = """
        SELECT * FROM GRAPH_TABLE (pg19_param_graph
            MATCH (p IS Person)
            WHERE p.tags IS NOT DISTINCT FROM $tags
            COLUMNS (p.id, p.name)
        )
        """
        rows_null = session.execute(null_query, {"tags": None})
        assert len(rows_null) == 1
        assert rows_null[0] == {"id": 3, "name": "Charlie"}

        # 3. Duplicate parameter name ($target used multiple times)
        dup_query = """
        SELECT * FROM GRAPH_TABLE (pg19_param_graph
            MATCH (p IS Person)
            WHERE p.name = $target OR p.name = $target
            COLUMNS (p.id, p.name)
        )
        """
        rows_dup = session.execute(dup_query, {"target": "Bob"})
        assert len(rows_dup) == 1
        assert rows_dup[0] == {"id": 2, "name": "Bob"}

    def test_pg19_bridge_hostile_string_stays_data(self, pg19_conn):
        """Verifies hostile injection strings are bound as literal values and cannot alter query AST."""
        bridge = PostgresBridge(pg19_conn)
        session = Session(bridge=bridge, dialect="sql_pgq")

        hostile = "x') OR 1=1 --"
        query = """
        SELECT * FROM GRAPH_TABLE (pg19_param_graph
            MATCH (p IS Person)
            WHERE p.name = $name
            COLUMNS (p.id, p.name)
        )
        """
        # When hostile string matches existing row 4, returns exactly row 4
        rows = session.execute(query, {"name": hostile})
        assert len(rows) == 1
        assert rows[0] == {"id": 4, "name": hostile}

        # When hostile string does not match any row, returns empty (does not return all rows via 1=1)
        rows_empty = session.execute(query, {"name": "not_in_table') OR 1=1 --"})
        assert len(rows_empty) == 0

    def test_pg19_bridge_and_native_parity(self, pg19_conn):
        """Verifies backend='bridge' and backend='native' return identical rows for scalar parameters."""
        pg_uri = "postgresql://postgres:voyagerpass123@127.0.0.1:5456/voyager_graph"
        bridge = PostgresBridge(pg19_conn)
        session_bridge = Session(bridge=bridge, dialect="sql_pgq")
        session_native = Session(pg_uri, backend="native", dialect="sql_pgq")

        test_cases: list[tuple[str, str, dict[str, Any]]] = [
            (
                "string",
                "SELECT * FROM GRAPH_TABLE (pg19_param_graph MATCH (p IS Person) WHERE p.name = $name COLUMNS (p.id, p.name))",
                {"name": "Alice"},
            ),
            (
                "int/float",
                "SELECT * FROM GRAPH_TABLE (pg19_param_graph MATCH (p IS Person) WHERE p.age = $age AND p.score >= $score COLUMNS (p.id, p.name))",
                {"age": 30, "score": 90.0},
            ),
            (
                "bool",
                "SELECT * FROM GRAPH_TABLE (pg19_param_graph MATCH (p IS Person) WHERE p.active = $active COLUMNS (p.id, p.name))",
                {"active": True},
            ),
            (
                "null",
                "SELECT * FROM GRAPH_TABLE (pg19_param_graph MATCH (p IS Person) WHERE p.tags IS NOT DISTINCT FROM $tags COLUMNS (p.id, p.name))",
                {"tags": None},
            ),
            (
                "hostile",
                "SELECT * FROM GRAPH_TABLE (pg19_param_graph MATCH (p IS Person) WHERE p.name = $name COLUMNS (p.id, p.name))",
                {"name": "x') OR 1=1 --"},
            ),
        ]

        for case_name, q, p in test_cases:
            res_b = sorted(session_bridge.execute(q, p), key=lambda x: x["id"])
            res_n = sorted(session_native.execute(q, p), key=lambda x: x["id"])
            assert res_b == res_n, (
                f"Parity mismatch in {case_name}: bridge={res_b!r} vs native={res_n!r}"
            )

    def test_pg19_bridge_and_native_list_parameter_parity(self, pg19_conn):
        """Verifies backend='bridge' and backend='native' return identical rows for list parameters (Issue #283).

        Tests plain SQL `= ANY(...)` and GRAPH_TABLE queries with:
        - strings list
        - ints list
        - empty list
        """
        pg_uri = "postgresql://postgres:voyagerpass123@127.0.0.1:5456/voyager_graph"
        bridge = PostgresBridge(pg19_conn)
        session_bridge = Session(bridge=bridge, dialect="sql_pgq")
        session_native = Session(pg_uri, backend="native", dialect="sql_pgq")

        test_cases: list[tuple[str, str, dict[str, Any], int]] = [
            # Plain SQL = ANY(...)
            (
                "plain SQL = ANY(...) strings",
                "SELECT id, name FROM pg19_param_person WHERE name = ANY($names) ORDER BY id",
                {"names": ["Alice", "Bob"]},
                2,
            ),
            (
                "plain SQL = ANY(...) ints",
                "SELECT id, name FROM pg19_param_person WHERE id = ANY($ids) ORDER BY id",
                {"ids": [1, 3]},
                2,
            ),
            (
                "plain SQL = ANY(...) empty list",
                "SELECT id, name FROM pg19_param_person WHERE id = ANY($ids) ORDER BY id",
                {"ids": []},
                0,
            ),
            # GRAPH_TABLE with list parameter (= ANY(...))
            (
                "GRAPH_TABLE = ANY(...) strings",
                (
                    "SELECT * FROM GRAPH_TABLE (pg19_param_graph "
                    "MATCH (p IS Person) WHERE p.name = ANY($names) "
                    "COLUMNS (p.id, p.name)) ORDER BY id"
                ),
                {"names": ["Alice", "Charlie"]},
                2,
            ),
            (
                "GRAPH_TABLE = ANY(...) ints",
                (
                    "SELECT * FROM GRAPH_TABLE (pg19_param_graph "
                    "MATCH (p IS Person) WHERE p.id = ANY($ids) "
                    "COLUMNS (p.id, p.name)) ORDER BY id"
                ),
                {"ids": [2, 4]},
                2,
            ),
            (
                "GRAPH_TABLE = ANY(...) empty list",
                (
                    "SELECT * FROM GRAPH_TABLE (pg19_param_graph "
                    "MATCH (p IS Person) WHERE p.id = ANY($ids) "
                    "COLUMNS (p.id, p.name)) ORDER BY id"
                ),
                {"ids": []},
                0,
            ),
            # GRAPH_TABLE array column equality (tags = $tags)
            (
                "GRAPH_TABLE array column equality strings",
                (
                    "SELECT * FROM GRAPH_TABLE (pg19_param_graph "
                    "MATCH (p IS Person) WHERE p.tags = $tags "
                    "COLUMNS (p.id, p.name)) ORDER BY id"
                ),
                {"tags": ["admin", "staff"]},
                1,
            ),
        ]

        for case_name, q, p, expected_count in test_cases:
            res_b = sorted(session_bridge.execute(q, p), key=lambda x: x["id"])
            res_n = sorted(session_native.execute(q, p), key=lambda x: x["id"])
            assert res_b == res_n, (
                f"Parity mismatch in {case_name}: bridge={res_b!r} vs native={res_n!r}"
            )
            assert len(res_b) == expected_count, (
                f"Unexpected row count for {case_name}: expected {expected_count}, got {len(res_b)}"
            )

    @pytest.mark.asyncio
    async def test_pg19_async_bridge_bound_parameters(self, pg19_conn):
        """Verifies AsyncPostgresBridge and AsyncSession correctly bind parameters."""
        bridge = AsyncPostgresBridge(pg19_conn)
        session = AsyncSession(bridge=bridge, dialect="sql_pgq")

        query = """
        SELECT * FROM GRAPH_TABLE (pg19_param_graph
            MATCH (p IS Person)
            WHERE p.name = $name AND p.age >= $age
            COLUMNS (p.id, p.name)
        )
        """
        rows = await session.execute(query, {"name": "Bob", "age": 20})
        assert len(rows) == 1
        assert rows[0] == {"id": 2, "name": "Bob"}

    def test_pg19_transaction_bound_parameters(self, pg19_conn):
        """Verifies bound parameters inside a real PostgreSQL database transaction."""
        bridge = PostgresBridge(pg19_conn)
        session = Session(bridge=bridge, dialect="sql_pgq")

        query = """
        SELECT * FROM GRAPH_TABLE (pg19_param_graph
            MATCH (p IS Person)
            WHERE p.name = $name
            COLUMNS (p.id, p.name)
        )
        """
        with session.transaction() as tx:
            rows = tx.execute(query, {"name": "Alice"})
            assert len(rows) == 1
            assert rows[0]["name"] == "Alice"

    def test_pg19_bridge_starts_with_ends_with_contains_live(self, pg19_conn):
        """Verifies STARTS WITH, ENDS WITH, and CONTAINS work on PostgreSQL 19 with PostgresBridge (#305)."""
        bridge = PostgresBridge(pg19_conn)
        session = Session(bridge=bridge, dialect="sql_pgq")

        # 1. STARTS WITH ($p0 || '%')
        q_starts = (
            "SELECT * FROM GRAPH_TABLE (pg19_param_graph "
            "MATCH (p IS Person) WHERE p.name LIKE $p0 || '%' COLUMNS (p.id, p.name)) ORDER BY id"
        )
        res_starts = session.execute(q_starts, {"p0": "Al"})
        assert res_starts == [{"id": 1, "name": "Alice"}]

        # 2. ENDS WITH ('%' || $p0)
        q_ends = (
            "SELECT * FROM GRAPH_TABLE (pg19_param_graph "
            "MATCH (p IS Person) WHERE p.name LIKE '%' || $p0 COLUMNS (p.id, p.name)) ORDER BY id"
        )
        res_ends = session.execute(q_ends, {"p0": "lie"})
        assert res_ends == [{"id": 3, "name": "Charlie"}]

        # 3. CONTAINS ('%' || $p0 || '%')
        q_contains = (
            "SELECT * FROM GRAPH_TABLE (pg19_param_graph "
            "MATCH (p IS Person) WHERE p.name LIKE '%' || $p0 || '%' COLUMNS (p.id, p.name)) ORDER BY id"
        )
        res_contains = session.execute(q_contains, {"p0": "arl"})
        assert res_contains == [{"id": 3, "name": "Charlie"}]

        # 4. Without parameters (raw LIKE with literal %)
        q_raw = (
            "SELECT * FROM GRAPH_TABLE (pg19_param_graph "
            "MATCH (p IS Person) WHERE p.name LIKE 'Bo%' COLUMNS (p.id, p.name)) ORDER BY id"
        )
        res_raw = session.execute(q_raw)
        assert res_raw == [{"id": 2, "name": "Bob"}]

    def test_pg19_bridge_string_predicates_async_and_transaction(self, pg19_conn):
        """Verifies string predicates work inside transactions on PostgreSQL 19 (#305)."""
        bridge = PostgresBridge(pg19_conn)
        session = Session(bridge=bridge, dialect="sql_pgq")

        q = (
            "SELECT * FROM GRAPH_TABLE (pg19_param_graph "
            "MATCH (p IS Person) WHERE p.name LIKE $p0 || '%' COLUMNS (p.id, p.name))"
        )

        with session.transaction() as tx:
            rows = tx.execute(q, {"p0": "Al"})
            assert rows == [{"id": 1, "name": "Alice"}]

    @pytest.mark.asyncio
    async def test_pg19_async_bridge_string_predicates(self, pg19_conn):
        """Verifies AsyncPostgresBridge correctly binds string predicate parameters (#305)."""
        bridge = AsyncPostgresBridge(pg19_conn)
        session = AsyncSession(bridge=bridge, dialect="sql_pgq")

        q = (
            "SELECT * FROM GRAPH_TABLE (pg19_param_graph "
            "MATCH (p IS Person) WHERE p.name LIKE $p0 || '%' COLUMNS (p.id, p.name))"
        )
        rows = await session.execute(q, {"p0": "Al"})
        assert rows == [{"id": 1, "name": "Alice"}]
