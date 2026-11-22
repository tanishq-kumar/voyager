"""Regression tests for the SQL/PGQ emitter fixes (#268) and graph-name validation (#270).

#268: OPTIONAL MATCH, RETURN DISTINCT, LOAD CSV, subquery expressions
(EXISTS/COUNT/list comprehension/pattern comprehension) and the ``=~`` operator
must be translated or rejected with ``UnsupportedFeature`` -- never silently
dropped -- and an empty projection list must raise a clear error instead of
emitting ``COLUMNS ()``.

#270: the SQL/PGQ graph name is interpolated into ``GRAPH_TABLE (... )`` and must
be a plain identifier at every entry point, including ``compile_query_from_spec``.
"""

from __future__ import annotations

from typing import Any

import pytest
from conftest import is_port_open as _is_port_open
from voyager_ogm import (
    Field,
    Query,
    VoyagerError,
    compile_query_from_spec,
    list_comprehension,
    node,
    pattern_comprehension,
    relationship,
)

try:
    import psycopg
except ImportError:
    psycopg = None


@node(label="Person")
class Person:
    name: str = Field()
    age: int = Field()
    city: str = Field()
    skills: list[str] = Field(default_factory=list)


@node(label="Company")
class Company:
    name: str = Field()


@relationship(type_name="WORKS_AT")
class WorksAt:
    since: int = Field(default=2020)


def _base_spec() -> dict[str, Any]:
    """Returns the smallest valid spec that compiles for every dialect."""
    return {
        "spec_version": 1,
        "matches": [{"paths": [[("node", "p", ["Person"], None)]]}],
        "projections": [("field", "p", "name", None)],
    }


# ---------------------------------------------------------------------------
# #268: fluent API
# ---------------------------------------------------------------------------


def test_sql_pgq_optional_match_raises() -> None:
    p = Person("p")
    c = Company("c")
    query = Query.match(p).optional_match(c).return_(p.name, c.name)

    with pytest.raises(ValueError, match="OPTIONAL MATCH"):
        query.compile("sql_pgq", graph_name="g")


def test_sql_pgq_distinct_is_emitted() -> None:
    p = Person("p")
    compiled = Query.match(p).distinct().return_(p.city).compile("sql_pgq", graph_name="g")

    assert compiled.statement == (
        "SELECT DISTINCT * FROM GRAPH_TABLE (g MATCH (p IS Person) COLUMNS (p.city))"
    )


def test_sql_pgq_load_csv_raises() -> None:
    p = Person("p")
    query = (
        Query.load_csv("file:///persons.csv", with_headers=True, alias="row")
        .match(p)
        .return_(p.name)
    )

    with pytest.raises(ValueError, match="LOAD CSV"):
        query.compile("sql_pgq", graph_name="g")


def test_sql_pgq_regex_operator_raises() -> None:
    p = Person("p")
    query = Query.match(p).where(p.name.regex("A.*")).return_(p.name)

    with pytest.raises(ValueError, match="Regular expression") as exc_info:
        query.compile("sql_pgq", graph_name="g")
    assert "NodeHandle(" not in str(exc_info.value)


def test_sql_pgq_subquery_expressions_raise_without_ast_dump() -> None:
    p = Person("p")
    c = Company("c")

    exists_query = (
        Query.match(p).where(Query.exists(Query.match(p).to(WorksAt).node(c))).return_(p.name)
    )
    with pytest.raises(ValueError, match="EXISTS") as exists_err:
        exists_query.compile("sql_pgq", graph_name="g")
    assert "QueryStatement {" not in str(exists_err.value)

    count_query = Query.match(p).return_(Query.count(Query.match(c)))
    with pytest.raises(ValueError, match="COUNT") as count_err:
        count_query.compile("sql_pgq", graph_name="g")
    assert "QueryStatement {" not in str(count_err.value)

    list_query = Query.match(p).return_(
        p.name,
        list_comprehension("x", p.skills, None, None).as_("skills"),
    )
    with pytest.raises(ValueError, match="List comprehension") as list_err:
        list_query.compile("sql_pgq", graph_name="g")
    assert "ListComprehension {" not in str(list_err.value)

    pattern_query = Query.match(p).return_(
        p.name,
        pattern_comprehension(
            path=Query.match(p).to(WorksAt).node(c),
            proj=c.name,
        ).as_("companies"),
    )
    with pytest.raises(ValueError, match="Pattern comprehension") as pattern_err:
        pattern_query.compile("sql_pgq", graph_name="g")
    assert "PatternComprehension {" not in str(pattern_err.value)


def test_sql_pgq_requires_a_projection() -> None:
    p = Person("p")
    with pytest.raises(ValueError, match="at least one projection") as exc_info:
        Query.match(p).compile("sql_pgq", graph_name="g")
    assert "COLUMNS ()" not in str(exc_info.value)


# ---------------------------------------------------------------------------
# #268: Query Spec path
# ---------------------------------------------------------------------------


def test_spec_optional_match_raises() -> None:
    spec = _base_spec()
    spec["matches"].append({"optional": True, "paths": [[("node", "c", ["Company"], None)]]})

    with pytest.raises(ValueError, match="OPTIONAL MATCH"):
        compile_query_from_spec(spec, "sql_pgq", graph_name="g")


def test_spec_distinct_is_emitted() -> None:
    spec = _base_spec()
    spec["distinct"] = True

    result = compile_query_from_spec(spec, "sql_pgq", graph_name="g")

    assert result["statement"] == (
        "SELECT DISTINCT * FROM GRAPH_TABLE (g MATCH (p IS Person) COLUMNS (p.name))"
    )


def test_spec_load_csv_raises() -> None:
    spec = _base_spec()
    spec["load_csv"] = ("file:///persons.csv", True, "row")

    with pytest.raises(ValueError, match="LOAD CSV"):
        compile_query_from_spec(spec, "sql_pgq", graph_name="g")


def test_spec_regex_operator_raises() -> None:
    spec = _base_spec()
    spec["matches"] = [
        {
            "paths": [[("node", "p", ["Person"], None)]],
            "where": [("bin", "regex", ("prop", "p", "name"), ("lit", "A.*"))],
        }
    ]

    with pytest.raises(ValueError, match="Regular expression") as exc_info:
        compile_query_from_spec(spec, "sql_pgq", graph_name="g")
    assert "NodeHandle(" not in str(exc_info.value)


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        (
            (
                "exists",
                {
                    "spec_version": 1,
                    "matches": [{"paths": [[("node", "c", ["Company"], None)]]}],
                },
            ),
            "EXISTS",
        ),
        (
            (
                "count",
                {
                    "spec_version": 1,
                    "matches": [{"paths": [[("node", "c", ["Company"], None)]]}],
                },
            ),
            "COUNT",
        ),
        (("list_comp", "x", ("prop", "p", "skills"), None, None), "List comprehension"),
        (
            ("pattern_comp", [("node", "p", ["Person"], None)], None, ("prop", "p", "name")),
            "Pattern comprehension",
        ),
    ],
)
def test_spec_subquery_expressions_raise(expression: Any, expected: str) -> None:
    spec = _base_spec()
    if expression[0] in {"exists", "count"}:
        spec["matches"][0]["where"] = [expression]
    else:
        spec["projections"] = [("expr", expression, None)]

    with pytest.raises(ValueError, match=expected) as exc_info:
        compile_query_from_spec(spec, "sql_pgq", graph_name="g")
    message = str(exc_info.value)
    assert "NodeHandle(" not in message
    assert "QueryStatement {" not in message
    assert "ListComprehension {" not in message
    assert "PatternComprehension {" not in message


def test_spec_without_projections_raises() -> None:
    spec = _base_spec()
    del spec["projections"]

    with pytest.raises(ValueError, match="at least one projection") as exc_info:
        compile_query_from_spec(spec, "sql_pgq", graph_name="g")
    assert "COLUMNS ()" not in str(exc_info.value)


# ---------------------------------------------------------------------------
# #270: graph name validation through the fluent API and Query Spec
# ---------------------------------------------------------------------------

VALID_GRAPH_NAMES = ["g", "_g", "g123", "a" * 63]
INVALID_GRAPH_NAMES = [
    "",
    "g) UNION SELECT 1 --",
    "g'",
    'g"',
    "g;",
    "g name",
    "g-name",
    "1g",
    "g.dot",
]


@pytest.mark.parametrize("graph_name", VALID_GRAPH_NAMES)
def test_fluent_sql_pgq_accepts_valid_graph_names(graph_name: str) -> None:
    p = Person("p")
    compiled = Query.match(p).return_(p.name).compile("sql_pgq", graph_name=graph_name)
    assert f"FROM GRAPH_TABLE ({graph_name} MATCH" in compiled.statement


@pytest.mark.parametrize("graph_name", INVALID_GRAPH_NAMES)
def test_fluent_sql_pgq_rejects_invalid_graph_names(graph_name: str) -> None:
    p = Person("p")
    with pytest.raises(ValueError, match="Invalid graph name"):
        Query.match(p).return_(p.name).compile("sql_pgq", graph_name=graph_name)


@pytest.mark.parametrize("graph_name", VALID_GRAPH_NAMES)
@pytest.mark.parametrize("dialect", ["sql_pgq", "duckpgq", "pgq", "sql"])
def test_compile_query_from_spec_accepts_valid_graph_names(graph_name: str, dialect: str) -> None:
    result = compile_query_from_spec(_base_spec(), dialect, graph_name=graph_name)
    assert f"FROM GRAPH_TABLE ({graph_name} MATCH" in result["statement"]


@pytest.mark.parametrize("graph_name", INVALID_GRAPH_NAMES)
@pytest.mark.parametrize("dialect", ["sql_pgq", "duckpgq"])
def test_compile_query_from_spec_rejects_invalid_graph_names(graph_name: str, dialect: str) -> None:
    with pytest.raises(ValueError, match="Invalid graph name") as exc_info:
        compile_query_from_spec(_base_spec(), dialect, graph_name=graph_name)
    assert "SQL:PGQ" in str(exc_info.value)


# ---------------------------------------------------------------------------
# #286: multi-label rejection, whole entity COLUMNS rejection, and supported forms
# ---------------------------------------------------------------------------


def test_sql_pgq_rejects_multi_label_node_fluent() -> None:
    """Verifies fluent API rejects node patterns with multiple labels for SQL:PGQ."""
    query = Query.match_node("p", labels=["Person", "Employee"]).return_("p.name")

    with pytest.raises(ValueError, match="multiple labels") as exc_info:
        query.compile("sql_pgq", graph_name="g")
    assert "SQL:2023 PGQ" in str(exc_info.value)


def test_sql_pgq_rejects_whole_entity_projection_node_fluent() -> None:
    """Verifies fluent API rejects returning whole node variable in COLUMNS."""
    query = Query.match_node("p", "Person").return_("p")

    with pytest.raises(ValueError, match="Whole entity column projection") as exc_info:
        query.compile("sql_pgq", graph_name="g")
    assert "COLUMNS clause only supports scalar expressions" in str(exc_info.value)


def test_sql_pgq_rejects_whole_entity_projection_edge_fluent() -> None:
    """Verifies fluent API rejects returning whole relationship variable in COLUMNS."""
    query = Query.match_node("a", "Person").to("KNOWS", "r").node("b", "Person").return_("r")

    with pytest.raises(ValueError, match="Whole entity column projection") as exc_info:
        query.compile("sql_pgq", graph_name="g")
    assert "COLUMNS clause only supports scalar expressions" in str(exc_info.value)


def test_sql_pgq_spec_rejects_multi_label_node() -> None:
    """Verifies compile_query_from_spec rejects node patterns with multiple labels."""
    spec = {
        "spec_version": 1,
        "matches": [{"paths": [[("node", "p", ["Person", "Employee"], None)]]}],
        "projections": [("field", "p", "name", None)],
    }

    with pytest.raises(VoyagerError, match="multiple labels") as exc_info:
        compile_query_from_spec(spec, "sql_pgq", graph_name="g")
    assert exc_info.value.code == "UNSUPPORTED_FEATURE"
    assert "SQL:2023 PGQ" in str(exc_info.value)


def test_sql_pgq_spec_rejects_whole_entity_projection() -> None:
    """Verifies compile_query_from_spec rejects whole node/edge variable in projections."""
    spec = {
        "spec_version": 1,
        "matches": [{"paths": [[("node", "p", ["Person"], None)]]}],
        "projections": [("field", "p", "", None)],
    }

    with pytest.raises(VoyagerError, match="Whole entity column projection") as exc_info:
        compile_query_from_spec(spec, "sql_pgq", graph_name="g")
    assert exc_info.value.code == "UNSUPPORTED_FEATURE"
    assert "COLUMNS clause only supports scalar expressions" in str(exc_info.value)


def test_sql_pgq_accepts_single_label_and_property_columns() -> None:
    """Verifies single label and property columns compile correctly."""
    query = Query.match_node("p", "Person").return_("p.name")
    compiled = query.compile("sql_pgq", graph_name="g")
    assert (
        compiled.statement == "SELECT * FROM GRAPH_TABLE (g MATCH (p IS Person) COLUMNS (p.name))"
    )


def test_sql_pgq_accepts_alternative_relationship_types() -> None:
    """Verifies alternative relationship types compile to -[r IS T1 | T2]->."""
    query = (
        Query.match_node("a", "Person")
        .to(["KNOWS", "HATES"], "r")
        .node("b", "Person")
        .return_("a.name", "b.name")
    )
    compiled = query.compile("sql_pgq", graph_name="g")
    assert (
        compiled.statement
        == "SELECT * FROM GRAPH_TABLE (g MATCH (a IS Person) -[r IS KNOWS | HATES]-> (b IS Person) COLUMNS (a.name, b.name))"
    )


def test_duckpgq_shares_sql_pgq_validation_behavior() -> None:
    """Verifies DuckPGQ shares the SQL/PGQ emitter path and validation rules."""
    query_multi_label = Query.match_node("p", labels=["Person", "Employee"]).return_("p.name")
    with pytest.raises(ValueError, match="multiple labels"):
        query_multi_label.compile("duckpgq", graph_name="g")

    query_whole_entity = Query.match_node("p", "Person").return_("p")
    with pytest.raises(ValueError, match="Whole entity column projection"):
        query_whole_entity.compile("duckpgq", graph_name="g")


def test_sql_pgq_accepts_node_label_disjunction_fluent() -> None:
    """Verifies node label disjunction compiles to (n IS Person | Company)."""
    query = Query.match_node("n", labels="Person | Company").return_("n.name")
    compiled = query.compile("sql_pgq", graph_name="g")
    assert (
        compiled.statement
        == "SELECT * FROM GRAPH_TABLE (g MATCH (n IS Person | Company) COLUMNS (n.name))"
    )


def test_sql_pgq_spec_accepts_node_label_disjunction() -> None:
    """Verifies compile_query_from_spec accepts node label disjunction."""
    spec = {
        "spec_version": 1,
        "matches": [{"paths": [[("node", "n", ["Person | Company"], None)]]}],
        "projections": [("field", "n", "name", None)],
    }
    compiled = compile_query_from_spec(spec, "sql_pgq", graph_name="g")
    assert (
        compiled["statement"]
        == "SELECT * FROM GRAPH_TABLE (g MATCH (n IS Person | Company) COLUMNS (n.name))"
    )


def test_sql_pgq_rejects_node_label_negation_fluent() -> None:
    """Verifies fluent API rejects node label negation with accurate error message."""
    query = Query.match_node("n", labels="!Inactive").return_("n.name")
    with pytest.raises(
        ValueError, match=r"(?i)label expression with negation/conjunction/grouping"
    ) as exc_info:
        query.compile("sql_pgq", graph_name="g")
    assert "multiple labels" not in str(exc_info.value).lower()


def test_sql_pgq_spec_rejects_node_label_negation() -> None:
    """Verifies compile_query_from_spec rejects node label negation with accurate error message."""
    spec = {
        "spec_version": 1,
        "matches": [{"paths": [[("node", "n", ["!Inactive"], None)]]}],
        "projections": [("field", "n", "name", None)],
    }
    with pytest.raises(
        VoyagerError, match=r"(?i)label expression with negation/conjunction/grouping"
    ) as exc_info:
        compile_query_from_spec(spec, "sql_pgq", graph_name="g")
    assert exc_info.value.code == "UNSUPPORTED_FEATURE"
    assert "multiple labels" not in str(exc_info.value).lower()


def test_sql_pgq_rejects_complex_label_expression_fluent() -> None:
    """Verifies fluent API rejects grouping/conjunction/negation with accurate error message."""
    query = Query.match_node("n", labels="(Person | Company) & !Inactive").return_("n.name")
    with pytest.raises(
        ValueError, match=r"(?i)label expression with negation/conjunction/grouping"
    ) as exc_info:
        query.compile("sql_pgq", graph_name="g")
    assert "multiple labels" not in str(exc_info.value).lower()


def test_sql_pgq_rejects_edge_relationship_type_expression_negation() -> None:
    """Verifies fluent API rejects relationship type expressions using negation/conjunction/grouping."""
    query = Query.match_node("a", "Person").to("!KNOWS", "r").node("b", "Person").return_("a.name")
    with pytest.raises(ValueError, match=r"(?i)with negation/conjunction/grouping") as exc_info:
        query.compile("sql_pgq", graph_name="g")
    assert "multiple labels" not in str(exc_info.value).lower()


# ---------------------------------------------------------------------------
# Live Database Verification: PostgreSQL 19 (Port 5456, Container postgres19)
# ---------------------------------------------------------------------------


@pytest.mark.live
def test_live_postgres19_supported_forms() -> None:
    """Verifies live PostgreSQL 19 execution with supported single-label, label disjunction, and alternative edge types."""
    if psycopg is None:
        pytest.skip("psycopg is not installed in this environment")
    if not _is_port_open("127.0.0.1", 5456):
        pytest.skip("PostgreSQL 19 Beta container not available on port 5456")

    conn_str = (
        "host=127.0.0.1 port=5456 user=postgres password=voyagerpass123 "
        "dbname=voyager_graph connect_timeout=3"
    )

    with psycopg.connect(conn_str, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP PROPERTY GRAPH IF EXISTS live_pg19_fix_286_graph CASCADE;")
            cur.execute("DROP TABLE IF EXISTS live_pg19_fix_286_knows CASCADE;")
            cur.execute("DROP TABLE IF EXISTS live_pg19_fix_286_hates CASCADE;")
            cur.execute("DROP TABLE IF EXISTS live_pg19_fix_286_company CASCADE;")
            cur.execute("DROP TABLE IF EXISTS live_pg19_fix_286_person CASCADE;")
            try:
                cur.execute(
                    """
                    CREATE TABLE live_pg19_fix_286_person (id INT PRIMARY KEY, name TEXT NOT NULL);
                    CREATE TABLE live_pg19_fix_286_company (id INT PRIMARY KEY, name TEXT NOT NULL);
                    CREATE TABLE live_pg19_fix_286_knows (src_id INT, dst_id INT, PRIMARY KEY (src_id, dst_id));
                    CREATE TABLE live_pg19_fix_286_hates (src_id INT, dst_id INT, PRIMARY KEY (src_id, dst_id));
                    INSERT INTO live_pg19_fix_286_person VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');
                    INSERT INTO live_pg19_fix_286_company VALUES (10, 'Acme Corp');
                    INSERT INTO live_pg19_fix_286_knows VALUES (1, 2);
                    INSERT INTO live_pg19_fix_286_hates VALUES (1, 3);
                    CREATE PROPERTY GRAPH live_pg19_fix_286_graph
                        VERTEX TABLES (
                            live_pg19_fix_286_person LABEL Person,
                            live_pg19_fix_286_company LABEL Company
                        )
                        EDGE TABLES (
                            live_pg19_fix_286_knows
                                SOURCE KEY (src_id) REFERENCES live_pg19_fix_286_person (id)
                                DESTINATION KEY (dst_id) REFERENCES live_pg19_fix_286_person (id)
                                LABEL KNOWS,
                            live_pg19_fix_286_hates
                                SOURCE KEY (src_id) REFERENCES live_pg19_fix_286_person (id)
                                DESTINATION KEY (dst_id) REFERENCES live_pg19_fix_286_person (id)
                                LABEL HATES
                        );
                    """
                )

                # 1. Alternative relationship types test: -[r IS KNOWS | HATES]->
                query_edge = (
                    Query.match_node("a", "Person")
                    .to(["KNOWS", "HATES"], "r")
                    .node("b", "Person")
                    .return_(src="a.name", dst="b.name")
                )
                compiled_edge = query_edge.compile("sql_pgq", graph_name="live_pg19_fix_286_graph")

                assert "-[r IS KNOWS | HATES]->" in compiled_edge.statement
                assert "COLUMNS (a.name AS src, b.name AS dst)" in compiled_edge.statement

                cur.execute(f"{compiled_edge.statement} ORDER BY src, dst")
                rows = cur.fetchall()
                assert rows == [("Alice", "Bob"), ("Alice", "Charlie")]

                # 2. Node label disjunction test: (n IS Person | Company)
                query_disj = Query.match_node("n", labels="Person | Company").return_(val="n.name")
                compiled_disj = query_disj.compile("sql_pgq", graph_name="live_pg19_fix_286_graph")
                assert "(n IS Person | Company)" in compiled_disj.statement
                assert "COLUMNS (n.name AS val)" in compiled_disj.statement

                cur.execute(f"{compiled_disj.statement} ORDER BY val")
                disj_rows = cur.fetchall()
                assert disj_rows == [("Acme Corp",), ("Alice",), ("Bob",), ("Charlie",)]
            finally:
                cur.execute("DROP PROPERTY GRAPH IF EXISTS live_pg19_fix_286_graph CASCADE;")
                cur.execute("DROP TABLE IF EXISTS live_pg19_fix_286_knows CASCADE;")
                cur.execute("DROP TABLE IF EXISTS live_pg19_fix_286_hates CASCADE;")
                cur.execute("DROP TABLE IF EXISTS live_pg19_fix_286_company CASCADE;")
                cur.execute("DROP TABLE IF EXISTS live_pg19_fix_286_person CASCADE;")
