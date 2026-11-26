"""Regression tests for Apache AGE emitter column uniqueness (#285) and unsupported features (#286).

#285: Apache AGE column definition list must emit unique column names for any mix of
entity, property and aliased projections (preferring variable or alias name and
resolving clashes deterministically with sequential suffixes).

#286: Apache AGE must reject node patterns with multiple labels and relationship patterns
with alternative types at compile time with UnsupportedFeature.
"""

from __future__ import annotations

import pytest
from conftest import get_test_host
from conftest import is_port_open as _is_port_open
from voyager_ogm import (
    Field,
    Node,
    Query,
    Relationship,
    VoyagerError,
    compile_query_from_spec,
    node,
    relationship,
)

try:
    import psycopg
except ImportError:
    psycopg = None


@node(label="Person")
class Person(Node):
    name: str = Field()
    age: int = Field(default=30)


@relationship(type_name="KNOWS")
class Knows(Relationship):
    since: int = Field(default=2020)


def _base_age_spec() -> dict:
    return {
        "spec_version": 1,
        "matches": [
            {
                "paths": [
                    [
                        ("node", "a", ["Person"], None),
                        ("edge", "->", ["KNOWS"], "r", None, None, None),
                        ("node", "b", ["Person"], None),
                    ]
                ]
            }
        ],
        "projections": [("field", "a", "name", None)],
    }


# ---------------------------------------------------------------------------
# #285: Unique column names and clash resolution (Fluent API & Query Spec)
# ---------------------------------------------------------------------------


def test_age_multiple_whole_entities_emit_unique_columns() -> None:
    """Verifies that returning multiple whole entities emits unique column names."""
    a = Person(alias="a")
    b = Person(alias="b")
    r = Knows(alias="r")

    query = Query.match(a).to(r).node(b).return_(a, r, b)
    compiled = query.compile("apache_age", graph_name="test_graph")

    assert (
        compiled.statement
        == "SELECT * FROM cypher('test_graph', $$ MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, r, b $$) AS (a agtype, r agtype, b agtype)"
    )


def test_age_column_clash_resolution() -> None:
    """Verifies deterministic sequential collision resolution (_1, _2, ...) for colliding columns."""
    query_same_entity = Query.match_node("a", "Person").return_("a", "a")
    c1 = query_same_entity.compile("apache_age", graph_name="test_graph")
    assert "AS (a agtype, a_1 agtype)" in c1.statement

    query_entity_and_prop = Query.match_node("a", "Person").return_("a", "a.name")
    c2 = query_entity_and_prop.compile("apache_age", graph_name="test_graph")
    assert "AS (a agtype, name agtype)" in c2.statement

    query_same_prop = Query.match_node("a", "Person").return_("a.name", "a.name")
    c3 = query_same_prop.compile("apache_age", graph_name="test_graph")
    assert "AS (name agtype, name_1 agtype)" in c3.statement

    query_triple_clash = Query.match_node("a", "Person").return_("a", "a", "a")
    c4 = query_triple_clash.compile("apache_age", graph_name="test_graph")
    assert "AS (a agtype, a_1 agtype, a_2 agtype)" in c4.statement


def test_age_spec_multiple_whole_entities_emit_unique_columns() -> None:
    """Verifies that compile_query_from_spec emits unique column names for whole entities."""
    spec = _base_age_spec()
    spec["projections"] = [
        ("field", "a", "", None),
        ("field", "r", "", None),
        ("field", "b", "", None),
    ]

    compiled = compile_query_from_spec(spec, "apache_age", graph_name="test_graph")
    assert (
        compiled["statement"]
        == "SELECT * FROM cypher('test_graph', $$ MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, r, b $$) AS (a agtype, r agtype, b agtype)"
    )


# ---------------------------------------------------------------------------
# #286: UnsupportedFeature compile-time rejection in Apache AGE
# ---------------------------------------------------------------------------


def test_age_rejects_multi_label_node_fluent() -> None:
    """Verifies fluent API rejects node patterns with multiple labels for Apache AGE."""
    query = Query.match_node("p", labels=["Person", "Employee"]).return_("p.name")

    with pytest.raises(ValueError, match="multiple labels") as exc_info:
        query.compile("apache_age", graph_name="test_graph")
    assert "Apache AGE" in str(exc_info.value)


def test_age_rejects_alternative_relationship_types_fluent() -> None:
    """Verifies fluent API rejects relationship patterns with alternative types for Apache AGE."""
    query = Query.match_node("a").to(["KNOWS", "FOLLOWS"]).node("b").return_("a.name")

    with pytest.raises(ValueError, match="alternative types") as exc_info:
        query.compile("apache_age", graph_name="test_graph")
    assert "Apache AGE" in str(exc_info.value)


def test_age_spec_rejects_multi_label_node() -> None:
    """Verifies compile_query_from_spec rejects node patterns with multiple labels for Apache AGE."""
    spec = {
        "spec_version": 1,
        "matches": [{"paths": [[("node", "p", ["Person", "Employee"], None)]]}],
        "projections": [("field", "p", "name", None)],
    }

    with pytest.raises(VoyagerError, match="multiple labels") as exc_info:
        compile_query_from_spec(spec, "apache_age", graph_name="test_graph")
    assert exc_info.value.code == "UNSUPPORTED_FEATURE"
    assert "Apache AGE" in str(exc_info.value)


def test_age_spec_rejects_alternative_relationship_types() -> None:
    """Verifies compile_query_from_spec rejects relationship patterns with alternative types for Apache AGE."""
    spec = {
        "spec_version": 1,
        "matches": [
            {
                "paths": [
                    [
                        ("node", "a", ["Person"], None),
                        ("edge", "->", ["KNOWS", "FOLLOWS"], "r", None, None, None),
                        ("node", "b", ["Person"], None),
                    ]
                ]
            }
        ],
        "projections": [("field", "a", "name", None)],
    }

    with pytest.raises(VoyagerError, match="alternative types") as exc_info:
        compile_query_from_spec(spec, "apache_age", graph_name="test_graph")
    assert exc_info.value.code == "UNSUPPORTED_FEATURE"
    assert "Apache AGE" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Live Database Verification: Apache AGE (Port 5455, Container apache-age)
# ---------------------------------------------------------------------------


@pytest.mark.live
def test_live_age_multiple_entities_in_one_row() -> None:
    """Verifies live Apache AGE execution returning two nodes and a relationship in one row.

    Confirms that unique column names AS (a agtype, r agtype, b agtype) prevent the
    PostgreSQL error 'column name \"entity\" specified more than once' and results
    are correctly mapped to Python rows.
    """
    if psycopg is None:
        pytest.skip("psycopg is not installed in this environment")
    host = get_test_host()
    if not _is_port_open(host, 5455):
        pytest.skip(f"Apache AGE container not reachable on {host}:5455")

    conn_str = (
        f"host={host} port=5455 user=postgres password=voyagerpass123 "
        "dbname=voyager_graph connect_timeout=3"
    )

    with psycopg.connect(conn_str, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute('SET search_path = ag_catalog, "$user", public;')
            cur.execute("SELECT count(*) FROM ag_catalog.ag_graph WHERE name = 'live_age_fix_285';")
            if cur.fetchone()[0] > 0:
                cur.execute("SELECT drop_graph('live_age_fix_285', true);")
            cur.execute("SELECT create_graph('live_age_fix_285');")
            try:
                # Seed test graph
                cur.execute(
                    """
                    SELECT * FROM cypher('live_age_fix_285', $$
                        CREATE (a:Person {name: 'Alice', age: 30})-[:KNOWS {since: 2021}]->(b:Person {name: 'Bob', age: 25})
                    $$) AS (res agtype);
                    """
                )

                # Query returning two nodes and a relationship in one row
                query = (
                    Query.match_node("a", "Person")
                    .to("KNOWS", "r")
                    .node("b", "Person")
                    .return_("a", "r", "b")
                )
                compiled = query.compile("apache_age", graph_name="live_age_fix_285")

                assert "AS (a agtype, r agtype, b agtype)" in compiled.statement

                cur.execute(compiled.statement)
                col_names = [desc[0] for desc in cur.description]
                assert col_names == ["a", "r", "b"]

                rows = cur.fetchall()
                assert len(rows) == 1
                row = rows[0]
                assert len(row) == 3
                # Verify that all 3 AGType entities are returned non-null
                assert row[0] is not None
                assert row[1] is not None
                assert row[2] is not None
            finally:
                cur.execute(
                    "SELECT count(*) FROM ag_catalog.ag_graph WHERE name = 'live_age_fix_285';"
                )
                if cur.fetchone()[0] > 0:
                    cur.execute("SELECT drop_graph('live_age_fix_285', true);")
