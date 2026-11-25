"""Live Session result decoding and native AGE setup tests (#353, #354).

Both Apache AGE backends run against a plain ``postgresql://`` URI without AGE
query parameters, so the native backend must learn AGE intent from the Session
dialect and graph name. Results are also compared with Neo4j when it is
reachable.
"""

from __future__ import annotations

import os
import socket
import uuid
from typing import Any

import pytest
from spec_corpus_live_lib import canonicalize
from voyager_ogm import Field, Node, Query, Session

pytestmark = pytest.mark.live

HOST = os.environ.get("VOYAGER_TEST_HOST", "127.0.0.1")
AGE_PORT = int(os.environ.get("VOYAGER_TEST_AGE_PORT", "5455"))
PG19_PORT = int(os.environ.get("VOYAGER_TEST_PG19_PORT", "5456"))
NEO4J_PORT = int(os.environ.get("VOYAGER_TEST_NEO4J_PORT", "7687"))

AGE_URI = f"postgresql://postgres:voyagerpass123@{HOST}:{AGE_PORT}/voyager_graph"
PG19_URI = f"postgresql://postgres:voyagerpass123@{HOST}:{PG19_PORT}/voyager_graph"
NEO4J_URI = f"bolt://{HOST}:{NEO4J_PORT}"

LABEL = "zzAgeResult353"
REL_TYPE = "zzAgeKnows353"
GRAPH = f"zz_age_results_{uuid.uuid4().hex[:8]}"

BACKENDS = ("bridge", "native")


class AgeBuilderPerson(Node):
    """Model used to execute a query-builder statement on AGE."""

    __label__ = LABEL
    name: str = Field(primary_key=True)


def _port_open(port: int) -> bool:
    try:
        with socket.create_connection((HOST, port), timeout=1.0):
            return True
    except OSError:
        return False


@pytest.fixture(scope="module")
def age_graph() -> Any:
    """Creates an isolated AGE graph with one path used by every test."""
    if not _port_open(AGE_PORT):
        pytest.skip(f"Apache AGE not reachable on {HOST}:{AGE_PORT}")
    psycopg = pytest.importorskip("psycopg")
    conn = psycopg.connect(AGE_URI, autocommit=True, connect_timeout=3)
    try:
        with conn.cursor() as cur:
            cur.execute("LOAD 'age';")
            cur.execute('SET search_path = ag_catalog, "$user", public;')
            cur.execute("SELECT ag_catalog.create_graph(%s);", (GRAPH,))
            cur.execute(
                f"SELECT * FROM ag_catalog.cypher('{GRAPH}', $$ "
                f"CREATE (a:{LABEL} {{name: 'A', score: 10, ratio: 1.5, active: true}}), "
                f"       (b:{LABEL} {{name: 'B', score: 20, ratio: 2.5, active: false}}), "
                f"       (a)-[:{REL_TYPE} {{since: 2020}}]->(b) "
                "$$) AS (res agtype);"
            )
    except Exception:
        conn.close()
        raise
    yield conn
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT ag_catalog.drop_graph(%s, true);", (GRAPH,))
    finally:
        conn.close()


@pytest.fixture(scope="module")
def neo4j_data() -> Any:
    """Seeds the same small path in Neo4j for shape comparisons."""
    if not _port_open(NEO4J_PORT):
        pytest.skip(f"Neo4j not reachable on {HOST}:{NEO4J_PORT}")
    neo4j = pytest.importorskip("neo4j")
    driver = neo4j.GraphDatabase.driver(NEO4J_URI, auth=("neo4j", "voyagerpass123"))
    try:
        driver.verify_connectivity()
    except Exception as err:
        driver.close()
        pytest.skip(f"Neo4j is not reachable: {err}")
    with driver.session() as session:
        session.run(f"MATCH (n:{LABEL}) DETACH DELETE n").consume()
        session.run(
            f"CREATE (a:{LABEL} {{name: 'A', score: 10, ratio: 1.5, active: true}}), "
            f"       (b:{LABEL} {{name: 'B', score: 20, ratio: 2.5, active: false}}), "
            f"       (a)-[:{REL_TYPE} {{since: 2020}}]->(b)"
        ).consume()
    yield driver
    with driver.session() as session:
        session.run(f"MATCH (n:{LABEL}) DETACH DELETE n").consume()
    driver.close()


def _cypher_query(cypher: str, columns: dict[str, str]) -> str:
    projection = ", ".join(f"{name} {type_name}" for name, type_name in columns.items())
    return f"SELECT * FROM ag_catalog.cypher('{GRAPH}', $$ {cypher} $$) AS ({projection});"


def _age_value(value: Any) -> Any:
    return canonicalize(value, "age", parse_age=False)


def _neo4j_value(value: Any) -> Any:
    return canonicalize(value, "neo4j")


@pytest.mark.parametrize("backend", BACKENDS)
def test_live_age_results_decode_on_both_backends(age_graph: Any, backend: str) -> None:
    """Every agtype shape decodes into Python values on the bridge and native backends."""
    session = Session(AGE_URI, dialect="age", graph_name=GRAPH, backend=backend)
    try:
        node_query = _cypher_query(f"MATCH (a:{LABEL} {{name: 'A'}}) RETURN a", {"a": "agtype"})
        node = session.execute(node_query)[0]["a"]
        assert node["kind"] == "node"
        assert node["labels"] == [LABEL]
        assert node["properties"]["name"] == "A"
        assert node["properties"]["score"] == 10

        rel_query = _cypher_query(
            f"MATCH (a:{LABEL} {{name: 'A'}})-[r:{REL_TYPE}]->(b) RETURN r",
            {"r": "agtype"},
        )
        relationship = session.execute(rel_query)[0]["r"]
        assert relationship["kind"] == "relationship"
        assert relationship["type"] == REL_TYPE
        assert relationship["properties"]["since"] == 2020
        assert relationship["start_id"] == node["id"]

        path_query = _cypher_query(
            f"MATCH p=(a:{LABEL} {{name: 'A'}})-[r:{REL_TYPE}]->(b) RETURN p",
            {"p": "agtype"},
        )
        path = session.execute(path_query)[0]["p"]
        assert path["kind"] == "path"
        assert [item["properties"]["name"] for item in path["nodes"]] == ["A", "B"]
        assert [item["type"] for item in path["relationships"]] == [REL_TYPE]

        collect_query = _cypher_query(
            f"MATCH (n:{LABEL}) RETURN collect(n) AS nodes", {"nodes": "agtype"}
        )
        nodes = session.execute(collect_query)[0]["nodes"]
        assert [item["kind"] for item in nodes] == ["node", "node"]
        assert sorted(item["properties"]["name"] for item in nodes) == ["A", "B"]

        collect_rel_query = _cypher_query(
            f"MATCH ()-[r:{REL_TYPE}]->() RETURN collect(r) AS rels",
            {"rels": "agtype"},
        )
        relationships = session.execute(collect_rel_query)[0]["rels"]
        assert [item["kind"] for item in relationships] == ["relationship"]
        assert relationships[0]["type"] == REL_TYPE

        nested_query = _cypher_query(
            f"MATCH (a:{LABEL} {{name: 'A'}})-[r:{REL_TYPE}]->(b:{LABEL}) "
            "RETURN [a, r, b] AS triple, [[a], {node: b, n: 1}] AS nested",
            {"triple": "agtype", "nested": "agtype"},
        )
        nested_row = session.execute(nested_query)[0]
        assert [item["kind"] for item in nested_row["triple"]] == [
            "node",
            "relationship",
            "node",
        ]
        assert nested_row["nested"][0][0]["kind"] == "node"
        assert nested_row["nested"][1]["node"]["properties"]["name"] == "B"

        scalar_query = _cypher_query(
            "RETURN 'A' AS s, 30 AS i, 1.5 AS f, 30::numeric AS num, true AS b, null AS nul",
            {
                "s": "agtype",
                "i": "agtype",
                "f": "agtype",
                "num": "agtype",
                "b": "agtype",
                "nul": "agtype",
            },
        )
        scalars = session.execute(scalar_query)[0]
        assert scalars["s"] == "A"
        assert scalars["i"] == 30
        assert scalars["f"] == 1.5
        assert scalars["num"] == 30
        assert scalars["b"] is True
        assert scalars["nul"] is None

        map_query = _cypher_query(
            "RETURN {id: 1, label: 'x', properties: {city: 'Berlin'}} AS m",
            {"m": "agtype"},
        )
        plain_map = session.execute(map_query)[0]["m"]
        assert plain_map == {"id": 1, "label": "x", "properties": {"city": "Berlin"}}
        assert "kind" not in plain_map
    finally:
        session.close()


def test_live_age_results_match_neo4j_shape(age_graph: Any, neo4j_data: Any) -> None:
    """The same query returns the same canonical shape on AGE and Neo4j."""
    age_session = Session(AGE_URI, dialect="age", graph_name=GRAPH, backend="bridge")
    neo4j_session = neo4j_data.session()
    try:
        cases = [
            (
                f"MATCH (a:{LABEL} {{name: 'A'}}) RETURN a",
                {"a": "agtype"},
                "a",
            ),
            (
                f"MATCH (a:{LABEL} {{name: 'A'}})-[r:{REL_TYPE}]->(b) RETURN r",
                {"r": "agtype"},
                "r",
            ),
            (
                f"MATCH p=(a:{LABEL} {{name: 'A'}})-[r:{REL_TYPE}]->(b) RETURN p",
                {"p": "agtype"},
                "p",
            ),
            (
                f"MATCH (n:{LABEL}) RETURN collect(n) AS nodes",
                {"nodes": "agtype"},
                "nodes",
            ),
            (
                f"MATCH (a:{LABEL} {{name: 'A'}}) RETURN {{node: a, n: 1}} AS m",
                {"m": "agtype"},
                "m",
            ),
            (
                "RETURN 'A' AS s, 30 AS i, 1.5 AS f, true AS b, null AS nul",
                {
                    "s": "agtype",
                    "i": "agtype",
                    "f": "agtype",
                    "b": "agtype",
                    "nul": "agtype",
                },
                None,
            ),
        ]
        for cypher, columns, key in cases:
            age_row = age_session.execute(_cypher_query(cypher, columns))[0]
            neo4j_row = neo4j_session.run(cypher).single()
            if key is None:
                for column in columns:
                    assert _age_value(age_row[column]) == _neo4j_value(neo4j_row[column])
            else:
                assert _age_value(age_row[key]) == _neo4j_value(neo4j_row[key])
    finally:
        age_session.close()
        neo4j_session.close()


def test_live_age_native_setup_without_uri_parameters(age_graph: Any) -> None:
    """A native Session with dialect age sets up AGE on plain-postgresql connections."""
    session = Session(AGE_URI, dialect="age", graph_name=GRAPH, backend="native")
    try:
        raw = _cypher_query(f"MATCH (a:{LABEL} {{name: 'A'}}) RETURN a", {"a": "agtype"})
        assert session.execute(raw)[0]["a"]["properties"]["name"] == "A"

        person = AgeBuilderPerson("p")
        compiled = Query.match(person).where(person.name == "A").return_(person.name)
        assert session.execute(compiled)[0]["name"] == "A"

        with session.transaction():
            assert session.execute(raw)[0]["a"]["properties"]["name"] == "A"
            assert session.execute(compiled)[0]["name"] == "A"
    finally:
        session.close()


def test_live_postgres19_native_skips_age_setup() -> None:
    """A non-AGE native Session leaves search_path alone and runs normally."""
    if not _port_open(PG19_PORT):
        pytest.skip(f"PostgreSQL 19 not reachable on {HOST}:{PG19_PORT}")
    session = Session(PG19_URI, dialect="sql_pgq", backend="native")
    try:
        search_path = session.execute("SHOW search_path")[0]["search_path"]
        assert "ag_catalog" not in search_path
        assert session.execute("SELECT 1 AS n")[0]["n"] == 1
    finally:
        session.close()
