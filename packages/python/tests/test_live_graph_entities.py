"""Live cross-engine parity tests for unified graph entity results (#372)."""

from __future__ import annotations

import json
import os
import socket
import uuid
from typing import Any

import polars as pl
import pyarrow as pa
import pytest
from voyager_ogm import GraphNode, GraphPath, GraphRelationship, Session

pytestmark = pytest.mark.live

HOST = os.environ.get("VOYAGER_TEST_HOST", "127.0.0.1")
NEO4J_PORT = int(os.environ.get("VOYAGER_TEST_NEO4J_PORT", "7687"))
MEMGRAPH_PORT = int(os.environ.get("VOYAGER_TEST_MEMGRAPH_PORT", "7688"))
FALKORDB_PORT = int(os.environ.get("VOYAGER_TEST_FALKORDB_PORT", "6379"))
AGE_PORT = int(os.environ.get("VOYAGER_TEST_AGE_PORT", "5455"))


def _port_open(port: int) -> bool:
    try:
        with socket.create_connection((HOST, port), timeout=1.0):
            return True
    except OSError:
        return False


def _canonical_without_identity(obj: Any) -> Any:
    """Normalizes entity snapshots across engines where element_id rules differ."""
    if isinstance(obj, GraphNode):
        return {
            "kind": "node",
            "labels": sorted(obj.labels),
            "properties": {k: _canonical_without_identity(v) for k, v in obj.properties.items()},
        }
    if isinstance(obj, GraphRelationship):
        return {
            "kind": "relationship",
            "type": obj.type,
            "properties": {k: _canonical_without_identity(v) for k, v in obj.properties.items()},
        }
    if isinstance(obj, GraphPath):
        return {
            "kind": "path",
            "nodes": [_canonical_without_identity(n) for n in obj.nodes],
            "relationships": [_canonical_without_identity(r) for r in obj.relationships],
        }
    if isinstance(obj, list):
        return [_canonical_without_identity(item) for item in obj]
    if isinstance(obj, dict):
        return {k: _canonical_without_identity(v) for k, v in obj.items()}
    return obj


def _assert_row_entities(
    row: dict[str, Any],
    *,
    expected_labels_a: frozenset[str],
    expected_labels_b: frozenset[str],
    expect_element_id: bool,
) -> None:
    node_a = row["a"]
    rel_k = row["k"]
    path_p = row["p"]
    coll_bs = row["bs"]
    map_m = row["m"]

    assert isinstance(node_a, GraphNode)
    assert node_a.labels == expected_labels_a
    assert node_a["name"] == "A"
    assert isinstance(node_a.id, int)
    if expect_element_id:
        assert isinstance(node_a.element_id, str)
    else:
        assert node_a.element_id is None

    assert isinstance(rel_k, GraphRelationship)
    assert rel_k.type == "ZK"
    assert rel_k["w"] == 1
    assert isinstance(rel_k.id, int)
    assert rel_k.start_id == node_a.id

    assert isinstance(path_p, GraphPath)
    assert len(path_p) == 1
    assert len(path_p.nodes) == 2
    assert len(path_p.relationships) == 1
    assert path_p.start_node == node_a
    assert path_p.end_node.labels == expected_labels_b
    assert path_p.end_node["name"] == "B"
    assert path_p.relationships[0] == rel_k
    assert rel_k.end_id == path_p.end_node.id

    assert isinstance(coll_bs, list)
    assert len(coll_bs) == 1
    assert isinstance(coll_bs[0], GraphNode)
    assert coll_bs[0] == path_p.end_node

    assert isinstance(map_m, dict)
    assert isinstance(map_m["node"], GraphNode)
    assert map_m["node"] == node_a


def _assert_arrow_and_polars(
    arrow_table: pa.Table,
    df: pl.DataFrame,
    row: dict[str, Any],
) -> None:
    assert isinstance(arrow_table, pa.Table)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 1

    for col in ("a", "k", "p"):
        field = arrow_table.schema.field(col)
        assert field.metadata is not None
        assert field.metadata.get(b"voyager.encoding") == b"json"

    assert arrow_table.schema.field("a").metadata.get(b"voyager.kind") == b"node"
    assert arrow_table.schema.field("k").metadata.get(b"voyager.kind") == b"relationship"
    assert arrow_table.schema.field("p").metadata.get(b"voyager.kind") == b"path"

    assert json.loads(df["a"][0]) == row["a"].to_dict()
    assert json.loads(df["k"][0]) == row["k"].to_dict()
    assert json.loads(df["p"][0]) == row["p"].to_dict()
    assert json.loads(df["bs"][0]) == [item.to_dict() for item in row["bs"]]
    assert json.loads(df["m"][0]) == {"node": row["a"].to_dict()}


def test_live_neo4j_graph_entities_bridge_and_native() -> None:
    """Verifies Neo4j bridge/native parity, multi-label nodes, and Arrow/Polars JSON."""
    if not _port_open(NEO4J_PORT):
        pytest.skip(f"Neo4j not reachable on {HOST}:{NEO4J_PORT}")

    tag = uuid.uuid4().hex[:8]
    label_a = f"ZzEntityA_{tag}"
    label_extra = f"ZzExtra_{tag}"
    label_b = f"ZzEntityB_{tag}"
    uri = f"bolt://neo4j:voyagerpass123@{HOST}:{NEO4J_PORT}"

    query = (
        f"MATCH p=(a:{label_a})-[k:ZK]->(b:{label_b}) "
        "RETURN a, k, p, collect(b) AS bs, {node: a} AS m"
    )

    bridge_session = Session(uri, dialect="neo4j", backend="bridge")
    native_session = Session(uri, dialect="neo4j", backend="native")
    try:
        bridge_session.execute(
            f"CREATE (:{label_a}:{label_extra} {{name:'A'}})-[:ZK {{w:1}}]->(:{label_b} {{name:'B'}})"
        )
        bridge_row = bridge_session.execute(query)[0]
        native_row = native_session.execute(query)[0]

        _assert_row_entities(
            bridge_row,
            expected_labels_a=frozenset({label_a, label_extra}),
            expected_labels_b=frozenset({label_b}),
            expect_element_id=True,
        )
        _assert_row_entities(
            native_row,
            expected_labels_a=frozenset({label_a, label_extra}),
            expected_labels_b=frozenset({label_b}),
            expect_element_id=True,
        )
        assert bridge_row["a"] == native_row["a"]
        assert bridge_row["k"] == native_row["k"]
        assert bridge_row["p"] == native_row["p"]
        assert bridge_row["a"].to_dict() == native_row["a"].to_dict()
        assert bridge_row["k"].to_dict() == native_row["k"].to_dict()
        assert bridge_row["p"].to_dict() == native_row["p"].to_dict()

        _assert_arrow_and_polars(
            native_session.execute_to_arrow(query),
            native_session.execute_to_polars(query),
            native_row,
        )
    finally:
        bridge_session.execute(
            f"MATCH (n) WHERE '{label_a}' IN labels(n) OR '{label_b}' IN labels(n) DETACH DELETE n"
        )
        bridge_session.close()
        native_session.close()


def test_live_memgraph_graph_entities_bridge_and_native() -> None:
    """Verifies Memgraph bridge/native parity and Arrow/Polars JSON."""
    if not _port_open(MEMGRAPH_PORT):
        pytest.skip(f"Memgraph not reachable on {HOST}:{MEMGRAPH_PORT}")

    tag = uuid.uuid4().hex[:8]
    label_a = f"ZzMgA_{tag}"
    label_extra = f"ZzMgExtra_{tag}"
    label_b = f"ZzMgB_{tag}"
    uri = f"bolt://{HOST}:{MEMGRAPH_PORT}"

    query = (
        f"MATCH p=(a:{label_a})-[k:ZK]->(b:{label_b}) "
        "RETURN a, k, p, collect(b) AS bs, {node: a} AS m"
    )

    bridge_session = Session(uri, dialect="memgraph", backend="bridge")
    native_session = Session(uri, dialect="memgraph", backend="native")
    try:
        bridge_session.execute(
            f"CREATE (:{label_a}:{label_extra} {{name:'A'}})-[:ZK {{w:1}}]->(:{label_b} {{name:'B'}})"
        )
        bridge_row = bridge_session.execute(query)[0]
        native_row = native_session.execute(query)[0]

        _assert_row_entities(
            bridge_row,
            expected_labels_a=frozenset({label_a, label_extra}),
            expected_labels_b=frozenset({label_b}),
            expect_element_id=True,  # neo4j Python driver synthesizes str(id) on Bolt v4
        )
        _assert_row_entities(
            native_row,
            expected_labels_a=frozenset({label_a, label_extra}),
            expected_labels_b=frozenset({label_b}),
            expect_element_id=False,  # Bolt v4 wire protocol has no element_id
        )
        assert bridge_row["a"].id == native_row["a"].id
        assert bridge_row["k"].id == native_row["k"].id
        assert _canonical_without_identity(bridge_row) == _canonical_without_identity(native_row)

        _assert_arrow_and_polars(
            native_session.execute_to_arrow(query),
            native_session.execute_to_polars(query),
            native_row,
        )
    finally:
        bridge_session.execute(
            f"MATCH (n) WHERE '{label_a}' IN labels(n) OR '{label_b}' IN labels(n) DETACH DELETE n"
        )
        bridge_session.close()
        native_session.close()


def test_live_falkordb_graph_entities_bridge_and_native() -> None:
    """Verifies FalkorDB bridge/native exact entity parity and Arrow/Polars JSON."""
    if not _port_open(FALKORDB_PORT):
        pytest.skip(f"FalkorDB not reachable on {HOST}:{FALKORDB_PORT}")

    falkordb_pkg = pytest.importorskip("falkordb")
    graph_name = f"zz_entities_{uuid.uuid4().hex[:8]}"
    query = "MATCH p=(a:ZzS)-[k:ZK]->(b:ZzS) RETURN a, k, p, collect(b) AS bs, {node: a} AS m"

    client = falkordb_pkg.FalkorDB(host=HOST, port=FALKORDB_PORT)
    graph = client.select_graph(graph_name)
    bridge_session = Session(graph, dialect="falkordb", backend="bridge")
    native_session = Session(
        f"redis://{HOST}:{FALKORDB_PORT}/{graph_name}", dialect="falkordb", backend="native"
    )
    try:
        bridge_session.execute("CREATE (:ZzS:ZzExtra {name:'A'})-[:ZK {w:1}]->(:ZzS {name:'B'})")
        bridge_row = bridge_session.execute(query)[0]
        native_row = native_session.execute(query)[0]

        _assert_row_entities(
            bridge_row,
            expected_labels_a=frozenset({"ZzS", "ZzExtra"}),
            expected_labels_b=frozenset({"ZzS"}),
            expect_element_id=False,
        )
        _assert_row_entities(
            native_row,
            expected_labels_a=frozenset({"ZzS", "ZzExtra"}),
            expected_labels_b=frozenset({"ZzS"}),
            expect_element_id=False,
        )
        assert bridge_row["a"] == native_row["a"]
        assert bridge_row["k"] == native_row["k"]
        assert bridge_row["p"] == native_row["p"]
        assert bridge_row["a"].to_dict() == native_row["a"].to_dict()
        assert bridge_row["k"].to_dict() == native_row["k"].to_dict()
        assert bridge_row["p"].to_dict() == native_row["p"].to_dict()

        _assert_arrow_and_polars(
            native_session.execute_to_arrow(query),
            native_session.execute_to_polars(query),
            native_row,
        )
    finally:
        bridge_session.close()
        native_session.close()
        try:
            graph.delete()
        except Exception:
            pass


def test_live_age_graph_entities_bridge_and_native() -> None:
    """Verifies Apache AGE bridge/native exact entity parity and Arrow/Polars JSON."""
    if not _port_open(AGE_PORT):
        pytest.skip(f"Apache AGE not reachable on {HOST}:{AGE_PORT}")

    psycopg = pytest.importorskip("psycopg")
    graph_name = f"zz_entities_age_{uuid.uuid4().hex[:8]}"
    uri = f"postgresql://postgres:voyagerpass123@{HOST}:{AGE_PORT}/voyager_graph"

    raw_conn = psycopg.connect(uri, autocommit=True, connect_timeout=3)
    try:
        with raw_conn.cursor() as cur:
            cur.execute("LOAD 'age';")
            cur.execute('SET search_path = ag_catalog, "$user", public;')
            cur.execute("SELECT ag_catalog.create_graph(%s);", (graph_name,))
            cur.execute(
                f"SELECT * FROM ag_catalog.cypher('{graph_name}', $$ "
                "CREATE (:ZzS {name:'A'})-[:ZK {w:1}]->(:ZzS {name:'B'}) "
                "$$) AS (res agtype);"
            )

        query = (
            f"SELECT * FROM ag_catalog.cypher('{graph_name}', $$ "
            "MATCH p=(a:ZzS)-[k:ZK]->(b:ZzS) "
            "RETURN a, k, p, collect(b) AS bs, {node: a} AS m "
            "$$) AS (a agtype, k agtype, p agtype, bs agtype, m agtype);"
        )

        bridge_session = Session(uri, dialect="age", graph_name=graph_name, backend="bridge")
        native_session = Session(uri, dialect="age", graph_name=graph_name, backend="native")
        try:
            bridge_row = bridge_session.execute(query)[0]
            native_row = native_session.execute(query)[0]

            _assert_row_entities(
                bridge_row,
                expected_labels_a=frozenset({"ZzS"}),
                expected_labels_b=frozenset({"ZzS"}),
                expect_element_id=False,
            )
            _assert_row_entities(
                native_row,
                expected_labels_a=frozenset({"ZzS"}),
                expected_labels_b=frozenset({"ZzS"}),
                expect_element_id=False,
            )
            assert bridge_row["a"] == native_row["a"]
            assert bridge_row["k"] == native_row["k"]
            assert bridge_row["p"] == native_row["p"]
            assert bridge_row["a"].to_dict() == native_row["a"].to_dict()
            assert bridge_row["k"].to_dict() == native_row["k"].to_dict()
            assert bridge_row["p"].to_dict() == native_row["p"].to_dict()

            _assert_arrow_and_polars(
                native_session.execute_to_arrow(query),
                native_session.execute_to_polars(query),
                native_row,
            )
        finally:
            bridge_session.close()
            native_session.close()
    finally:
        try:
            with raw_conn.cursor() as cur:
                cur.execute("LOAD 'age';")
                cur.execute("SELECT ag_catalog.drop_graph(%s, true);", (graph_name,))
        finally:
            raw_conn.close()
