"""Live Apache AGE test for COLLECT(n) and COLLECT(r) decoding (#350).

Runs COLLECT over vertices and relationships, a list literal and a map literal
through Voyager's Session API on Apache AGE. Session now returns decoded values;
the corpus harness decoder is applied only to keep its idempotent fallback
covered, and entities must still be recognised by their ::vertex / ::edge /
::path annotations.
"""

from __future__ import annotations

import socket
import uuid

import pytest
from conftest import get_test_host
from voyager_ogm import GraphNode, GraphRelationship, Session, create_bridge

pytestmark = pytest.mark.live

HOST = get_test_host()
PORT = 5455
AGE_URI = f"postgresql://postgres:voyagerpass123@{HOST}:{PORT}/voyager_graph"


def _is_age_reachable() -> bool:
    try:
        with socket.create_connection((HOST, PORT), timeout=1.0):
            return True
    except OSError:
        return False


def test_live_age_collect_nodes_and_relationships() -> None:
    """Verifies COLLECT(n) and COLLECT(r) execution on Apache AGE via Session."""
    if not _is_age_reachable():
        pytest.skip(f"Apache AGE not reachable on {HOST}:{PORT}")

    psycopg = pytest.importorskip("psycopg")

    graph_name = f"zz_collect_350_{uuid.uuid4().hex[:8]}"
    node_label = "zzPerson350"
    rel_type = "zzKnows350"

    raw_conn = psycopg.connect(AGE_URI, autocommit=True, connect_timeout=3)
    try:
        with raw_conn.cursor() as cur:
            cur.execute("LOAD 'age';")
            cur.execute('SET search_path = ag_catalog, "$user", public;')
            cur.execute("SELECT ag_catalog.create_graph(%s);", (graph_name,))
            cur.execute(
                f"SELECT * FROM ag_catalog.cypher('{graph_name}', $$ "
                f"CREATE (a:{node_label} {{name: 'Alice', age: 30}}), "
                f"       (b:{node_label} {{name: 'Bob', age: 25}}), "
                f"       (a)-[r:{rel_type} {{since: 2021}}]->(b) "
                f"$$) AS (res agtype);"
            )

        bridge = create_bridge(AGE_URI)
        session = Session(bridge=bridge, dialect="age", graph_name=graph_name)
        try:
            # 1. COLLECT(n)
            query_nodes = (
                f"SELECT * FROM ag_catalog.cypher('{graph_name}', $$ "
                f"MATCH (n:{node_label}) RETURN collect(n) AS persons "
                f"$$) AS (persons agtype);"
            )
            res_nodes = session.execute(query_nodes)
            assert len(res_nodes) == 1
            decoded_persons = res_nodes[0]["persons"]

            assert isinstance(decoded_persons, list)
            assert len(decoded_persons) == 2
            assert all(isinstance(p, GraphNode) for p in decoded_persons)
            names = sorted(p["name"] for p in decoded_persons)
            assert names == ["Alice", "Bob"]
            for p in decoded_persons:
                assert p.labels == frozenset({node_label})
                assert p.to_dict()["kind"] == "node"

            # 2. COLLECT(r)
            query_rels = (
                f"SELECT * FROM ag_catalog.cypher('{graph_name}', $$ "
                f"MATCH ()-[r:{rel_type}]->() RETURN collect(r) AS rels "
                f"$$) AS (rels agtype);"
            )
            res_rels = session.execute(query_rels)
            assert len(res_rels) == 1
            decoded_rels = res_rels[0]["rels"]

            assert isinstance(decoded_rels, list)
            assert len(decoded_rels) == 1
            rel = decoded_rels[0]
            assert isinstance(rel, GraphRelationship)
            assert rel.type == rel_type
            assert rel["since"] == 2021
            assert rel.to_dict()["kind"] == "relationship"

            # 3. List literal [a, r, b] (stays a list of three, NOT a path)
            query_triple = (
                f"SELECT * FROM ag_catalog.cypher('{graph_name}', $$ "
                f"MATCH (a:{node_label})-[r:{rel_type}]->(b:{node_label}) "
                f"RETURN [a, r, b] AS triple "
                f"$$) AS (triple agtype);"
            )
            res_triple = session.execute(query_triple)
            assert len(res_triple) == 1
            decoded_triple = res_triple[0]["triple"]

            assert isinstance(decoded_triple, list)
            assert len(decoded_triple) == 3
            assert isinstance(decoded_triple[0], GraphNode)
            assert decoded_triple[0]["name"] == "Alice"
            assert isinstance(decoded_triple[1], GraphRelationship)
            assert decoded_triple[1].type == rel_type
            assert isinstance(decoded_triple[2], GraphNode)
            assert decoded_triple[2]["name"] == "Bob"

            # 4. Map literal with id/label/properties keys (stays a map, NOT a node)
            query_map = (
                f"SELECT * FROM ag_catalog.cypher('{graph_name}', $$ "
                f"RETURN {{id: 1, label: 'x', properties: {{city: 'Berlin'}}}} AS m "
                f"$$) AS (m agtype);"
            )
            res_map = session.execute(query_map)
            assert len(res_map) == 1
            decoded_map = res_map[0]["m"]

            assert isinstance(decoded_map, dict)
            assert "kind" not in decoded_map
            assert decoded_map == {
                "id": 1,
                "label": "x",
                "properties": {"city": "Berlin"},
            }

        finally:
            session.close()

    finally:
        try:
            with raw_conn.cursor() as cur:
                cur.execute("LOAD 'age';")
                cur.execute("SELECT ag_catalog.drop_graph(%s, true);", (graph_name,))
            raw_conn.close()
        except Exception:
            pass
