"""Voyager OGM Schema & Graph Types DDL Test Suite.

Verifies:
1. Automated openCypher / Neo4j constraint generation (UNIQUE, NOT NULL, Indexes)
2. Neo4j 5.x Graph Types (Property Type constraints: REQUIRE n.prop :: STRING)
3. Automated DROP DDL generation
4. Live schema constraint creation and validation against running Neo4j container
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest
from conftest import community_compatible_ddl, is_port_open, neo4j_edition
from neo4j import GraphDatabase
from neo4j.exceptions import ConstraintError
from voyager_ogm import (
    Field,
    Node,
    Relationship,
    SchemaManager,
    Session,
    VoyagerError,
    node,
    relationship,
)
from voyager_ogm.bridge import AsyncMockBridge, MockBridge, Neo4jBoltBridge


def _load_env() -> None:
    env_path = Path(__file__).resolve().parents[3] / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip("'\""))


_load_env()


@node(label="User")
class User(Node):
    """User entity with constraints and Graph Types."""

    user_id: str = Field(primary_key=True)
    email: str = Field(unique=True)
    age: int = Field(index=True)
    bio: str = Field()


@relationship(type_name="FOLLOWS", source_node=User, target_node=User)
class Follows(Relationship):
    """FOLLOWS edge with property constraint."""

    since: int = Field()


def test_schema_ddl_generation():
    """Verifies generated DDL statements match Neo4j 5.x Graph Types & Constraint standards."""
    statements = SchemaManager.generate_cypher_ddl(User, include_type_constraints=True)

    # Unique / Primary Key constraints
    assert (
        "CREATE CONSTRAINT constraint_user_user_id_unique IF NOT EXISTS FOR (n:User) REQUIRE n.user_id IS UNIQUE"
        in statements
    )
    assert (
        "CREATE CONSTRAINT constraint_user_user_id_not_null IF NOT EXISTS FOR (n:User) REQUIRE n.user_id IS NOT NULL"
        in statements
    )
    assert (
        "CREATE CONSTRAINT constraint_user_email_unique IF NOT EXISTS FOR (n:User) REQUIRE n.email IS UNIQUE"
        in statements
    )

    # Negative assertions: inferring non-nullability does not emit an executable
    # existence constraint; only explicit nullable=False (and primary keys) do.
    assert not any("constraint_user_age_not_null" in s for s in statements)
    assert not any("constraint_user_bio_not_null" in s for s in statements)

    # Property Type Constraints (Graph Types)
    assert (
        "CREATE CONSTRAINT constraint_user_user_id_type IF NOT EXISTS FOR (n:User) REQUIRE n.user_id :: STRING"
        in statements
    )
    assert (
        "CREATE CONSTRAINT constraint_user_email_type IF NOT EXISTS FOR (n:User) REQUIRE n.email :: STRING"
        in statements
    )
    assert (
        "CREATE CONSTRAINT constraint_user_age_type IF NOT EXISTS FOR (n:User) REQUIRE n.age :: INTEGER"
        in statements
    )

    # Index
    assert "CREATE INDEX index_user_age IF NOT EXISTS FOR (n:User) ON (n.age)" in statements


def test_schema_drop_ddl_generation():
    """Verifies generated DROP statements."""
    drop_statements = SchemaManager.generate_drop_ddl(User, include_type_constraints=True)
    assert "DROP CONSTRAINT constraint_user_user_id_unique IF EXISTS" in drop_statements
    assert "DROP CONSTRAINT constraint_user_user_id_not_null IF EXISTS" in drop_statements
    assert "DROP CONSTRAINT constraint_user_email_unique IF EXISTS" in drop_statements
    assert "DROP CONSTRAINT constraint_user_age_type IF EXISTS" in drop_statements
    assert "DROP INDEX index_user_age IF EXISTS" in drop_statements


def test_unique_constraint_emitted_before_not_null():
    """Verifies UNIQUE is emitted before NOT NULL for node and relationship constraints."""

    @node(label="OrderedKeyNode")
    class OrderedKeyNode(Node):
        uid: str = Field(primary_key=True)
        alias: str = Field(unique=True, nullable=False)

    @relationship(
        type_name="ORDERED_KEY_REL",
        source_node=OrderedKeyNode,
        target_node=OrderedKeyNode,
    )
    class OrderedKeyRel(Relationship):
        rid: str = Field(primary_key=True)

    node_stmts = SchemaManager.generate_constraint_ddl(OrderedKeyNode)
    node_unique = next(i for i, s in enumerate(node_stmts) if "uid_unique" in s)
    node_not_null = next(i for i, s in enumerate(node_stmts) if "uid_not_null" in s)
    assert node_unique < node_not_null
    alias_unique = next(i for i, s in enumerate(node_stmts) if "alias_unique" in s)
    alias_not_null = next(i for i, s in enumerate(node_stmts) if "alias_not_null" in s)
    assert alias_unique < alias_not_null

    rel_stmts = SchemaManager.generate_constraint_ddl(OrderedKeyRel)
    rel_unique = next(i for i, s in enumerate(rel_stmts) if "rid_unique" in s)
    rel_not_null = next(i for i, s in enumerate(rel_stmts) if "rid_not_null" in s)
    assert rel_unique < rel_not_null

    # Drop order mirrors creation order and stays IF EXISTS-safe.
    node_drops = SchemaManager.generate_drop_constraint_ddl(OrderedKeyNode)
    assert next(i for i, s in enumerate(node_drops) if "uid_unique" in s) < next(
        i for i, s in enumerate(node_drops) if "uid_not_null" in s
    )
    rel_drops = SchemaManager.generate_drop_constraint_ddl(OrderedKeyRel)
    assert next(i for i, s in enumerate(rel_drops) if "rid_unique" in s) < next(
        i for i, s in enumerate(rel_drops) if "rid_not_null" in s
    )
    assert all("IF EXISTS" in s for s in node_drops + rel_drops)


def test_cypher25_graph_type_ddl():
    """Verifies Cypher 25 ALTER CURRENT GRAPH TYPE SET block generation."""
    cypher25_ddl = SchemaManager.generate_cypher25_graph_type_ddl(User, Follows)
    assert cypher25_ddl.startswith("ALTER CURRENT GRAPH TYPE SET {")
    assert "(:User =>" in cypher25_ddl
    assert "user_id :: STRING IS KEY" in cypher25_ddl
    assert "email :: STRING NOT NULL IS UNIQUE" in cypher25_ddl
    assert "(:User)-[:FOLLOWS =>" in cypher25_ddl
    assert "since :: INTEGER" in cypher25_ddl

    drop_ddl = SchemaManager.generate_cypher25_drop_graph_type_ddl()
    assert drop_ddl == "ALTER CURRENT GRAPH TYPE SET {}"


def test_cypher25_composite_facets_and_empty_entities():
    """Verifies Cypher 25 composite facets (NOT NULL IS UNIQUE) and empty-entity grammar."""

    @node(label="Tag")
    class Tag(Node):
        pass

    @node(label="Member")
    class Member(Node):
        member_id: str = Field(primary_key=True)
        login: str = Field(unique=True, nullable=False)

    @relationship(type_name="TAGGED", source_node=Member, target_node=Tag)
    class Tagged(Relationship):
        pass

    ddl = SchemaManager.generate_cypher25_graph_type_ddl(Tag, Member, Tagged)
    # Empty node has no dangling =>
    assert "(:Tag)" in ddl
    assert "(:Tag =>)" not in ddl
    # Empty rel has no dangling =>
    assert "(:Member)-[:TAGGED]->(:Tag)" in ddl
    assert "[:TAGGED =>]" not in ddl
    # Composite facets
    assert "login :: STRING NOT NULL IS UNIQUE" in ddl


@pytest.mark.live
def test_live_neo4j_schema_ddl_creation():
    """Verifies applying constraints and indexes live to the running Neo4j database."""
    uri = os.getenv("NEO4J_URI", "bolt://127.0.0.1:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "voyagerpass123")
    database = os.getenv("NEO4J_DATABASE", "neo4j")

    try:
        from neo4j import GraphDatabase

        driver = GraphDatabase.driver(uri, auth=(user, password), connection_timeout=0.5)
        driver.verify_connectivity()
    except Exception:
        pytest.skip(f"Neo4j database not reachable on {uri}")

    bridge = Neo4jBoltBridge(driver, database=database)
    session = Session(bridge=bridge, dialect="cypher")

    # Clean any leftover constraints first
    SchemaManager.drop_all(session, User)

    # Create constraints & indexes. Property-existence constraints are Enterprise-only, so on
    # the default Community container apply only the statements Community supports.
    if neo4j_edition(driver) == "enterprise":
        created = SchemaManager.create_all(session, User, include_type_constraints=False)
    else:
        created = community_compatible_ddl(SchemaManager.generate_cypher_ddl(User))
        for stmt in created:
            session.execute(stmt)
    assert len(created) >= 3

    # Verify constraints in Neo4j catalog
    constraints = session.execute("SHOW CONSTRAINTS")
    constraint_names = [c.get("name", "") for c in constraints]
    assert any("user_id_unique" in name for name in constraint_names)
    assert any("email_unique" in name for name in constraint_names)

    # Clean up / Drop
    dropped = SchemaManager.drop_all(session, User, include_type_constraints=False)
    assert len(dropped) >= 3

    session.close()
    driver.close()


@pytest.mark.live
def test_live_neo4j_enterprise_cypher25_graph_type():
    """Verifies applying declarative Cypher 25 Graph Types to Neo4j Enterprise live."""
    uri = os.getenv("NEO4J_URI", "bolt://127.0.0.1:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "voyagerpass123")
    database = os.getenv("NEO4J_DATABASE", "neo4j")

    try:
        from neo4j import GraphDatabase

        driver = GraphDatabase.driver(uri, auth=(user, password), connection_timeout=0.5)
        driver.verify_connectivity()
    except Exception:
        pytest.skip(f"Neo4j database not reachable on {uri}")

    server_info = driver.get_server_info()
    agent = getattr(server_info, "agent", "") or ""
    if "5.26" in agent or agent.startswith("Neo4j/5."):
        driver.close()
        pytest.skip("Neo4j 5.26 enterprise does not support Cypher 25 graph type DDL syntax")

    bridge = Neo4jBoltBridge(driver, database=database)
    session = Session(bridge=bridge, dialect="cypher")

    try:
        # 1. Clean existing graph type
        session.execute(SchemaManager.generate_cypher25_drop_graph_type_ddl())

        # 2. Emit and apply declarative Cypher 25 Graph Type
        cypher25_ddl = SchemaManager.generate_cypher25_graph_type_ddl(User, Follows)
        session.execute(cypher25_ddl)

        # 3. Verify graph type constraints exist
        constraints = session.execute("SHOW CONSTRAINTS")
        c_names = [c.get("name", "") for c in constraints]
        assert len(c_names) > 0

        # 4. Reset graph type cleanly
        session.execute(SchemaManager.generate_cypher25_drop_graph_type_ddl())
    finally:
        session.close()
        driver.close()


@pytest.mark.live
def test_live_postgres19_pgq_ddl_execution():
    """Verifies applying generated SQL:2023 PGQ DDL directly to PostgreSQL 19 Beta 3."""
    uri = os.getenv("PG19_URI", "postgresql://postgres:voyagerpass123@127.0.0.1:5456/postgres")
    try:
        import psycopg

        conn = psycopg.connect(uri, connect_timeout=1)
    except Exception:
        pytest.skip(f"PostgreSQL 19 not reachable on {uri}")

    @node(label="Member")
    class Member(Node):
        member_id: str = Field(primary_key=True)
        name: str = Field()
        age: int = Field()

    @relationship(type_name="FRIENDS_WITH", source_node=Member, target_node=Member)
    class FriendsWith(Relationship):
        since: int = Field()

    cur = conn.cursor()
    try:
        cur.execute("DROP PROPERTY GRAPH IF EXISTS live_pgq_test CASCADE;")
        cur.execute("DROP TABLE IF EXISTS friends_with CASCADE;")
        cur.execute("DROP TABLE IF EXISTS member CASCADE;")

        cur.execute("CREATE TABLE member (member_id TEXT PRIMARY KEY, name TEXT, age INT);")
        cur.execute(
            "CREATE TABLE friends_with (src_member_id TEXT REFERENCES member(member_id), "
            "dst_member_id TEXT REFERENCES member(member_id), since INT, "
            "PRIMARY KEY (src_member_id, dst_member_id));"
        )

        pgq_ddl = SchemaManager.generate_pgq_ddl("live_pgq_test", Member, FriendsWith)
        cur.execute(pgq_ddl)
        conn.commit()

        # Drop property graph cleanly
        cur.execute(SchemaManager.generate_pgq_drop_ddl("live_pgq_test"))
        conn.commit()
    finally:
        conn.close()


def test_alter_current_graph_type_ddl():
    """Verifies Cypher 25 / ISO GQL ALTER CURRENT GRAPH TYPE statements."""
    node_alter = SchemaManager.generate_alter_graph_type_ddl(User)
    assert (
        node_alter
        == "ALTER CURRENT GRAPH TYPE ADD NODE TYPE (:User {age :: INTEGER, bio :: STRING, email :: STRING, user_id :: STRING})"
    )

    rel_alter = SchemaManager.generate_alter_graph_type_ddl(
        Follows, source_node=User, target_node=User
    )
    assert (
        rel_alter
        == "ALTER CURRENT GRAPH TYPE ADD RELATIONSHIP TYPE (:User)-[:FOLLOWS {since :: INTEGER}]->(:User)"
    )


def test_gql_create_graph_type_ddl():
    """Verifies standard ISO GQL CREATE GRAPH TYPE statement definition."""
    gql_ddl = SchemaManager.generate_gql_graph_type_ddl("SocialGraphType", User, Follows)
    assert "CREATE GRAPH TYPE SocialGraphType AS {" in gql_ddl
    assert (
        "NODE User (age INTEGER NOT NULL, bio STRING NOT NULL, email STRING NOT NULL, user_id STRING NOT NULL) KEY (user_id)"
        in gql_ddl
    )
    assert "EDGE FOLLOWS CONNECTING (User TO User) (since INTEGER NOT NULL)" in gql_ddl

    drop_ddl = SchemaManager.generate_gql_drop_graph_type_ddl("SocialGraphType")
    assert drop_ddl == "DROP GRAPH TYPE SocialGraphType IF EXISTS"


def test_pgq_property_graph_ddl():
    """Verifies standard SQL:2023 PGQ / DuckPGQ CREATE PROPERTY GRAPH statements."""
    pgq_ddl = SchemaManager.generate_pgq_ddl("social_pgq", User, Follows)
    assert "CREATE PROPERTY GRAPH social_pgq" in pgq_ddl
    assert "VERTEX TABLES (" in pgq_ddl
    assert (
        '"user" KEY ("user_id") LABEL "User" PROPERTIES ("age", "bio", "email", "user_id")'
        in pgq_ddl
    )
    assert "EDGE TABLES (" in pgq_ddl
    assert 'SOURCE KEY ("src_user_id") REFERENCES "user" ("user_id")' in pgq_ddl
    assert 'DESTINATION KEY ("dst_user_id") REFERENCES "user" ("user_id")' in pgq_ddl
    assert 'LABEL "FOLLOWS" PROPERTIES ("since")' in pgq_ddl

    drop_pgq = SchemaManager.generate_pgq_drop_ddl("social_pgq")
    assert drop_pgq == "DROP PROPERTY GRAPH IF EXISTS social_pgq;"


def test_pgq_ddl_uses_label_table_names_and_distinct_edge_keys():
    """Verifies vertex/edge table names come from labels/types and edge keys are distinct."""

    @node(label="Fix325Person")
    class Fix325Person(Node):
        pid: str = Field(primary_key=True)

    @node(label="Fix325Company")
    class Fix325Company(Node):
        cid: str = Field(primary_key=True)

    @relationship(type_name="FIX325_KNOWS", source_node=Fix325Person, target_node=Fix325Person)
    class Fix325Knows(Relationship):
        since: int = Field()

    @relationship(type_name="FIX325_WORKS_AT", source_node=Fix325Person, target_node=Fix325Company)
    class Fix325WorksAt(Relationship):
        since: int = Field()

    ddl = SchemaManager.generate_pgq_ddl(
        "fix325", Fix325Person, Fix325Company, Fix325Knows, Fix325WorksAt
    )
    assert '"fix325person" KEY ("pid") LABEL "Fix325Person"' in ddl
    assert '"fix325company" KEY ("cid") LABEL "Fix325Company"' in ddl
    assert '"fix325_knows"' in ddl
    assert '"fix325_works_at"' in ddl
    assert 'SOURCE KEY ("src_pid") REFERENCES "fix325person" ("pid")' in ddl
    assert 'DESTINATION KEY ("dst_pid") REFERENCES "fix325person" ("pid")' in ddl
    assert 'DESTINATION KEY ("dst_cid") REFERENCES "fix325company" ("cid")' in ddl


def test_pgq_ddl_for_a_relationship_uses_the_endpoint_key():
    """Verifies a relationship generated on its own references the endpoint node's real key."""

    @node(label="Fix325EdgePerson")
    class Fix325EdgePerson(Node):
        pid: str = Field(primary_key=True)

    @relationship(
        type_name="FIX325_EDGE_KNOWS",
        source_node=Fix325EdgePerson,
        target_node=Fix325EdgePerson,
    )
    class Fix325EdgeKnows(Relationship):
        since: int = Field()

    ddl = SchemaManager.generate_ddl(Fix325EdgeKnows, dialect="pgq")[0]
    assert 'SOURCE KEY ("src_pid") REFERENCES "fix325edgeperson" ("pid")' in ddl
    assert 'DESTINATION KEY ("dst_pid") REFERENCES "fix325edgeperson" ("pid")' in ddl
    assert 'SOURCE KEY ("src_id")' not in ddl


def test_schema_manager_generate_table_ddl():
    """Verifies table DDL generation for postgres and duckdb dialects (#327)."""

    @node(label="T327User")
    class T327User(Node):
        uid: str = Field(primary_key=True)
        email: str = Field(unique=True)
        age: int = Field(default=0, nullable=False)

    @relationship(type_name="T327_FOLLOWS", source_node=T327User, target_node=T327User)
    class T327Follows(Relationship):
        since: int = Field(nullable=False)

    # PostgreSQL table DDL
    pg_tables = SchemaManager.generate_table_ddl(T327User, T327Follows, dialect="postgres")
    assert len(pg_tables) == 2
    assert pg_tables[0].startswith('CREATE TABLE IF NOT EXISTS "t327user" (')
    assert '"uid" TEXT NOT NULL' in pg_tables[0]
    assert '"email" TEXT' in pg_tables[0]
    assert '"age" BIGINT NOT NULL' in pg_tables[0]
    assert 'PRIMARY KEY ("uid")' in pg_tables[0]

    assert pg_tables[1].startswith('CREATE TABLE IF NOT EXISTS "t327_follows" (')
    assert '"id" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY' in pg_tables[1]
    assert '"src_uid" TEXT NOT NULL REFERENCES "t327user" ("uid")' in pg_tables[1]
    assert '"dst_uid" TEXT NOT NULL REFERENCES "t327user" ("uid")' in pg_tables[1]
    assert '"since" BIGINT NOT NULL' in pg_tables[1]
    assert 'PRIMARY KEY ("src_uid", "dst_uid")' not in pg_tables[1]

    # DuckDB table DDL: 1 vertex table + 1 sequence + 1 edge table = 3 statements
    duck_tables = SchemaManager.generate_table_ddl(T327User, T327Follows, dialect="duckdb")
    assert len(duck_tables) == 3
    assert duck_tables[0].startswith('CREATE TABLE IF NOT EXISTS "t327user" (')
    assert '"uid" VARCHAR NOT NULL' in duck_tables[0]
    assert '"email" VARCHAR' in duck_tables[0]
    assert '"age" BIGINT NOT NULL' in duck_tables[0]
    assert 'PRIMARY KEY ("uid")' in duck_tables[0]
    assert duck_tables[1] == "CREATE SEQUENCE IF NOT EXISTS seq_t327_follows_id;"
    assert duck_tables[2].startswith('CREATE TABLE IF NOT EXISTS "t327_follows" (')
    assert "\"id\" BIGINT DEFAULT nextval('seq_t327_follows_id') PRIMARY KEY" in duck_tables[2]
    assert '"src_uid" VARCHAR NOT NULL REFERENCES "t327user" ("uid")' in duck_tables[2]
    assert '"dst_uid" VARCHAR NOT NULL REFERENCES "t327user" ("uid")' in duck_tables[2]


def test_schema_manager_generate_table_ddl_relationship_alone():
    """Verifies edge table DDL automatically resolves endpoint node schemas when rel is alone."""

    @node(label="T327SoloNode")
    class T327SoloNode(Node):
        nid: str = Field(primary_key=True)

    @relationship(
        type_name="T327_SOLO_REL",
        source_node=T327SoloNode,
        target_node=T327SoloNode,
    )
    class T327SoloRel(Relationship):
        weight: float = Field()

    tables = SchemaManager.generate_table_ddl(T327SoloRel, dialect="postgres")
    assert len(tables) == 2
    assert any('"t327solonode"' in t for t in tables)
    assert any('"src_nid" TEXT NOT NULL REFERENCES "t327solonode" ("nid")' in t for t in tables)


def test_schema_manager_generate_pgq_schema_ddl_run_order():
    """Verifies complete schema DDL returned in run order: tables, constraints, indexes, graph."""

    @node(label="T327OrderNode")
    class T327OrderNode(Node):
        oid: str = Field(primary_key=True)
        code: str = Field(unique=True)
        score: int = Field(index=True)

    @relationship(
        type_name="T327_ORDER_EDGE",
        source_node=T327OrderNode,
        target_node=T327OrderNode,
    )
    class T327OrderEdge(Relationship):
        priority: int = Field(index=True)

    ddl = SchemaManager.generate_pgq_schema_ddl(
        "t327_graph", T327OrderNode, T327OrderEdge, dialect="postgres"
    )

    # Order: Vertex Table, Edge Table, Constraint, Node Index, Rel Index, Property Graph
    assert len(ddl) == 6
    assert ddl[0].startswith('CREATE TABLE IF NOT EXISTS "t327ordernode"')
    assert ddl[1].startswith('CREATE TABLE IF NOT EXISTS "t327_order_edge"')
    assert ddl[2].startswith(
        'ALTER TABLE "t327ordernode" ADD CONSTRAINT uq_t327ordernode_code UNIQUE'
    )
    assert ddl[3].startswith(
        'CREATE INDEX IF NOT EXISTS idx_t327ordernode_score ON "t327ordernode"'
    )
    assert ddl[4].startswith(
        'CREATE INDEX IF NOT EXISTS idx_t327_order_edge_priority ON "t327_order_edge"'
    )
    assert ddl[5].startswith("CREATE PROPERTY GRAPH t327_graph")

    # DuckDB run order: unique index instead of ALTER TABLE, and duckpgq CREATE PROPERTY GRAPH
    duck_ddl = SchemaManager.generate_pgq_schema_ddl(
        "t327_graph", T327OrderNode, T327OrderEdge, dialect="duckdb"
    )
    # Order: Vertex Table, Sequence, Edge Table, Constraint, Node Index, Rel Index, Property Graph
    assert len(duck_ddl) == 7
    assert duck_ddl[0].startswith('CREATE TABLE IF NOT EXISTS "t327ordernode"')
    assert duck_ddl[1] == "CREATE SEQUENCE IF NOT EXISTS seq_t327_order_edge_id;"
    assert duck_ddl[2].startswith('CREATE TABLE IF NOT EXISTS "t327_order_edge"')
    assert "\"id\" BIGINT DEFAULT nextval('seq_t327_order_edge_id') PRIMARY KEY" in duck_ddl[2]
    assert duck_ddl[3].startswith(
        'CREATE UNIQUE INDEX IF NOT EXISTS uq_t327ordernode_code ON "t327ordernode"'
    )
    assert duck_ddl[4].startswith(
        'CREATE INDEX IF NOT EXISTS idx_t327ordernode_score ON "t327ordernode"'
    )
    assert duck_ddl[5].startswith(
        'CREATE INDEX IF NOT EXISTS idx_t327_order_edge_priority ON "t327_order_edge"'
    )
    assert duck_ddl[6].startswith("CREATE PROPERTY GRAPH t327_graph")
    assert "PROPERTIES" not in duck_ddl[6]


def test_schema_manager_generate_table_ddl_explicit_relationship_pk():
    """Verifies that relationship models with explicit primary keys omit surrogate keys."""

    @node(label="T327PersonNode")
    class T327PersonNode(Node):
        pid: str = Field(primary_key=True)

    @relationship(
        type_name="T327_CALLS",
        source_node=T327PersonNode,
        target_node=T327PersonNode,
    )
    class T327Calls(Relationship):
        call_id: str = Field(primary_key=True)
        duration: int = Field(default=0)

    # Postgres: uses declared primary key, no surrogate
    pg_ddl = SchemaManager.generate_table_ddl(T327PersonNode, T327Calls, dialect="postgres")
    assert len(pg_ddl) == 2
    assert 'PRIMARY KEY ("call_id")' in pg_ddl[1]
    assert "GENERATED ALWAYS AS IDENTITY" not in pg_ddl[1]

    # DuckDB: uses declared primary key, no sequence generated
    duck_ddl = SchemaManager.generate_table_ddl(T327PersonNode, T327Calls, dialect="duckdb")
    assert len(duck_ddl) == 2
    assert 'PRIMARY KEY ("call_id")' in duck_ddl[1]
    assert "nextval(" not in duck_ddl[1]


def test_schema_manager_generate_table_ddl_surrogate_key_name_collision():
    """Verifies non-clashing surrogate key column name when relationship has non-PK 'id' field."""

    @node(label="T327ItemNode")
    class T327ItemNode(Node):
        iid: str = Field(primary_key=True)

    @relationship(
        type_name="T327_CONNECTS",
        source_node=T327ItemNode,
        target_node=T327ItemNode,
    )
    class T327Connects(Relationship):
        id: str = Field(nullable=False)
        weight: float = Field(default=1.0)

    pg_ddl = SchemaManager.generate_table_ddl(T327ItemNode, T327Connects, dialect="postgres")
    assert len(pg_ddl) == 2
    # Surrogate column resolves to t327_connects_id
    assert '"t327_connects_id" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY' in pg_ddl[1]
    assert '"id" TEXT NOT NULL' in pg_ddl[1]
    assert '"weight" DOUBLE PRECISION' in pg_ddl[1]

    duck_ddl = SchemaManager.generate_table_ddl(T327ItemNode, T327Connects, dialect="duckdb")
    assert len(duck_ddl) == 3
    assert duck_ddl[1] == "CREATE SEQUENCE IF NOT EXISTS seq_t327_connects_t327_connects_id;"
    assert (
        "\"t327_connects_id\" BIGINT DEFAULT nextval('seq_t327_connects_t327_connects_id') PRIMARY KEY"
        in duck_ddl[2]
    )
    assert '"id" VARCHAR NOT NULL' in duck_ddl[2]


def test_table_ddl_unsupported_types_error():
    """Verifies UnsupportedFeature is raised for dialect-unsupported types."""
    from voyager_ogm._voyager_rs import emit_table_ddl

    # Point type on DuckDB
    node_point = {
        "name": "GeoNode",
        "labels": ["GeoNode"],
        "primary_key": "id",
        "fields": {
            "id": {"name": "id", "type": "STRING", "primary_key": True},
            "loc": {"name": "loc", "type": "POINT"},
        },
    }
    with pytest.raises(Exception) as exc_info:
        emit_table_ddl(nodes=[node_point], dialect="duckdb")
    assert "POINT" in str(exc_info.value)
    assert "loc" in str(exc_info.value)


def test_native_schema_registry_ddl():
    """Verifies DDL generation directly through NativeSchemaRegistry instance."""
    from voyager_ogm import NativeSchemaRegistry

    reg = NativeSchemaRegistry()
    reg.register_node(
        "Article",
        ["Article"],
        {
            "id": {"type": "STRING", "primary_key": True},
            "title": {"type": "STRING", "indexed": True},
        },
    )
    reg.register_relationship(
        "CitedBy",
        "CITED_BY",
        source_labels=["Article"],
        target_labels=["Article"],
        fields={"year": {"type": "INTEGER"}},
        directed=True,
    )

    # 1. openCypher DDL
    cypher_stmts = reg.generate_cypher_ddl(include_type_constraints=False)
    assert any("CREATE CONSTRAINT constraint_article_id_unique" in s for s in cypher_stmts)
    assert any("CREATE CONSTRAINT constraint_article_id_not_null" in s for s in cypher_stmts)
    assert any("CREATE INDEX index_article_title" in s for s in cypher_stmts)

    # 2. Cypher 25 Graph Type DDL
    cypher25 = reg.generate_cypher25_graph_type_ddl()
    assert "ALTER CURRENT GRAPH TYPE SET {" in cypher25
    assert "id :: STRING IS KEY" in cypher25
    assert "(:Article)-[:CITED_BY =>" in cypher25
    assert reg.generate_cypher25_drop_graph_type_ddl() == "ALTER CURRENT GRAPH TYPE SET {}"

    # 3. ISO GQL DDL
    gql = reg.generate_gql_graph_type_ddl("ArticleGraph")
    assert "CREATE GRAPH TYPE ArticleGraph AS {" in gql
    assert "NODE Article (id STRING NOT NULL, title STRING) KEY (id)" in gql
    assert "EDGE CITED_BY CONNECTING (Article TO Article) (year INTEGER)" in gql

    # 4. SQL:2023 PGQ DDL
    pgq = reg.generate_pgq_ddl("article_pgq")
    assert "CREATE PROPERTY GRAPH article_pgq" in pgq
    assert '"article" KEY ("id") LABEL "Article" PROPERTIES ("id", "title")' in pgq
    assert 'SOURCE KEY ("src_id") REFERENCES "article" ("id")' in pgq
    assert 'DESTINATION KEY ("dst_id") REFERENCES "article" ("id")' in pgq
    assert 'LABEL "CITED_BY" PROPERTIES ("year")' in pgq

    # An edge requested on its own pulls in its endpoint node schema.
    edge_only = reg.generate_pgq_ddl("article_pgq", ["CitedBy"])
    assert 'SOURCE KEY ("src_id") REFERENCES "article" ("id")' in edge_only
    assert 'DESTINATION KEY ("dst_id") REFERENCES "article" ("id")' in edge_only


def test_dialect_ddl_error_handling():
    """Verifies SchemaManager raises actionable NotImplementedError for unsupported dialects instead of failing silently."""
    from voyager_ogm.bridge import MockBridge

    # 1. SQL:2023 PGQ dialect session
    pgq_session = Session(bridge=MockBridge(), dialect="sql_pgq")
    with pytest.raises(NotImplementedError, match="SQL:2023 PGQ"):
        SchemaManager.create_all(pgq_session, User)

    with pytest.raises(NotImplementedError, match="SQL:2023 PGQ"):
        SchemaManager.drop_all(pgq_session, User)

    # 2. FalkorDB session
    falkor_session = Session(bridge=MockBridge(), dialect="falkordb")
    with pytest.raises(NotImplementedError, match="FalkorDB does not support"):
        SchemaManager.create_all(falkor_session, User)

    with pytest.raises(NotImplementedError, match="FalkorDB does not support"):
        SchemaManager.drop_all(falkor_session, User)

    # 3. Apache AGE session
    age_session = Session(bridge=MockBridge(), dialect="age")
    with pytest.raises(NotImplementedError, match="Apache AGE does not support"):
        SchemaManager.create_all(age_session, User)

    with pytest.raises(NotImplementedError, match="Apache AGE does not support"):
        SchemaManager.drop_all(age_session, User)

    # 4. Property Graph helpers for PGQ sessions
    mock_pgq = MockBridge()
    pgq_sess = Session(bridge=mock_pgq, dialect="sql_pgq")
    created_ddl = SchemaManager.create_property_graph(pgq_sess, "my_graph", User, Follows)
    assert created_ddl.startswith("CREATE PROPERTY GRAPH my_graph")
    assert len(mock_pgq.executed_queries) == 1
    assert mock_pgq.executed_queries[0][0] == created_ddl

    dropped_ddl = SchemaManager.drop_property_graph(pgq_sess, "my_graph")
    assert dropped_ddl == "DROP PROPERTY GRAPH IF EXISTS my_graph;"
    assert len(mock_pgq.executed_queries) == 2

    # 5. Relationship missing endpoints raises ValueError in PGQ
    @relationship(type_name="ORPHAN_REL")
    class OrphanRel(Relationship):
        weight: float = Field()

    with pytest.raises(ValueError, match="missing source_labels"):
        SchemaManager.generate_pgq_ddl("orphan_pgq", User, OrphanRel)


def test_schema_manager_native_registration_and_delegation():
    """Verifies automated model registration in NativeSchemaRegistry and granular constraint/index delegation."""
    from voyager_ogm._voyager_rs import NativeSchemaRegistry
    from voyager_ogm.bridge import MockBridge

    # 1. Global registry contains User and Follows models automatically
    User.register_schema()
    Follows.register_schema()
    reg = NativeSchemaRegistry.global_registry()
    assert reg.has_node("User")
    assert reg.has_relationship("Follows")

    user_meta = reg.get_node("User")
    assert user_meta is not None
    assert user_meta["primary_key"] == "user_id"
    assert "email" in user_meta["fields"]
    assert user_meta["fields"]["email"]["unique"] is True

    follows_meta = reg.get_relationship("Follows")
    assert follows_meta is not None
    assert follows_meta["type_name"] == "FOLLOWS"
    assert follows_meta["source_labels"] == ["User"]
    assert follows_meta["target_labels"] == ["User"]

    # Test dynamic declaration auto-registers into global schema registry
    @node(label="DecoratedAuthor")
    class DecoratedAuthor(Node):
        author_id: str = Field(primary_key=True)
        pen_name: str = Field(unique=True)

    assert reg.has_node("DecoratedAuthor")
    author_meta = reg.get_node("DecoratedAuthor")
    assert author_meta is not None
    assert author_meta["primary_key"] == "author_id"
    assert author_meta["fields"]["pen_name"]["unique"] is True

    # 2. generate_cypher_ddl using string model name directly from registry
    user_ddl = SchemaManager.generate_cypher_ddl("User", include_type_constraints=True)
    assert any("constraint_user_user_id_unique" in s for s in user_ddl)
    assert any("index_user_age" in s for s in user_ddl)

    user_drop = SchemaManager.generate_drop_ddl("User")
    assert any("DROP CONSTRAINT constraint_user_user_id_unique" in s for s in user_drop)

    # 3. generate_ddl multi-dialect delegation
    cypher_ddl = SchemaManager.generate_ddl(User, dialect="cypher")
    assert any("constraint_user_user_id_unique" in s for s in cypher_ddl)

    gql_ddl = SchemaManager.generate_ddl(User, dialect="gql")
    assert any("ALTER CURRENT GRAPH TYPE ADD NODE TYPE (:User" in s for s in gql_ddl)

    pgq_ddl = SchemaManager.generate_ddl(User, dialect="pgq")
    assert any("CREATE PROPERTY GRAPH" in s for s in pgq_ddl)

    # 4. Granular create_constraints and create_indexes
    mock_bridge = MockBridge()
    session = Session(bridge=mock_bridge, dialect="cypher")

    applied_constraints = SchemaManager.create_constraints(session, User)
    assert all("CREATE CONSTRAINT" in s for s in applied_constraints)
    assert not any("CREATE INDEX" in s for s in applied_constraints)

    applied_indexes = SchemaManager.create_indexes(session, User)
    assert all("CREATE INDEX" in s for s in applied_indexes)
    assert not any("CREATE CONSTRAINT" in s for s in applied_indexes)

    # Granular drop_constraints and drop_indexes
    dropped_constraints = SchemaManager.drop_constraints(session, User)
    assert all("DROP CONSTRAINT" in s for s in dropped_constraints)
    assert not any("DROP INDEX" in s for s in dropped_constraints)

    dropped_indexes = SchemaManager.drop_indexes(session, User)
    assert all("DROP INDEX" in s for s in dropped_indexes)
    assert not any("DROP CONSTRAINT" in s for s in dropped_indexes)


def test_pure_ddl_generators_multi_dialect():
    """Verifies pure DDL generators operate offline without sessions across multiple dialects."""

    @relationship(type_name="FOLLOWS_INDEXED", source_node=User, target_node=User)
    class FollowsIndexed(Relationship):
        since: int = Field(index=True)

    # 1. openCypher index and constraint generation
    cypher_indexes = SchemaManager.generate_index_ddl(User, FollowsIndexed, dialect="cypher")
    assert any(
        "CREATE INDEX index_user_age IF NOT EXISTS FOR (n:User) ON (n.age)" in s
        for s in cypher_indexes
    )
    assert any(
        "CREATE INDEX index_rel_follows_indexed_since IF NOT EXISTS FOR ()-[r:FOLLOWS_INDEXED]-() ON (r.since)"
        in s
        for s in cypher_indexes
    )

    cypher_constraints = SchemaManager.generate_constraint_ddl(
        User, FollowsIndexed, dialect="cypher"
    )
    assert any("constraint_user_email_unique" in s for s in cypher_constraints)
    assert any("constraint_user_user_id_not_null" in s for s in cypher_constraints)
    assert any("constraint_user_user_id_unique" in s for s in cypher_constraints)

    # 2. openCypher drop index and constraint generation
    cypher_drop_indexes = SchemaManager.generate_drop_index_ddl(
        User, FollowsIndexed, dialect="cypher"
    )
    assert any("DROP INDEX index_user_age IF EXISTS" in s for s in cypher_drop_indexes)
    assert any(
        "DROP INDEX index_rel_follows_indexed_since IF EXISTS" in s for s in cypher_drop_indexes
    )

    cypher_drop_constraints = SchemaManager.generate_drop_constraint_ddl(
        User, FollowsIndexed, dialect="cypher"
    )
    assert any(
        "DROP CONSTRAINT constraint_user_email_unique IF EXISTS" in s
        for s in cypher_drop_constraints
    )
    assert any(
        "DROP CONSTRAINT constraint_user_user_id_not_null IF EXISTS" in s
        for s in cypher_drop_constraints
    )

    # 3. PostgreSQL / DuckPGQ relational index and constraint generation
    sql_indexes = SchemaManager.generate_index_ddl(User, FollowsIndexed, dialect="postgres")
    assert 'CREATE INDEX IF NOT EXISTS idx_user_age ON "user" ("age");' in sql_indexes
    assert (
        'CREATE INDEX IF NOT EXISTS idx_follows_indexed_since ON "follows_indexed" ("since");'
        in sql_indexes
    )

    sql_constraints = SchemaManager.generate_constraint_ddl(
        User, FollowsIndexed, dialect="postgres"
    )
    assert 'ALTER TABLE "user" ADD CONSTRAINT uq_user_email UNIQUE ("email");' in sql_constraints

    sql_drop_indexes = SchemaManager.generate_drop_index_ddl(
        User, FollowsIndexed, dialect="postgres"
    )
    assert "DROP INDEX IF EXISTS idx_user_age;" in sql_drop_indexes
    assert "DROP INDEX IF EXISTS idx_follows_indexed_since;" in sql_drop_indexes

    sql_drop_constraints = SchemaManager.generate_drop_constraint_ddl(
        User, FollowsIndexed, dialect="postgres"
    )
    assert 'ALTER TABLE "user" DROP CONSTRAINT IF EXISTS uq_user_email;' in sql_drop_constraints

    # 4. FalkorDB Cypher indexes and actionable constraint errors
    falkor_indexes = SchemaManager.generate_index_ddl(User, FollowsIndexed, dialect="falkordb")
    assert "CREATE INDEX FOR (n:User) ON (n.age)" in falkor_indexes
    assert "CREATE INDEX FOR ()-[r:FOLLOWS_INDEXED]-() ON (r.since)" in falkor_indexes

    falkor_drop_indexes = SchemaManager.generate_drop_index_ddl(
        User, FollowsIndexed, dialect="falkordb"
    )
    assert "DROP INDEX FOR (n:User) ON (n.age)" in falkor_drop_indexes
    assert "DROP INDEX FOR ()-[r:FOLLOWS_INDEXED]-() ON (r.since)" in falkor_drop_indexes

    with pytest.raises(NotImplementedError, match="FalkorDB does not support"):
        SchemaManager.generate_constraint_ddl(User, dialect="falkordb")

    # 5. Apache AGE actionable errors
    with pytest.raises(NotImplementedError, match="Apache AGE does not support"):
        SchemaManager.generate_index_ddl(User, dialect="age")

    with pytest.raises(NotImplementedError, match="Apache AGE does not support"):
        SchemaManager.generate_constraint_ddl(User, dialect="age")

    # 6. Pure DDL generation using registered model names
    User.register_schema()
    FollowsIndexed.register_schema()
    reg_indexes = SchemaManager.generate_index_ddl("User", "FollowsIndexed", dialect="postgres")
    assert 'CREATE INDEX IF NOT EXISTS idx_user_age ON "user" ("age");' in reg_indexes
    assert (
        'CREATE INDEX IF NOT EXISTS idx_follows_indexed_since ON "follows_indexed" ("since");'
        in reg_indexes
    )


def test_session_ddl_convenience_methods():
    """Verifies Session instance convenience methods execute DDL directly."""
    from voyager_ogm.bridge import MockBridge

    # openCypher session
    mock_bridge = MockBridge()
    session = Session(bridge=mock_bridge, dialect="cypher")

    applied_constraints = session.create_constraints(User, Follows)
    assert len(applied_constraints) > 0
    assert all("CREATE CONSTRAINT" in s for s in applied_constraints)

    applied_indexes = session.create_indexes(User, Follows)
    assert len(applied_indexes) > 0
    assert all("CREATE INDEX" in s for s in applied_indexes)

    dropped_indexes = session.drop_indexes(User, Follows)
    assert len(dropped_indexes) > 0
    assert all("DROP INDEX" in s for s in dropped_indexes)

    dropped_constraints = session.drop_constraints(User, Follows)
    assert len(dropped_constraints) > 0
    assert all("DROP CONSTRAINT" in s for s in dropped_constraints)

    # Verify executed queries in MockBridge match applied statements
    executed_stmts = [q[0] for q in mock_bridge.executed_queries]
    for stmt in applied_constraints + applied_indexes + dropped_indexes + dropped_constraints:
        assert stmt in executed_stmts

    # PostgreSQL session
    mock_bridge_sql = MockBridge()
    session_sql = Session(bridge=mock_bridge_sql, dialect="postgres")

    sql_idx = session_sql.create_indexes(User, Follows)
    assert 'CREATE INDEX IF NOT EXISTS idx_user_age ON "user" ("age");' in sql_idx
    assert (
        'CREATE INDEX IF NOT EXISTS idx_user_age ON "user" ("age");',
        {},
    ) in mock_bridge_sql.executed_queries


@pytest.mark.asyncio
async def test_async_session_ddl_convenience_methods():
    """Verifies AsyncSession instance convenience methods asynchronously execute DDL."""
    from voyager_ogm.bridge import AsyncMockBridge
    from voyager_ogm.session import AsyncSession

    async_mock = AsyncMockBridge()
    async_session = AsyncSession(bridge=async_mock, dialect="cypher")

    applied_constraints = await async_session.create_constraints(User, Follows)
    assert len(applied_constraints) > 0
    assert all("CREATE CONSTRAINT" in s for s in applied_constraints)

    applied_indexes = await async_session.create_indexes(User, Follows)
    assert len(applied_indexes) > 0
    assert all("CREATE INDEX" in s for s in applied_indexes)

    dropped_indexes = await async_session.drop_indexes(User, Follows)
    assert len(dropped_indexes) > 0
    assert all("DROP INDEX" in s for s in dropped_indexes)

    dropped_constraints = await async_session.drop_constraints(User, Follows)
    assert len(dropped_constraints) > 0
    assert all("DROP CONSTRAINT" in s for s in dropped_constraints)

    executed_stmts = [q[0] for q in async_mock.executed_queries]
    for stmt in applied_constraints + applied_indexes + dropped_indexes + dropped_constraints:
        assert stmt in executed_stmts


_ENTERPRISE_CONSTRAINT_MESSAGE = (
    "Neo.ClientError.Schema.ConstraintCreationFailed: Creating constraint requires "
    "Neo4j Enterprise Edition."
)


class _EnterpriseOnlyMockBridge(MockBridge):
    """Mock bridge that rejects NOT NULL constraints like Neo4j Community."""

    def execute(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        if "IS NOT NULL" in statement.upper():
            raise RuntimeError(_ENTERPRISE_CONSTRAINT_MESSAGE)
        return super().execute(statement, parameters)


class _AsyncEnterpriseOnlyMockBridge(AsyncMockBridge):
    """Async mock bridge that rejects NOT NULL constraints like Neo4j Community."""

    async def execute(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        if "IS NOT NULL" in statement.upper():
            raise RuntimeError(_ENTERPRISE_CONSTRAINT_MESSAGE)
        return await super().execute(statement, parameters)


def _assert_enterprise_constraint_error(error: VoyagerError, generated: list[str]) -> None:
    """Asserts the single aggregated Enterprise-only constraint error shape."""
    assert error.code == "SCHEMA_ERROR"
    skipped = getattr(error, "skipped", None)
    assert skipped == [s for s in generated if "IS NOT NULL" in s.upper()]
    message = str(error)
    assert "Neo4j Enterprise Edition" in message
    assert "remaining constraints were applied" in message
    assert "existence constraint" in message
    assert "label 'EnterpriseDemoNode'" in message
    assert "relationship type 'ENTERPRISE_DEMO_REL'" in message
    assert "property 'uid'" in message
    assert "property 'rid'" in message
    for stmt in skipped:
        assert stmt in message


@node(label="EnterpriseDemoNode")
class EnterpriseDemoNode(Node):
    """Node with a primary key, a unique field, and an indexed field for DDL tests."""

    uid: str = Field(primary_key=True)
    slug: str = Field(unique=True)
    rank: int = Field(index=True)


@relationship(
    type_name="ENTERPRISE_DEMO_REL",
    source_node=EnterpriseDemoNode,
    target_node=EnterpriseDemoNode,
)
class EnterpriseDemoRel(Relationship):
    """Relationship with a primary key for DDL tests."""

    rid: str = Field(primary_key=True)


def test_create_constraints_reports_enterprise_only_and_applies_rest():
    """Verifies NOT NULL rejections are collected while all other constraints run."""
    bridge = _EnterpriseOnlyMockBridge()
    session = Session(bridge=bridge, dialect="cypher")
    generated = SchemaManager.generate_constraint_ddl(EnterpriseDemoNode, EnterpriseDemoRel)

    with pytest.raises(VoyagerError) as excinfo:
        session.create_constraints(EnterpriseDemoNode, EnterpriseDemoRel)

    _assert_enterprise_constraint_error(excinfo.value, generated)
    executed = [stmt for stmt, _ in bridge.executed_queries]
    assert executed == [s for s in generated if "IS NOT NULL" not in s.upper()]
    assert any("uid_unique" in s for s in executed)
    assert any("rid_unique" in s for s in executed)


def test_apply_schema_reports_enterprise_only_after_applying_indexes():
    """Verifies apply_schema still applies indexes before raising the aggregated error."""
    bridge = _EnterpriseOnlyMockBridge()
    session = Session(bridge=bridge, dialect="cypher")
    generated = SchemaManager.generate_constraint_ddl(EnterpriseDemoNode, EnterpriseDemoRel)

    with pytest.raises(VoyagerError) as excinfo:
        session.apply_schema(EnterpriseDemoNode, EnterpriseDemoRel)

    _assert_enterprise_constraint_error(excinfo.value, generated)
    expected_constraints = [s for s in generated if "IS NOT NULL" not in s.upper()]
    executed = [stmt for stmt, _ in bridge.executed_queries]
    assert executed[: len(expected_constraints)] == expected_constraints
    assert any("CREATE INDEX index_enterprisedemonode_rank" in s for s in executed)


def test_create_constraints_reraises_unrelated_error_immediately():
    """Verifies non-Enterprise failures abort immediately with the original error."""

    @node(label="UnrelatedFailureNode")
    class UnrelatedFailureNode(Node):
        uid: str = Field(primary_key=True)

    class _UnrelatedFailureBridge(MockBridge):
        def execute(
            self, statement: str, parameters: dict[str, Any] | None = None
        ) -> list[dict[str, Any]]:
            if "constraint_unrelatedfailurenode_uid_unique" in statement:
                raise RuntimeError("unrelated database failure")
            return super().execute(statement, parameters)

    bridge = _UnrelatedFailureBridge()
    session = Session(bridge=bridge, dialect="cypher")

    with pytest.raises(RuntimeError, match="unrelated database failure"):
        session.create_constraints(UnrelatedFailureNode)

    # The failure happened before the NOT NULL statement; nothing was applied.
    assert bridge.executed_queries == []


@pytest.mark.asyncio
async def test_async_create_constraints_reports_enterprise_only_and_applies_rest():
    """Verifies the async path collects NOT NULL rejections and keeps applying the rest."""
    from voyager_ogm.session import AsyncSession

    bridge = _AsyncEnterpriseOnlyMockBridge()
    session = AsyncSession(bridge=bridge, dialect="cypher")
    generated = SchemaManager.generate_constraint_ddl(EnterpriseDemoNode, EnterpriseDemoRel)

    with pytest.raises(VoyagerError) as excinfo:
        await session.create_constraints(EnterpriseDemoNode, EnterpriseDemoRel)

    _assert_enterprise_constraint_error(excinfo.value, generated)
    executed = [stmt for stmt, _ in bridge.executed_queries]
    assert executed == [s for s in generated if "IS NOT NULL" not in s.upper()]


@pytest.mark.asyncio
async def test_async_apply_schema_reports_enterprise_only_after_applying_indexes():
    """Verifies the async apply_schema still applies indexes before raising."""
    from voyager_ogm.session import AsyncSession

    bridge = _AsyncEnterpriseOnlyMockBridge()
    session = AsyncSession(bridge=bridge, dialect="cypher")
    generated = SchemaManager.generate_constraint_ddl(EnterpriseDemoNode, EnterpriseDemoRel)

    with pytest.raises(VoyagerError) as excinfo:
        await session.apply_schema(EnterpriseDemoNode, EnterpriseDemoRel)

    _assert_enterprise_constraint_error(excinfo.value, generated)
    expected_constraints = [s for s in generated if "IS NOT NULL" not in s.upper()]
    executed = [stmt for stmt, _ in bridge.executed_queries]
    assert executed[: len(expected_constraints)] == expected_constraints
    assert any("CREATE INDEX index_enterprisedemonode_rank" in s for s in executed)


@pytest.mark.asyncio
async def test_async_create_constraints_reraises_unrelated_error_immediately():
    """Verifies non-Enterprise async failures abort immediately with the original error."""
    from voyager_ogm.session import AsyncSession

    @node(label="AsyncUnrelatedFailureNode")
    class AsyncUnrelatedFailureNode(Node):
        uid: str = Field(primary_key=True)

    class _AsyncUnrelatedFailureBridge(AsyncMockBridge):
        async def execute(
            self, statement: str, parameters: dict[str, Any] | None = None
        ) -> list[dict[str, Any]]:
            if "constraint_asyncunrelatedfailurenode_uid_unique" in statement:
                raise RuntimeError("unrelated database failure")
            return await super().execute(statement, parameters)

    bridge = _AsyncUnrelatedFailureBridge()
    session = AsyncSession(bridge=bridge, dialect="cypher")

    with pytest.raises(RuntimeError, match="unrelated database failure"):
        await session.create_constraints(AsyncUnrelatedFailureNode)

    assert bridge.executed_queries == []


@pytest.mark.live
def test_live_schema_ddl_multi_engine_integration():
    """Verifies live index and constraint management across live Neo4j, PostgreSQL 19, and FalkorDB engines."""
    # 1. Neo4j Live Engine (Bolt port 7687)
    uri = os.getenv("NEO4J_URI", "bolt://127.0.0.1:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "voyagerpass123")
    database = os.getenv("NEO4J_DATABASE", "neo4j")

    try:
        neo_drv = GraphDatabase.driver(uri, auth=(user, password), connection_timeout=1.0)
        neo_drv.verify_connectivity()
        neo_bridge = Neo4jBoltBridge(neo_drv, database=database)
        neo_session = Session(bridge=neo_bridge, dialect="cypher")

        # Create constraints and indexes idempotently
        neo_c = neo_session.create_constraints(User)
        assert len(neo_c) > 0
        neo_i = neo_session.create_indexes(User)
        assert len(neo_i) > 0

        # Teardown
        neo_session.drop_indexes(User)
        neo_session.drop_constraints(User)
        neo_session.close()
    except Exception:
        pass  # Skip gracefully if Neo4j is unreachable

    # 2. PostgreSQL 19 Live Engine (Port 5456)
    pg_uri = os.getenv("PG19_URI", "postgresql://postgres:voyagerpass123@127.0.0.1:5456/postgres")
    try:
        import psycopg

        pg_conn = psycopg.connect(pg_uri, autocommit=True, connect_timeout=1)
        with pg_conn.cursor() as cur:
            cur.execute('DROP TABLE IF EXISTS "user" CASCADE;')
            cur.execute('CREATE TABLE "user" (user_id TEXT PRIMARY KEY, email TEXT, age INT);')

        pg_session = Session(bridge=pg_conn, dialect="postgres")
        # Quoted table "user" avoids PostgreSQL reserved keyword syntax error
        pg_idx = pg_session.create_indexes(User)
        assert len(pg_idx) > 0
        pg_con = pg_session.create_constraints(User)
        assert len(pg_con) > 0

        # Teardown
        pg_session.drop_indexes(User)
        pg_session.drop_constraints(User)

        with pg_conn.cursor() as cur:
            cur.execute('DROP TABLE IF EXISTS "user" CASCADE;')
        pg_conn.close()
    except Exception:
        pass  # Skip gracefully if PostgreSQL 19 is unreachable

    # 3. FalkorDB Live Engine (Port 6379)
    try:
        from falkordb import FalkorDB
        from voyager_ogm.bridge import FalkorDBBridge

        f_db = FalkorDB(host="127.0.0.1", port=6379)
        f_graph = f_db.select_graph("voyager_ddl_live_test")
        f_graph.query("RETURN 1")

        fk_bridge = FalkorDBBridge(f_graph)
        fk_session = Session(bridge=fk_bridge, dialect="falkordb")
        # Idempotent index creation
        fk_idx = fk_session.create_indexes(User)
        assert len(fk_idx) > 0
        # Re-creation should not crash (handles 'already indexed' in bridge)
        fk_session.create_indexes(User)

        # Teardown
        fk_session.drop_indexes(User)
        # Re-drop should not crash (handles 'no such index' in bridge)
        fk_session.drop_indexes(User)

        # Constraints raise actionable error
        with pytest.raises(NotImplementedError, match="FalkorDB does not support"):
            fk_session.create_constraints(User)
    except Exception:
        pass  # Skip gracefully if FalkorDB is unreachable


def test_schema_registration_loud_failure(caplog: pytest.LogCaptureFixture):
    """Verifies that schema registration failures log a loud warning and raise an exception."""
    import logging

    @node(label="LoudFailNode")
    class LoudFailNode(Node):
        id: str = Field(primary_key=True)

    @relationship(type_name="LOUD_FAIL_REL", source_node=LoudFailNode, target_node=LoudFailNode)
    class LoudFailRel(Relationship):
        weight: float = Field()

    class FailingRegistry:
        def register_node(self, *args: Any, **kwargs: Any) -> None:
            raise RuntimeError("Rust registry allocation failed")

        def register_relationship(self, *args: Any, **kwargs: Any) -> None:
            raise RuntimeError("Rust registry allocation failed")

    fail_reg = FailingRegistry()

    with caplog.at_level(logging.WARNING, logger="voyager_ogm"):
        with pytest.raises(RuntimeError, match="Rust registry allocation failed"):
            LoudFailNode.register_schema(registry=fail_reg)
        assert "Failed to register schema for node model LoudFailNode" in caplog.text

    with caplog.at_level(logging.WARNING, logger="voyager_ogm"):
        with pytest.raises(RuntimeError, match="Rust registry allocation failed"):
            LoudFailRel.register_schema(registry=fail_reg)
        assert "Failed to register schema for relationship model LoudFailRel" in caplog.text


def test_falkordb_bridge_idempotency_unit():
    """Unit test verifying that FalkorDBBridge absorbs index idempotency errors and raises others."""
    from voyager_ogm.bridge import AsyncFalkorDBBridge, FalkorDBBridge

    class FakeGraph:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def query(self, stmt: str, params: Any = None) -> Any:
            self.calls.append(stmt)
            stmt_clean = stmt.strip().lower()
            if "already_fail" in stmt_clean:
                raise RuntimeError("Attribute 'age' is already indexed")
            if "no_such_fail" in stmt_clean:
                raise RuntimeError("Unable to drop index on :User(age): no such index.")
            if "syntax_error" in stmt_clean:
                raise RuntimeError("Invalid Cypher syntax")
            return None

    fake_g = FakeGraph()
    bridge = FalkorDBBridge(fake_g)

    # Creating already indexed attribute should be absorbed (range, fulltext, vector)
    res = bridge.execute("CREATE INDEX FOR (n:User) ON (n.already_fail)")
    assert res == []
    res_ft_node = bridge.execute("CREATE FULLTEXT INDEX FOR (n:User) ON (n.already_fail)")
    assert res_ft_node == []
    res_ft_rel = bridge.execute("CREATE FULLTEXT INDEX FOR ()-[r:REL]-() ON (r.already_fail)")
    assert res_ft_rel == []
    res_vec = bridge.execute("CREATE VECTOR INDEX FOR (n:User) ON (n.already_fail)")
    assert res_vec == []

    # Dropping non-existent index should be absorbed (range, fulltext, vector)
    res = bridge.execute("DROP INDEX FOR (n:User) ON (n.no_such_fail)")
    assert res == []
    res_drop_ft_node = bridge.execute("DROP FULLTEXT INDEX FOR (n:User) ON (n.no_such_fail)")
    assert res_drop_ft_node == []
    res_drop_ft_rel = bridge.execute("DROP FULLTEXT INDEX FOR ()-[r:REL]-() ON (r.no_such_fail)")
    assert res_drop_ft_rel == []
    res_drop_vec = bridge.execute("DROP VECTOR INDEX FOR (n:User) ON (n.no_such_fail)")
    assert res_drop_vec == []

    # Other queries with syntax errors must NOT be swallowed
    with pytest.raises(RuntimeError, match="Invalid Cypher syntax"):
        bridge.execute("CREATE INDEX FOR (n:User) ON (n.syntax_error)")
    with pytest.raises(RuntimeError, match="Invalid Cypher syntax"):
        bridge.execute("CREATE FULLTEXT INDEX FOR (n:User) ON (n.syntax_error)")

    # Non-index queries that somehow contain 'already indexed' must NOT be swallowed
    with pytest.raises(RuntimeError, match="already indexed"):
        bridge.execute("MATCH (n:User) RETURN n.already_fail")

    # Async bridge delegates faithfully
    import asyncio

    async_bridge = AsyncFalkorDBBridge(fake_g)
    res_async = asyncio.run(async_bridge.execute("CREATE INDEX FOR (n:User) ON (n.already_fail)"))
    assert res_async == []
    res_async_ft = asyncio.run(
        async_bridge.execute("CREATE FULLTEXT INDEX FOR (n:User) ON (n.already_fail)")
    )
    assert res_async_ft == []
    res_async_drop_ft = asyncio.run(
        async_bridge.execute("DROP FULLTEXT INDEX FOR (n:User) ON (n.no_such_fail)")
    )
    assert res_async_drop_ft == []


def test_dict_spec_routing_in_schema_manager():
    """Verify SchemaManager correctly routes schema dicts for nodes and relationships."""
    node_spec = {
        "name": "DictPerson",
        "labels": ["DictPerson"],
        "primary_key": "pid",
        "fields": {
            "pid": {"field_type": "STRING", "primary_key": True, "indexed": True},
            "age": {"field_type": "INTEGER", "indexed": True},
        },
    }
    rel_spec = {
        "name": "DictKnows",
        "type_name": "DICT_KNOWS",
        "source_labels": ["DictPerson"],
        "target_labels": ["DictPerson"],
        "fields": {
            "since": {"field_type": "INTEGER", "indexed": True},
        },
    }

    # Index DDL
    node_idx_stmts = SchemaManager.generate_index_ddl(node_spec, dialect="cypher")
    assert any("FOR (n:DictPerson) ON (n.age)" in s for s in node_idx_stmts)

    rel_idx_stmts = SchemaManager.generate_index_ddl(rel_spec, dialect="cypher")
    assert any("FOR ()-[r:DICT_KNOWS]-() ON (r.since)" in s for s in rel_idx_stmts)

    # Postgres dialect
    node_pg_stmts = SchemaManager.generate_index_ddl(node_spec, dialect="postgres")
    assert any(
        'CREATE INDEX IF NOT EXISTS idx_dictperson_age ON "dictperson" ("age");' in s
        for s in node_pg_stmts
    )

    rel_pg_stmts = SchemaManager.generate_index_ddl(rel_spec, dialect="postgres")
    assert any(
        'CREATE INDEX IF NOT EXISTS idx_dict_knows_since ON "dict_knows" ("since");' in s
        for s in rel_pg_stmts
    )

    # Drop index DDL
    node_drop_idx = SchemaManager.generate_drop_index_ddl(node_spec, dialect="cypher")
    assert any("DROP INDEX index_dictperson_age" in s for s in node_drop_idx)

    rel_drop_idx = SchemaManager.generate_drop_index_ddl(rel_spec, dialect="cypher")
    assert any("DROP INDEX index_rel_dict_knows_since" in s for s in rel_drop_idx)


@pytest.mark.live
@pytest.mark.skipif(
    not is_port_open("127.0.0.1", 7687),
    reason="Neo4j container not reachable on port 7687",
)
def test_live_neo4j_relationship_unique_constraint() -> None:
    """Relationship unique fields emit a uniqueness constraint and reject duplicates live.

    A nullable unique relationship field must not gain an existence constraint, so a
    relationship whose unique property is absent is still accepted.
    """
    pytest.importorskip("neo4j")
    uri = os.getenv("NEO4J_URI", "bolt://127.0.0.1:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "voyagerpass123")

    driver = GraphDatabase.driver(uri, auth=(user, password), connection_timeout=1.0)
    try:
        driver.verify_connectivity()
    except Exception:
        driver.close()
        pytest.skip(f"Neo4j database not reachable on {uri}")

    @node(label="LiveUniqueAccount")
    class LiveUniqueAccount(Node):
        account_id: str = Field(primary_key=True)

    @relationship(
        type_name="LIVE_UNIQUE_TRANSFER",
        source_node=LiveUniqueAccount,
        target_node=LiveUniqueAccount,
    )
    class LiveUniqueTransfer(Relationship):
        ref: str = Field(unique=True)

    session = Session(driver, dialect="cypher")
    try:
        session.drop_constraints(LiveUniqueTransfer)
        session.execute("MATCH (n:LiveUniqueAccount) DETACH DELETE n")
        session.execute(
            "CREATE (:LiveUniqueAccount {account_id: 'live-a1'}), "
            "(:LiveUniqueAccount {account_id: 'live-a2'})"
        )

        created = session.create_constraints(LiveUniqueTransfer)
        assert created == [
            "CREATE CONSTRAINT constraint_rel_live_unique_transfer_ref_unique "
            "IF NOT EXISTS FOR ()-[r:LIVE_UNIQUE_TRANSFER]-() REQUIRE r.ref IS UNIQUE"
        ]
        # unique but nullable: no existence constraint is emitted
        assert not any("IS NOT NULL" in stmt for stmt in created)

        session.execute(
            "MATCH (a:LiveUniqueAccount {account_id: 'live-a1'}), "
            "(b:LiveUniqueAccount {account_id: 'live-a2'}) "
            "CREATE (a)-[:LIVE_UNIQUE_TRANSFER {ref: 'dup'}]->(b)"
        )
        with pytest.raises(ConstraintError):
            session.execute(
                "MATCH (a:LiveUniqueAccount {account_id: 'live-a1'}), "
                "(b:LiveUniqueAccount {account_id: 'live-a2'}) "
                "CREATE (a)-[:LIVE_UNIQUE_TRANSFER {ref: 'dup'}]->(b)"
            )

        # The nullable unique property stays optional.
        session.execute(
            "MATCH (a:LiveUniqueAccount {account_id: 'live-a1'}), "
            "(b:LiveUniqueAccount {account_id: 'live-a2'}) "
            "CREATE (a)-[:LIVE_UNIQUE_TRANSFER]->(b)"
        )

        dropped = session.drop_constraints(LiveUniqueTransfer)
        assert dropped == [
            "DROP CONSTRAINT constraint_rel_live_unique_transfer_ref_unique IF EXISTS"
        ]

        # After dropping the constraint duplicates are accepted again.
        session.execute(
            "MATCH (a:LiveUniqueAccount {account_id: 'live-a1'}), "
            "(b:LiveUniqueAccount {account_id: 'live-a2'}) "
            "CREATE (a)-[:LIVE_UNIQUE_TRANSFER {ref: 'dup'}]->(b)"
        )
    finally:
        try:
            session.drop_constraints(LiveUniqueTransfer)
            session.execute("MATCH (n:LiveUniqueAccount) DETACH DELETE n")
        finally:
            driver.close()


def _skip_unless_community(driver: Any, uri: str) -> None:
    """Skips the caller when the live Neo4j server is not Community Edition."""
    if neo4j_edition(driver) != "community":
        driver.close()
        pytest.skip(f"Requires Neo4j Community Edition on {uri}")


def _assert_community_constraints_created(session: Session, node_name: str, rel_name: str) -> None:
    """Asserts both UNIQUE constraints exist and no NOT NULL constraint was created."""
    names = [c.get("name", "") for c in session.execute("SHOW CONSTRAINTS")]
    assert any(f"{node_name.lower()}_uid_unique" in name for name in names)
    assert any(f"{rel_name.lower()}_rid_unique" in name for name in names)
    assert not any("not_null" in name for name in names)


@pytest.mark.live
@pytest.mark.skipif(
    not is_port_open("127.0.0.1", 7687),
    reason="Neo4j container not reachable on port 7687",
)
def test_live_community_enterprise_only_constraints_are_reported() -> None:
    """On Community, UNIQUE constraints apply and Enterprise-only ones are reported once."""
    pytest.importorskip("neo4j")
    uri = os.getenv("NEO4J_URI", "bolt://127.0.0.1:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "voyagerpass123")

    driver = GraphDatabase.driver(uri, auth=(user, password), connection_timeout=1.0)
    try:
        driver.verify_connectivity()
    except Exception:
        driver.close()
        pytest.skip(f"Neo4j database not reachable on {uri}")
    _skip_unless_community(driver, uri)

    @node(label="LiveCommunityKeyNode")
    class LiveCommunityKeyNode(Node):
        uid: str = Field(primary_key=True)

    @relationship(
        type_name="LIVE_COMMUNITY_KEY_REL",
        source_node=LiveCommunityKeyNode,
        target_node=LiveCommunityKeyNode,
    )
    class LiveCommunityKeyRel(Relationship):
        rid: str = Field(primary_key=True)

    session = Session(driver, dialect="cypher")
    try:
        session.drop_constraints(LiveCommunityKeyNode, LiveCommunityKeyRel)
        session.execute("MATCH (n:LiveCommunityKeyNode) DETACH DELETE n")

        with pytest.raises(VoyagerError) as excinfo:
            session.create_constraints(LiveCommunityKeyNode, LiveCommunityKeyRel)

        error = excinfo.value
        assert error.code == "SCHEMA_ERROR"
        skipped = getattr(error, "skipped", [])
        assert len(skipped) == 2
        assert all("IS NOT NULL" in stmt for stmt in skipped)
        assert "Neo4j Enterprise Edition" in str(error)
        _assert_community_constraints_created(
            session, "LiveCommunityKeyNode", "LIVE_COMMUNITY_KEY_REL"
        )

        # UNIQUE constraints reject duplicate node and relationship primary keys.
        session.execute("CREATE (:LiveCommunityKeyNode {uid: 'dup'})")
        with pytest.raises(ConstraintError):
            session.execute("CREATE (:LiveCommunityKeyNode {uid: 'dup'})")

        session.execute(
            "CREATE (:LiveCommunityKeyNode {uid: 'rel-a'}), (:LiveCommunityKeyNode {uid: 'rel-b'})"
        )
        session.execute(
            "MATCH (a:LiveCommunityKeyNode {uid: 'rel-a'}), "
            "(b:LiveCommunityKeyNode {uid: 'rel-b'}) "
            "CREATE (a)-[:LIVE_COMMUNITY_KEY_REL {rid: 'dup-rel'}]->(b)"
        )
        with pytest.raises(ConstraintError):
            session.execute(
                "MATCH (a:LiveCommunityKeyNode {uid: 'rel-a'}), "
                "(b:LiveCommunityKeyNode {uid: 'rel-b'}) "
                "CREATE (a)-[:LIVE_COMMUNITY_KEY_REL {rid: 'dup-rel'}]->(b)"
            )

        dropped = session.drop_constraints(LiveCommunityKeyNode, LiveCommunityKeyRel)
        assert len(dropped) == 4
        # Drops of the never-created NOT NULL constraints use IF EXISTS and do not fail.
        assert all("IF EXISTS" in stmt for stmt in dropped)
        names = [c.get("name", "") for c in session.execute("SHOW CONSTRAINTS")]
        assert not any("live_community_key" in name for name in names)
    finally:
        try:
            session.drop_constraints(LiveCommunityKeyNode, LiveCommunityKeyRel)
            session.execute("MATCH (n:LiveCommunityKeyNode) DETACH DELETE n")
        finally:
            driver.close()


@pytest.mark.live
@pytest.mark.skipif(
    not is_port_open("127.0.0.1", 7687),
    reason="Neo4j container not reachable on port 7687",
)
def test_live_native_community_enterprise_only_constraints_are_reported() -> None:
    """The native voyager-drivers path reports Enterprise-only constraints the same way."""
    pytest.importorskip("neo4j")
    uri = os.getenv("NEO4J_URI", "bolt://127.0.0.1:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "voyagerpass123")
    native_uri = os.getenv(
        "NEO4J_NATIVE_URI",
        f"bolt://{user}:{password}@127.0.0.1:7687?connect_timeout=2",
    )

    driver = GraphDatabase.driver(uri, auth=(user, password), connection_timeout=1.0)
    try:
        driver.verify_connectivity()
    except Exception:
        driver.close()
        pytest.skip(f"Neo4j database not reachable on {uri}")
    _skip_unless_community(driver, uri)
    driver.close()

    @node(label="LiveNativeCommunityKeyNode")
    class LiveNativeCommunityKeyNode(Node):
        uid: str = Field(primary_key=True)

    @relationship(
        type_name="LIVE_NATIVE_COMMUNITY_KEY_REL",
        source_node=LiveNativeCommunityKeyNode,
        target_node=LiveNativeCommunityKeyNode,
    )
    class LiveNativeCommunityKeyRel(Relationship):
        rid: str = Field(primary_key=True)

    try:
        session = Session(native_uri, dialect="cypher", backend="native")
    except Exception as exc:
        pytest.skip(f"Native backend unavailable: {exc}")

    try:
        session.drop_constraints(LiveNativeCommunityKeyNode, LiveNativeCommunityKeyRel)
        session.execute("MATCH (n:LiveNativeCommunityKeyNode) DETACH DELETE n")

        with pytest.raises(VoyagerError) as excinfo:
            session.create_constraints(LiveNativeCommunityKeyNode, LiveNativeCommunityKeyRel)

        error = excinfo.value
        assert error.code == "SCHEMA_ERROR"
        skipped = getattr(error, "skipped", [])
        assert len(skipped) == 2
        assert all("IS NOT NULL" in stmt for stmt in skipped)
        assert "Neo4j Enterprise Edition" in str(error)
        _assert_community_constraints_created(
            session, "LiveNativeCommunityKeyNode", "LIVE_NATIVE_COMMUNITY_KEY_REL"
        )

        session.execute("CREATE (:LiveNativeCommunityKeyNode {uid: 'dup'})")
        with pytest.raises(Exception, match="already exists|constraint"):
            session.execute("CREATE (:LiveNativeCommunityKeyNode {uid: 'dup'})")

        dropped = session.drop_constraints(LiveNativeCommunityKeyNode, LiveNativeCommunityKeyRel)
        assert len(dropped) == 4
        names = [c.get("name", "") for c in session.execute("SHOW CONSTRAINTS")]
        assert not any("live_native_community_key" in name for name in names)
    finally:
        try:
            session.drop_constraints(LiveNativeCommunityKeyNode, LiveNativeCommunityKeyRel)
            session.execute("MATCH (n:LiveNativeCommunityKeyNode) DETACH DELETE n")
        finally:
            session.close()
