"""Live checks for Memgraph constraint create and drop parity (#335).

Verifies that constraint DDL emitted for Memgraph uses native un-named syntax
(CREATE CONSTRAINT ON (n:Label) ASSERT ... / DROP CONSTRAINT ON (n:Label) ASSERT ...),
that all created constraints can be dropped by Voyager's drop DDL, that SHOW CONSTRAINT INFO
reflects existence and uniqueness constraints, that relationship constraints safely no-op,
and that duplicate create and drop calls are idempotent.

Set ``VOYAGER_TEST_HOST`` to run against remote Memgraph instance (via ``conftest.get_test_host()``).
"""

from __future__ import annotations

import os
import socket
from typing import Any

import pytest
from conftest import get_test_host
from voyager_ogm import Field, Node, Relationship, SchemaManager, Session, node, relationship

pytestmark = pytest.mark.live

HOST = get_test_host()
PID = os.getpid()

LABEL = f"Zz335MemgraphNode_{PID}"
REL_TYPE = f"ZZ335_REL_{PID}"


@node(LABEL)
class Zz335MemgraphNode(Node):
    item_id: str = Field(primary_key=True)
    email: str = Field(unique=True)
    code: str = Field(nullable=False)
    note: str | None = Field(default=None)


@relationship(REL_TYPE)
class Zz335MemgraphRel(Relationship):
    weight: float = Field(default=1.0)
    req_val: str = Field(nullable=False)


def _reachable(port: int = 7688) -> bool:
    try:
        with socket.create_connection((HOST, port), timeout=0.5):
            return True
    except OSError:
        return False


def _memgraph_driver():
    neo4j = pytest.importorskip("neo4j")
    return neo4j.GraphDatabase.driver(
        f"bolt://{HOST}:7688",
        auth=None,
        connection_timeout=2.0,
    )


def _get_memgraph_constraints(driver) -> list[dict[str, Any]]:
    with driver.session() as s:
        res = list(s.run("SHOW CONSTRAINT INFO;"))
        return [dict(r) for r in res if r.get("label") == LABEL]


def test_live_memgraph_create_and_drop_constraints_parity() -> None:
    """Memgraph creates and drops existence and uniqueness constraints with native syntax."""
    if not _reachable(7688):
        pytest.skip(f"Memgraph is not reachable on {HOST}:7688")
    driver = _memgraph_driver()
    try:
        driver.verify_connectivity()
    except Exception:
        driver.close()
        pytest.skip(f"Memgraph connectivity verification failed on {HOST}:7688")

    session = Session(bridge=driver, dialect="memgraph")
    try:
        # Pre-cleanup in case of aborted previous runs
        session.drop_constraints(Zz335MemgraphNode)
        session.execute(f"MATCH (n:{LABEL}) DETACH DELETE n")

        # 1. Initial state: no constraints for our label
        assert len(_get_memgraph_constraints(driver)) == 0

        # 2. Create constraints via session API
        created = session.create_constraints(Zz335MemgraphNode)
        assert len(created) == 4, f"Expected 4 created constraint statements, got: {created}"
        assert any(
            f"CREATE CONSTRAINT ON (n:{LABEL}) ASSERT n.email IS UNIQUE;" == stmt
            for stmt in created
        )
        assert any(
            f"CREATE CONSTRAINT ON (n:{LABEL}) ASSERT n.item_id IS UNIQUE;" == stmt
            for stmt in created
        )
        assert any(
            f"CREATE CONSTRAINT ON (n:{LABEL}) ASSERT EXISTS (n.item_id);" == stmt
            for stmt in created
        )
        assert any(
            f"CREATE CONSTRAINT ON (n:{LABEL}) ASSERT EXISTS (n.code);" == stmt for stmt in created
        )

        # 3. Verify in Memgraph SHOW CONSTRAINT INFO
        constraints = _get_memgraph_constraints(driver)
        assert len(constraints) == 4, f"Expected 4 constraints in Memgraph, got: {constraints}"
        unique_props = {
            r["properties"][0] if isinstance(r["properties"], list) else r["properties"]
            for r in constraints
            if r["constraint type"] == "unique"
        }
        exists_props = {
            r["properties"][0] if isinstance(r["properties"], list) else r["properties"]
            for r in constraints
            if r["constraint type"] == "exists"
        }
        assert unique_props == {"email", "item_id"}
        assert exists_props == {"code", "item_id"}

        # 4. Relationship constraint emission safely no-ops on Memgraph
        rel_created = session.create_constraints(Zz335MemgraphRel)
        assert rel_created == [], "Relationship constraint creation must be empty list on Memgraph"

        # 5. Idempotent re-run of create_constraints
        created_again = session.create_constraints(Zz335MemgraphNode)
        assert len(created_again) == 4
        # Constraints in Memgraph should still be exactly 4
        assert len(_get_memgraph_constraints(driver)) == 4

        # 6. Drop constraints via session API
        dropped = session.drop_constraints(Zz335MemgraphNode)
        assert len(dropped) == 4, f"Expected 4 dropped constraint statements, got: {dropped}"
        assert any(
            f"DROP CONSTRAINT ON (n:{LABEL}) ASSERT n.email IS UNIQUE;" == stmt for stmt in dropped
        )
        assert any(
            f"DROP CONSTRAINT ON (n:{LABEL}) ASSERT n.item_id IS UNIQUE;" == stmt
            for stmt in dropped
        )
        assert any(
            f"DROP CONSTRAINT ON (n:{LABEL}) ASSERT EXISTS (n.item_id);" == stmt for stmt in dropped
        )
        assert any(
            f"DROP CONSTRAINT ON (n:{LABEL}) ASSERT EXISTS (n.code);" == stmt for stmt in dropped
        )

        # 7. Verify dropped in Memgraph SHOW CONSTRAINT INFO
        assert len(_get_memgraph_constraints(driver)) == 0

        # 8. Relationship constraint drop safely no-ops on Memgraph
        rel_dropped = session.drop_constraints(Zz335MemgraphRel)
        assert rel_dropped == [], "Relationship constraint drop must be empty list on Memgraph"

        # 9. Idempotent re-run of drop_constraints
        dropped_again = session.drop_constraints(Zz335MemgraphNode)
        assert len(dropped_again) == 4
        assert len(_get_memgraph_constraints(driver)) == 0

    finally:
        try:
            session.drop_constraints(Zz335MemgraphNode)
            session.execute(f"MATCH (n:{LABEL}) DETACH DELETE n")
        finally:
            driver.close()


def test_live_memgraph_schema_manager_create_all_and_drop_all() -> None:
    """SchemaManager create_all, drop_all, and apply_schema work idempotently on Memgraph."""
    if not _reachable(7688):
        pytest.skip(f"Memgraph is not reachable on {HOST}:7688")
    driver = _memgraph_driver()
    try:
        driver.verify_connectivity()
    except Exception:
        driver.close()
        pytest.skip(f"Memgraph connectivity verification failed on {HOST}:7688")

    session = Session(bridge=driver, dialect="memgraph")
    try:
        # Pre-cleanup
        SchemaManager.drop_all(session, Zz335MemgraphNode)
        session.execute(f"MATCH (n:{LABEL}) DETACH DELETE n")

        # 1. SchemaManager.create_all
        created = SchemaManager.create_all(session, Zz335MemgraphNode)
        assert any("ASSERT n.email IS UNIQUE" in s for s in created)
        assert any("ASSERT EXISTS (n.code)" in s for s in created)
        assert len(_get_memgraph_constraints(driver)) == 4

        # 2. Idempotent create_all re-execution
        created_again = SchemaManager.create_all(session, Zz335MemgraphNode)
        assert len(created_again) > 0
        assert len(_get_memgraph_constraints(driver)) == 4

        # 3. SchemaManager.drop_all
        dropped = SchemaManager.drop_all(session, Zz335MemgraphNode)
        assert any("ASSERT n.email IS UNIQUE" in s for s in dropped)
        assert any("ASSERT EXISTS (n.code)" in s for s in dropped)
        assert len(_get_memgraph_constraints(driver)) == 0

        # 4. Idempotent drop_all re-execution
        dropped_again = SchemaManager.drop_all(session, Zz335MemgraphNode)
        assert len(dropped_again) > 0
        assert len(_get_memgraph_constraints(driver)) == 0

        # 5. session.apply_schema
        applied = session.apply_schema(Zz335MemgraphNode)
        assert len(applied) > 0
        assert len(_get_memgraph_constraints(driver)) == 4

        # 6. Idempotent apply_schema re-execution
        applied_again = session.apply_schema(Zz335MemgraphNode)
        assert len(applied_again) > 0
        assert len(_get_memgraph_constraints(driver)) == 4

        # Clean drop
        SchemaManager.drop_all(session, Zz335MemgraphNode)
        assert len(_get_memgraph_constraints(driver)) == 0

    finally:
        try:
            SchemaManager.drop_all(session, Zz335MemgraphNode)
            session.execute(f"MATCH (n:{LABEL}) DETACH DELETE n")
        finally:
            driver.close()
