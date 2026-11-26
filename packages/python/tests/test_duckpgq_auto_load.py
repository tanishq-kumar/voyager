"""DuckDB sessions load the DuckPGQ extension automatically for SQL/PGQ.

These tests use embedded DuckDB only. They never install the extension unless
the test exercises the explicit ``install_duckpgq`` opt-in with a fake
connection, and they skip with a clear reason when DuckPGQ is not installed
locally.
"""

from __future__ import annotations

from typing import Any

import pytest
from voyager_ogm import Field, Node, Query, Session, node
from voyager_ogm.bridge import DuckDbBridge


@node(label="Person")
class Person(Node):
    """Minimal model for a live DuckPGQ GRAPH_TABLE query."""

    id = Field(primary_key=True)
    name = Field()
    age = Field()


def _connect_duckdb():
    duckdb = pytest.importorskip("duckdb")
    return duckdb.connect(":memory:")


def _duckpgq_available() -> bool:
    """Checks whether DuckPGQ is installed locally without downloading it."""
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(
        ":memory:",
        config={
            "autoinstall_known_extensions": "false",
            "autoload_known_extensions": "false",
        },
    )
    try:
        con.execute("LOAD duckpgq;")
        return True
    except Exception:
        return False
    finally:
        con.close()


def _require_duckpgq() -> None:
    if not _duckpgq_available():
        pytest.skip("duckpgq extension is not installed locally; run INSTALL duckpgq;")


def _seed_person_table(con) -> None:
    con.execute(
        """
        CREATE TABLE person (
            id BIGINT PRIMARY KEY,
            name VARCHAR,
            age INTEGER
        );
        INSERT INTO person VALUES
            (1, 'Alice', 30),
            (2, 'Bob', 25);
        """
    )


def _assert_person_names(records) -> None:
    assert [record["name"] for record in records] == ["Alice", "Bob"]


def test_fresh_connection_session_loads_duckpgq() -> None:
    """A Session on a connection that never loaded DuckPGQ runs GRAPH_TABLE."""
    _require_duckpgq()
    con = _connect_duckdb()
    try:
        _seed_person_table(con)
        session = Session(bridge=con, dialect="duck_pgq", graph_name="zz_person_pg")
        session.execute('CREATE PROPERTY GRAPH zz_person_pg VERTEX TABLES (person LABEL "Person")')

        loaded = con.execute(
            "SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'duckpgq'"
        ).fetchone()
        assert loaded == (True,)

        person = Person(alias="p")
        _assert_person_names(session.execute(Query.match(person).return_(name=person.name)).all())
    finally:
        con.close()


def test_duckdb_uri_session_loads_duckpgq() -> None:
    """A duckdb:// URI session loads DuckPGQ without extra setup."""
    _require_duckpgq()
    session = Session(bridge="duckdb://:memory:", dialect="duck_pgq", graph_name="zz_uri_pg")
    con = session.bridge.con
    try:
        _seed_person_table(con)
        session.execute('CREATE PROPERTY GRAPH zz_uri_pg VERTEX TABLES (person LABEL "Person")')

        person = Person(alias="p")
        _assert_person_names(session.execute(Query.match(person).return_(name=person.name)).all())
    finally:
        session.close()


def test_connection_that_already_loaded_duckpgq_keeps_working() -> None:
    """A caller-prepared connection is detected and not disturbed."""
    _require_duckpgq()
    con = _connect_duckdb()
    try:
        con.execute("LOAD duckpgq;")
        _seed_person_table(con)
        con.execute('CREATE PROPERTY GRAPH zz_prepared_pg VERTEX TABLES (person LABEL "Person")')

        session = Session(bridge=con, dialect="duckpgq", graph_name="zz_prepared_pg")
        person = Person(alias="p")
        _assert_person_names(session.execute(Query.match(person).return_(name=person.name)).all())

        loaded = con.execute(
            "SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'duckpgq'"
        ).fetchone()
        assert loaded == (True,)
    finally:
        con.close()


def test_bare_bridge_loads_duckpgq_before_graph_statements() -> None:
    """A DuckDbBridge used without a Session loads DuckPGQ lazily."""
    _require_duckpgq()
    con = _connect_duckdb()
    bridge = DuckDbBridge(con)
    try:
        _seed_person_table(con)
        bridge.execute('CREATE PROPERTY GRAPH zz_bridge_pg VERTEX TABLES (person LABEL "Person")')
        rows = bridge.execute(
            'SELECT name FROM GRAPH_TABLE (zz_bridge_pg MATCH (p IS "Person") '
            "COLUMNS (p.name AS name))"
        )
        assert [row["name"] for row in rows] == ["Alice", "Bob"]
    finally:
        con.close()


def test_missing_extension_error_names_duckpgq_and_install_command(tmp_path) -> None:
    """Without DuckPGQ installed, the error says what to install."""
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(
        ":memory:",
        config={
            "extension_directory": str(tmp_path),
            "autoinstall_known_extensions": "false",
            "autoload_known_extensions": "false",
        },
    )
    try:
        with pytest.raises(RuntimeError) as exc_info:
            Session(bridge=con, dialect="duck_pgq")
        message = str(exc_info.value)
        assert "DuckPGQ" in message
        assert "INSTALL duckpgq;" in message
    finally:
        con.close()


class _RecordingConnection:
    """Fake DuckDB connection that records extension statements."""

    def __init__(self, *, fail_first_load: bool) -> None:
        self.calls: list[str] = []
        self._fail_first_load = fail_first_load
        self._load_attempts = 0

    def execute(self, statement: str, _parameters: Any = None) -> _RecordingConnection:
        self.calls.append(statement)
        if statement.startswith("LOAD duckpgq;"):
            self._load_attempts += 1
            if self._fail_first_load and self._load_attempts == 1:
                raise RuntimeError("Extension not found")
        return self

    def fetchone(self) -> tuple[bool]:
        return (False,)


def test_install_is_opt_in_only() -> None:
    """Without the opt-in, a failed LOAD raises and never calls INSTALL."""
    con = _RecordingConnection(fail_first_load=True)
    bridge = DuckDbBridge(con)
    with pytest.raises(RuntimeError) as exc_info:
        bridge.ensure_duckpgq()
    assert "INSTALL duckpgq;" in str(exc_info.value)
    assert not any(statement.startswith("INSTALL") for statement in con.calls)


def test_install_opt_in_installs_then_loads() -> None:
    """install_duckpgq=True installs the extension before retrying LOAD."""
    con = _RecordingConnection(fail_first_load=True)
    bridge = DuckDbBridge(con, install_duckpgq=True)
    bridge.ensure_duckpgq()
    assert "INSTALL duckpgq;" in con.calls
    assert con.calls[-1] == "LOAD duckpgq;"
