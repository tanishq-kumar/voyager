"""Live pool health, EOF eviction, and circuit-breaker tests across network engines."""

from __future__ import annotations

import os
import socket
import threading
import time

import pytest
from conftest import get_test_host, is_port_open
from voyager_ogm import NativeClient, Session

pytestmark = pytest.mark.live


def _start_tcp_proxy(target_host: str, target_port: int) -> tuple[int, dict[str, object]]:
    """Starts a local TCP proxy to `target_host:target_port` that allows dropping active sockets."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.bind(("127.0.0.1", 0))
    srv.listen(8)
    srv.settimeout(0.1)
    port = srv.getsockname()[1]

    state: dict[str, object] = {
        "stop": threading.Event(),
        "lock": threading.Lock(),
        "active_sockets": [],
    }

    def _pipe(src: socket.socket, dst: socket.socket) -> None:
        try:
            while True:
                data = src.recv(65536)
                if not data:
                    break
                dst.sendall(data)
        except OSError:
            pass
        finally:
            try:
                dst.shutdown(socket.SHUT_WR)
            except OSError:
                pass

    def _accept() -> None:
        stop_ev = state["stop"]
        assert isinstance(stop_ev, threading.Event)
        lock = state["lock"]
        assert isinstance(lock, threading.Lock)
        active = state["active_sockets"]
        assert isinstance(active, list)
        while not stop_ev.is_set():
            try:
                client_sock, _ = srv.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            try:
                upstream_sock = socket.create_connection((target_host, target_port), timeout=5.0)
                client_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                upstream_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError:
                client_sock.close()
                continue
            with lock:
                active.append((client_sock, upstream_sock))
            threading.Thread(target=_pipe, args=(client_sock, upstream_sock), daemon=True).start()
            threading.Thread(target=_pipe, args=(upstream_sock, client_sock), daemon=True).start()

    thread = threading.Thread(target=_accept, daemon=True)
    thread.start()
    state["srv"] = srv
    state["thread"] = thread
    return port, state


def _kill_proxy_sockets(state: dict[str, object]) -> int:
    lock = state["lock"]
    active = state["active_sockets"]
    assert isinstance(lock, threading.Lock)
    assert isinstance(active, list)
    killed = 0
    with lock:
        while active:
            c_sock, u_sock = active.pop()
            for s in (c_sock, u_sock):
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    s.close()
                except OSError:
                    pass
            killed += 1
    return killed


def _stop_tcp_proxy(state: dict[str, object]) -> None:
    stop_ev = state["stop"]
    srv = state["srv"]
    thread = state["thread"]
    assert isinstance(stop_ev, threading.Event)
    assert isinstance(srv, socket.socket)
    assert isinstance(thread, threading.Thread)
    stop_ev.set()
    _kill_proxy_sockets(state)
    srv.close()
    thread.join(timeout=2.0)


def test_live_pg19_pool_recovers_terminated_own_backend() -> None:
    """Terminating our own PG19 backend PID must recover transparently with circuit closed."""
    host = get_test_host()
    if not is_port_open(host, 5456):
        pytest.skip(f"PostgreSQL 19 not reachable on {host}:5456")

    uri = f"postgresql://postgres:voyagerpass123@{host}:5456/voyager_graph"
    session = Session(uri, dialect="sql_pgq", backend="auto", pool_size=1)
    admin = NativeClient(uri, min_idle=0, max_size=1)
    try:
        pid1 = session.execute("SELECT pg_backend_pid() AS pid").all()[0]["pid"]
        term_rows = admin.execute_sync(
            "SELECT pg_terminate_backend($pid::int) AS killed", {"pid": pid1}
        ).to_dicts()
        assert term_rows[0]["killed"] is True

        time.sleep(0.05)

        rows = session.execute("SELECT pg_backend_pid() AS pid, 42 AS v").all()
        assert len(rows) == 1
        assert rows[0]["v"] == 42
        assert rows[0]["pid"] != pid1
        assert session.consecutive_native_failures == 0
        assert session.backend == "native"
    finally:
        admin.close()
        session.close()


def test_live_neo4j_pool_recovers_killed_own_connection() -> None:
    """Terminating our own Neo4j Bolt socket recovers transparently with circuit closed."""
    host = get_test_host()
    if not is_port_open(host, 7687):
        pytest.skip(f"Neo4j not reachable on {host}:7687")

    direct_uri = f"bolt://neo4j:voyagerpass123@{host}:7687"
    admin = NativeClient(direct_uri, min_idle=0, max_size=1)
    try:
        procs = admin.execute_sync(
            "SHOW PROCEDURES YIELD name WHERE name IN ['dbms.listConnections', 'dbms.killConnection'] RETURN name",
            {},
        ).to_dicts()
        proc_names = {row["name"] for row in procs}
        has_kill = {"dbms.listConnections", "dbms.killConnection"}.issubset(proc_names)

        if has_kill:
            # Identify our own connection IDs by diffing Voyager-OGM connections from our client IP.
            before_rows = admin.execute_sync(
                "CALL dbms.listConnections() YIELD connectionId, clientAddress, userAgent "
                "WHERE userAgent STARTS WITH 'Voyager-OGM' "
                "RETURN connectionId, clientAddress",
                {},
            ).to_dicts()
            my_ips = {r["clientAddress"].rsplit(":", 1)[0] for r in before_rows}
            before_ids = {r["connectionId"] for r in before_rows}
            session = Session(direct_uri, dialect="cypher", backend="auto", pool_size=1)
            try:
                assert session.execute("RETURN 1 AS ok").all() == [{"ok": 1}]
                after_rows = admin.execute_sync(
                    "CALL dbms.listConnections() YIELD connectionId, clientAddress, userAgent "
                    "WHERE userAgent STARTS WITH 'Voyager-OGM' "
                    "RETURN connectionId, clientAddress",
                    {},
                ).to_dicts()
                new_ids = [
                    r["connectionId"]
                    for r in after_rows
                    if r["connectionId"] not in before_ids
                    and r["clientAddress"].rsplit(":", 1)[0] in my_ips
                ]
                assert len(new_ids) == 1, f"expected 1 new voyager connection, got {new_ids}"
                admin.execute_sync("CALL dbms.killConnection($cid)", {"cid": new_ids[0]})
                time.sleep(0.05)

                assert session.execute("RETURN 2 AS ok").all() == [{"ok": 2}]
                assert session.consecutive_native_failures == 0
                assert session.backend == "native"
                return
            finally:
                session.close()
    finally:
        admin.close()

    # Fallback when dbms.killConnection is unavailable (e.g. Neo4j Community): local TCP proxy
    proxy_port, proxy_state = _start_tcp_proxy(host, 7687)
    proxy_uri = f"bolt://neo4j:voyagerpass123@127.0.0.1:{proxy_port}"
    session = Session(proxy_uri, dialect="cypher", backend="auto", pool_size=1)
    try:
        assert session.execute("RETURN 1 AS ok").all() == [{"ok": 1}]
        assert _kill_proxy_sockets(proxy_state) == 1
        time.sleep(0.05)

        assert session.execute("RETURN 2 AS ok").all() == [{"ok": 2}]
        assert session.consecutive_native_failures == 0
        assert session.backend == "native"
    finally:
        session.close()
        _stop_tcp_proxy(proxy_state)


def test_live_memgraph_smoke_new_pool_defaults() -> None:
    """Smoke check Memgraph with new pool defaults (test_on_acquire=False, idle=30s)."""
    host = get_test_host()
    if not is_port_open(host, 7688):
        pytest.skip(f"Memgraph not reachable on {host}:7688")

    label = f"ZzPool457Mg_{os.getpid()}"
    session = Session(f"bolt://{host}:7688", dialect="cypher", backend="native", pool_size=2)
    try:
        session.execute(f"CREATE (:{label} {{id: 1, name: 'ok'}})").all()
        rows = session.execute(f"MATCH (n:{label}) RETURN n.id AS id, n.name AS name").all()
        assert rows == [{"id": 1, "name": "ok"}]
        assert session.ping() is True
    finally:
        try:
            session.execute(f"MATCH (n:{label}) DETACH DELETE n").all()
        finally:
            session.close()


def test_live_falkordb_smoke_new_pool_defaults() -> None:
    """Smoke check FalkorDB with new pool defaults (test_on_acquire=False, idle=30s)."""
    host = get_test_host()
    if not is_port_open(host, 6379):
        pytest.skip(f"FalkorDB not reachable on {host}:6379")

    import redis

    graph = f"zz_pool457_fk_{os.getpid()}"
    session = Session(
        f"redis://{host}:6379/{graph}", dialect="cypher", backend="native", pool_size=2
    )
    try:
        session.execute("CREATE (:ZzNode {id: 1, name: 'ok'})").all()
        rows = session.execute("MATCH (n:ZzNode) RETURN n.id AS id, n.name AS name").all()
        assert rows == [{"id": 1, "name": "ok"}]
        assert session.ping() is True
    finally:
        session.close()
        r = redis.Redis(host=host, port=6379)
        try:
            r.delete(graph)
        finally:
            r.close()
