"""FFI boundary hardening, Arrow C-Stream lifecycle, and dual-backend integration tests."""

from __future__ import annotations

import datetime
import threading
import uuid
from decimal import Decimal

import polars as pl
import pyarrow as pa
import pytest
from conftest import get_test_host
from voyager_ogm import (
    AsyncSession,
    NativeClient,
    Query,
    Session,
    col,
    generate_synthetic_stream,
    lit,
)
from voyager_ogm._voyager_rs import NativeQueryBuilder, compile_query_from_spec

HOST = get_test_host()
FALKORDB_URI = f"redis://{HOST}:6379?connect_timeout=2"
NEO4J_URI = f"bolt://neo4j:voyagerpass123@{HOST}:7687?connect_timeout=2"


def _is_falkordb_online() -> bool:
    try:
        c = NativeClient(FALKORDB_URI, min_idle=1, max_size=2)
        online = c.ping_sync()
        c.close()
        return online
    except Exception:
        return False


def _is_neo4j_online() -> bool:
    try:
        c = NativeClient(NEO4J_URI, min_idle=1, max_size=2)
        online = c.ping_sync()
        c.close()
        return online
    except Exception:
        return False


FALKORDB_ONLINE = _is_falkordb_online()
NEO4J_ONLINE = _is_neo4j_online()


# ---------------------------------------------------------------------------
# 1. AST Recursion Depth Guard (MAX_AST_DEPTH = 256)
# ---------------------------------------------------------------------------


def _run_with_1mb_stack(target):
    """Run target callable inside a thread with 1 MB stack size (proving Windows thread condition)."""
    result = None
    exception = None

    def worker():
        nonlocal result, exception
        try:
            result = target()
        except BaseException as exc:
            exception = exc

    old_stack = threading.stack_size(1024 * 1024)
    try:
        t = threading.Thread(target=worker)
        t.start()
        t.join()
    finally:
        threading.stack_size(old_stack)

    if exception is not None:
        raise exception
    return result


def test_ast_max_depth_guard_raises_value_error():
    """Verify that deeply nested AST expressions (>256) trigger a controlled PyValueError."""
    nested_spec: tuple = ("lit", 1)
    for _ in range(260):
        nested_spec = ("bin", "+", nested_spec, ("lit", 1))

    query_spec = {
        "spec_version": 1,
        "matches": [
            {
                "paths": [[("node", "n", ["Person"], None)]],
                "where": [nested_spec],
            }
        ],
    }

    with pytest.raises(ValueError, match="Maximum AST expression recursion depth exceeded"):
        _run_with_1mb_stack(lambda: compile_query_from_spec(query_spec, "cypher"))


def test_ast_within_depth_limit_succeeds():
    """Verify that nested AST expressions within the depth limit (<256) compile properly."""
    nested_spec: tuple = ("lit", 1)
    for _ in range(50):
        nested_spec = ("bin", "+", nested_spec, ("lit", 1))

    query_spec = {
        "spec_version": 1,
        "matches": [
            {
                "paths": [[("node", "n", ["Person"], None)]],
                "where": [nested_spec],
            }
        ],
    }

    compiled = _run_with_1mb_stack(lambda: compile_query_from_spec(query_spec, "cypher"))
    assert "MATCH (n:Person)" in compiled["statement"]
    assert "WHERE" in compiled["statement"]


def test_ast_max_depth_guard_across_exists_subqueries():
    """Verify the depth budget survives exists-subquery re-entry instead of resetting to 0."""
    nested_spec: dict = {
        "spec_version": 1,
        "matches": [{"paths": [[("node", "n", ["Person"], None)]]}],
    }
    for _ in range(300):
        nested_spec = {
            "spec_version": 1,
            "matches": [
                {
                    "paths": [[("node", "n", ["Person"], None)]],
                    "where": [["exists", nested_spec]],
                }
            ],
        }

    query_spec = {
        "spec_version": 1,
        "matches": [
            {
                "paths": [[("node", "n", ["Person"], None)]],
                "where": [["exists", nested_spec]],
            }
        ],
    }

    with pytest.raises(ValueError, match="Maximum AST expression recursion depth exceeded"):
        compile_query_from_spec(query_spec, "cypher", use_cache=False)


def test_spec_version_validation():
    """Verify that compile_query_from_spec strictly enforces spec_version == 1."""
    # 1. Missing spec_version
    unversioned_spec = {
        "matches": [
            {
                "paths": [[("node", "n", ["Person"], None)]],
            }
        ],
    }
    with pytest.raises(
        ValueError, match="Missing required 'spec_version' in query spec dictionary"
    ):
        compile_query_from_spec(unversioned_spec, "cypher")

    # 2. Unsupported spec_version
    unsupported_spec = {
        "spec_version": 2,
        "matches": [
            {
                "paths": [[("node", "n", ["Person"], None)]],
            }
        ],
    }
    with pytest.raises(ValueError, match="Unsupported query spec_version: 2"):
        compile_query_from_spec(unsupported_spec, "cypher")

    # 3. Inner match clause with incompatible spec_version
    invalid_inner_match = {
        "spec_version": 1,
        "matches": [
            {
                "spec_version": 99,
                "paths": [[("node", "n", ["Person"], None)]],
            }
        ],
    }
    with pytest.raises(ValueError, match="Unsupported match clause spec_version: 99"):
        compile_query_from_spec(invalid_inner_match, "cypher")


# ---------------------------------------------------------------------------
# 2. Dynamic Parameter Expansion in FFI Boundary
# ---------------------------------------------------------------------------


def test_py_to_literal_parameter_expansion():
    """Verify conversion of dict, datetime, date, UUID, and Decimal into native literals."""
    test_uuid = uuid.uuid4()
    test_now = datetime.datetime(2026, 9, 10, 12, 0, 0, tzinfo=datetime.UTC)
    test_date = datetime.date(2026, 9, 10)
    test_decimal = Decimal("123.456")
    test_dict = {"key": "val", "count": 42}

    qb = NativeQueryBuilder()
    qb.match()
    qb.node("n", ["Person"])
    qb.where_expr(("lit", test_dict))
    qb.where_expr(("lit", test_uuid))
    qb.where_expr(("lit", test_now))
    qb.where_expr(("lit", test_date))
    qb.where_expr(("lit", test_decimal))

    compiled = qb.compile(dialect="cypher")
    stmt = compiled["statement"]
    assert "WHERE" in stmt
    assert "Person" in stmt


def test_query_expressions_with_expanded_types():
    """Verify that Query DSL serializes date, UUID, Decimal, and dict seamlessly."""
    u = uuid.uuid4()
    d = datetime.date(2026, 1, 1)
    dec = Decimal("99.95")

    q = (
        Query()
        .match()
        .node("n", labels=["Person"])
        .where(col("n.uuid") == lit(u))
        .where(col("n.created") >= lit(d))
        .where(col("n.amount") == lit(dec))
        .return_("n")
    )
    compiled = q.compile("cypher")
    assert "$p0" in compiled.statement
    assert "$p1" in compiled.statement
    assert "$p2" in compiled.statement
    assert compiled.parameters["p0"] == str(u)
    assert compiled.parameters["p1"] == "2026-01-01"
    assert abs(compiled.parameters["p2"] - 99.95) < 1e-6


# ---------------------------------------------------------------------------
# 3. Arrow C Stream Single-Pass Lifecycle Validation
# ---------------------------------------------------------------------------


def test_arrow_stream_single_pass_consumption():
    """Verify that ArrowStream obeys the Arrow C Data Interface single-pass constraint."""
    stream = generate_synthetic_stream(10)
    assert not stream.is_consumed

    # First consumption via Polars DataFrame
    df = pl.DataFrame(stream)
    assert df.height == 10
    assert stream.is_consumed

    # Second consumption must raise RuntimeError
    with pytest.raises(RuntimeError, match="ArrowArrayStream has already been consumed"):
        pl.DataFrame(stream)


def test_arrow_stream_to_dicts_does_not_consume():
    """Verify that calling to_dicts() extracts Python dicts without consuming the C stream."""
    stream = generate_synthetic_stream(5)
    assert not stream.is_consumed

    # to_dicts() does not consume the underlying C stream
    records = stream.to_dicts()
    assert len(records) == 5
    assert not stream.is_consumed

    # Afterwards, the C stream can still be consumed by PyArrow or Polars
    table = pa.RecordBatchReader.from_stream(stream).read_all()
    assert table.num_rows == 5
    assert stream.is_consumed


# ---------------------------------------------------------------------------
# 4. NativeClient Sync & Async Execution
# ---------------------------------------------------------------------------


def test_native_client_invalid_uri():
    """Verify that constructing NativeClient with an invalid URI raises a ValueError."""
    with pytest.raises(ValueError, match="Unsupported database URI scheme"):
        NativeClient("unknown://host:1234")


@pytest.mark.live
@pytest.mark.skipif(not FALKORDB_ONLINE, reason="Live FalkorDB container not available")
def test_native_client_falkordb_sync_and_async():
    """Verify synchronous and asynchronous query execution on FalkorDB via NativeClient."""
    import asyncio

    client = NativeClient(FALKORDB_URI, min_idle=1, max_size=5)
    assert client.uri == FALKORDB_URI
    assert "Redis" in client.protocol
    assert client.ping_sync() is True

    # Sync execution
    res_sync = client.execute_sync("RETURN 101 AS sync_val", {})
    assert res_sync.num_rows == 1
    assert res_sync.records == [{"sync_val": 101}]
    assert res_sync.columns == ["sync_val"]
    assert res_sync.execution_time_ms >= 0

    # Async execution
    async def run_async():
        res_async = await client.execute("RETURN 202 AS async_val", {})
        assert res_async.num_rows == 1
        assert res_async.records == [{"async_val": 202}]
        assert await client.ping() is True

    asyncio.run(run_async())
    client.close()


@pytest.mark.live
@pytest.mark.skipif(not NEO4J_ONLINE, reason="Live Neo4j container not available")
def test_native_client_neo4j_sync_and_async():
    """Verify synchronous and asynchronous query execution on Neo4j via NativeClient."""
    import asyncio

    client = NativeClient(NEO4J_URI, min_idle=1, max_size=5)
    assert client.uri == NEO4J_URI
    assert client.protocol == "Bolt"
    assert client.ping_sync() is True

    # Sync execution
    res_sync = client.execute_sync("RETURN 303 AS sync_val", {})
    assert res_sync.num_rows == 1
    assert res_sync.records == [{"sync_val": 303}]

    # Async execution
    async def run_async():
        res_async = await client.execute("RETURN 404 AS async_val", {})
        assert res_async.num_rows == 1
        assert res_async.records == [{"async_val": 404}]
        assert await client.ping() is True

    asyncio.run(run_async())
    client.close()


# ---------------------------------------------------------------------------
# 5. Dual-Backend Integration in Session & AsyncSession
# ---------------------------------------------------------------------------


def test_session_backend_selection():
    """Verify backend selection behavior across native, bridge, and auto."""
    # Mock bridge defaults to bridge backend even with auto
    s_mock = Session("mock://memory", backend="auto")
    assert s_mock.backend == "bridge"
    assert s_mock.native_client is None

    # Invalid string URI with backend="native" raises ValueError
    with pytest.raises(ValueError, match="Native backend requires a valid URI string"):
        Session(None, backend="native")

    with pytest.raises(RuntimeError, match="Unsupported database URI scheme"):
        Session("unsupported://127.0.0.1:9999", backend="native")


@pytest.mark.live
@pytest.mark.skipif(not FALKORDB_ONLINE, reason="Live FalkorDB container not available")
def test_session_native_falkordb():
    """Verify Session full query workflow using native backend against FalkorDB."""
    with Session(FALKORDB_URI, backend="native") as s:
        assert s.backend == "native"
        assert s.native_client is not None
        assert s.ping() is True

        res = s.execute("RETURN 42 AS answer, 'voyager' AS framework")
        assert res.first() == {"answer": 42, "framework": "voyager"}
        assert res.statement == "RETURN 42 AS answer, 'voyager' AS framework"

        # Conversion to Polars
        df = res.to_polars()
        assert df.shape == (1, 2)
        assert df["answer"][0] == 42
        assert df["framework"][0] == "voyager"

        # Direct execute_to_polars
        df2 = s.execute_to_polars("RETURN 99 AS n")
        assert df2.shape == (1, 1)
        assert df2["n"][0] == 99

        # Direct execute_to_arrow
        tbl = s.execute_to_arrow("RETURN 123 AS num")
        assert tbl.num_rows == 1
        assert tbl.column("num")[0].as_py() == 123


@pytest.mark.live
@pytest.mark.skipif(not FALKORDB_ONLINE, reason="Live FalkorDB container not available")
@pytest.mark.asyncio
async def test_async_session_native_falkordb():
    """Verify AsyncSession full query workflow using native backend against FalkorDB."""
    async with AsyncSession(FALKORDB_URI, backend="native") as s:
        assert s.backend == "native"
        assert s.native_client is not None
        assert await s.ping() is True

        res = await s.execute("RETURN 555 AS lucky")
        assert res.first() == {"lucky": 555}

        # Conversion to Arrow
        tbl = res.to_arrow()
        assert tbl.num_rows == 1
        assert tbl.column("lucky")[0].as_py() == 555

        # Direct execute_to_polars
        df = await s.execute_to_polars("RETURN 777 AS val")
        assert df.shape == (1, 1)
        assert df["val"][0] == 777


def test_execution_result_lazy_stream_materialization():
    """Verify that ExecutionResult wraps an ArrowStream lazily without eager dict materialization."""
    from voyager_ogm.session import ExecutionResult

    stream = generate_synthetic_stream(5_000)
    res = ExecutionResult(stream=stream)

    # Stream must NOT be consumed upon __init__
    assert not stream.is_consumed

    # O(1) len(res) inspection reads stream.num_rows without consuming stream
    assert len(res) == 5_000
    assert not stream.is_consumed

    # .to_polars() consumes the stream directly into Polars DataFrame without allocating dicts
    df = res.to_polars()
    assert df.shape == (5_000, 6)
    assert stream.is_consumed

    # Subsequent access (indexing, .first(), .all(), .to_arrow()) uses cached dataframe without re-consuming
    assert res.first()["id"] == 0
    assert res[1]["id"] == 1
    assert len(res.all()) == 5_000
    tbl = res.to_arrow()
    assert tbl.num_rows == 5_000
    assert len(res) == 5_000


def _pg_msg(tag: bytes, payload: bytes) -> bytes:
    import struct

    return tag + struct.pack("!I", len(payload) + 4) + payload


def _pg_row_description(cols: list[tuple[str, int, int]]) -> bytes:
    import struct

    out = bytearray(struct.pack("!H", len(cols)))
    for name, oid, typmod in cols:
        out.extend(name.encode("utf-8") + b"\x00")
        out.extend(struct.pack("!IHIHIH", 0, 0, oid, 0xFFFF, typmod & 0xFFFFFFFF, 0))
    return _pg_msg(b"T", bytes(out))


def _pg_data_row(values: list[bytes | None]) -> bytes:
    import struct

    out = bytearray(struct.pack("!H", len(values)))
    for v in values:
        if v is None:
            out.extend(struct.pack("!i", -1))
        else:
            out.extend(struct.pack("!i", len(v)) + v)
    return _pg_msg(b"D", bytes(out))


def test_native_pg_stub_interval_numeric_scale_and_era_fallback():
    """Offline PG wire stub verifies Duration(us) days=0, numeric(10,2) scale, and BC era fallback."""
    import socket
    import struct
    import threading
    from decimal import Decimal

    from voyager_ogm import Duration, NativeClient

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.settimeout(3.0)
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    port = srv.getsockname()[1]

    def _recv_exact(sock: socket.socket, n: int) -> bytes:
        out = bytearray()
        while len(out) < n:
            chunk = sock.recv(n - len(out))
            if not chunk:
                return bytes(out)
            out.extend(chunk)
        return bytes(out)

    def _serve() -> None:
        conn, _ = srv.accept()
        with conn:
            conn.settimeout(3.0)
            # Read startup packet (len + payload)
            hdr = _recv_exact(conn, 4)
            if len(hdr) < 4:
                return
            plen = struct.unpack("!I", hdr)[0]
            _recv_exact(conn, plen - 4)
            conn.sendall(_pg_msg(b"R", struct.pack("!I", 0)))
            conn.sendall(_pg_msg(b"K", struct.pack("!II", 1, 2)))
            conn.sendall(_pg_msg(b"Z", b"I"))

            typmod_10_2 = ((10 << 16) | 2) + 4
            rd = _pg_row_description(
                [
                    ("iv", 1186, -1),
                    ("num", 1700, typmod_10_2),
                    ("ts_bc", 1184, -1),
                    ("t_bc", 1266, -1),
                ]
            )
            dr = _pg_data_row(
                [
                    b"25:00:00",
                    b"1.50",
                    b"0044-03-15 10:00:00+00 BC",
                    b"10:00:00+00 BC",
                ]
            )

            while True:
                msg_hdr = _recv_exact(conn, 5)
                if len(msg_hdr) < 5:
                    break
                tag = msg_hdr[:1]
                mlen = struct.unpack("!I", msg_hdr[1:])[0]
                _recv_exact(conn, max(0, mlen - 4))
                if tag == b"Q":
                    conn.sendall(rd + dr + _pg_msg(b"C", b"SELECT 1\x00") + _pg_msg(b"Z", b"I"))
                elif tag == b"S":
                    conn.sendall(
                        _pg_msg(b"1", b"")
                        + _pg_msg(b"2", b"")
                        + rd
                        + dr
                        + _pg_msg(b"C", b"SELECT 1\x00")
                        + _pg_msg(b"Z", b"I")
                    )
                elif tag == b"X":
                    break

    t = threading.Thread(target=_serve, daemon=True)
    t.start()
    try:
        client = NativeClient(
            f"postgresql://postgres@127.0.0.1:{port}/voyager_graph?sslmode=disable",
            min_idle=1,
            max_size=1,
        )
        try:
            rows = client.execute_sync("SELECT 1", {}).records
            assert len(rows) == 1
            row = rows[0]
            assert row["iv"] == Duration(months=0, days=0, seconds=90_000, nanoseconds=0)
            assert row["iv"].days == 0 and row["iv"].seconds == 90_000
            assert isinstance(row["num"], Decimal) and str(row["num"]) == "1.50"
            assert row["ts_bc"] == "0044-03-15 10:00:00+00 BC"
            assert row["t_bc"] == "10:00:00+00 BC"
        finally:
            client.close()
    finally:
        srv.close()
        t.join(timeout=2)


def test_native_bolt_v4_0x66_local_zone_stub():
    """Offline Bolt v4.4 wire stub verifies 0x66 local wall-clock seconds convert via ZoneInfo."""
    import socket
    import struct
    import threading
    import zoneinfo

    from voyager_ogm import NativeClient

    def _pack_chunks(msg: bytes) -> bytes:
        return struct.pack("!H", len(msg)) + msg + b"\x00\x00"

    def _ps_str(s: str) -> bytes:
        b = s.encode("utf-8")
        if len(b) < 16:
            return bytes([0x80 + len(b)]) + b
        return bytes([0xD0, len(b)]) + b

    def _ps_int(v: int) -> bytes:
        if -16 <= v <= 127:
            return struct.pack("!b", v)
        return b"\xca" + struct.pack("!i", v)

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    port = srv.getsockname()[1]

    def _read_msg(conn: socket.socket) -> bytes:
        out = bytearray()
        while True:
            hdr = conn.recv(2)
            if len(hdr) < 2:
                return bytes(out)
            sz = struct.unpack("!H", hdr)[0]
            if sz == 0:
                return bytes(out)
            while sz > 0:
                chunk = conn.recv(sz)
                out.extend(chunk)
                sz -= len(chunk)

    def _serve() -> None:
        conn, _ = srv.accept()
        with conn:
            conn.recv(20)  # magic + 4 versions
            conn.sendall(bytes([0x00, 0x00, 0x04, 0x04]))  # negotiate Bolt 4.4
            _read_msg(conn)  # HELLO
            conn.sendall(_pack_chunks(b"\xb1\x70\xa0"))  # SUCCESS {}
            while True:
                msg = _read_msg(conn)
                if not msg:
                    break
                tag = msg[1] if len(msg) > 1 else 0
                if tag == 0x10:  # RUN
                    _read_msg(conn)  # PULL
                    # SUCCESS {"fields": ["dt"]}
                    run_ok = b"\xb1\x70\xa1" + _ps_str("fields") + b"\x91" + _ps_str("dt")
                    # RECORD [0x66(1704164645, 0, "Europe/Berlin")] -> 2024-01-02 03:04:05 local
                    dtz = (
                        b"\xb3\x66" + _ps_int(1_704_164_645) + _ps_int(0) + _ps_str("Europe/Berlin")
                    )
                    rec = b"\xb1\x71\x91" + dtz
                    pull_ok = b"\xb1\x70\xa0"
                    conn.sendall(_pack_chunks(run_ok) + _pack_chunks(rec) + _pack_chunks(pull_ok))
                elif tag == 0x0F:  # RESET
                    conn.sendall(_pack_chunks(b"\xb1\x70\xa0"))
                elif tag == 0x02:  # GOODBYE
                    break

    t = threading.Thread(target=_serve, daemon=True)
    t.start()
    try:
        client = NativeClient(f"bolt://127.0.0.1:{port}", min_idle=1, max_size=1)
        try:
            rows = client.execute_sync("RETURN 1 AS dt", {}).records
            assert len(rows) == 1
            row = rows[0]
            expected = datetime.datetime(
                2024, 1, 2, 3, 4, 5, tzinfo=zoneinfo.ZoneInfo("Europe/Berlin")
            )
            assert row["dt"] == expected
            assert row["dt"].utcoffset() == datetime.timedelta(hours=1)
        finally:
            client.close()
    finally:
        srv.close()
        t.join(timeout=2)


def test_native_client_pool_health_and_idle_threshold_python() -> None:
    """Verify NativeClient test_on_acquire / test_on_idle_after options and local EOF eviction."""
    import socket
    import struct
    import threading
    import time

    from voyager_ogm import NativeClient, Session

    with pytest.raises(ValueError, match="test_on_idle_after"):
        NativeClient("bolt://127.0.0.1:7687", min_idle=0, test_on_idle_after=-1.0)
    with pytest.raises(ValueError, match="test_on_idle_after"):
        NativeClient("bolt://127.0.0.1:7687", min_idle=0, test_on_idle_after=float("nan"))

    def _pack_chunks(msg: bytes) -> bytes:
        return struct.pack("!H", len(msg)) + msg + b"\x00\x00"

    def _read_msg(conn: socket.socket) -> bytes:
        out = bytearray()
        while True:
            hdr = conn.recv(2)
            if len(hdr) < 2:
                return bytes(out)
            sz = struct.unpack("!H", hdr)[0]
            if sz == 0:
                return bytes(out)
            while sz > 0:
                chunk = conn.recv(sz)
                if not chunk:
                    return bytes(out)
                out.extend(chunk)
                sz -= len(chunk)

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.bind(("127.0.0.1", 0))
    srv.listen(8)
    port = srv.getsockname()[1]

    stop_flag = threading.Event()
    reset_count = 0
    drop_after_first_query = threading.Event()
    dropped_once = False
    lock = threading.Lock()

    def _handle(conn: socket.socket) -> None:
        nonlocal reset_count, dropped_once
        with conn:
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            if len(conn.recv(20)) < 20:
                return
            conn.sendall(bytes([0x00, 0x00, 0x04, 0x04]))
            _read_msg(conn)  # HELLO
            conn.sendall(_pack_chunks(b"\xb1\x70\xa0"))
            while not stop_flag.is_set():
                msg = _read_msg(conn)
                if not msg:
                    break
                tag = msg[1] if len(msg) > 1 else 0
                if tag == 0x10:  # RUN
                    _read_msg(conn)  # PULL
                    run_ok = b"\xb1\x70\xa1\x86fields\x91\x81v"
                    rec = b"\xb1\x71\x91\x01"
                    pull_ok = b"\xb1\x70\xa0"
                    conn.sendall(_pack_chunks(run_ok) + _pack_chunks(rec) + _pack_chunks(pull_ok))
                    with lock:
                        should_drop = drop_after_first_query.is_set() and not dropped_once
                        if should_drop:
                            dropped_once = True
                    if should_drop:
                        break
                elif tag == 0x0F:  # RESET
                    with lock:
                        reset_count += 1
                    conn.sendall(_pack_chunks(b"\xb1\x70\xa0"))
                elif tag == 0x02:  # GOODBYE
                    break

    def _accept_loop() -> None:
        srv.settimeout(0.1)
        while not stop_flag.is_set():
            try:
                conn, _ = srv.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            threading.Thread(target=_handle, args=(conn,), daemon=True).start()

    accept_t = threading.Thread(target=_accept_loop, daemon=True)
    accept_t.start()
    try:
        # 1. Default: 0 RESETs within threshold, 1 RESET after idle > threshold.
        client = NativeClient(
            f"bolt://127.0.0.1:{port}",
            min_idle=0,
            max_size=1,
            test_on_acquire=False,
            test_on_idle_after=0.05,
        )
        try:
            for _ in range(4):
                assert client.execute_sync("RETURN 1 AS v", {}).to_dicts() == [{"v": 1}]
            assert reset_count == 0
            time.sleep(0.07)
            assert client.execute_sync("RETURN 1 AS v", {}).to_dicts() == [{"v": 1}]
            assert reset_count == 1
        finally:
            client.close()

        # 2. Opt-in test_on_acquire=True sends RESET on every reused checkout.
        before = reset_count
        client_toa = NativeClient(
            f"bolt://127.0.0.1:{port}",
            min_idle=0,
            max_size=1,
            test_on_acquire=True,
            test_on_idle_after=30.0,
        )
        try:
            for _ in range(3):
                client_toa.execute_sync("RETURN 1 AS v", {})
            assert reset_count - before == 2
        finally:
            client_toa.close()

        # 3. Session(backend="auto") recovers transparently from a server-closed idle socket.
        drop_after_first_query.set()
        sess = Session(
            f"bolt://127.0.0.1:{port}",
            dialect="cypher",
            backend="auto",
            fallback="mock",
            pool_size=1,
        )
        try:
            assert sess.execute("RETURN 1 AS v").all() == [{"v": 1}]
            time.sleep(0.03)
            assert sess.execute("RETURN 1 AS v").all() == [{"v": 1}]
            assert sess.consecutive_native_failures == 0
            assert sess.backend == "native"
        finally:
            sess.close()
    finally:
        stop_flag.set()
        srv.close()
        accept_t.join(timeout=2)
