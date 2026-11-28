"""Live streaming matrix for #458: Session.stream across all six engines."""

from __future__ import annotations

import asyncio
import gc
import json
import os
import subprocess
import sys
import uuid

import pytest
from conftest import get_test_host, is_port_open
from voyager_ogm import Session

pytestmark = pytest.mark.live

HOST = get_test_host()
NEO4J_PORT = int(os.environ.get("VOYAGER_TEST_NEO4J_PORT", "7687"))
MEMGRAPH_PORT = int(os.environ.get("VOYAGER_TEST_MEMGRAPH_PORT", "7688"))
FALKORDB_PORT = int(os.environ.get("VOYAGER_TEST_FALKORDB_PORT", "6379"))
PG19_PORT = int(os.environ.get("VOYAGER_TEST_PG19_PORT", "5456"))
AGE_PORT = int(os.environ.get("VOYAGER_TEST_AGE_PORT", "5455"))
PG_USER = "postgres"
PG_PASSWORD = "voyagerpass123"

NEO4J_URI = f"bolt://neo4j:{PG_PASSWORD}@{HOST}:{NEO4J_PORT}"
MEMGRAPH_URI = f"bolt://{HOST}:{MEMGRAPH_PORT}"
PG19_URI = f"postgresql://{PG_USER}:{PG_PASSWORD}@{HOST}:{PG19_PORT}/voyager_graph"
AGE_URI = f"postgresql://{PG_USER}:{PG_PASSWORD}@{HOST}:{AGE_PORT}/voyager_graph"

ENGINES = ("neo4j", "memgraph", "pg19", "age", "falkordb", "duckdb")
TRANSACTION_ENGINES = ("neo4j", "memgraph", "pg19", "age", "duckdb")
STREAMING_ENGINES = ("neo4j", "memgraph", "pg19", "age", "duckdb")
RSS_LIMIT_KB = {"duckdb": 5 * 1024}
DEFAULT_RSS_LIMIT_KB = 3 * 1024


def _require(engine: str) -> None:
    """Skips the test when the engine's port is not reachable."""
    ports = {
        "neo4j": NEO4J_PORT,
        "memgraph": MEMGRAPH_PORT,
        "falkordb": FALKORDB_PORT,
        "pg19": PG19_PORT,
        "age": AGE_PORT,
        "duckdb": 0,
    }
    port = ports[engine]
    if port and not is_port_open(HOST, port):
        pytest.skip(f"{engine} is not reachable on {HOST}:{port}")


def _query(engine: str, rows: int) -> str:
    """Returns a scalar two-column query that produces exactly ``rows`` rows."""
    if engine in ("neo4j", "memgraph", "falkordb"):
        return f"UNWIND range(1, {rows}) AS i RETURN i, 'r' + toString(i) AS s"
    if engine == "duckdb":
        return f"SELECT i, 'r' || CAST(i AS VARCHAR) AS s FROM range(1, {rows + 1}) AS t(i)"
    return f"SELECT i, 'r' || i::text AS s FROM generate_series(1, {rows}) AS t(i)"


def _ping_query(engine: str) -> str:
    """Returns a one-row probe statement for the engine's dialect."""
    return "SELECT 1 AS n" if engine in ("pg19", "age", "duckdb") else "RETURN 1 AS n"


def _make_session(engine: str, *, pool_size: int = 2) -> Session:
    """Builds a native (or DuckDB bridge) session for ``engine``."""
    if engine == "neo4j":
        return Session(NEO4J_URI, dialect="cypher", backend="native", pool_size=pool_size)
    if engine == "memgraph":
        return Session(MEMGRAPH_URI, dialect="cypher", backend="native", pool_size=pool_size)
    if engine == "pg19":
        return Session(PG19_URI, dialect="sql_pgq", backend="native", pool_size=pool_size)
    if engine == "age":
        return Session(AGE_URI, dialect="age", backend="native", pool_size=pool_size)
    if engine == "falkordb":
        import falkordb

        graph = f"zz_stream_falkor_{uuid.uuid4().hex[:8]}"
        client = falkordb.FalkorDB(host=HOST, port=FALKORDB_PORT)
        connection = client.select_graph(graph)
        return Session(connection, dialect="falkordb", backend="bridge", graph_name=graph)
    import duckdb

    return Session(bridge=duckdb.connect(), dialect="duckpgq")


def _cleanup(engine: str, session: Session) -> None:
    """Removes any FalkorDB graph created for a streaming measurement."""
    if engine != "falkordb":
        return
    graph_name = session.graph_name
    if not graph_name:
        return
    try:
        import falkordb

        client = falkordb.FalkorDB(host=HOST, port=FALKORDB_PORT)
        client.select_graph(graph_name).delete()
    except Exception:
        pass


@pytest.mark.parametrize("engine", ENGINES)
def test_live_stream_rows_match_execute(engine: str) -> None:
    """Streamed batches contain exactly the rows returned by execute()."""
    _require(engine)
    session = _make_session(engine)
    try:
        query = _query(engine, 250)
        expected = session.execute(query).all()
        with session.stream(query, fetch_size=50) as stream:
            assert stream.columns == ("i", "s")
            assert stream.is_server_streaming is (engine in STREAMING_ENGINES)
            rows = [row for batch in stream for row in batch]
        assert len(rows) == 250
        assert rows == expected
        assert stream.is_closed is True
        assert session.active_stream_count == 0
    finally:
        _cleanup(engine, session)
        session.close()


@pytest.mark.parametrize("engine", ENGINES)
def test_live_stream_early_break_and_followup(engine: str) -> None:
    """Breaking after one batch discards unread rows and leaves the session usable."""
    _require(engine)
    session = _make_session(engine)
    try:
        with session.stream(_query(engine, 2_000), fetch_size=100) as stream:
            assert len(next(iter(stream))) == 100
            assert stream.is_server_streaming is (engine in STREAMING_ENGINES)
        assert session.active_stream_count == 0
        assert session.execute(_query(engine, 3)).all() == [
            {"i": 1, "s": "r1"},
            {"i": 2, "s": "r2"},
            {"i": 3, "s": "r3"},
        ]
    finally:
        _cleanup(engine, session)
        session.close()


@pytest.mark.parametrize("engine", TRANSACTION_ENGINES)
def test_live_transaction_stream_fail_fast_and_followup(engine: str) -> None:
    """A stream inside a transaction blocks tx.execute until it is closed."""
    _require(engine)
    session = _make_session(engine, pool_size=1)
    try:
        with session.transaction() as tx:
            with tx.stream(_query(engine, 250), fetch_size=25) as stream:
                assert len(next(iter(stream))) == 25
                with pytest.raises(RuntimeError, match="active stream"):
                    tx.execute(_ping_query(engine))
            assert tx.execute(_ping_query(engine)).all() == [{"n": 1}]
    finally:
        _cleanup(engine, session)
        session.close()


@pytest.mark.parametrize("engine", ENGINES)
@pytest.mark.asyncio
async def test_live_async_stream_cancel_then_reuse(engine: str) -> None:
    """Cancelling an async stream open still leaves the engine usable."""
    from voyager_ogm import AsyncSession

    _require(engine)
    if engine == "falkordb":
        pytest.skip("FalkorDB live async streaming is covered by the bridge sync path")
    if engine == "duckdb":
        import duckdb

        session = AsyncSession(bridge=duckdb.connect(), dialect="duckpgq")
    elif engine == "neo4j":
        session = AsyncSession(NEO4J_URI, dialect="cypher", backend="native")
    elif engine == "memgraph":
        session = AsyncSession(MEMGRAPH_URI, dialect="cypher", backend="native")
    elif engine == "pg19":
        session = AsyncSession(PG19_URI, dialect="sql_pgq", backend="native")
    else:
        session = AsyncSession(AGE_URI, dialect="age", backend="native")
    try:
        stream = session.stream(_query(engine, 100_000), fetch_size=10)
        task = asyncio.create_task(stream.open())
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.sleep(0)
        gc.collect()
        await stream.aclose()
        assert (await session.execute(_ping_query(engine))).all() == [{"n": 1}]
        assert session.active_stream_count == 0
    finally:
        await session.close()


_MEASURE_SCRIPT = r"""
import json, os, time
import duckdb
from voyager_ogm import Session

config = json.loads(os.environ["ZZ_STREAM_CONFIG"])
engine = config["engine"]
rows = config["rows"]
host = config["host"]

def hwm_kb():
    with open("/proc/self/status", encoding="utf-8") as handle:
        for line in handle:
            if line.startswith("VmHWM:"):
                return int(line.split()[1])
    return 0

if engine == "neo4j":
    session = Session(config["uri"], dialect="cypher", backend="native")
elif engine == "memgraph":
    session = Session(config["uri"], dialect="cypher", backend="native")
elif engine == "pg19":
    session = Session(config["uri"], dialect="sql_pgq", backend="native")
elif engine == "age":
    session = Session(config["uri"], dialect="age", backend="native")
elif engine == "falkordb":
    import falkordb
    client = falkordb.FalkorDB(host=host, port=int(config["port"]))
    session = Session(client.select_graph(config["graph"]), dialect="falkordb")
else:
    session = Session(bridge=duckdb.connect(), dialect="duckpgq")

# Warm the query path (imports, pool, planner) before measuring the delta.
with session.stream(config["warmup"], fetch_size=1_000) as warm:
    for _batch in warm:
        pass

base = hwm_kb()
start = time.perf_counter()
first = None
total = 0
with session.stream(config["query"], fetch_size=1_000) as stream:
    for batch in stream:
        if first is None:
            first = time.perf_counter() - start
        total += len(batch)
peak = hwm_kb()
elapsed = time.perf_counter() - start
print(json.dumps({
    "rows": total,
    "base_kb": base,
    "peak_kb": peak,
    "delta_kb": peak - base,
    "ttfb_ms": (first or 0.0) * 1000.0,
    "ttlb_ms": elapsed * 1000.0,
}))
session.close()
"""


def _measure(engine: str, rows: int) -> dict[str, float]:
    """Runs one clean-subprocess streaming measurement and returns its metrics."""
    session = _make_session(engine)
    graph = session.graph_name or f"zz_stream_falkor_{uuid.uuid4().hex[:8]}"
    session.close()
    config = {
        "engine": engine,
        "rows": rows,
        "host": HOST,
        "port": FALKORDB_PORT,
        "graph": graph,
        "query": _query(engine, rows),
        "warmup": _query(engine, 10),
        "uri": {
            "neo4j": NEO4J_URI,
            "memgraph": MEMGRAPH_URI,
            "pg19": PG19_URI,
            "age": AGE_URI,
        }.get(engine, ""),
    }
    env = dict(os.environ)
    env["ZZ_STREAM_CONFIG"] = json.dumps(config)
    result = subprocess.run(
        [sys.executable, "-c", _MEASURE_SCRIPT],
        capture_output=True,
        text=True,
        env=env,
        timeout=180,
        check=False,
    )
    assert result.returncode == 0, result.stderr[-2_000:]
    return json.loads(result.stdout.strip().splitlines()[-1])


@pytest.mark.parametrize("engine", ENGINES)
def test_live_stream_rss_and_ttfb(engine: str) -> None:
    """1k vs 100k peak RSS stays flat and time-to-first-batch stays below 10% of total."""
    _require(engine)
    one = _measure(engine, 1_000)
    # FalkorDB's default server result-set cap is 10,000 rows, so its buffered
    # stream is measured at 10k instead of 100k.
    big_rows = 10_000 if engine == "falkordb" else 100_000
    big = _measure(engine, big_rows)
    assert one["rows"] == 1_000, one
    assert big["rows"] == big_rows, big
    print(
        f"{engine}: 1k delta={one['delta_kb']}KB ttfb={one['ttfb_ms']:.2f}ms "
        f"ttlb={one['ttlb_ms']:.2f}ms | 100k delta={big['delta_kb']}KB "
        f"ttfb={big['ttfb_ms']:.2f}ms ttlb={big['ttlb_ms']:.2f}ms"
    )
    if engine in STREAMING_ENGINES:
        assert big["delta_kb"] <= RSS_LIMIT_KB.get(engine, DEFAULT_RSS_LIMIT_KB), big
        assert big["ttfb_ms"] < 0.10 * big["ttlb_ms"], big
    else:
        # FalkorDB has no server-side cursor: the complete RESP reply is
        # buffered, so its peak RSS and time to first batch track total rows.
        assert big["delta_kb"] > one["delta_kb"], (one, big)
    if engine == "duckdb":
        assert big["delta_kb"] <= 5 * 1024, big
