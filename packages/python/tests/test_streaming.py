"""Offline tests for #458 Stage 6: Python pull-based result streams."""

from __future__ import annotations

import asyncio
import gc
import time
from typing import Any

import duckdb
import pyarrow as pa
import pytest
from test_native_result_stream import _BoltStreamStub
from voyager_ogm import (
    AsyncMockBridge,
    AsyncResultStream,
    AsyncSession,
    MockBridge,
    Neo4jBoltBridge,
    ResultStream,
    Session,
)


def _records(count: int) -> list[dict[str, Any]]:
    return [{"i": index, "s": f"r{index}"} for index in range(1, count + 1)]


def _mock_session(count: int) -> tuple[Session, MockBridge]:
    bridge = MockBridge()
    bridge.queue_result(_records(count))
    return Session(bridge=bridge, dialect="cypher"), bridge


def test_mock_stream_rows_batches_and_formats() -> None:
    """MockBridge streams rows, chunks, Arrow record batches, and Polars frames."""
    session, _ = _mock_session(25)
    with session.stream("MATCH (n) RETURN n", fetch_size=10) as stream:
        assert isinstance(stream, ResultStream)
        assert stream.columns == ("i", "s")
        assert stream.is_server_streaming is False
        assert [len(batch) for batch in stream] == [10, 10, 5]
    assert stream.is_closed is True
    assert stream.is_closed is True
    assert session.active_stream_count == 0

    session, _ = _mock_session(25)
    with session.stream("MATCH (n) RETURN n", fetch_size=7) as stream:
        assert len(list(stream.iter_rows())) == 25
    assert session.active_stream_count == 0

    session, _ = _mock_session(25)
    with session.stream("MATCH (n) RETURN n", fetch_size=10, as_format="arrow") as stream:
        batches = list(stream)
    assert all(isinstance(batch, pa.RecordBatch) for batch in batches)
    assert [batch.num_rows for batch in batches] == [10, 10, 5]
    assert pa.Table.from_batches(batches).column_names == ["i", "s"]

    session, _ = _mock_session(25)
    with session.stream("MATCH (n) RETURN n", fetch_size=10, as_format="polars") as stream:
        frames = list(stream)
    assert [frame.height for frame in frames] == [10, 10, 5]
    assert frames[0].columns == ["i", "s"]


def test_mock_stream_iter_batches_and_arrow_reader() -> None:
    """iter_batches and to_arrow_reader expose bounded chunks and one locked schema."""
    session, _ = _mock_session(30)
    with session.stream("MATCH (n) RETURN n", fetch_size=10) as stream:
        assert [len(batch) for batch in stream.iter_batches()] == [10, 10, 10]

    session, _ = _mock_session(30)
    with session.stream("MATCH (n) RETURN n", fetch_size=10) as stream:
        reader = stream.to_arrow_reader()
        table = reader.read_all()
        assert table.num_rows == 30
        assert table.column_names == ["i", "s"]
        with pytest.raises(RuntimeError, match="only be called once"):
            stream.to_arrow_reader()
    assert session.active_stream_count == 0


def test_mock_stream_early_break_and_exception_inside_with() -> None:
    """Early break and body exceptions close the stream without leaking the lease."""
    session, _ = _mock_session(100)
    with session.stream("MATCH (n) RETURN n", fetch_size=10) as stream:
        assert len(next(iter(stream))) == 10
    assert stream.is_closed is True
    assert session.active_stream_count == 0

    session, _ = _mock_session(100)
    with pytest.raises(RuntimeError, match="boom"):
        with session.stream("MATCH (n) RETURN n", fetch_size=10) as stream:
            next(iter(stream))
            raise RuntimeError("boom")
    assert stream.is_closed is True
    assert session.active_stream_count == 0


def test_unclosed_stream_emits_resource_warning_and_releases() -> None:
    """Dropping an unclosed stream warns and releases the session stream count."""
    session, _ = _mock_session(10)

    def leak() -> None:
        stream = session.stream("MATCH (n) RETURN n", fetch_size=5)
        next(iter(stream))

    with pytest.warns(ResourceWarning, match="Unclosed ResultStream"):
        leak()
        gc.collect()
    assert session.active_stream_count == 0


@pytest.mark.asyncio
async def test_async_mock_stream_context_await_and_rows() -> None:
    """AsyncSession.stream supports async-with, await, and row iteration."""
    bridge = AsyncMockBridge()
    bridge.queue_result(_records(25))
    session = AsyncSession(bridge=bridge, dialect="cypher")
    assert isinstance(session.stream("MATCH (n) RETURN n"), AsyncResultStream)

    async with session.stream("MATCH (n) RETURN n", fetch_size=10) as stream:
        assert [len(batch) for batch in await _collect(stream)] == [10, 10, 5]
    assert stream.is_closed is True
    assert session.active_stream_count == 0

    bridge = AsyncMockBridge()
    bridge.queue_result(_records(25))
    session = AsyncSession(bridge=bridge, dialect="cypher")
    stream = await session.stream("MATCH (n) RETURN n", fetch_size=5)
    rows = [row async for row in stream.iter_rows()]
    assert len(rows) == 25
    assert rows[0] == {"i": 1, "s": "r1"}
    await stream.aclose()
    assert session.active_stream_count == 0


async def _collect(stream: AsyncResultStream) -> list[Any]:
    return [batch async for batch in stream]


def test_native_stream_lifecycle_arrow_and_pool_circuit_guard() -> None:
    """Native Session.stream streams, exports Arrow, and explains self-inflicted pool exhaustion."""
    stub = _BoltStreamStub()
    session = Session(stub.uri, dialect="cypher", pool_size=1, acquire_timeout=0.2)
    try:
        with session.stream("ROWS 25", fetch_size=10) as stream:
            assert stream.is_server_streaming is True
            assert stream.columns == ("i", "label")
            assert [len(batch) for batch in stream] == [10, 10, 5]
        assert session.active_stream_count == 0

        with session.stream("ROWS 25", fetch_size=10) as stream:
            reader = stream.to_arrow_reader()
            table = reader.read_all()
            assert table.num_rows == 25
            assert table.column_names == ["i", "label"]
            assert table.column("i").to_pylist()[-1] == 25
        assert session.active_stream_count == 0

        stream = session.stream("ROWS 100", fetch_size=10)
        with pytest.raises(ValueError, match="active stream") as excinfo:
            session.execute("RETURN 1")
        assert "close open streams" in str(excinfo.value)
        assert session.circuit_router.state == "closed"
        assert session.backend == "native"
        assert session.active_stream_count == 1
        stream.close()
        assert session.execute("RETURN 2").all() == [{"n": 1}]
        assert session.circuit_router.state == "closed"
        assert session.active_stream_count == 0
    finally:
        session.close()
        stub.close()


def test_native_stream_early_break_reuses_connection() -> None:
    """Breaking out of a native stream discards unread rows and reuses the same connection."""
    stub = _BoltStreamStub()
    session = Session(stub.uri, dialect="cypher", pool_size=1, acquire_timeout=0.2)
    try:
        with session.stream("ROWS 5000", fetch_size=100) as stream:
            assert len(next(iter(stream))) == 100
        assert stream.is_closed is True
        metrics = session.native_client._pool_metrics()
        assert metrics["connections_created"] == 1
        assert session.execute("RETURN 3").all() == [{"n": 1}]
        assert session.native_client._pool_metrics()["connections_created"] == 1
    finally:
        session.close()
        stub.close()


def test_native_transaction_stream_fail_fast_and_followup() -> None:
    """An in-transaction stream blocks tx.execute/commit until it is closed."""
    stub = _BoltStreamStub()
    session = Session(stub.uri, dialect="cypher", pool_size=1, acquire_timeout=0.2)
    try:
        with session.transaction() as tx:
            with tx.stream("ROWS 50", fetch_size=10) as stream:
                assert len(next(iter(stream))) == 10
                with pytest.raises(RuntimeError, match="active stream"):
                    tx.execute("RETURN 1")
            assert tx.execute("RETURN 99").all() == [{"n": 1}]

        with session.transaction() as tx:
            stream = tx.stream("ROWS 50", fetch_size=10)
            assert len(next(iter(stream))) == 10
            with pytest.raises(RuntimeError, match="active stream"):
                tx.commit()
            stream.close()
    finally:
        session.close()
        stub.close()


@pytest.mark.asyncio
async def test_async_native_stream_cancel_then_reuse() -> None:
    """Cancelling a task while opening a native stream leaves the session usable."""
    stub = _BoltStreamStub(stall_on_pull_index=1)
    session = AsyncSession(stub.uri, dialect="cypher", pool_size=1, acquire_timeout=0.5)
    try:
        stream = session.stream("ROWS 100", fetch_size=10)
        task = asyncio.create_task(stream.open())
        assert await asyncio.to_thread(stub.pull_stalled.wait, 5.0)
        task.cancel()
        stub.release_stall.set()
        # The cancellation may race with the native open completing; either way
        # the session must return to a usable state.
        outcome = await asyncio.gather(task, return_exceptions=True)
        assert not outcome or isinstance(outcome[0], (BaseException, AsyncResultStream))
        await asyncio.sleep(0)
        gc.collect()
        await stream.aclose()
        assert (await session.execute("RETURN 7")).all() == [{"n": 1}]
        assert session.active_stream_count == 0
    finally:
        await session.close()
        stub.close()


def test_duckdb_stream_batch_sizes_and_canonical_arrow() -> None:
    """DuckDB streams exact-size batches whose Arrow equals execute_to_arrow()."""
    connection = duckdb.connect()
    session = Session(bridge=connection, dialect="duckpgq")
    query = (
        "SELECT i, CAST(i AS HUGEINT) AS h, CAST(i AS DOUBLE) / 3 AS f, "
        "'r' || CAST(i AS VARCHAR) AS s FROM range(1, 1001) AS t(i)"
    )
    try:
        with session.stream(query, fetch_size=100) as stream:
            assert stream.is_server_streaming is True
            assert len(next(iter(stream))) == 100
            # The child cursor keeps its own active result slot, so another
            # query can run on the main connection while the stream is open.
            assert session.execute("SELECT 42 AS n").all() == [{"n": 42}]
        assert session.active_stream_count == 0

        with session.stream(query, fetch_size=100, as_format="arrow") as stream:
            batches = list(stream)
        assert [batch.num_rows for batch in batches] == [100] * 10
        assert pa.Table.from_batches(batches).equals(session.execute_to_arrow(query))

        with session.stream(query, fetch_size=250) as stream:
            streamed_rows = list(stream.iter_rows())
        assert streamed_rows == session.execute(query).all()

        with session.stream("SELECT i FROM range(1, 1) AS t(i)", fetch_size=10) as stream:
            assert list(stream) == []
            assert stream.columns == ("i",)
    finally:
        session.close()


def test_duckdb_transaction_stream_sees_writes_and_fails_fast() -> None:
    """A DuckDB transaction stream observes uncommitted rows and blocks tx.execute/commit."""
    connection = duckdb.connect()
    session = Session(bridge=connection, dialect="duckpgq")
    try:
        with session.transaction() as tx:
            tx.execute("CREATE TABLE zz_stream_tx AS SELECT i FROM range(1, 51) AS t(i)")
            with tx.stream("SELECT i FROM zz_stream_tx", fetch_size=10) as stream:
                assert len(next(iter(stream))) == 10
                with pytest.raises(RuntimeError, match="active stream"):
                    tx.execute("SELECT 1")
            assert tx.execute("SELECT count(*) AS n FROM zz_stream_tx").all() == [{"n": 50}]

        with session.transaction() as tx:
            stream = tx.stream("SELECT 1 AS i")
            assert len(next(iter(stream))) == 1
            with pytest.raises(RuntimeError, match="active stream"):
                tx.commit()
            stream.close()
    finally:
        session.close()


@pytest.mark.asyncio
async def test_async_duckdb_stream_and_transaction() -> None:
    """AsyncDuckDbBridge streams batches and its transaction stream fails fast."""
    connection = duckdb.connect()
    session = AsyncSession(bridge=connection, dialect="duckpgq")
    query = "SELECT i FROM range(1, 26) AS t(i)"
    try:
        async with session.stream(query, fetch_size=10) as stream:
            sizes = [len(batch) async for batch in stream]
        assert sizes == [10, 10, 5]

        async with session.transaction() as tx:
            await tx.execute(
                "CREATE TABLE zz_async_stream_tx AS SELECT i FROM range(1, 16) AS t(i)"
            )
            async with tx.stream("SELECT i FROM zz_async_stream_tx", fetch_size=5) as stream:
                assert len(await stream.__anext__()) == 5
                with pytest.raises(RuntimeError, match="active stream"):
                    await tx.execute("SELECT 1")
            assert (await tx.execute("SELECT count(*) AS n FROM zz_async_stream_tx")).all() == [
                {"n": 15}
            ]
    finally:
        await session.close()


def test_stream_input_validation() -> None:
    """Invalid fetch sizes and formats fail at the call site."""
    session, _ = _mock_session(1)
    with pytest.raises(ValueError, match="fetch_size"):
        session.stream("MATCH (n) RETURN n", fetch_size=0)
    with pytest.raises(ValueError, match="Unsupported stream format"):
        session.stream("MATCH (n) RETURN n", as_format="rows")
    assert session.active_stream_count == 0


def test_stream_chunk_creation_is_bounded() -> None:
    """Each buffered chunk holds at most fetch_size rows."""
    session, _ = _mock_session(1000)
    start = time.perf_counter()
    with session.stream("MATCH (n) RETURN n", fetch_size=64) as stream:
        sizes = [len(batch) for batch in stream]
    assert sizes == [64] * 15 + [40]
    assert time.perf_counter() - start < 5.0


def test_mock_transaction_stream_fail_fast() -> None:
    """A bridge transaction stream blocks tx.execute and can be closed early."""
    bridge = MockBridge()
    bridge.queue_result(_records(30))
    session = Session(bridge=bridge, dialect="cypher")
    with session.transaction() as tx:
        with tx.stream("MATCH (n) RETURN n", fetch_size=10) as stream:
            assert len(next(iter(stream))) == 10
            with pytest.raises(RuntimeError, match="active stream"):
                tx.execute("MATCH (n) RETURN n")
        assert tx.execute("RETURN 1").all() == []
    assert session.active_stream_count == 0


def test_neo4j_bridge_stream_requests_driver_fetch_size() -> None:
    """Neo4jBoltBridge.stream leases one session with the driver fetch size."""

    class FakeRecord:
        def __init__(self, index: int) -> None:
            self._index = index

        def data(self) -> dict[str, Any]:
            return {"i": self._index}

    class FakeDriver:
        def __init__(self) -> None:
            self.kwargs: dict[str, Any] = {}
            self.closed = False

        def session(self, **kwargs: Any) -> FakeDriver:
            self.kwargs = kwargs
            return self

        def run(self, statement: str, parameters: dict[str, Any]) -> list[FakeRecord]:
            return [FakeRecord(index) for index in range(1, 6)]

        def close(self) -> None:
            self.closed = True

    driver = FakeDriver()
    session = Session(bridge=Neo4jBoltBridge(driver), dialect="cypher")
    with session.stream("MATCH (n) RETURN n", fetch_size=2) as stream:
        assert driver.kwargs["fetch_size"] == 2
        assert [len(batch) for batch in stream] == [2, 2, 1]
        assert stream.is_server_streaming is True
    assert driver.closed is True
