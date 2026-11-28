"""Live Neo4j & Memgraph tests for Stage 5 (#458) PyO3 NativeResultStream."""

from __future__ import annotations

import asyncio
import gc
import time
from typing import Any

import polars as pl
import pyarrow as pa
import pytest
from conftest import get_test_host, is_port_open
from voyager_ogm._voyager_rs import NativeClient

HOST = get_test_host()
NEO4J_PORT = 7687
MEMGRAPH_PORT = 7688
NEO4J_URI = f"bolt://neo4j:voyagerpass123@{HOST}:{NEO4J_PORT}"
MEMGRAPH_URI = f"bolt://{HOST}:{MEMGRAPH_PORT}"


def _run_live_stream_lifecycle(uri: str, label: str) -> None:
    client = NativeClient(uri, min_idle=0, max_size=1, acquire_timeout=0.25)
    try:
        # 1. Full sync stream (2,500 rows, fetch_size=1000) -> 3 chunks, row-backed, eager release
        stream = client.stream_sync("UNWIND range(1, 2500) AS i RETURN i", fetch_size=1000)
        assert stream.columns == ["i"]
        assert stream.is_server_streaming is True
        assert client._pool_metrics()["active_count"] == 1

        chunk_sizes: list[int] = []
        last_val = 0
        for chunk in stream:
            assert chunk._backing == "rows"
            assert chunk._arrow_materialized is False
            chunk_sizes.append(chunk.num_rows)
            dicts = chunk.to_dicts()
            assert chunk._arrow_materialized is False
            last_val = dicts[-1]["i"]
        assert chunk_sizes == [1000, 1000, 500], f"{label}: {chunk_sizes}"
        assert last_val == 2500
        assert stream.is_closed is True
        assert client._pool_metrics()["active_count"] == 0
        assert client._pool_metrics()["idle_count"] == 1
        assert client._pool_metrics()["connections_created"] == 1

        # 2. Early break + close_sync reuses the exact same pooled connection
        with client.stream_sync("UNWIND range(1, 5000) AS i RETURN i", fetch_size=100) as s2:
            for chunk in s2:
                assert chunk.num_rows == 100
                break
        assert s2.is_closed is True
        assert client._pool_metrics()["connections_created"] == 1
        assert client.execute_sync("RETURN 42 AS n").to_dicts() == [{"n": 42}]
        assert client._pool_metrics()["connections_created"] == 1

        # 3. Pool max_size=1 blocks/times out while stream is open and recovers after close
        s3 = client.stream_sync("UNWIND range(1, 1000) AS i RETURN i", fetch_size=100)
        with pytest.raises(ValueError, match="exhausted"):
            client.execute_sync("RETURN 1 AS n")
        s3.close_sync()
        assert client.execute_sync("RETURN 43 AS n").to_dicts() == [{"n": 43}]

        # 4. In-transaction stream + fail-fast on tx.execute_sync + follow-up + commit
        tx = client.begin_transaction_sync()
        tx_stream = tx.stream_sync("UNWIND range(1, 500) AS i RETURN i", fetch_size=100)
        assert tx.has_open_stream is True
        with pytest.raises(RuntimeError, match="active stream"):
            tx.execute_sync("RETURN 1 AS x")
        with pytest.raises(RuntimeError, match="active stream"):
            tx.commit_sync()
        c1 = tx_stream.next_chunk_sync()
        assert c1 is not None and c1.num_rows == 100
        tx_stream.close_sync()
        assert tx.has_open_stream is False
        assert tx.execute_sync("RETURN 99 AS x").to_dicts() == [{"x": 99}]
        tx.commit_sync()
        assert client._pool_metrics()["connections_created"] == 1

        # 5. Arrow C stream export with locked schema (PyArrow + Polars)
        s4 = client.stream_sync("UNWIND range(1, 250) AS i RETURN i, i * 1.5 AS f", fetch_size=100)
        tbl = pa.RecordBatchReader.from_stream(s4).read_all()
        assert tbl.num_rows == 250
        assert tbl.column_names == ["i", "f"]
        assert s4.is_closed is True

        s5 = client.stream_sync("UNWIND range(1, 250) AS i RETURN i", fetch_size=100)
        df = pl.DataFrame(s5)
        assert df.height == 250
        assert s5.is_closed is True

        # 6. GC without close replaces connection cleanly without deadlock
        def open_and_drop() -> None:
            leaked = client.stream_sync("UNWIND range(1, 1000) AS i RETURN i", fetch_size=50)
            first = leaked.next_chunk_sync()
            assert first is not None and first.num_rows == 50

        open_and_drop()
        gc.collect()
        assert client._pool_metrics()["active_count"] == 0
        assert client.execute_sync("RETURN 77 AS n").to_dicts() == [{"n": 77}]
        assert client._pool_metrics()["connections_created"] == 2

        # 7. Async lifecycle + asyncio.Task.cancel() mid-await followed by healthy query
        async def run_async() -> None:
            as1 = await client.stream("UNWIND range(1, 350) AS i RETURN i", fetch_size=100)
            async_sizes: list[int] = []
            async for ch in as1:
                assert ch._backing == "rows"
                async_sizes.append(ch.num_rows)
                _ = ch.to_dicts()
                assert ch._arrow_materialized is False
            assert async_sizes == [100, 100, 100, 50]

            # Cancel mid-await on a heavy query, then ensure client still works
            as2 = await client.stream(
                "UNWIND range(1, 2000000) AS i WITH i WHERE i % 500000 = 0 RETURN i",
                fetch_size=1,
            )

            async def pull_second() -> Any:
                first = await as2.next_chunk()
                assert first is not None
                return await as2.next_chunk()

            task = asyncio.create_task(pull_second())
            await asyncio.sleep(0.01)
            if not task.done():
                task.cancel()
                with pytest.raises((asyncio.CancelledError, ValueError)):
                    await task
            await as2.close()
            for _ in range(50):
                if client._pool_metrics()["active_count"] == 0:
                    break
                await asyncio.sleep(0.01)
            assert client._pool_metrics()["active_count"] == 0
            follow_up = (await client.execute("RETURN 88 AS n")).to_dicts()
            assert follow_up == [{"n": 88}]

        t0 = time.monotonic()
        asyncio.run(run_async())
        assert time.monotonic() - t0 < 15.0
    finally:
        client.close()


@pytest.mark.live
def test_live_neo4j_native_result_stream_lifecycle() -> None:
    if not is_port_open(HOST, NEO4J_PORT):
        pytest.skip(f"Live Neo4j not reachable on {HOST}:{NEO4J_PORT}")
    _run_live_stream_lifecycle(NEO4J_URI, "neo4j")


@pytest.mark.live
def test_live_memgraph_native_result_stream_lifecycle() -> None:
    if not is_port_open(HOST, MEMGRAPH_PORT):
        pytest.skip(f"Live Memgraph not reachable on {HOST}:{MEMGRAPH_PORT}")
    _run_live_stream_lifecycle(MEMGRAPH_URI, "memgraph")
