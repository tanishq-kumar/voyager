"""Voyager OGM: Bounded Result Streaming with `Session.stream()`.

Teaches:
    - Streaming query results in bounded `fetch_size` batches with
      `Session.stream()` / `AsyncSession.stream()` / `Transaction.stream()`
    - `ResultStream` / `AsyncResultStream`: `dicts`, `arrow`, and `polars` batches,
      `iter_rows()`, `iter_batches()`, and `to_arrow_reader()`
    - Exporting 200,000 embedded DuckDB rows to Parquet with flat memory instead of
      materializing the whole result through `execute()`
    - Ending a stream early with `break` inside `with`, then reusing the session
    - Streaming inside a transaction, including the fail-fast guard on `tx.execute()`
    - Async iteration with `async for` over `AsyncSession.stream()`
    - Optional live streaming from Neo4j (7687) and PostgreSQL 19 (5456)

Why stream instead of `execute()`:
    `execute()` materializes every row before your code sees the first one, so a
    large scan costs memory proportional to the result size. A stream pulls at
    most `fetch_size` rows at a time, so you can export, aggregate, or feed a
    pipeline with a bounded working set. For small OLTP reads, `execute()` is
    still the right tool: streams pay one round trip per batch.

    FalkorDB has no server-side cursor. Its `Session.stream()` receives the whole
    RESP reply and then hands it out in batches, so the same code works there but
    memory still grows with the result size. Neo4j, Memgraph, PostgreSQL/AGE, and
    DuckDB pull incrementally, so memory stays flat.

Prerequisites:
    - Engine: None (embedded DuckDB, runs offline)
    - Live section (optional): Neo4j on 7687 and/or PostgreSQL 19 on 5456,
      database `voyager_graph`, host from `VOYAGER_HOST` (default `localhost`)
    - Extras: `duckdb`, `neo4j`, `psycopg` for the live section

Run with:
    `uv run python examples/python/33_result_streaming.py`
"""

from __future__ import annotations

import asyncio
import gc
import os
import socket
import tempfile
import time
import tracemalloc
from collections.abc import Callable
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from voyager_ogm import AsyncSession, Session, create_bridge

ROWS = 200_000
BATCH_SIZE = 10_000

EVENTS_QUERY = "SELECT id, bucket, payload FROM zz_stream_events ORDER BY id"


def _port_open(host: str, port: int, timeout: float = 2.0) -> bool:
    """Returns True when a TCP port accepts connections."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _measure_peak_mib(run: Callable[[], None]) -> float:
    """Runs ``run`` and returns the peak Python heap growth in MiB.

    ``tracemalloc`` tracks allocations made through the Python allocator
    (row dictionaries, slices, and table wrappers). Arrow buffers that engines
    hand back are native allocations, so this measures the Python-side working
    set: the part that `execute()` grows with the result size and a stream keeps
    bounded by `fetch_size`.
    """
    gc.collect()
    tracemalloc.start()
    tracemalloc.reset_peak()
    before, _ = tracemalloc.get_traced_memory()
    try:
        run()
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    return (peak - before) / (1024 * 1024)


def _format_heap(mib: float) -> str:
    """Formats a MiB figure so a near-zero stream growth is visible in KiB."""
    if mib < 1.0:
        return f"{mib * 1024:.0f} KiB"
    return f"{mib:.1f} MiB"


def _export_with_stream(session: Session, path: Path) -> tuple[int, int]:
    """Writes the query to Parquet one `fetch_size` Arrow batch at a time."""
    rows = 0
    batches = 0
    with session.stream(EVENTS_QUERY, fetch_size=BATCH_SIZE, as_format="arrow") as stream:
        reader = stream.to_arrow_reader()
        with pq.ParquetWriter(path, reader.schema) as writer:
            for batch in reader:
                if batch.num_rows == 0:
                    continue
                writer.write_batch(batch)
                rows += batch.num_rows
                batches += 1
    return rows, batches


def _export_with_execute(session: Session, path: Path) -> int:
    """Writes the same query to Parquet after `execute()` materialized every row."""
    rows = session.execute(EVENTS_QUERY).all()
    writer: pq.ParquetWriter | None = None
    try:
        for start in range(0, len(rows), BATCH_SIZE):
            table = pa.Table.from_pylist(rows[start : start + BATCH_SIZE])
            if writer is None:
                writer = pq.ParquetWriter(path, table.schema)
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()
    return len(rows)


def _offline_duckdb(tmp_dir: Path) -> None:
    """Runs every offline section against embedded DuckDB."""
    session = Session(
        bridge=create_bridge("duckdb://:memory:"),
        dialect="duckpgq",
    )
    try:
        # ----------------------------------------------------------------
        # 1. Seed a table large enough that materializing it is noticeable.
        # ----------------------------------------------------------------
        print("--- [1] Embedded DuckDB: seed 200,000 rows ---")
        session.execute(
            "CREATE TABLE zz_stream_events AS "
            "SELECT i AS id, i % 97 AS bucket, i * 3 AS payload "
            f"FROM range(1, {ROWS + 1}) AS t(i)"
        )
        seeded = session.execute("SELECT count(*) AS n FROM zz_stream_events").all()
        print(f"  seeded rows: {seeded[0]['n']:,}\n")

        # ----------------------------------------------------------------
        # 2. Parquet export: streaming vs execute(), same output shape.
        # ----------------------------------------------------------------
        print("--- [2] Parquet export: bounded stream vs execute() ---")
        stream_path = tmp_dir / "events_stream.parquet"
        execute_path = tmp_dir / "events_execute.parquet"

        stream_rows, stream_batches = 0, 0
        execute_rows = 0

        def run_stream_export() -> None:
            nonlocal stream_rows, stream_batches
            stream_rows, stream_batches = _export_with_stream(session, stream_path)

        def run_execute_export() -> None:
            nonlocal execute_rows
            execute_rows = _export_with_execute(session, execute_path)

        stream_peak = _measure_peak_mib(run_stream_export)
        execute_peak = _measure_peak_mib(run_execute_export)

        stream_meta = pq.ParquetFile(stream_path).metadata
        execute_meta = pq.ParquetFile(execute_path).metadata
        print(
            f"  stream  : {stream_rows:,} rows in {stream_batches} batches, "
            f"peak Python heap +{_format_heap(stream_peak)}"
        )
        print(
            f"  execute : {execute_rows:,} rows materialized first, "
            f"peak Python heap +{_format_heap(execute_peak)}"
        )
        print(
            "  parquet : "
            f"stream={stream_meta.num_rows:,} rows / {stream_meta.num_row_groups} row groups, "
            f"execute={execute_meta.num_rows:,} rows / {execute_meta.num_row_groups} row groups"
        )
        print(
            "  why     : the stream holds one batch at a time; execute() holds "
            "every row dictionary before writing\n"
        )

        # ----------------------------------------------------------------
        # 3. First N rows with an early break: the context manager closes.
        # ----------------------------------------------------------------
        print("--- [3] First rows with an early break ---")
        with session.stream(EVENTS_QUERY, fetch_size=5) as stream:
            print(
                f"  columns available before the first batch: {stream.columns}\n"
                f"  server streaming: {stream.is_server_streaming}"
            )
            rows_read = 0
            for batch in stream:
                rows_read += len(batch)
                print(f"  first batch size: {len(batch):,}")
                if rows_read >= 5:
                    break
        print(f"  read {rows_read} rows, then left the with block early")
        print(
            f"  after close: is_closed={stream.is_closed}, "
            f"active streams={session.active_stream_count}"
        )
        follow_up = session.execute("SELECT count(*) AS n FROM zz_stream_events").all()
        print(f"  follow-up execute() on the same session: {follow_up[0]['n']:,} rows\n")

        # ----------------------------------------------------------------
        # 4. iter_rows(): row-by-row processing with a running aggregate.
        # ----------------------------------------------------------------
        print("--- [4] iter_rows(): running aggregate over 200,000 rows ---")
        seen, payload_total = 0, 0
        with session.stream("SELECT id, payload FROM zz_stream_events", fetch_size=2_000) as stream:
            for row in stream.iter_rows():
                seen += 1
                payload_total += row["payload"]
        print(f"  rows: {seen:,}, sum(payload): {payload_total:,}\n")

        # ----------------------------------------------------------------
        # 5. Polars batches: vectorized work per batch without copying rows.
        # ----------------------------------------------------------------
        print("--- [5] Polars batches (as_format='polars') ---")
        polars_batches, polars_total = 0, 0
        first_shape: tuple[int, int] = (0, 0)
        with session.stream(
            "SELECT id, payload FROM zz_stream_events",
            fetch_size=25_000,
            as_format="polars",
        ) as stream:
            for frame in stream:
                if polars_batches == 0:
                    first_shape = frame.shape
                polars_batches += 1
                polars_total += int(frame["payload"].sum())
        print(
            f"  {polars_batches} Polars batches, first shape={first_shape}, "
            f"sum(payload)={polars_total:,}\n"
        )

        # ----------------------------------------------------------------
        # 6. A stream inside a transaction: fail-fast, then success.
        # ----------------------------------------------------------------
        print("--- [6] Stream inside a transaction ---")
        with session.transaction() as tx:
            tx.execute(
                "CREATE OR REPLACE TABLE zz_stream_tx AS SELECT i FROM range(1, 5001) AS t(i)"
            )
            with tx.stream("SELECT i FROM zz_stream_tx", fetch_size=1_000) as stream:
                first_batch = next(iter(stream))
                print(f"  first in-transaction batch: {len(first_batch):,} rows")
                try:
                    tx.execute("SELECT 1")
                except RuntimeError as err:
                    print(f"  tx.execute() while open -> RuntimeError: {err}")
            tx_rows = tx.execute("SELECT count(*) AS n FROM zz_stream_tx").all()[0]["n"]
            print(f"  tx.execute() after close -> {tx_rows:,} rows\n")
    finally:
        session.close()


async def _async_section() -> None:
    """Streams DuckDB rows with AsyncSession and `async for`."""
    print("--- [7] AsyncSession.stream() with async for ---")
    session = AsyncSession(
        bridge=create_bridge("duckdb://:memory:", is_async=True),
        dialect="duckpgq",
    )
    try:
        await session.execute(
            "CREATE TABLE zz_async_stream AS SELECT i, i * 2 AS doubled FROM range(1, 5001) AS t(i)"
        )
        batches, rows, doubled_total = 0, 0, 0
        async with session.stream(
            "SELECT i, doubled FROM zz_async_stream ORDER BY i",
            fetch_size=500,
        ) as stream:
            async for batch in stream:
                batches += 1
                rows += len(batch)
                doubled_total += sum(row["doubled"] for row in batch)
        print(
            f"  async streamed {rows:,} rows in {batches} batches, sum(doubled)={doubled_total:,}\n"
        )
    finally:
        await session.close()


def _stream_live_engine(
    label: str, host: str, port: int, uri: str, dialect: str, query: str
) -> bool:
    """Streams generated rows from one live engine, skipping when unreachable."""
    if not _port_open(host, port):
        print(
            f"[SKIP] {label} is not reachable on {host}:{port} "
            "(start it with your Compose runner, or set VOYAGER_HOST)"
        )
        return False
    session: Session | None = None
    try:
        session = Session(uri, dialect=dialect)
        # Warm up first: opening the connection, authenticating and planning a
        # cold query can take far longer than the stream itself, and would
        # otherwise show up as "time to first batch".
        session.execute("RETURN 1" if dialect == "cypher" else "SELECT 1").all()
        started = time.perf_counter()
        first_batch_at: float | None = None
        rows, batches = 0, 0
        with session.stream(query, fetch_size=BATCH_SIZE) as stream:
            for batch in stream:
                if first_batch_at is None:
                    first_batch_at = time.perf_counter() - started
                rows += len(batch)
                batches += 1
        total = time.perf_counter() - started
        first_ms = (first_batch_at or 0.0) * 1_000
        print(
            f"  {label}: {rows:,} rows in {batches} batches, "
            f"first batch {first_ms:.1f} ms, total {total * 1_000:.1f} ms"
        )
        return True
    except Exception as err:
        print(f"[SKIP] {label} connected but failed: {err}")
        return False
    finally:
        if session is not None:
            session.close()


def _live_section() -> None:
    """Optionally streams 100,000 generated rows from Neo4j and PostgreSQL 19."""
    host = os.environ.get("VOYAGER_HOST", "localhost")
    print(f"--- [8] Live engines on {host} (optional, read-only generated rows) ---")
    tested = 0
    tested += int(
        _stream_live_engine(
            "neo4j",
            host,
            7687,
            f"bolt://neo4j:voyagerpass123@{host}:7687",
            "cypher",
            "UNWIND range(1, 100000) AS i RETURN i",
        )
    )
    tested += int(
        _stream_live_engine(
            "postgres19",
            host,
            5456,
            f"postgresql://postgres:voyagerpass123@{host}:5456/voyager_graph",
            "sql_pgq",
            "SELECT i FROM generate_series(1, 100000) AS t(i)",
        )
    )
    if tested == 0:
        print(
            "  Note: no live engine was reachable; the offline DuckDB sections above "
            "covered the streaming API."
        )
    else:
        print(f"  Live streaming succeeded on {tested} engine(s).")
    print()


def main() -> None:
    print("=" * 72)
    print("Voyager OGM: Bounded Result Streaming with Session.stream()")
    print("=" * 72)

    with tempfile.TemporaryDirectory(prefix="voyager_stream_") as tmp:
        _offline_duckdb(Path(tmp))

    asyncio.run(_async_section())
    _live_section()

    print("=" * 72)
    print("Streaming demo complete.")
    print("=" * 72)


if __name__ == "__main__":
    main()
