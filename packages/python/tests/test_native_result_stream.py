"""Offline tests for Stage 5 (#458): PyO3 NativeResultStream and schema reconciliation."""

from __future__ import annotations

import asyncio
import gc
import socket
import struct
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import polars as pl
import pyarrow as pa
import pytest
from test_native_result_types import (
    _bolt_recv_frame,
    _bolt_send_frame,
    _ps_float,
    _ps_int,
    _ps_map,
    _ps_str,
    _ps_struct,
)
from voyager_ogm._voyager_rs import NativeClient

_PS_NULL = b"\xc0"
_PS_TRUE = b"\xc3"
_PS_FALSE = b"\xc2"


def _ps_int_any(n: int) -> bytes:
    if -16 <= n <= 127:
        return bytes([n & 0xFF])
    if -128 <= n < -16:
        return b"\xc8" + struct.pack(">b", n)
    if -32768 <= n <= 32767:
        return b"\xc9" + struct.pack(">h", n)
    if -2147483648 <= n <= 2147483647:
        return b"\xca" + struct.pack(">i", n)
    return b"\xcb" + struct.pack(">q", n)


def _ps_str_any(s: str) -> bytes:
    raw = s.encode("utf-8")
    n = len(raw)
    if n < 16:
        return bytes([0x80 | n]) + raw
    if n < 256:
        return b"\xd0" + bytes([n]) + raw
    return b"\xd1" + struct.pack(">H", n) + raw


def _ps_list_any(items: list[bytes]) -> bytes:
    n = len(items)
    body = b"".join(items)
    if n < 16:
        return bytes([0x90 | n]) + body
    if n < 256:
        return b"\xd4" + bytes([n]) + body
    return b"\xd5" + struct.pack(">H", n) + body


def _ps_map_any(items: list[tuple[str, bytes]]) -> bytes:
    n = len(items)
    body = b"".join(_ps_str_any(k) + v for k, v in items)
    if n < 16:
        return bytes([0xA0 | n]) + body
    if n < 256:
        return b"\xd8" + bytes([n]) + body
    return b"\xd9" + struct.pack(">H", n) + body


def _decode_ps(buf: bytes, pos: int = 0) -> tuple[Any, int]:
    marker = buf[pos]
    pos += 1
    if marker <= 0x7F:
        return marker, pos
    if marker >= 0xF0:
        return marker - 256, pos
    if 0x80 <= marker <= 0x8F:
        n = marker & 0x0F
        return buf[pos : pos + n].decode("utf-8"), pos + n
    if 0x90 <= marker <= 0x9F:
        n = marker & 0x0F
        out = []
        for _ in range(n):
            v, pos = _decode_ps(buf, pos)
            out.append(v)
        return out, pos
    if 0xA0 <= marker <= 0xAF:
        n = marker & 0x0F
        out_m: dict[str, Any] = {}
        for _ in range(n):
            k, pos = _decode_ps(buf, pos)
            v, pos = _decode_ps(buf, pos)
            out_m[str(k)] = v
        return out_m, pos
    if 0xB0 <= marker <= 0xBF:
        n = marker & 0x0F
        tag = buf[pos]
        pos += 1
        fields = []
        for _ in range(n):
            v, pos = _decode_ps(buf, pos)
            fields.append(v)
        return ("struct", tag, fields), pos
    if marker == 0xC0:
        return None, pos
    if marker == 0xC2:
        return False, pos
    if marker == 0xC3:
        return True, pos
    if marker == 0xC8:
        return struct.unpack(">b", buf[pos : pos + 1])[0], pos + 1
    if marker == 0xC9:
        return struct.unpack(">h", buf[pos : pos + 2])[0], pos + 2
    if marker == 0xCA:
        return struct.unpack(">i", buf[pos : pos + 4])[0], pos + 4
    if marker == 0xCB:
        return struct.unpack(">q", buf[pos : pos + 8])[0], pos + 8
    if marker == 0xD0:
        n = buf[pos]
        pos += 1
        return buf[pos : pos + n].decode("utf-8"), pos + n
    if marker == 0xD1:
        n = struct.unpack(">H", buf[pos : pos + 2])[0]
        pos += 2
        return buf[pos : pos + n].decode("utf-8"), pos + n
    if marker == 0xD4:
        n = buf[pos]
        pos += 1
        out = []
        for _ in range(n):
            v, pos = _decode_ps(buf, pos)
            out.append(v)
        return out, pos
    if marker == 0xD8:
        n = buf[pos]
        pos += 1
        out_m = {}
        for _ in range(n):
            k, pos = _decode_ps(buf, pos)
            v, pos = _decode_ps(buf, pos)
            out_m[str(k)] = v
        return out_m, pos
    raise ValueError(f"unsupported PackStream marker 0x{marker:02x}")


@dataclass
class _StubCounters:
    run_count: int = 0
    pull_count: int = 0
    discard_count: int = 0
    begin_count: int = 0
    commit_count: int = 0
    rollback_count: int = 0
    reset_count: int = 0
    connection_count: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)


class _BoltStreamStub:
    """Thread-backed Bolt 5.4 stub server with PULL {n}, DISCARD, qid, and delay hooks."""

    def __init__(
        self,
        row_provider: Callable[[str], tuple[list[str], list[list[bytes]]]] | None = None,
        stall_on_pull_index: int | None = None,
    ) -> None:
        self.counters = _StubCounters()
        self.row_provider = row_provider or self._default_rows
        self.stall_on_pull_index = stall_on_pull_index
        self.pull_stalled = threading.Event()
        self.release_stall = threading.Event()
        self._stop = threading.Event()
        self._srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._srv.bind(("127.0.0.1", 0))
        self._srv.listen(8)
        self._srv.settimeout(0.2)
        self.port = self._srv.getsockname()[1]
        self.uri = f"bolt://127.0.0.1:{self.port}"
        self._threads: list[threading.Thread] = []
        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._accept_thread.start()

    @staticmethod
    def _default_rows(query: str) -> tuple[list[str], list[list[bytes]]]:
        if query.startswith("ROWS "):
            n = int(query.split()[1])
            return ["i", "label"], [[_ps_int_any(k), _ps_str_any(f"r{k}")] for k in range(1, n + 1)]
        if query == "EMPTY":
            return ["i", "label"], []
        return ["n"], [[_ps_int_any(1)]]

    def _accept_loop(self) -> None:
        while not self._stop.is_set():
            try:
                conn, _ = self._srv.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            with self.counters.lock:
                self.counters.connection_count += 1
            t = threading.Thread(target=self._handle_conn, args=(conn,), daemon=True)
            t.start()
            self._threads.append(t)

    def _handle_conn(self, conn: socket.socket) -> None:
        try:
            with conn:
                hs = b""
                while len(hs) < 20:
                    part = conn.recv(20 - len(hs))
                    if not part:
                        return
                    hs += part
                conn.sendall(b"\x00\x00\x04\x05")

                in_tx = False
                next_qid = 0
                active_qid: int | None = None
                cols: list[str] = ["n"]
                rows: list[list[bytes]] = []
                cursor = 0

                while not self._stop.is_set():
                    frame = _bolt_recv_frame(conn)
                    if not frame:
                        break
                    decoded, _ = _decode_ps(frame)
                    assert isinstance(decoded, tuple) and decoded[0] == "struct"
                    _, tag, fields = decoded

                    if tag == 0x02:  # GOODBYE
                        break
                    if tag == 0x01:  # HELLO
                        _bolt_send_frame(
                            conn,
                            _ps_struct(
                                0x70,
                                [
                                    _ps_map(
                                        [
                                            ("server", _ps_str("Neo4j/5.26.0")),
                                            ("connection_id", _ps_str("bolt-stub")),
                                        ]
                                    )
                                ],
                            ),
                        )
                    elif tag == 0x6A:  # LOGON
                        _bolt_send_frame(conn, _ps_struct(0x70, [_ps_map([])]))
                    elif tag == 0x0F:  # RESET
                        with self.counters.lock:
                            self.counters.reset_count += 1
                        in_tx = False
                        active_qid = None
                        rows = []
                        cursor = 0
                        _bolt_send_frame(conn, _ps_struct(0x70, [_ps_map([])]))
                    elif tag == 0x11:  # BEGIN
                        with self.counters.lock:
                            self.counters.begin_count += 1
                        in_tx = True
                        next_qid = 0
                        active_qid = None
                        _bolt_send_frame(conn, _ps_struct(0x70, [_ps_map([])]))
                    elif tag == 0x12:  # COMMIT
                        with self.counters.lock:
                            self.counters.commit_count += 1
                        in_tx = False
                        active_qid = None
                        _bolt_send_frame(
                            conn,
                            _ps_struct(0x70, [_ps_map([("bookmark", _ps_str("stub:bm-1"))])]),
                        )
                    elif tag == 0x13:  # ROLLBACK
                        with self.counters.lock:
                            self.counters.rollback_count += 1
                        in_tx = False
                        active_qid = None
                        _bolt_send_frame(conn, _ps_struct(0x70, [_ps_map([])]))
                    elif tag == 0x10:  # RUN
                        with self.counters.lock:
                            self.counters.run_count += 1
                        query = str(fields[0]) if fields else ""
                        cols, rows = self.row_provider(query)
                        cursor = 0
                        meta_items: list[tuple[str, bytes]] = [
                            ("fields", _ps_list_any([_ps_str_any(c) for c in cols])),
                            ("t_first", _ps_int_any(1)),
                        ]
                        if in_tx:
                            active_qid = next_qid
                            next_qid += 1
                            meta_items.append(("qid", _ps_int_any(active_qid)))
                        else:
                            active_qid = None
                        _bolt_send_frame(conn, _ps_struct(0x70, [_ps_map_any(meta_items)]))
                    elif tag == 0x3F:  # PULL
                        with self.counters.lock:
                            self.counters.pull_count += 1
                            current_pull = self.counters.pull_count
                        extra = fields[0] if fields and isinstance(fields[0], dict) else {}
                        req_n = int(extra.get("n", -1))
                        if (
                            self.stall_on_pull_index is not None
                            and current_pull == self.stall_on_pull_index
                        ):
                            self.pull_stalled.set()
                            self.release_stall.wait(timeout=5.0)
                            if self._stop.is_set():
                                break
                        end = len(rows) if req_n < 0 else min(len(rows), cursor + req_n)
                        for r in rows[cursor:end]:
                            _bolt_send_frame(conn, _ps_struct(0x71, [_ps_list_any(r)]))
                        cursor = end
                        has_more = cursor < len(rows)
                        if has_more:
                            _bolt_send_frame(
                                conn,
                                _ps_struct(0x70, [_ps_map([("has_more", _PS_TRUE)])]),
                            )
                        else:
                            _bolt_send_frame(
                                conn,
                                _ps_struct(
                                    0x70,
                                    [
                                        _ps_map(
                                            [
                                                ("has_more", _PS_FALSE),
                                                ("t_last", _ps_int(2)),
                                            ]
                                        )
                                    ],
                                ),
                            )
                    elif tag == 0x2F:  # DISCARD
                        with self.counters.lock:
                            self.counters.discard_count += 1
                        cursor = len(rows)
                        _bolt_send_frame(
                            conn,
                            _ps_struct(0x70, [_ps_map([("t_last", _ps_int(1))])]),
                        )
                    else:
                        _bolt_send_frame(conn, _ps_struct(0x70, [_ps_map([])]))
        except OSError:
            pass

    def close(self) -> None:
        self._stop.set()
        self.release_stall.set()
        self._srv.close()
        self._accept_thread.join(timeout=2)
        for t in self._threads:
            t.join(timeout=2)


def test_sync_and_async_stream_lifecycle_chunk_counts_and_row_backing() -> None:
    stub = _BoltStreamStub()
    client = NativeClient(stub.uri, min_idle=0, max_size=2)
    try:
        # 1. Sync lifecycle + fetch_size chunking + row-backed chunks (0 Arrow work)
        stream = client.stream_sync("ROWS 25", fetch_size=10)
        assert stream.columns == ["i", "label"]
        assert stream.is_server_streaming is True
        assert stream.is_closed is False

        chunk_lengths: list[int] = []
        collected: list[dict[str, Any]] = []
        for chunk in stream:
            assert chunk._backing == "rows"
            assert chunk._arrow_materialized is False
            chunk_lengths.append(chunk.num_rows)
            collected.extend(chunk.to_dicts())
            # .to_dicts() must not materialize Arrow
            assert chunk._arrow_materialized is False
            # Lazy Arrow export works when requested explicitly
            tbl = pa.RecordBatchReader.from_stream(chunk).read_all()
            assert tbl.num_rows == chunk.num_rows
            assert chunk._arrow_materialized is True

        assert chunk_lengths == [10, 10, 5]
        assert len(collected) == 25
        assert collected[0] == {"i": 1, "label": "r1"}
        assert collected[-1] == {"i": 25, "label": "r25"}
        assert stream.is_closed is True
        assert stream.next_chunk_sync() is None
        assert stub.counters.pull_count == 3
        assert stub.counters.discard_count == 0
        assert client._pool_metrics()["active_count"] == 0
        assert client._pool_metrics()["idle_count"] == 1

        # 2. Async lifecycle + async for + next_chunk
        async def run_async() -> None:
            async_stream = await client.stream("ROWS 23", fetch_size=10)
            assert async_stream.columns == ["i", "label"]
            sizes: list[int] = []
            async for chunk in async_stream:
                assert chunk._backing == "rows"
                assert chunk._arrow_materialized is False
                sizes.append(chunk.num_rows)
                _ = chunk.to_dicts()
                assert chunk._arrow_materialized is False
            assert sizes == [10, 10, 3]
            assert async_stream.is_closed is True
            assert await async_stream.next_chunk() is None
            await async_stream.close()

        asyncio.run(run_async())
        assert stub.counters.connection_count == 1
        assert stub.counters.pull_count == 6
    finally:
        client.close()
        stub.close()


def test_early_close_break_and_unread_close_reuse_connection() -> None:
    stub = _BoltStreamStub()
    client = NativeClient(stub.uri, min_idle=0, max_size=1)
    try:
        # 1. Explicit early close after chunk 1
        stream = client.stream_sync("ROWS 100", fetch_size=10)
        c1 = stream.next_chunk_sync()
        assert c1 is not None and c1.num_rows == 10
        stream.close_sync()
        stream.close_sync()  # idempotent
        assert stream.is_closed is True
        assert stub.counters.discard_count == 1
        assert client.execute_sync("RETURN 1").to_dicts() == [{"n": 1}]
        assert stub.counters.connection_count == 1

        # 2. `break` out of sync `with` loop sends DISCARD and reuses connection
        with client.stream_sync("ROWS 100", fetch_size=10) as s2:
            for chunk in s2:
                assert chunk.num_rows == 10
                break
        assert s2.is_closed is True
        assert stub.counters.discard_count == 2
        assert client.execute_sync("RETURN 1").to_dicts() == [{"n": 1}]
        assert stub.counters.connection_count == 1

        # 3. Close before reading chunk 1 drains & discards cleanly
        s3 = client.stream_sync("ROWS 50", fetch_size=10)
        s3.close_sync()
        assert s3.is_closed is True
        assert stub.counters.discard_count == 3
        assert client.execute_sync("RETURN 1").to_dicts() == [{"n": 1}]
        assert stub.counters.connection_count == 1

        # 4. `break` out of async `async with` loop sends DISCARD and reuses connection
        async def async_break() -> None:
            async with await client.stream("ROWS 100", fetch_size=10) as s4:
                async for chunk in s4:
                    assert chunk.num_rows == 10
                    break
            assert s4.is_closed is True

        asyncio.run(async_break())
        assert stub.counters.discard_count == 4
        assert stub.counters.connection_count == 1
    finally:
        client.close()
        stub.close()


def test_gc_without_close_replaces_connection_and_does_not_block_event_loop() -> None:
    stub = _BoltStreamStub()
    client = NativeClient(stub.uri, min_idle=0, max_size=1, acquire_timeout=1.0)
    try:

        def open_and_drop_sync() -> None:
            s = client.stream_sync("ROWS 100", fetch_size=10)
            c1 = s.next_chunk_sync()
            assert c1 is not None and c1.num_rows == 10
            assert client._pool_metrics()["active_count"] == 1
            # Dropped without close()

        open_and_drop_sync()
        gc.collect()
        assert client._pool_metrics()["active_count"] == 0
        assert client._pool_metrics()["idle_count"] == 0
        # Follow-up opens a replacement connection and succeeds
        assert client.execute_sync("RETURN 1").to_dicts() == [{"n": 1}]
        assert stub.counters.connection_count == 2

        # Also inside a running asyncio event loop: GC must not block or deadlock
        async def gc_inside_loop() -> None:
            async def open_and_drop_async() -> None:
                s = await client.stream("ROWS 100", fetch_size=10)
                c1 = await s.next_chunk()
                assert c1 is not None and c1.num_rows == 10
                assert client._pool_metrics()["active_count"] == 1

            await open_and_drop_async()
            t0 = time.monotonic()
            gc.collect()
            elapsed = time.monotonic() - t0
            assert elapsed < 0.2
            assert client._pool_metrics()["active_count"] == 0
            res = (await client.execute("RETURN 1")).to_dicts()
            assert res == [{"n": 1}]

        asyncio.run(gc_inside_loop())
        assert stub.counters.connection_count == 3
    finally:
        client.close()
        stub.close()


def test_asyncio_task_cancel_mid_await_recovers_and_replaces_connection() -> None:
    # Pull #1 returns chunk 1, Pull #2 stalls until cancelled.
    stub = _BoltStreamStub(stall_on_pull_index=2)
    client = NativeClient(stub.uri, min_idle=0, max_size=1, acquire_timeout=1.0)
    try:

        async def run() -> None:
            stream = await client.stream("ROWS 100", fetch_size=10)
            c1 = await stream.next_chunk()
            assert c1 is not None and c1.num_rows == 10

            async def pull_next() -> Any:
                return await stream.next_chunk()

            task = asyncio.create_task(pull_next())
            # Wait until stub server receives PULL #2 and enters stall
            await asyncio.to_thread(stub.pull_stalled.wait, 2.0)
            assert stub.pull_stalled.is_set()

            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

            # Once asyncio dispatches the cancellation callback, StreamFutureGuard
            # marks the stream closed and drops the unclean connection on its own.
            for _ in range(50):
                if stream.is_closed and client._pool_metrics()["active_count"] == 0:
                    break
                await asyncio.sleep(0.005)
            assert stream.is_closed is True
            assert client._pool_metrics()["active_count"] == 0
            # close() after cancellation must be an instant no-op
            await stream.close()

            # Allow the stalled server thread to exit cleanly, then run a follow-up query
            stub.release_stall.set()
            stub.stall_on_pull_index = None
            res = (await client.execute("RETURN 1")).to_dicts()
            assert res == [{"n": 1}]

        asyncio.run(run())
        assert stub.counters.connection_count == 2
    finally:
        client.close()
        stub.close()


def test_pool_max_size_1_waits_or_times_out_and_succeeds_after_close() -> None:
    stub = _BoltStreamStub()
    client = NativeClient(stub.uri, min_idle=0, max_size=1, acquire_timeout=0.15)
    try:
        # 1. Synchronous timeout while stream holds the sole pool slot
        stream = client.stream_sync("ROWS 50", fetch_size=10)
        assert client._pool_metrics()["active_count"] == 1
        with pytest.raises(ValueError, match="exhausted"):
            client.execute_sync("RETURN 1")
        stream.close_sync()
        assert client._pool_metrics()["active_count"] == 0
        assert client.execute_sync("RETURN 1").to_dicts() == [{"n": 1}]

        # 2. Async waiter succeeds once the open stream closes before acquire_timeout
        async def waiter_unblocks() -> None:
            s = await client.stream("ROWS 50", fetch_size=10)
            c1 = await s.next_chunk()
            assert c1 is not None and c1.num_rows == 10

            async def delayed_close() -> None:
                await asyncio.sleep(0.05)
                await s.close()

            closer = asyncio.create_task(delayed_close())
            res = (await client.execute("RETURN 1")).to_dicts()
            await closer
            assert res == [{"n": 1}]

        asyncio.run(waiter_unblocks())
    finally:
        client.close()
        stub.close()


def test_in_transaction_stream_fail_fast_commit_rollback_and_cancel() -> None:
    stub = _BoltStreamStub()
    client = NativeClient(stub.uri, min_idle=0, max_size=1, acquire_timeout=1.0)
    try:
        # 1. Sync tx stream + fail-fast guards + follow-up + commit
        tx = client.begin_transaction_sync()
        s = tx.stream_sync("ROWS 25", fetch_size=10)
        assert tx.has_open_stream is True

        with pytest.raises(RuntimeError, match="active stream"):
            tx.execute_sync("RETURN 1")
        with pytest.raises(RuntimeError, match="active stream"):
            tx.execute_arrow_sync("RETURN 1")
        with pytest.raises(RuntimeError, match="active stream"):
            tx.stream_sync("RETURN 1")
        with pytest.raises(RuntimeError, match="active stream"):
            tx.commit_sync()

        assert tx.is_active is True
        c1 = s.next_chunk_sync()
        assert c1 is not None and c1.num_rows == 10
        s.close_sync()
        assert tx.has_open_stream is False
        assert tx.execute_sync("RETURN 1").to_dicts() == [{"n": 1}]
        tx.commit_sync()
        assert tx.state == "COMMITTED"
        assert stub.counters.begin_count == 1
        assert stub.counters.commit_count == 1

        # 2. Rollback while stream is open succeeds and leaves connection usable
        tx2 = client.begin_transaction_sync()
        s2 = tx2.stream_sync("ROWS 50", fetch_size=10)
        assert tx2.has_open_stream is True
        tx2.rollback_sync()
        assert tx2.state == "ROLLED_BACK"
        s2.close_sync()
        assert client._pool_metrics()["active_count"] == 0
        assert client.execute_sync("RETURN 1").to_dicts() == [{"n": 1}]

        # 3. Dropping an unclosed in-tx stream marks tx FAILED without blocking
        tx3 = client.begin_transaction_sync()
        s3 = tx3.stream_sync("ROWS 50", fetch_size=10)
        _ = s3.next_chunk_sync()
        del s3
        gc.collect()
        assert tx3.state == "FAILED"
        assert tx3.has_open_stream is False
        tx3.rollback_sync()
        assert tx3.state == "ROLLED_BACK"
        assert client._pool_metrics()["active_count"] == 0
        assert client.execute_sync("RETURN 1").to_dicts() == [{"n": 1}]

        # 4. Async tx stream + fail-fast + cancellation mid-await
        async def run_async_tx() -> None:
            atx = await client.begin_transaction()
            as1 = await atx.stream("ROWS 25", fetch_size=10)
            assert atx.has_open_stream is True
            with pytest.raises(RuntimeError, match="active stream"):
                await atx.execute("RETURN 1")
            with pytest.raises(RuntimeError, match="active stream"):
                await atx.commit()
            rows: list[int] = []
            async for ch in as1:
                rows.extend(r["i"] for r in ch.to_dicts())
            assert len(rows) == 25
            assert atx.has_open_stream is False
            assert (await atx.execute("RETURN 1")).to_dicts() == [{"n": 1}]
            await atx.commit()
            assert atx.state == "COMMITTED"

            # Cancel mid-await inside tx stream
            stub.pull_stalled.clear()
            stub.release_stall.clear()
            with stub.counters.lock:
                stub.stall_on_pull_index = stub.counters.pull_count + 2
            atx2 = await client.begin_transaction()
            as2 = await atx2.stream("ROWS 50", fetch_size=10)
            _ = await as2.next_chunk()

            async def pull_tx_next() -> Any:
                return await as2.next_chunk()

            task = asyncio.create_task(pull_tx_next())
            await asyncio.to_thread(stub.pull_stalled.wait, 2.0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            await as2.close()
            for _ in range(50):
                if (
                    as2.is_closed
                    and atx2.state == "FAILED"
                    and client._pool_metrics()["active_count"] == 0
                ):
                    break
                await asyncio.sleep(0.005)
            assert as2.is_closed is True
            assert atx2.state == "FAILED"
            assert atx2.is_active is False
            stub.release_stall.set()
            stub.stall_on_pull_index = None
            assert client._pool_metrics()["active_count"] == 0
            assert (await client.execute("RETURN 1")).to_dicts() == [{"n": 1}]

        asyncio.run(run_async_tx())
    finally:
        client.close()
        stub.close()


def test_arrow_c_stream_locked_schema_widening_and_drift_error() -> None:
    def provider(query: str) -> tuple[list[str], list[list[bytes]]]:
        if query == "WIDEN_NULL":
            return ["v"], [[_ps_int_any(1)], [_ps_int_any(2)], [_PS_NULL], [_PS_NULL]]
        if query == "WIDEN_FLOAT":
            return ["v"], [[_ps_float(1.5)], [_ps_float(2.5)], [_ps_int_any(3)], [_ps_int_any(4)]]
        if query == "DRIFT_INT_TO_FLOAT":
            return ["v"], [[_ps_int_any(1)], [_ps_int_any(2)], [_ps_float(3.5)], [_ps_float(4.5)]]
        if query == "EMPTY":
            return ["a", "b"], []
        return ["n"], [[_ps_int_any(1)]]

    stub = _BoltStreamStub(row_provider=provider)
    client = NativeClient(stub.uri, min_idle=0, max_size=1)
    try:
        # 1. All-null chunk 2 widens cleanly into locked Int64 schema
        s1 = client.stream_sync("WIDEN_NULL", fetch_size=2)
        reader1 = pa.RecordBatchReader.from_stream(s1)
        assert reader1.schema == pa.schema([pa.field("v", pa.int64(), nullable=True)])
        tbl1 = reader1.read_all()
        assert tbl1.to_pydict() == {"v": [1, 2, None, None]}
        assert s1.is_consumed is True
        assert s1.is_closed is True
        with pytest.raises(RuntimeError, match="already been consumed"):
            s1.__arrow_c_stream__()

        # 2. Int64 chunk 2 widens cleanly into locked Float64 schema (and works with Polars)
        s2 = client.stream_sync("WIDEN_FLOAT", fetch_size=2)
        df2 = pl.DataFrame(s2)
        assert df2.schema == {"v": pl.Float64}
        assert df2["v"].to_list() == [1.5, 2.5, 3.0, 4.0]
        assert s2.is_closed is True

        # 3. Incompatible drift (Int64 -> Float64) fails loudly with ValueError / ArrowInvalid
        s3 = client.stream_sync("DRIFT_INT_TO_FLOAT", fetch_size=2)
        reader3 = pa.RecordBatchReader.from_stream(s3)
        with pytest.raises(ValueError, match="SchemaDriftError"):
            reader3.read_all()
        assert s3.is_closed is True
        # Connection remains usable after clean close on drift error
        assert client.execute_sync("RETURN 1").to_dicts() == [{"n": 1}]

        # 4. 0-row Arrow C stream export yields empty table with column schema
        s4 = client.stream_sync("EMPTY", fetch_size=10)
        tbl4 = pa.RecordBatchReader.from_stream(s4).read_all()
        assert tbl4.num_rows == 0
        assert tbl4.column_names == ["a", "b"]
        assert s4.is_closed is True
        assert client._pool_metrics()["active_count"] == 0

        # 5. Early drop of Arrow RecordBatchReader without reading all batches releases pool slot
        s5 = client.stream_sync("WIDEN_NULL", fetch_size=2)
        reader5 = pa.RecordBatchReader.from_stream(s5)
        first_batch = reader5.read_next_batch()
        assert first_batch.num_rows == 2
        del reader5
        del s5
        gc.collect()
        assert client._pool_metrics()["active_count"] == 0
        assert client.execute_sync("RETURN 1").to_dicts() == [{"n": 1}]
    finally:
        client.close()
        stub.close()
