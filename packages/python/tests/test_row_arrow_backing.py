"""Stage 2 (#462): row-backed execute() vs Arrow-backed execute_to_arrow()."""

from __future__ import annotations

import asyncio
import socket
import threading
from typing import Any

from test_native_result_types import (
    _bolt_recv_frame,
    _bolt_send_frame,
    _ps_int,
    _ps_list,
    _ps_map,
    _ps_str,
    _ps_struct,
)
from voyager_ogm import AsyncSession, Session


def _start_stub(cols: list[str], row_fields: list[bytes]) -> tuple[int, threading.Thread]:
    """Serves one Bolt 5.2 connection for the duration of a test."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    port = srv.getsockname()[1]

    def serve() -> None:
        try:
            conn, _ = srv.accept()
            with conn:
                hs = b""
                while len(hs) < 20:
                    chunk = conn.recv(20 - len(hs))
                    if not chunk:
                        return
                    hs += chunk
                conn.sendall(b"\x00\x00\x02\x05")
                while True:
                    frame = _bolt_recv_frame(conn)
                    if not frame:
                        break
                    tag = frame[1] if len(frame) >= 2 else 0
                    if tag == 0x02:  # GOODBYE
                        break
                    if tag == 0x01:  # HELLO
                        _bolt_send_frame(
                            conn,
                            _ps_struct(0x70, [_ps_map([("server", _ps_str("Memgraph/2.20"))])]),
                        )
                    elif tag == 0x6A:  # LOGON
                        _bolt_send_frame(conn, _ps_struct(0x70, [_ps_map([])]))
                    elif tag == 0x10:  # RUN
                        _bolt_send_frame(
                            conn,
                            _ps_struct(
                                0x70,
                                [_ps_map([("fields", _ps_list([_ps_str(c) for c in cols]))])],
                            ),
                        )
                    elif tag == 0x3F:  # PULL
                        _bolt_send_frame(conn, _ps_struct(0x71, [_ps_list(row_fields)]))
                        _bolt_send_frame(
                            conn, _ps_struct(0x70, [_ps_map([("t_last", _ps_int(1))])])
                        )
                    else:
                        _bolt_send_frame(conn, _ps_struct(0x70, [_ps_map([])]))
        finally:
            srv.close()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    return port, thread


def _assert_row_and_arrow_backing(session: Any) -> None:
    result = session.execute("RETURN 1")
    assert result.stream._backing == "rows"
    assert result.stream._arrow_materialized is False
    assert result.all() == [{"n": 1, "s": "a"}]
    # The row-backed stream stays alive after .all(), and slicing it into
    # batches still does not build Arrow.
    assert result.stream._backing == "rows"
    assert result.stream._arrow_materialized is False
    chunks = result.stream.iter_batches(1)
    assert len(chunks) == 1
    assert all(chunk._arrow_materialized is False for chunk in chunks)
    assert result.stream._arrow_materialized is False
    table = result.to_arrow()
    assert table.to_pylist() == [{"n": 1, "s": "a"}]
    assert result.stream._arrow_materialized is True

    arrow_result = session._native_client.execute_arrow_sync("RETURN 1")
    assert arrow_result.stream._backing == "arrow"
    assert arrow_result.to_dicts() == [{"n": 1, "s": "a"}]


def test_sync_execute_is_row_backed_and_arrow_export_is_lazy() -> None:
    port, thread = _start_stub(["n", "s"], [_ps_int(1), _ps_str("a")])
    session = Session(f"bolt://127.0.0.1:{port}", dialect="memgraph", backend="native", pool_size=1)
    try:
        _assert_row_and_arrow_backing(session)
    finally:
        session.close()
        thread.join(timeout=2)


def test_async_execute_and_execute_to_arrow_use_the_arrow_only_path() -> None:
    port, thread = _start_stub(["n", "s"], [_ps_int(1), _ps_str("a")])

    async def run() -> None:
        session = AsyncSession(
            f"bolt://127.0.0.1:{port}", dialect="memgraph", backend="native", pool_size=1
        )
        try:
            result = await session.execute("RETURN 1")
            assert result.stream._backing == "rows"
            assert result.all() == [{"n": 1, "s": "a"}]
            assert result.stream._arrow_materialized is False
            table = await session.execute_to_arrow("RETURN 1")
            assert table.to_pylist() == [{"n": 1, "s": "a"}]
            arrow_result = await session._native_client.execute_arrow("RETURN 1")
            assert arrow_result.stream._backing == "arrow"
        finally:
            await session.close()

    try:
        asyncio.run(run())
    finally:
        thread.join(timeout=2)


def test_transaction_execute_is_row_backed_and_arrow_export_is_arrow_backed() -> None:
    port, thread = _start_stub(["n", "s"], [_ps_int(1), _ps_str("a")])
    session = Session(f"bolt://127.0.0.1:{port}", dialect="memgraph", backend="native", pool_size=1)
    try:
        with session.transaction() as tx:
            result = tx.execute("RETURN 1")
            assert result.stream._backing == "rows"
            assert result.all() == [{"n": 1, "s": "a"}]
            assert tx.execute_to_arrow("RETURN 1").to_pylist() == [{"n": 1, "s": "a"}]
    finally:
        session.close()
        thread.join(timeout=2)
