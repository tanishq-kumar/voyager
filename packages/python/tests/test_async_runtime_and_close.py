"""Offline tests for #460: unified fork-safe Tokio runtime and non-blocking close."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys

import pytest
from voyager_ogm import AsyncSession

_STUB_SERVER_SCRIPT = """
import json
import socket
import socketserver
import struct
import sys

def pack_chunks(msg: bytes) -> bytes:
    return struct.pack("!H", len(msg)) + msg + b"\\x00\\x00"

def ps_str(s: str) -> bytes:
    b = s.encode("utf-8")
    if len(b) < 16:
        return bytes([0x80 + len(b)]) + b
    return bytes([0xD0, len(b)]) + b

def read_msg(conn: socket.socket) -> bytes:
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

class Handler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        conn = self.request
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        if len(conn.recv(20)) < 20:
            return
        conn.sendall(bytes([0x00, 0x00, 0x04, 0x04]))  # Bolt 4.4
        read_msg(conn)  # HELLO
        conn.sendall(pack_chunks(b"\\xb1\\x70\\xa0"))  # SUCCESS {}
        while True:
            msg = read_msg(conn)
            if not msg:
                break
            tag = msg[1] if len(msg) > 1 else 0
            if tag == 0x10:  # RUN
                read_msg(conn)  # PULL
                run_ok = b"\\xb1\\x70\\xa1" + ps_str("fields") + b"\\x91" + ps_str("n")
                rec = b"\\xb1\\x71\\x91\\x01"
                pull_ok = b"\\xb1\\x70\\xa0"
                conn.sendall(pack_chunks(run_ok) + pack_chunks(rec) + pack_chunks(pull_ok))
            elif tag == 0x0F:  # RESET
                conn.sendall(pack_chunks(b"\\xb1\\x70\\xa0"))
            elif tag == 0x02:  # GOODBYE
                break

class Server(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True

srv = Server(("127.0.0.1", 0), Handler)
print(json.dumps({"port": srv.server_address[1]}), flush=True)
srv.serve_forever()
"""


def _start_stub_server() -> tuple[subprocess.Popen[str], int]:
    proc = subprocess.Popen(
        [sys.executable, "-c", _STUB_SERVER_SCRIPT],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert proc.stdout is not None
    line = proc.stdout.readline()
    assert line, f"stub server failed to start: {proc.stderr.read() if proc.stderr else ''}"
    port = int(json.loads(line)["port"])
    return proc, port


def _stop_stub_server(proc: subprocess.Popen[str]) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


@pytest.mark.skipif(sys.platform != "linux", reason="/proc/self/task is Linux-only")
def test_single_tokio_runtime_thread_count_after_sync_and_async() -> None:
    proc, port = _start_stub_server()
    try:
        script = f"""
import asyncio, json, os
from voyager_ogm._voyager_rs import NativeClient

def tokio_threads() -> int:
    count = 0
    for tid in os.listdir("/proc/self/task"):
        try:
            with open(f"/proc/self/task/{{tid}}/comm", encoding="utf-8") as fh:
                if "tokio" in fh.read():
                    count += 1
        except OSError:
            pass
    return count

before = tokio_threads()
client = NativeClient("bolt://127.0.0.1:{port}", min_idle=1, max_size=2)
res_sync = client.execute_sync("RETURN 1 AS n", {{}}).to_dicts()
assert res_sync == [{{"n": 1}}]
after_sync = tokio_threads()

async def run_async() -> tuple[int, int]:
    res_async = (await client.execute("RETURN 1 AS n", {{}})).to_dicts()
    assert res_async == [{{"n": 1}}]
    first = tokio_threads()
    res_async_2 = (await client.execute("RETURN 1 AS n", {{}})).to_dicts()
    assert res_async_2 == [{{"n": 1}}]
    second = tokio_threads()
    await client.close_async()
    return first, second

after_async, after_async_2 = asyncio.run(run_async())
print(json.dumps({{
    "before": before,
    "after_sync": after_sync,
    "after_async": after_async,
    "after_async_2": after_async_2,
}}))
"""
        out = subprocess.check_output([sys.executable, "-c", script], text=True)
        counts = json.loads(out.strip().splitlines()[-1])
        assert counts["before"] == 0
        assert counts["after_sync"] > 0
        # Single runtime: shares the same worker threads and spawns at most 1 lazy
        # blocking thread for GIL re-entry, never a second worker pool (2x).
        assert counts["after_async"] <= counts["after_sync"] + 1
        assert counts["after_async"] < counts["after_sync"] * 2
        assert counts["after_async_2"] == counts["after_async"]
    finally:
        _stop_stub_server(proc)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="os.fork() is Unix-only")
def test_async_queries_work_in_forked_child_after_parent_queries() -> None:
    proc, port = _start_stub_server()
    try:
        script = f"""
import asyncio, os, sys
from voyager_ogm import AsyncSession, _voyager_rs
from voyager_ogm._voyager_rs import NativeClient

parent_pid = os.getpid()
parent_client = NativeClient("bolt://127.0.0.1:{port}", min_idle=1, max_size=2)
assert parent_client.execute_sync("RETURN 1 AS n", {{}}).to_dicts() == [{{"n": 1}}]

async def parent_async() -> None:
    res = (await parent_client.execute("RETURN 1 AS n", {{}})).to_dicts()
    assert res == [{{"n": 1}}]
    await parent_client.close_async()

asyncio.run(parent_async())
assert _voyager_rs.get_runtime_pid() == parent_pid

pid = os.fork()
if pid == 0:
    try:
        child_pid = os.getpid()
        assert child_pid != parent_pid

        async def child_async() -> None:
            session = AsyncSession("bolt://127.0.0.1:{port}", dialect="cypher", backend="native")
            try:
                rows = (await session.execute("RETURN 1 AS n")).all()
                assert rows == [{{"n": 1}}]
                assert _voyager_rs.get_runtime_pid() == child_pid
            finally:
                await session.close()

        asyncio.run(child_async())
        os._exit(0)
    except BaseException as exc:
        print(f"child error: {{exc!r}}", file=sys.stderr, flush=True)
        os._exit(1)

_, status = os.waitpid(pid, 0)
assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0
"""
        completed = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            timeout=20,
        )
        assert completed.returncode == 0, completed.stderr
    finally:
        _stop_stub_server(proc)


@pytest.mark.asyncio
async def test_async_session_close_awaits_close_async_without_blocking_concurrent_task() -> None:
    class _SlowAsyncCloseClient:
        def __init__(self) -> None:
            self.close_async_called = False
            self.close_sync_called = False

        async def execute(self, statement: str, parameters: dict | None = None) -> object:
            return type("Res", (), {"stream": None, "summary": None})()

        def execute_sync(self, statement: str, parameters: dict | None = None) -> object:
            raise AssertionError("sync execute should not be called")

        async def close_async(self) -> None:
            self.close_async_called = True
            await asyncio.sleep(0.08)

        def close(self) -> None:
            self.close_sync_called = True
            raise AssertionError("blocking sync close() must not be called by AsyncSession.close()")

    client = _SlowAsyncCloseClient()
    session = AsyncSession(bridge=client, dialect="cypher", backend="native")

    ticks = 0
    stop_ticking = asyncio.Event()

    async def ticker() -> None:
        nonlocal ticks
        while not stop_ticking.is_set():
            ticks += 1
            await asyncio.sleep(0.005)

    ticker_task = asyncio.create_task(ticker())
    try:
        await session.close()
    finally:
        stop_ticking.set()
        await ticker_task

    assert client.close_async_called is True
    assert client.close_sync_called is False
    assert ticks >= 5


@pytest.mark.asyncio
async def test_native_client_close_async_is_awaitable_on_real_extension() -> None:
    proc, port = _start_stub_server()
    try:
        session = AsyncSession(f"bolt://127.0.0.1:{port}", dialect="cypher", backend="native")
        rows = (await session.execute("RETURN 1 AS n")).all()
        assert rows == [{"n": 1}]

        ticks = 0
        stop_ticking = asyncio.Event()

        async def ticker() -> None:
            nonlocal ticks
            while not stop_ticking.is_set():
                ticks += 1
                await asyncio.sleep(0)

        ticker_task = asyncio.create_task(ticker())
        try:
            await session.close()
            await asyncio.sleep(0)
        finally:
            stop_ticking.set()
            await ticker_task

        assert ticks >= 1
    finally:
        _stop_stub_server(proc)
