"""Regression tests for the OLTP quick fixes in issue #459."""

from __future__ import annotations

import pickle
from typing import Any

import pytest
import voyager_ogm.types as types_module
from voyager_ogm import AsyncSession, Session, create_bridge
from voyager_ogm.bridges.falkordb import AsyncFalkorDBBridge, FalkorDBBridge
from voyager_ogm.types import Point


class _FakeFalkorGraph:
    __module__ = "falkordb"

    def __init__(self, name: str) -> None:
        self.name = name
        self.queries: list[tuple[str, dict[str, Any] | None]] = []

    def query(self, statement: str, params: dict[str, Any] | None = None) -> Any:
        self.queries.append((statement, params))
        return type("Result", (), {"header": [], "result_set": []})()


class _FakeFalkorClient:
    __module__ = "falkordb"

    def __init__(self) -> None:
        self.selected: list[str] = []
        self.graphs: dict[str, _FakeFalkorGraph] = {}

    def select_graph(self, name: str) -> _FakeFalkorGraph:
        self.selected.append(name)
        graph = self.graphs.setdefault(name, _FakeFalkorGraph(name))
        return graph


def test_point_native_expr_is_built_lazily() -> None:
    point = Point(1.0, 2.0)
    assert "_native_expr" not in point.__dict__

    first = point._native_expr
    assert "_native_expr" in point.__dict__
    assert point._native_expr is first


def test_point_lazy_expr_keeps_equality_hash_and_pickle() -> None:
    point = Point(latitude=52.52, longitude=13.405)
    same = Point(latitude=52.52, longitude=13.405)
    # Access the native expression before comparing and pickling so the lazy
    # cache is populated (the previous eager implementation always had it).
    assert point._native_expr is not None

    assert point == same
    assert hash(point) == hash(same)

    for protocol in range(pickle.HIGHEST_PROTOCOL + 1):
        restored = pickle.loads(pickle.dumps(point, protocol=protocol))
        assert restored == point
        assert hash(restored) == hash(point)
        assert "_native_expr" not in restored.__dict__


def test_point_lazy_expr_falls_back_to_none_without_extension(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(types_module, "HAS_VOYAGER_RS", False)
    monkeypatch.setattr(types_module, "AstExpr", None)
    point = Point(1.0, 2.0)
    assert point._native_expr is None
    # The None fallback is cached, matching the previous eager assignment.
    assert point.__dict__["_native_expr"] is None


def test_sync_falkordb_client_graph_name_selects_graph() -> None:
    client = _FakeFalkorClient()
    bridge = create_bridge(client)
    assert isinstance(bridge, FalkorDBBridge)
    assert bridge.graph_name == "voyager_graph"
    assert client.selected == ["voyager_graph"]

    bridge.graph_name = "zz_graph"
    assert bridge.graph_name == "zz_graph"
    assert client.selected == ["voyager_graph", "zz_graph"]
    assert bridge.graph is client.graphs["zz_graph"]


def test_create_bridge_passes_graph_name_to_async_falkordb_client() -> None:
    client = _FakeFalkorClient()
    bridge = create_bridge(client, is_async=True, graph_name="zz_async")
    assert isinstance(bridge, AsyncFalkorDBBridge)
    assert bridge.graph_name == "zz_async"
    assert client.selected[-1] == "zz_async"
    assert bridge.sync_bridge.graph is client.graphs["zz_async"]


def test_async_falkordb_bridge_graph_name_setter() -> None:
    client = _FakeFalkorClient()
    bridge = AsyncFalkorDBBridge(client)
    bridge.graph_name = "zz_setter"
    assert bridge.graph_name == "zz_setter"
    assert bridge.sync_bridge.graph is client.graphs["zz_setter"]


def test_create_bridge_honours_graph_instance_name() -> None:
    graph = _FakeFalkorGraph("zz_own_graph")
    bridge = create_bridge(graph)
    assert isinstance(bridge, FalkorDBBridge)
    assert bridge.graph_name == "zz_own_graph"

    async_bridge = create_bridge(graph, is_async=True, graph_name="zz_override")
    assert isinstance(async_bridge, AsyncFalkorDBBridge)
    assert async_bridge.graph_name == "zz_override"


class _FakeNativeResult:
    stream = None
    columns: list[str] = []
    has_age_columns = False

    def to_dicts(self) -> list[dict[str, Any]]:
        return []


class _FakeNativeClient:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.args = args
        self.kwargs = kwargs
        self.closed = False

    def execute_sync(self, statement: str, params: dict[str, Any]) -> _FakeNativeResult:
        return _FakeNativeResult()

    async def execute(self, statement: str, params: dict[str, Any]) -> _FakeNativeResult:
        return _FakeNativeResult()

    def close(self) -> None:
        self.closed = True


def test_native_backend_uri_session_does_not_create_bridge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[Any] = []

    def _fail_create_bridge(*args: Any, **kwargs: Any) -> Any:
        calls.append((args, kwargs))
        raise AssertionError("create_bridge must not run for backend='native'")

    monkeypatch.setattr("voyager_ogm.session.create_bridge", _fail_create_bridge)
    monkeypatch.setattr("voyager_ogm._voyager_rs.NativeClient", _FakeNativeClient)

    session = Session("bolt://127.0.0.1:7687", backend="native")
    try:
        assert calls == []
        assert isinstance(session.native_client, _FakeNativeClient)
        assert session._bridge is None
        with pytest.raises(RuntimeError, match='backend="native"'):
            _ = session.bridge
    finally:
        session.close()


def test_native_backend_rejects_bridge_only_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("voyager_ogm.session.create_bridge", lambda *a, **k: None)
    monkeypatch.setattr("voyager_ogm._voyager_rs.NativeClient", _FakeNativeClient)
    session = Session("bolt://127.0.0.1:7687", backend="native")
    try:
        # Simulate the native client being unavailable after construction: the
        # bridge fallback must now fail clearly instead of AttributeError.
        session._native_client = None
        with pytest.raises(RuntimeError, match="requires a driver bridge"):
            session.execute("RETURN 1")
    finally:
        session.close()


@pytest.mark.asyncio
async def test_async_native_backend_does_not_create_bridge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _fail_create_bridge(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("create_bridge must not run for backend='native'")

    monkeypatch.setattr("voyager_ogm.session.create_bridge", _fail_create_bridge)
    monkeypatch.setattr("voyager_ogm._voyager_rs.NativeClient", _FakeNativeClient)

    session = AsyncSession("bolt://127.0.0.1:7687", backend="native")
    assert session._bridge is None
    with pytest.raises(RuntimeError, match="requires a driver bridge"):
        _ = session.bridge
    await session.close()
