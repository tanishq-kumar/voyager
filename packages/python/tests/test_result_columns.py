"""Offline unit tests for ExecutionResult.columns and keys() across bridges and streams (#378)."""

from __future__ import annotations

from typing import Any

import polars as pl
import pyarrow as pa
import pytest
from voyager_ogm import (
    AsyncFalkorDBBridge,
    AsyncMockBridge,
    AsyncNeo4jBoltBridge,
    AsyncPostgresBridge,
    AsyncSession,
    DuckDbBridge,
    FalkorDBBridge,
    MockBridge,
    Neo4jBoltBridge,
    PostgresBridge,
    QueryResult,
    RelateBridge,
    Session,
    generate_synthetic_stream,
)
from voyager_ogm.session import ExecutionResult


def test_execution_result_columns_read_only_and_keys() -> None:
    """ExecutionResult.columns is a read-only tuple and keys() returns a list."""
    res = ExecutionResult(
        records=[{"name": "Alice", "age": 30}],
        columns=("name", "age"),
    )
    assert res.columns == ("name", "age")
    assert isinstance(res.columns, tuple)
    assert res.keys() == ["name", "age"]
    assert res.mappings().columns == ("name", "age")
    assert res.mappings().keys() == ["name", "age"]

    attr = "columns"
    with pytest.raises(AttributeError):
        setattr(res, attr, ("other",))


def test_execution_result_empty_with_explicit_columns_exports_schema() -> None:
    """Empty ExecutionResult with columns exports 0-row Polars DataFrame and Arrow Table with schema."""
    res = ExecutionResult(records=[], columns=("person_name", "person_age"))
    assert res.columns == ("person_name", "person_age")
    assert res.keys() == ["person_name", "person_age"]
    assert res.all() == []

    df = res.to_polars()
    assert isinstance(df, pl.DataFrame)
    assert df.shape == (0, 2)
    assert df.columns == ["person_name", "person_age"]

    tbl = res.to_arrow()
    assert isinstance(tbl, pa.Table)
    assert tbl.num_rows == 0
    assert tbl.column_names == ["person_name", "person_age"]


def test_execution_result_write_without_return_has_empty_columns() -> None:
    """ExecutionResult with no records and no columns exposes empty tuple and empty exports."""
    res = ExecutionResult(records=[])
    assert res.columns == ()
    assert res.keys() == []
    assert res.to_polars().shape == (0, 0)
    assert res.to_arrow().num_rows == 0
    assert res.to_arrow().num_columns == 0


def test_arrow_stream_and_execution_result_columns_survive_consumption() -> None:
    """Native ArrowStream exposes columns from the Arrow schema even for 0 rows and after consumption."""
    stream = generate_synthetic_stream(0)
    assert tuple(stream.columns) == ("id", "label", "name", "age", "score", "active")

    qr = QueryResult(stream)
    assert qr.columns == ("id", "label", "name", "age", "score", "active")

    res = ExecutionResult(stream=stream)
    assert res.columns == ("id", "label", "name", "age", "score", "active")
    assert res.keys() == ["id", "label", "name", "age", "score", "active"]

    df = res.to_polars()
    assert stream.is_consumed
    assert df.shape == (0, 6)
    assert df.columns == ["id", "label", "name", "age", "score", "active"]
    assert res.columns == ("id", "label", "name", "age", "score", "active")

    tbl = res.to_arrow()
    assert tbl.num_rows == 0
    assert tbl.column_names == ["id", "label", "name", "age", "score", "active"]


def test_mock_bridge_columns_empty_and_non_empty() -> None:
    """MockBridge preserves columns for queued empty DataFrames, explicit columns, and writes."""
    bridge = MockBridge()
    session = Session(bridge=bridge, dialect="cypher")

    bridge.queue_result([{"person_name": "Alice", "person_age": 30}])
    res_rows = session.execute("MATCH (p) RETURN p.name AS person_name, p.age AS person_age")
    assert res_rows.columns == ("person_name", "person_age")
    assert res_rows.keys() == ["person_name", "person_age"]

    bridge.queue_result([], columns=["person_name", "person_age"])
    res_empty = session.execute("MATCH (p) RETURN p.name AS person_name, p.age AS person_age")
    assert res_empty.columns == ("person_name", "person_age")
    assert res_empty.all() == []
    assert res_empty.to_polars().columns == ["person_name", "person_age"]
    assert res_empty.to_arrow().column_names == ["person_name", "person_age"]

    bridge.queue_result([], columns=["person_name", "person_age"])
    df_empty = session.execute_to_polars(
        "MATCH (p) RETURN p.name AS person_name, p.age AS person_age"
    )
    assert df_empty.shape == (0, 2)
    assert df_empty.columns == ["person_name", "person_age"]

    bridge.queue_result(pl.DataFrame(schema=["col_a", "col_b"]))
    tbl_empty = session.execute_to_arrow("MATCH (p) RETURN p.a AS col_a, p.b AS col_b")
    assert tbl_empty.num_rows == 0
    assert tbl_empty.column_names == ["col_a", "col_b"]

    res_write = session.execute("CREATE (:Person {name: 'Bob'})")
    assert res_write.columns == ()
    assert res_write.keys() == []


@pytest.mark.asyncio
async def test_async_mock_bridge_columns_empty_and_non_empty() -> None:
    """AsyncSession preserves columns on AsyncMockBridge."""
    bridge = AsyncMockBridge()
    session = AsyncSession(bridge=bridge, dialect="cypher")

    bridge.queue_result([], columns=["x", "y"])
    res = await session.execute("RETURN 1 AS x, 2 AS y")
    assert res.columns == ("x", "y")
    assert res.keys() == ["x", "y"]
    assert res.to_polars().columns == ["x", "y"]
    assert res.to_arrow().column_names == ["x", "y"]

    bridge.queue_result([], columns=["x", "y"])
    df = await session.execute_to_polars("RETURN 1 AS x, 2 AS y")
    assert df.shape == (0, 2)
    assert df.columns == ["x", "y"]

    bridge.queue_result([], columns=["x", "y"])
    tbl = await session.execute_to_arrow("RETURN 1 AS x, 2 AS y")
    assert tbl.num_rows == 0
    assert tbl.column_names == ["x", "y"]


def test_fake_bolt_driver_sync_columns() -> None:
    """Neo4jBoltBridge extracts columns from Result.keys() for rows, 0 rows, and writes."""

    class FakeBoltResult:
        def __init__(self, keys: list[str], rows: list[dict[str, Any]]) -> None:
            self._keys = keys
            self._rows = rows

        def keys(self) -> list[str]:
            return list(self._keys)

        def __iter__(self):
            return iter(self._rows)

    class FakeBoltSession:
        def __init__(self, responses: list[FakeBoltResult]) -> None:
            self._responses = responses

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def run(self, stmt, params):
            return self._responses.pop(0)

    class FakeBoltDriver:
        def __init__(self, responses: list[FakeBoltResult]) -> None:
            self._responses = responses

        def session(self, **kwargs):
            return FakeBoltSession(self._responses)

    responses = [
        FakeBoltResult(["person_name", "person_age"], [{"person_name": "Alice", "person_age": 30}]),
        FakeBoltResult(["person_name", "person_age"], []),
        FakeBoltResult(["person_name", "person_age"], []),
        FakeBoltResult(["person_name", "person_age"], []),
        FakeBoltResult([], []),
    ]
    session = Session(bridge=Neo4jBoltBridge(FakeBoltDriver(responses)), dialect="cypher")

    r_nonempty = session.execute("MATCH (p) RETURN p.name AS person_name, p.age AS person_age")
    assert r_nonempty.columns == ("person_name", "person_age")
    assert r_nonempty.all() == [{"person_name": "Alice", "person_age": 30}]

    r_empty = session.execute(
        "MATCH (p) WHERE false RETURN p.name AS person_name, p.age AS person_age"
    )
    assert r_empty.columns == ("person_name", "person_age")
    assert r_empty.keys() == ["person_name", "person_age"]
    assert r_empty.to_polars().columns == ["person_name", "person_age"]
    assert r_empty.to_arrow().column_names == ["person_name", "person_age"]

    df_empty = session.execute_to_polars(
        "MATCH (p) WHERE false RETURN p.name AS person_name, p.age AS person_age"
    )
    assert df_empty.shape == (0, 2)
    assert df_empty.columns == ["person_name", "person_age"]

    tbl_empty = session.execute_to_arrow(
        "MATCH (p) WHERE false RETURN p.name AS person_name, p.age AS person_age"
    )
    assert tbl_empty.num_rows == 0
    assert tbl_empty.column_names == ["person_name", "person_age"]

    r_write = session.execute("CREATE (:Person {name: 'Bob'})")
    assert r_write.columns == ()
    assert r_write.keys() == []


@pytest.mark.asyncio
async def test_fake_bolt_driver_async_columns() -> None:
    """AsyncNeo4jBoltBridge extracts columns from AsyncResult.keys() for 0 rows and writes."""

    class FakeAsyncBoltResult:
        def __init__(self, keys: list[str], rows: list[dict[str, Any]]) -> None:
            self._keys = keys
            self._rows = rows

        def keys(self) -> list[str]:
            return list(self._keys)

        def __aiter__(self):
            self._idx = 0
            return self

        async def __anext__(self):
            if self._idx >= len(self._rows):
                raise StopAsyncIteration
            row = self._rows[self._idx]
            self._idx += 1
            return row

    class FakeAsyncBoltSession:
        def __init__(self, responses: list[FakeAsyncBoltResult]) -> None:
            self._responses = responses

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def run(self, stmt, params):
            return self._responses.pop(0)

    class FakeAsyncBoltDriver:
        def __init__(self, responses: list[FakeAsyncBoltResult]) -> None:
            self._responses = responses

        def session(self, **kwargs):
            return FakeAsyncBoltSession(self._responses)

    responses = [
        FakeAsyncBoltResult(["a", "b"], []),
        FakeAsyncBoltResult(["a", "b"], []),
        FakeAsyncBoltResult([], []),
    ]
    session = AsyncSession(
        bridge=AsyncNeo4jBoltBridge(FakeAsyncBoltDriver(responses)), dialect="cypher"
    )

    res = await session.execute("MATCH (p) WHERE false RETURN p.a AS a, p.b AS b")
    assert res.columns == ("a", "b")
    assert res.keys() == ["a", "b"]
    assert res.to_polars().columns == ["a", "b"]
    assert res.to_arrow().column_names == ["a", "b"]

    df = await session.execute_to_polars("MATCH (p) WHERE false RETURN p.a AS a, p.b AS b")
    assert df.shape == (0, 2)
    assert df.columns == ["a", "b"]

    res_write = await session.execute("CREATE (:Person)")
    assert res_write.columns == ()


@pytest.mark.asyncio
async def test_fake_falkordb_sync_and_async_columns() -> None:
    """FalkorDBBridge and AsyncFalkorDBBridge preserve header columns for 0 rows and empty writes."""

    class FakeFalkorResponse:
        def __init__(self, header: list[Any] | None, result_set: list[list[Any]]) -> None:
            self.header = header
            self.result_set = result_set

    class FakeFalkorGraph:
        def __init__(self, responses: list[FakeFalkorResponse]) -> None:
            self._responses = responses
            self.name = "fake_graph"

        def query(self, stmt, params):
            return self._responses.pop(0)

    responses = [
        FakeFalkorResponse([[1, "person_name"], [1, "person_age"]], [["Alice", 30]]),
        FakeFalkorResponse([[1, "person_name"], [1, "person_age"]], []),
        FakeFalkorResponse([[1, "person_name"], [1, "person_age"]], []),
        FakeFalkorResponse([[1, "person_name"], [1, "person_age"]], []),
        FakeFalkorResponse([], []),
    ]
    graph = FakeFalkorGraph(responses)
    session = Session(bridge=FalkorDBBridge(graph), dialect="cypher")

    r_nonempty = session.execute("MATCH (p) RETURN p.name AS person_name, p.age AS person_age")
    assert r_nonempty.columns == ("person_name", "person_age")

    r_empty = session.execute(
        "MATCH (p) WHERE false RETURN p.name AS person_name, p.age AS person_age"
    )
    assert r_empty.columns == ("person_name", "person_age")
    assert r_empty.to_polars().columns == ["person_name", "person_age"]
    assert r_empty.to_arrow().column_names == ["person_name", "person_age"]

    df_empty = session.execute_to_polars(
        "MATCH (p) WHERE false RETURN p.name AS person_name, p.age AS person_age"
    )
    assert df_empty.shape == (0, 2)
    assert df_empty.columns == ["person_name", "person_age"]

    tbl_empty = session.execute_to_arrow(
        "MATCH (p) WHERE false RETURN p.name AS person_name, p.age AS person_age"
    )
    assert tbl_empty.num_rows == 0
    assert tbl_empty.column_names == ["person_name", "person_age"]

    r_write = session.execute("CREATE (:Person)")
    assert r_write.columns == ()

    async_session = AsyncSession(
        bridge=AsyncFalkorDBBridge(FakeFalkorGraph([FakeFalkorResponse([[1, "col1"]], [])])),
        dialect="cypher",
    )
    async_res = await async_session.execute("MATCH (p) WHERE false RETURN p.x AS col1")
    assert async_res.columns == ("col1",)


@pytest.mark.asyncio
async def test_fake_postgres_cursor_columns_and_spec_corpus_single_execution() -> None:
    """PostgresBridge and _postgres_session_execute preserve cursor.description without re-executing."""
    import spec_corpus_live_lib as corpus_lib

    class FakePgCursor:
        def __init__(self, steps: list[tuple[Any, list[tuple[Any, ...]]]]) -> None:
            self._steps = steps
            self.description: Any = None
            self._rows: list[tuple[Any, ...]] = []
            self.execute_calls = 0

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, stmt, params=None):
            self.execute_calls += 1
            self.description, self._rows = self._steps.pop(0)

        def fetchall(self):
            return list(self._rows)

    class FakePgConn:
        __module__ = "psycopg.connection"
        __qualname__ = "Connection"

        def __init__(self, cursor: FakePgCursor) -> None:
            self._cursor = cursor

        def cursor(self):
            return self._cursor

    steps = [
        ([("person_name",), ("person_age",)], [("Alice", 30)]),
        ([("person_name",), ("person_age",)], []),
        ([("person_name",), ("person_age",)], []),
        ([("person_name",), ("person_age",)], []),
        (None, []),
    ]
    cursor = FakePgCursor(steps)
    conn = FakePgConn(cursor)
    bridge = PostgresBridge(conn)
    session = Session(bridge=bridge, dialect="sql_pgq")

    r_nonempty = session.execute("SELECT name AS person_name, age AS person_age FROM p")
    assert r_nonempty.columns == ("person_name", "person_age")

    r_empty = session.execute("SELECT name AS person_name, age AS person_age FROM p WHERE false")
    assert r_empty.columns == ("person_name", "person_age")
    assert r_empty.all() == []
    assert r_empty.to_polars().columns == ["person_name", "person_age"]
    assert r_empty.to_arrow().column_names == ["person_name", "person_age"]

    df_empty = session.execute_to_polars(
        "SELECT name AS person_name, age AS person_age FROM p WHERE false"
    )
    assert df_empty.shape == (0, 2)
    assert df_empty.columns == ["person_name", "person_age"]

    tbl_empty = session.execute_to_arrow(
        "SELECT name AS person_name, age AS person_age FROM p WHERE false"
    )
    assert tbl_empty.num_rows == 0
    assert tbl_empty.column_names == ["person_name", "person_age"]

    r_write = session.execute("INSERT INTO p VALUES (1, 'Alice', 30)")
    assert r_write.columns == ()

    # Async check
    async_cursor = FakePgCursor([([("col_z",)], [])])
    async_session = AsyncSession(
        bridge=AsyncPostgresBridge(FakePgConn(async_cursor)), dialect="sql_pgq"
    )
    async_res = await async_session.execute("SELECT z AS col_z FROM t WHERE false")
    assert async_res.columns == ("col_z",)

    # Verify _postgres_session_execute executes only once on 0-row queries (no raw cursor re-run)
    corpus_cursor = FakePgCursor([([("a",), ("b",)], [])])
    cols, rows = corpus_lib._postgres_session_execute(
        FakePgConn(corpus_cursor),
        "SELECT 1 AS a, 2 AS b WHERE false",
        {},
        dialect="sql_pgq",
        graph_name=None,
        engine="pg19",
        parse_age=True,
    )
    assert cols == ["a", "b"]
    assert rows == []
    assert corpus_cursor.execute_calls == 1


def test_duckdb_and_relate_embedded_columns() -> None:
    """Embedded DuckDB and Relate expose columns for rows, 0 rows, and Polars/Arrow exports."""
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE zz_items (id INT, label VARCHAR)")
    con.execute("INSERT INTO zz_items VALUES (1, 'alpha')")

    duck_session = Session(bridge=DuckDbBridge(con), dialect="sql_pgq")
    r_duck = duck_session.execute("SELECT id AS item_id, label AS item_label FROM zz_items")
    assert r_duck.columns == ("item_id", "item_label")
    assert r_duck.all() == [{"item_id": 1, "item_label": "alpha"}]

    r_duck_empty = duck_session.execute(
        "SELECT id AS item_id, label AS item_label FROM zz_items WHERE id = 999"
    )
    assert r_duck_empty.columns == ("item_id", "item_label")
    assert r_duck_empty.all() == []
    assert r_duck_empty.to_polars().columns == ["item_id", "item_label"]
    assert r_duck_empty.to_arrow().column_names == ["item_id", "item_label"]
    assert duck_session.execute_to_polars(
        "SELECT id AS item_id, label AS item_label FROM zz_items WHERE id = 999"
    ).columns == ["item_id", "item_label"]
    assert duck_session.execute_to_arrow(
        "SELECT id AS item_id, label AS item_label FROM zz_items WHERE id = 999"
    ).column_names == ["item_id", "item_label"]

    relate = pytest.importorskip("relate")
    rcon = relate.connect(":memory:")
    rel_session = Session(bridge=RelateBridge(rcon), dialect="cypher")

    r_rel_write = rel_session.execute("CREATE (:ZzItem {id: 1, name: 'one'})")
    assert r_rel_write.columns == ()
    assert r_rel_write.keys() == []

    r_rel = rel_session.execute("MATCH (n:ZzItem) RETURN n.id AS item_id, n.name AS item_name")
    assert r_rel.columns == ("item_id", "item_name")

    r_rel_empty = rel_session.execute(
        "MATCH (n:ZzItem {id: 999}) RETURN n.id AS item_id, n.name AS item_name"
    )
    assert r_rel_empty.columns == ("item_id", "item_name")
    assert r_rel_empty.all() == []
    assert r_rel_empty.to_polars().columns == ["item_id", "item_name"]
    assert r_rel_empty.to_arrow().column_names == ["item_id", "item_name"]
    assert rel_session.execute_to_polars(
        "MATCH (n:ZzItem {id: 999}) RETURN n.id AS item_id, n.name AS item_name"
    ).columns == ["item_id", "item_name"]
    assert rel_session.execute_to_arrow(
        "MATCH (n:ZzItem {id: 999}) RETURN n.id AS item_id, n.name AS item_name"
    ).column_names == ["item_id", "item_name"]


def test_transaction_preserves_columns_on_empty_results() -> None:
    """Explicit Session.transaction() preserves columns and empty Polars/Arrow exports."""
    bridge = MockBridge()
    session = Session(bridge=bridge, dialect="cypher")

    bridge.queue_result([], columns=["tx_col_a", "tx_col_b"])
    bridge.queue_result([], columns=["tx_col_a", "tx_col_b"])
    bridge.queue_result([], columns=["tx_col_a", "tx_col_b"])

    with session.transaction():
        res = session.execute("RETURN 1 AS tx_col_a, 2 AS tx_col_b")
        assert res.columns == ("tx_col_a", "tx_col_b")
        assert res.keys() == ["tx_col_a", "tx_col_b"]

        df = session.execute_to_polars("RETURN 1 AS tx_col_a, 2 AS tx_col_b")
        assert df.shape == (0, 2)
        assert df.columns == ["tx_col_a", "tx_col_b"]

        tbl = session.execute_to_arrow("RETURN 1 AS tx_col_a, 2 AS tx_col_b")
        assert tbl.num_rows == 0
        assert tbl.column_names == ["tx_col_a", "tx_col_b"]
