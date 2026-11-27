"""Regression tests for nested DuckDB UUID and JSON in Arrow, Polars, and execute() (#445)."""

from __future__ import annotations

import datetime
import json
import uuid
from typing import Any

import pyarrow as pa
import pytest
from voyager_ogm import AsyncSession, Duration, Session
from voyager_ogm.bridge import AsyncDuckDbBridge, DuckDbBridge


class _CountingConnection:
    """Delegates to a DuckDB connection and records execution calls."""

    def __init__(self, inner: Any) -> None:
        self.inner = inner
        self.execute_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
        self.sql_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)

    def reset(self) -> None:
        """Clears recorded calls."""
        self.execute_calls.clear()
        self.sql_calls.clear()

    def execute(self, *args: Any, **kwargs: Any) -> Any:
        """Records and forwards an eager DB-API execution."""
        self.execute_calls.append((args, kwargs))
        return self.inner.execute(*args, **kwargs)

    def sql(self, *args: Any, **kwargs: Any) -> Any:
        """Records and forwards a lazy relation creation."""
        self.sql_calls.append((args, kwargs))
        return self.inner.sql(*args, **kwargs)


@pytest.fixture
def duckdb_session():
    """Yields a Session over an in-memory DuckDB connection."""
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(":memory:")
    session = Session(bridge=DuckDbBridge(con), dialect="duckpgq")
    try:
        yield session
    finally:
        session.close()


def _assert_uuid_field(field: pa.Field) -> None:
    assert field.type == pa.binary(16)
    assert field.metadata is not None
    assert field.metadata[b"ARROW:extension:name"] == b"arrow.uuid"
    assert field.metadata[b"voyager.type"] == b"uuid"


def _assert_json_field(field: pa.Field) -> None:
    assert field.type == pa.string()
    assert field.metadata is not None
    assert field.metadata[b"voyager.encoding"] == b"json"
    assert field.metadata[b"voyager.type"] == b"json"


def test_duckdb_nested_uuid_and_json_arrow_polars_and_execute(duckdb_session: Session) -> None:
    """UUID and JSON inside STRUCT, MAP, LIST, and ARRAY keep #424 representations."""
    u1 = uuid.UUID("12345678-1234-5678-1234-567812345678")
    u2 = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    doc1 = {"a": [1, True, None]}
    doc2 = [1, "x", False]

    statement = """
        SELECT
            $u1::UUID AS top_u,
            $j1::JSON AS top_j,
            {
                'u': $u1::UUID,
                'j': $j1::JSON,
                'jnull': 'null'::JSON,
                'jstr': '"hello"'::JSON,
                'n': 7
            } AS s,
            MAP {'k': $u1::UUID} AS m_u,
            MAP {$u1::UUID: $j1::JSON} AS m_uj,
            [$u1::UUID, NULL] AS l_u,
            [$j1::JSON, 'null'::JSON, NULL] AS l_j,
            [{'u': $u2::UUID, 'j': $j2::JSON}] AS ls,
            [$u1::UUID, NULL]::UUID[2] AS arr_u,
            [$j1::JSON, NULL]::JSON[2] AS arr_j
    """
    params = {"u1": u1, "u2": u2, "j1": json.dumps(doc1), "j2": json.dumps(doc2)}

    def assert_all() -> pa.Table:
        row = duckdb_session.execute(statement, params).first()
        assert row == {
            "top_u": u1,
            "top_j": doc1,
            "s": {"u": u1, "j": doc1, "jnull": None, "jstr": "hello", "n": 7},
            "m_u": {"k": u1},
            "m_uj": {u1: doc1},
            "l_u": [u1, None],
            "l_j": [doc1, None, None],
            "ls": [{"u": u2, "j": doc2}],
            "arr_u": (u1, None),
            "arr_j": (doc1, None),
        }

        table = duckdb_session.execute_to_arrow(statement, params)
        _assert_uuid_field(table.schema.field("top_u"))
        _assert_json_field(table.schema.field("top_j"))

        s_type = table.schema.field("s").type
        assert pa.types.is_struct(s_type)
        _assert_uuid_field(s_type.field("u"))
        _assert_json_field(s_type.field("j"))
        _assert_json_field(s_type.field("jnull"))
        _assert_json_field(s_type.field("jstr"))
        assert s_type.field("n").type == pa.int32()
        assert not s_type.field("n").metadata

        m_u_type = table.schema.field("m_u").type
        assert pa.types.is_map(m_u_type)
        assert m_u_type.key_field.type == pa.string()
        _assert_uuid_field(m_u_type.item_field)

        m_uj_type = table.schema.field("m_uj").type
        assert pa.types.is_map(m_uj_type)
        _assert_uuid_field(m_uj_type.key_field)
        _assert_json_field(m_uj_type.item_field)

        l_u_type = table.schema.field("l_u").type
        assert pa.types.is_list(l_u_type)
        _assert_uuid_field(l_u_type.value_field)

        l_j_type = table.schema.field("l_j").type
        assert pa.types.is_list(l_j_type)
        _assert_json_field(l_j_type.value_field)

        ls_elem_type = table.schema.field("ls").type.value_field.type
        assert pa.types.is_struct(ls_elem_type)
        _assert_uuid_field(ls_elem_type.field("u"))
        _assert_json_field(ls_elem_type.field("j"))

        arr_u_type = table.schema.field("arr_u").type
        assert pa.types.is_fixed_size_list(arr_u_type)
        _assert_uuid_field(arr_u_type.value_field)

        arr_j_type = table.schema.field("arr_j").type
        assert pa.types.is_fixed_size_list(arr_j_type)
        _assert_json_field(arr_j_type.value_field)

        assert table.column("s").to_pylist() == [
            {
                "u": u1.bytes,
                "j": json.dumps(doc1),
                "jnull": "null",
                "jstr": '"hello"',
                "n": 7,
            }
        ]
        assert table.column("m_uj").to_pylist() == [[(u1.bytes, json.dumps(doc1))]]
        assert table.column("l_u").to_pylist() == [[u1.bytes, None]]
        assert table.column("arr_u").to_pylist() == [[u1.bytes, None]]

        pl = pytest.importorskip("polars")
        frame = duckdb_session.execute_to_polars(statement, params)
        assert frame.schema["top_u"] == pl.Binary
        assert frame.schema["top_j"] == pl.String
        assert frame.schema["s"] == pl.Struct(
            {
                "u": pl.Binary,
                "j": pl.String,
                "jnull": pl.String,
                "jstr": pl.String,
                "n": pl.Int32,
            }
        )
        assert frame.schema["m_uj"] == pl.List(pl.Struct({"key": pl.Binary, "value": pl.String}))
        assert frame.schema["l_u"] == pl.List(pl.Binary)
        assert frame.schema["l_j"] == pl.List(pl.String)
        assert frame.schema["arr_u"] == pl.Array(pl.Binary, shape=(2,))
        assert frame.schema["arr_j"] == pl.Array(pl.String, shape=(2,))
        assert frame["l_u"].to_list() == [[u1.bytes, None]]
        assert frame["arr_u"].to_list() == [[u1.bytes, None]]
        assert json.loads(frame["s"].to_list()[0]["j"]) == doc1
        assert frame["s"].to_list()[0]["u"] == u1.bytes
        return table

    auto_table = assert_all()
    with duckdb_session.transaction():
        tx_table = assert_all()
    assert tx_table.equals(auto_table, check_metadata=True)


def test_duckdb_union_uuid_and_json_arrow_and_execute(duckdb_session: Session) -> None:
    """UUID and JSON inside UNION (and LIST<UNION>) preserve member metadata and JSON tags."""
    u = uuid.UUID("12345678-1234-5678-1234-567812345678")
    doc = {"a": 1}
    statement = """
        SELECT
            union_value(u := $u::UUID)::UNION(u UUID, j JSON, s VARCHAR) AS un_u,
            union_value(j := $j::JSON)::UNION(u UUID, j JSON, s VARCHAR) AS un_j,
            union_value(s := $raw)::UNION(u UUID, j JSON, s VARCHAR) AS un_s,
            [
                union_value(j := $j::JSON)::UNION(u UUID, j JSON, s VARCHAR),
                union_value(s := $raw)::UNION(u UUID, j JSON, s VARCHAR)
            ] AS l_un
    """
    params = {"u": u, "j": json.dumps(doc), "raw": '{"a":1}'}

    def assert_all() -> pa.Table:
        row = duckdb_session.execute(statement, params).first()
        assert row is not None
        assert row["un_u"] == u
        assert row["un_j"] == doc
        assert type(row["un_s"]) is str
        assert row["un_s"] == '{"a":1}'
        assert row["l_un"] == [doc, '{"a":1}']
        assert type(row["l_un"][1]) is str

        table = duckdb_session.execute_to_arrow(statement, params)
        for col in ("un_u", "un_j", "un_s"):
            utype = table.schema.field(col).type
            assert pa.types.is_union(utype)
            _assert_uuid_field(utype.field(0))
            _assert_json_field(utype.field(1))
            assert utype.field(2).type == pa.string()
            assert not utype.field(2).metadata

        l_utype = table.schema.field("l_un").type.value_field.type
        assert pa.types.is_union(l_utype)
        _assert_uuid_field(l_utype.field(0))
        _assert_json_field(l_utype.field(1))
        assert not l_utype.field(2).metadata
        assert table.column("un_u").to_pylist() == [u.bytes]
        return table

    auto_table = assert_all()
    with duckdb_session.transaction():
        tx_table = assert_all()
    assert tx_table.equals(auto_table, check_metadata=True)


def test_duckdb_nested_uuid_json_with_interval_and_timetz(duckdb_session: Session) -> None:
    """Nested UUID and JSON coexist with nested TIMETZ and calendar INTERVAL."""
    u = uuid.UUID("12345678-1234-5678-1234-567812345678")
    doc = {"k": [1, 2]}
    statement = """
        SELECT
            {
                'u': $u::UUID,
                'j': $j::JSON,
                't': '10:00:00+02'::TIMETZ,
                'iv': INTERVAL '1 year 2 months'
            } AS s
    """
    params = {"u": u, "j": json.dumps(doc)}
    row = duckdb_session.execute(statement, params).first()
    assert row == {
        "s": {
            "u": u,
            "j": doc,
            "t": datetime.time(10, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=2))),
            "iv": Duration(months=14),
        }
    }

    table = duckdb_session.execute_to_arrow(statement, params)
    s_type = table.schema.field("s").type
    _assert_uuid_field(s_type.field("u"))
    _assert_json_field(s_type.field("j"))
    assert s_type.field("t").type == pa.string()
    assert s_type.field("t").metadata[b"voyager.type"] == b"time_tz"
    assert table.column("s").to_pylist() == [
        {
            "u": u.bytes,
            "j": json.dumps(doc),
            "t": "10:00:00+02:00",
            "iv": {"months": 14, "days": 0, "seconds": 0, "nanoseconds": 0},
        }
    ]

    frame = duckdb_session.execute_to_polars(statement, params)
    assert frame["s"].to_list()[0]["u"] == u.bytes
    assert json.loads(frame["s"].to_list()[0]["j"]) == doc
    assert frame["s"].to_list()[0]["t"] == "10:00:00+02:00"
    assert frame["s"].to_list()[0]["iv"] == {
        "months": 14,
        "days": 0,
        "seconds": 0,
        "nanoseconds": 0,
    }


def test_duckdb_nested_uuid_json_nulls_empty_and_map_json_keys(duckdb_session: Session) -> None:
    """Handles NULLs, 0-row schemas (including UNION), and MAP(JSON, INT) dict keys."""
    empty_sql = """
        SELECT
            {'u': uuid(), 'j': '{}'::JSON} AS s,
            MAP {uuid(): '{}'::JSON} AS m,
            [uuid()] AS l_u,
            ['{}'::JSON]::JSON[1] AS arr_j,
            union_value(u := uuid())::UNION(u UUID, j JSON) AS un
        WHERE 1=0
    """
    empty_rows = duckdb_session.execute(empty_sql)
    assert list(empty_rows) == []
    assert empty_rows.columns == ("s", "m", "l_u", "arr_j", "un")

    empty_table = duckdb_session.execute_to_arrow(empty_sql)
    assert empty_table.num_rows == 0
    _assert_uuid_field(empty_table.schema.field("s").type.field("u"))
    _assert_json_field(empty_table.schema.field("s").type.field("j"))
    _assert_uuid_field(empty_table.schema.field("m").type.key_field)
    _assert_json_field(empty_table.schema.field("m").type.item_field)
    _assert_uuid_field(empty_table.schema.field("l_u").type.value_field)
    _assert_json_field(empty_table.schema.field("arr_j").type.value_field)
    _assert_uuid_field(empty_table.schema.field("un").type.field(0))
    _assert_json_field(empty_table.schema.field("un").type.field(1))

    # MAP with JSON object key does not raise TypeError: unhashable type: 'dict'
    map_row = duckdb_session.execute("""SELECT MAP {'{"a":1}'::JSON: 42} AS mj""").first()
    assert map_row == {"mj": {'{"a":1}': 42}}


def test_duckdb_nested_uuid_json_runs_exactly_once() -> None:
    """Single query, multi-statement, and DML RETURNING with nested UUID/JSON execute once."""
    duckdb = pytest.importorskip("duckdb")
    inner = duckdb.connect(":memory:")
    counting = _CountingConnection(inner)
    session = Session(bridge=DuckDbBridge(counting), dialect="duckpgq")
    try:
        inner.execute("CREATE SEQUENCE zz_nested_seq START 1")
        inner.execute("CREATE TABLE zz_nested_tbl (id BIGINT, s STRUCT(u UUID, j JSON))")
        u = uuid.UUID("12345678-1234-5678-1234-567812345678")

        single_sql = (
            "SELECT nextval('zz_nested_seq') AS id, "
            f"{{'u': UUID '{u}', 'j': '{{\"a\":1}}'::JSON}} AS s"
        )
        counting.reset()
        table = session.execute_to_arrow(single_sql)
        assert table.column("id").to_pylist() == [1]
        _assert_uuid_field(table.schema.field("s").type.field("u"))
        _assert_json_field(table.schema.field("s").type.field("j"))
        assert len(counting.sql_calls) == 1
        assert counting.execute_calls == []

        counting.reset()
        frame = session.execute_to_polars(single_sql)
        assert frame["id"].to_list() == [2]
        assert frame["s"].to_list()[0]["u"] == u.bytes
        assert len(counting.sql_calls) == 1
        assert counting.execute_calls == []

        dml_sql = (
            "INSERT INTO zz_nested_tbl VALUES "
            f"(nextval('zz_nested_seq'), {{'u': UUID '{u}', 'j': '{{\"b\":2}}'::JSON}}) "
            "RETURNING id, s"
        )
        counting.reset()
        row = session.execute(dml_sql).first()
        assert row == {"id": 3, "s": {"u": u, "j": {"b": 2}}}
        assert len(counting.sql_calls) == 1
        assert counting.execute_calls == []

        counting.reset()
        with session.transaction():
            dml_table = session.execute_to_arrow(dml_sql)
        assert dml_table.column("id").to_pylist() == [4]
        _assert_uuid_field(dml_table.schema.field("s").type.field("u"))
        _assert_json_field(dml_table.schema.field("s").type.field("j"))
        assert len(counting.sql_calls) == 1
        assert [c[0][0] for c in counting.execute_calls] == ["BEGIN", "COMMIT"]
    finally:
        session.close()


def test_duckdb_nested_uuid_json_dbapi_fallback() -> None:
    """Connections without .sql() preserve nested UUID/JSON representations."""
    duckdb = pytest.importorskip("duckdb")
    inner = duckdb.connect(":memory:")

    class _NoSqlConnection:
        def __init__(self, wrapped: Any) -> None:
            self._wrapped = wrapped

        def execute(self, *args: Any, **kwargs: Any) -> Any:
            return self._wrapped.execute(*args, **kwargs)

    session = Session(bridge=DuckDbBridge(_NoSqlConnection(inner)), dialect="duckpgq")
    u = uuid.UUID("12345678-1234-5678-1234-567812345678")
    sql = (
        f"SELECT {{'u': UUID '{u}', 'j': '{{\"a\":1}}'::JSON}} AS s, "
        f"MAP {{UUID '{u}': '[1,2]'::JSON}} AS m, "
        "['true'::JSON] AS l, "
        f"[UUID '{u}']::UUID[1] AS arr"
    )
    try:
        row = session.execute(sql).first()
        assert row == {
            "s": {"u": u, "j": {"a": 1}},
            "m": {u: [1, 2]},
            "l": [True],
            "arr": (u,),
        }
        table = session.execute_to_arrow(sql)
        _assert_uuid_field(table.schema.field("s").type.field("u"))
        _assert_json_field(table.schema.field("s").type.field("j"))
        _assert_uuid_field(table.schema.field("m").type.key_field)
        _assert_json_field(table.schema.field("m").type.item_field)
        _assert_json_field(table.schema.field("l").type.value_field)
        _assert_uuid_field(table.schema.field("arr").type.value_field)

        frame = session.execute_to_polars(sql)
        assert frame["s"].to_list()[0]["u"] == u.bytes
        assert json.loads(frame["s"].to_list()[0]["j"]) == {"a": 1}
    finally:
        session.close()
        inner.close()


@pytest.mark.asyncio
async def test_async_duckdb_nested_uuid_json() -> None:
    """AsyncDuckDbBridge preserves nested UUID and JSON across execute, Arrow, and Polars."""
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(":memory:")
    session = AsyncSession(bridge=AsyncDuckDbBridge(con), dialect="duckpgq")
    u = uuid.UUID("12345678-1234-5678-1234-567812345678")
    sql = f"SELECT {{'u': UUID '{u}', 'j': '{{\"a\":1}}'::JSON}} AS s"
    try:
        row = (await session.execute(sql)).first()
        assert row == {"s": {"u": u, "j": {"a": 1}}}
        table = await session.execute_to_arrow(sql)
        _assert_uuid_field(table.schema.field("s").type.field("u"))
        _assert_json_field(table.schema.field("s").type.field("j"))
        frame = await session.execute_to_polars(sql)
        assert frame["s"].to_list()[0]["u"] == u.bytes
        assert json.loads(frame["s"].to_list()[0]["j"]) == {"a": 1}
    finally:
        await session.close()
