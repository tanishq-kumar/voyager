"""Offline tests for real database transactions routed through a session."""

from __future__ import annotations

import asyncio
import contextvars
import gc
import operator
import threading
import weakref
from types import SimpleNamespace
from typing import Any

import polars as pl
import pytest
from voyager_ogm import (
    AsyncFalkorDBBridge,
    AsyncMockBridge,
    AsyncSession,
    BulkExecutionResult,
    FalkorDBBridge,
    Field,
    MockBridge,
    Neo4jBoltBridge,
    Node,
    Session,
)


class TxPerson(Node):
    """Identity-mapped node used by transaction rollback tests."""

    id: str = Field(primary_key=True)
    name: str = Field()
    balance: float = Field(default=0.0)


class _StrictTransaction:
    """Fake transaction that records its own statements.

    The parent bridge raises if a statement bypasses the transaction, so every
    assertion here proves Session routing rather than shared bridge logging.
    """

    def __init__(self) -> None:
        self.queries: list[tuple[str, Any]] = []
        self.state = "ACTIVE"
        self._active = True
        self.supports_savepoints = True
        self._savepoints: dict[str, int] = {}

    @property
    def is_active(self) -> bool:
        """Returns whether the fake transaction accepts statements."""
        return self._active

    def _ensure(self) -> None:
        if self.state == "FAILED":
            raise RuntimeError("transaction is aborted; roll back before executing more statements")
        if not self._active:
            raise ValueError("Cannot execute query on inactive transaction")

    def _execute(self, statement: str, parameters: dict[str, Any] | None) -> None:
        self._ensure()
        if statement == "FAIL":
            self.state = "FAILED"
            self._active = False
            raise RuntimeError("simulated statement failure")
        self.queries.append((statement, parameters))

    def execute(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Records a statement executed through the transaction."""
        self._execute(statement, parameters)
        return []

    def execute_to_polars(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> pl.DataFrame:
        """Records a statement and returns an empty DataFrame."""
        self._execute(statement, parameters)
        return pl.DataFrame()

    def execute_bulk(
        self,
        plan_or_statement: Any,
        batches: Any = None,
    ) -> BulkExecutionResult:
        """Records a bulk plan executed through the transaction."""
        self._ensure()
        if isinstance(plan_or_statement, str):
            statement = plan_or_statement
            batch_list = batches or []
            total_records = sum(len(batch.get("batch", [])) for batch in batch_list)
            self.queries.append((statement, batches))
            total_batches = len(batch_list)
        else:
            statement = plan_or_statement.statement
            total_batches = 0
            total_records = 0
            for batch_item in plan_or_statement:
                total_batches += 1
                total_records += len(batch_item.parameters.get("batch", []))
            self.queries.append((statement, plan_or_statement))
        return BulkExecutionResult(
            total_batches=total_batches,
            total_records=total_records,
            duration_seconds=0.0,
            statement=statement,
        )

    def commit(self) -> None:
        """Marks the fake transaction committed."""
        self._ensure()
        self.state = "COMMITTED"
        self._active = False

    def rollback(self) -> None:
        """Marks the fake transaction rolled back."""
        if self.state not in ("ACTIVE", "FAILED"):
            raise ValueError("Cannot rollback inactive transaction")
        self.state = "ROLLED_BACK"
        self._active = False

    def savepoint(self, name: str) -> None:
        """Creates a fake savepoint."""
        self._ensure()
        self._savepoints[name] = len(self.queries)

    def rollback_to_savepoint(self, name: str) -> None:
        """Rolls back the fake query log."""
        self._ensure()
        del self.queries[self._savepoints[name] :]

    def release_savepoint(self, name: str) -> None:
        """Releases a fake savepoint."""
        self._ensure()
        self._savepoints.pop(name, None)


class _StrictBridge(MockBridge):
    """MockBridge that raises if the session bypasses its transaction."""

    def __init__(self) -> None:
        super().__init__()
        self.transaction: _StrictTransaction | None = None

    def begin_transaction(self) -> _StrictTransaction:
        """Opens a recording fake transaction."""
        self.transaction = _StrictTransaction()
        return self.transaction

    def execute(self, statement: str, parameters: dict[str, Any] | None = None) -> Any:
        raise AssertionError("session bypassed the active transaction (execute)")

    def execute_to_polars(self, statement: str, parameters: dict[str, Any] | None = None) -> Any:
        raise AssertionError("session bypassed the active transaction (execute_to_polars)")

    def execute_bulk(self, plan_or_statement: Any, batches: Any = None) -> Any:
        raise AssertionError("session bypassed the active transaction (execute_bulk)")


def _strict_session(enable_identity_map: bool = False) -> tuple[Session, _StrictBridge]:
    """Builds a session whose bridge refuses direct execution."""
    bridge = _StrictBridge()
    return Session(bridge=bridge, dialect="cypher", enable_identity_map=enable_identity_map), bridge


def _assert_no_active(session: Session | AsyncSession) -> None:
    """Asserts the session cleared its active transaction pointer."""
    assert session._active_transaction is None


def test_transaction_commit_and_every_execution_path_uses_it():
    session, bridge = _strict_session()

    with session.transaction() as tx:
        assert tx.is_active
        assert tx.state == "ACTIVE"
        assert tx.backend == "bridge"
        assert session._active_transaction is tx

        tx.execute("CREATE (:TxUser {name: 'Alice'})")
        tx.execute_to_polars("MATCH (n:TxUser) RETURN n.name AS name")
        session.execute_to_arrow("MATCH (n:TxUser) RETURN n.name AS name")

        plan = session.bulk_create(TxPerson, [{"id": "p1", "name": "Alice"}], batch_size=1)
        assert session.run_bulk(plan).total_records == 1

    assert tx.state == "COMMITTED"
    assert not tx.is_active
    _assert_no_active(session)
    assert bridge.transaction is not None
    statements = [statement for statement, _ in bridge.transaction.queries]
    assert "CREATE (:TxUser {name: 'Alice'})" in statements
    assert any("UNWIND" in statement.upper() for statement in statements)


def test_transaction_exception_rolls_back_and_reraises():
    session, bridge = _strict_session()
    tx_ref = None

    with pytest.raises(RuntimeError, match="boom"):
        with session.transaction() as tx:
            tx_ref = tx
            session.execute("CREATE (:TxUser {name: 'Alice'})")
            raise RuntimeError("boom")

    assert tx_ref is not None
    assert tx_ref.state == "ROLLED_BACK"
    assert not tx_ref.is_active
    _assert_no_active(session)
    assert bridge.transaction is not None
    assert [statement for statement, _ in bridge.transaction.queries] == [
        "CREATE (:TxUser {name: 'Alice'})"
    ]


def test_transaction_explicit_commit_and_rollback():
    session, _ = _strict_session()

    tx = session.transaction()
    session.execute("CREATE (:TxUser)")
    tx.commit()
    assert tx.state == "COMMITTED"
    _assert_no_active(session)

    tx = session.transaction()
    session.execute("CREATE (:TxUser)")
    tx.rollback()
    assert tx.state == "ROLLED_BACK"
    _assert_no_active(session)


def test_transaction_fails_closed_after_statement_error():
    session, _ = _strict_session()
    tx_ref = None

    with pytest.raises(RuntimeError, match="aborted"):
        with session.transaction() as tx:
            tx_ref = tx
            session.execute("CREATE (:TxUser)")
            with pytest.raises(RuntimeError, match="simulated statement failure"):
                session.execute("FAIL")
            assert tx.state == "FAILED"
            session.execute("CREATE (:TxUser)")

    assert tx_ref is not None
    assert tx_ref.state == "ROLLED_BACK"
    _assert_no_active(session)


def test_transaction_failed_statement_then_clean_exit_commit_raises():
    session, _ = _strict_session()
    tx_ref = None

    with pytest.raises(RuntimeError, match="aborted"):
        with session.transaction() as tx:
            tx_ref = tx
            session.execute("CREATE (:TxUser)")
            with pytest.raises(RuntimeError, match="simulated statement failure"):
                session.execute("FAIL")

    assert tx_ref is not None
    assert tx_ref.state == "ROLLED_BACK"
    _assert_no_active(session)


def test_transaction_original_exception_surfaces_not_rollback_error():
    session, _ = _strict_session()
    tx_ref = None

    with pytest.raises(RuntimeError, match="simulated statement failure"):
        with session.transaction() as tx:
            tx_ref = tx
            session.execute("CREATE (:TxUser)")
            session.execute("FAIL")

    assert tx_ref is not None
    assert tx_ref.state == "ROLLED_BACK"
    _assert_no_active(session)


def test_nested_transaction_begin_raises():
    session, _ = _strict_session()

    with session.transaction():
        with pytest.raises(RuntimeError, match="already active"):
            session.transaction()


def test_bolt_transaction_savepoints_raise():
    class FakeBoltTransaction:
        def __init__(self) -> None:
            self.committed = False

        def run(self, statement: str, parameters: dict[str, Any]) -> list[Any]:
            return []

        def commit(self) -> None:
            self.committed = True

        def rollback(self) -> None:
            pass

    class FakeBoltSession:
        def __init__(self) -> None:
            self.tx = FakeBoltTransaction()

        def begin_transaction(self) -> FakeBoltTransaction:
            return self.tx

        def close(self) -> None:
            pass

    class FakeDriver:
        def __init__(self) -> None:
            self.sessions: list[FakeBoltSession] = []

        def session(self, **kwargs: Any) -> FakeBoltSession:
            session = FakeBoltSession()
            self.sessions.append(session)
            return session

    driver = FakeDriver()
    session = Session(bridge=Neo4jBoltBridge(driver), dialect="cypher")

    with session.transaction() as tx:
        assert not tx.supports_savepoints
        with pytest.raises(RuntimeError, match="savepoint"):
            tx.savepoint("sp_bolt")

    assert driver.sessions[0].tx.committed is True


def test_falkordb_transaction_raises():
    class FakeGraph:
        def query(self, statement: str, parameters: dict[str, Any]) -> Any:
            return SimpleNamespace(header=[], result_set=[])

    session = Session(bridge=FalkorDBBridge(FakeGraph()), dialect="cypher")
    with pytest.raises(RuntimeError, match="FalkorDB does not support transactions"):
        session.transaction()


def test_transaction_rollback_restores_identity_map_entities():
    session, _ = _strict_session(enable_identity_map=True)
    person = TxPerson(id="p1", name="Alice", balance=100.0)
    session.register(person, is_clean=True)

    with pytest.raises(RuntimeError, match="boom"):
        with session.transaction():
            person.name = "Bob"
            person.balance = 999.0
            raise RuntimeError("boom")

    assert person.get("name") == "Alice"
    assert person.get("balance") == 100.0
    assert person.dirty_fields == {}


def test_transaction_commit_pairs_with_unit_of_work():
    session, _ = _strict_session(enable_identity_map=True)
    person = TxPerson(id="p1", name="Alice", balance=100.0)
    session.register(person, is_clean=True)

    with session.transaction() as tx:
        person.name = "Bob"
        tx.commit()

    assert tx.state == "COMMITTED"
    assert person.get("name") == "Bob"
    assert person.dirty_fields == {"name": "Bob"}


def test_transaction_flush_uses_active_transaction():
    session, bridge = _strict_session(enable_identity_map=True)
    person = TxPerson(id="p1", name="Alice", balance=100.0)
    session.register(person, is_clean=True)
    person.name = "Bob"

    with session.transaction():
        results = session.flush()

    assert len(results) == 1
    assert results[0].total_records == 1
    assert person.dirty_fields == {}
    assert bridge.transaction is not None
    assert any("MERGE" in statement.upper() for statement, _ in bridge.transaction.queries)


class _FakeNativeResult:
    stream = None
    summary = None


class _FakeNativeTransaction:
    def __init__(self) -> None:
        self.queries: list[tuple[str, dict[str, Any] | None]] = []
        self.state = "ACTIVE"
        self.is_active = True

    def execute_sync(self, statement: str, parameters: dict[str, Any] | None) -> Any:
        if self.state == "FAILED":
            raise RuntimeError("transaction is aborted; roll back before executing more statements")
        self.queries.append((statement, parameters))
        if statement == "FAIL":
            self.state = "FAILED"
            self.is_active = False
            raise RuntimeError("simulated native statement failure")
        return _FakeNativeResult()

    def commit(self) -> None:
        if self.state == "FAILED":
            raise RuntimeError("transaction is aborted; roll back instead of committing")
        self.state = "COMMITTED"
        self.is_active = False

    def commit_sync(self) -> None:
        self.commit()

    def rollback(self) -> None:
        self.state = "ROLLED_BACK"
        self.is_active = False

    def rollback_sync(self) -> None:
        self.rollback()


class _FakeNativeClient:
    def __init__(self) -> None:
        self.transactions: list[_FakeNativeTransaction] = []

    def execute_sync(self, statement: str, parameters: dict[str, Any] | None) -> Any:
        raise AssertionError("outside transaction should not be used")

    def begin_transaction_sync(self) -> _FakeNativeTransaction:
        tx = _FakeNativeTransaction()
        self.transactions.append(tx)
        return tx

    def close(self) -> None:
        pass


def test_native_transaction_route_is_chosen_once_and_used_for_statements():
    native = _FakeNativeClient()
    session = Session(bridge=native, dialect="cypher", backend="native")

    with session.transaction() as tx:
        assert tx.backend == "native"
        session.execute("CREATE (:TxUser)")
        session.execute("MATCH (n) RETURN n")

    assert len(native.transactions) == 1
    assert [query for query, _ in native.transactions[0].queries] == [
        "CREATE (:TxUser)",
        "MATCH (n) RETURN n",
    ]
    assert tx.state == "COMMITTED"


def test_native_session_ping_routes_through_active_transaction():
    native = _FakeNativeClient()
    session = Session(bridge=native, dialect="cypher", backend="native", pool_size=1)

    with session.transaction() as tx:
        assert session.ping() is True

    assert native.transactions[0].queries == [("RETURN 1", {})]
    assert tx.state == "COMMITTED"


def test_public_savepoint_context_is_the_in_memory_one():
    import voyager_ogm

    assert voyager_ogm.SavepointContext is not voyager_ogm.TransactionSavepoint
    assert voyager_ogm.SavepointContext.__module__ == "voyager_ogm.unit_of_work"
    assert voyager_ogm.TransactionSavepoint.__module__ == "voyager_ogm.transaction"


class _StrictAsyncTransaction:
    """Async fake transaction that records its own statements."""

    def __init__(self) -> None:
        self.queries: list[tuple[str, Any]] = []
        self.state = "ACTIVE"
        self._active = True
        self.supports_savepoints = True
        self._savepoints: dict[str, int] = {}

    @property
    def is_active(self) -> bool:
        """Returns whether the fake transaction accepts statements."""
        return self._active

    def _ensure(self) -> None:
        if self.state == "FAILED":
            raise RuntimeError("transaction is aborted; roll back before executing more statements")
        if not self._active:
            raise ValueError("Cannot execute query on inactive transaction")

    async def _execute(self, statement: str, parameters: dict[str, Any] | None) -> None:
        self._ensure()
        if statement == "FAIL":
            self.state = "FAILED"
            self._active = False
            raise RuntimeError("simulated statement failure")
        self.queries.append((statement, parameters))

    async def execute(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Records an async statement executed through the transaction."""
        await self._execute(statement, parameters)
        return []

    async def execute_to_polars(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> pl.DataFrame:
        """Records an async statement and returns an empty DataFrame."""
        await self._execute(statement, parameters)
        return pl.DataFrame()

    async def execute_bulk(self, plan_or_statement: Any, batches: Any = None) -> Any:
        """Records an async bulk plan executed through the transaction."""
        self._ensure()
        if isinstance(plan_or_statement, str):
            statement = plan_or_statement
            batch_list = batches or []
            total_records = sum(len(batch.get("batch", [])) for batch in batch_list)
            self.queries.append((statement, batches))
            total_batches = len(batch_list)
        else:
            statement = plan_or_statement.statement
            total_batches = 0
            total_records = 0
            for batch_item in plan_or_statement:
                total_batches += 1
                total_records += len(batch_item.parameters.get("batch", []))
            self.queries.append((statement, plan_or_statement))
        return BulkExecutionResult(
            total_batches=total_batches,
            total_records=total_records,
            duration_seconds=0.0,
            statement=statement,
        )

    async def commit(self) -> None:
        """Marks the fake async transaction committed."""
        self._ensure()
        self.state = "COMMITTED"
        self._active = False

    async def rollback(self) -> None:
        """Marks the fake async transaction rolled back."""
        if self.state not in ("ACTIVE", "FAILED"):
            raise ValueError("Cannot rollback inactive transaction")
        self.state = "ROLLED_BACK"
        self._active = False

    async def savepoint(self, name: str) -> None:
        """Creates a fake async savepoint."""
        self._ensure()
        self._savepoints[name] = len(self.queries)

    async def rollback_to_savepoint(self, name: str) -> None:
        """Rolls back the fake async query log."""
        self._ensure()
        del self.queries[self._savepoints[name] :]

    async def release_savepoint(self, name: str) -> None:
        """Releases a fake async savepoint."""
        self._ensure()
        self._savepoints.pop(name, None)


class _StrictAsyncBridge(AsyncMockBridge):
    """AsyncMockBridge that raises if the session bypasses its transaction."""

    def __init__(self) -> None:
        super().__init__()
        self.transaction: _StrictAsyncTransaction | None = None

    async def begin_transaction(self) -> _StrictAsyncTransaction:
        """Opens a recording fake async transaction."""
        self.transaction = _StrictAsyncTransaction()
        return self.transaction

    async def execute(self, statement: str, parameters: dict[str, Any] | None = None) -> Any:
        raise AssertionError("session bypassed the active transaction (execute)")

    async def execute_to_polars(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> Any:
        raise AssertionError("session bypassed the active transaction (execute_to_polars)")

    async def execute_bulk(self, plan_or_statement: Any, batches: Any = None) -> Any:
        raise AssertionError("session bypassed the active transaction (execute_bulk)")


@pytest.mark.asyncio
async def test_async_transaction_commit_rollback_and_nested_raise():
    bridge = _StrictAsyncBridge()
    session = AsyncSession(bridge=bridge, dialect="cypher")

    async with session.transaction() as tx:
        assert tx.is_active
        await session.execute("CREATE (:TxUser {name: 'Alice'})")
        with pytest.raises(RuntimeError, match="already active"):
            session.transaction()

    assert tx.state == "COMMITTED"
    assert bridge.transaction is not None
    assert [statement for statement, _ in bridge.transaction.queries] == [
        "CREATE (:TxUser {name: 'Alice'})"
    ]
    _assert_no_active(session)

    with pytest.raises(RuntimeError, match="boom"):
        async with session.transaction() as tx:
            await session.execute("CREATE (:TxUser)")
            raise RuntimeError("boom")
    assert tx.state == "ROLLED_BACK"
    _assert_no_active(session)


@pytest.mark.asyncio
async def test_async_transaction_polars_bulk_and_identity_map_rollback():
    bridge = _StrictAsyncBridge()
    session = AsyncSession(bridge=bridge, dialect="cypher", enable_identity_map=True)
    person = TxPerson(id="p1", name="Alice", balance=100.0)
    session.register(person, is_clean=True)

    with pytest.raises(RuntimeError, match="boom"):
        async with session.transaction():
            person.name = "Bob"
            await session.execute_to_polars("MATCH (n) RETURN n")
            plan = session.bulk_create(TxPerson, [{"id": "p2", "name": "Bob"}], batch_size=1)
            assert (await session.run_bulk(plan)).total_records == 1
            raise RuntimeError("boom")

    assert person.get("name") == "Alice"
    assert bridge.transaction is not None
    assert any("UNWIND" in statement.upper() for statement, _ in bridge.transaction.queries)


@pytest.mark.asyncio
async def test_async_transaction_fails_closed_after_statement_error():
    bridge = _StrictAsyncBridge()
    session = AsyncSession(bridge=bridge, dialect="cypher")
    tx_ref = None

    with pytest.raises(RuntimeError, match="aborted"):
        async with session.transaction() as tx:
            tx_ref = tx
            await session.execute("CREATE (:TxUser)")
            with pytest.raises(RuntimeError, match="simulated statement failure"):
                await session.execute("FAIL")
            assert tx.state == "FAILED"
            await session.execute("CREATE (:TxUser)")

    assert tx_ref is not None
    assert tx_ref.state == "ROLLED_BACK"
    _assert_no_active(session)


@pytest.mark.asyncio
async def test_async_cancellation_marks_failed_and_clears_session():
    started = asyncio.Event()

    class CancellableTransaction:
        state = "ACTIVE"
        is_active = True
        supports_savepoints = False

        async def execute(self, statement: str, parameters: dict[str, Any] | None) -> Any:
            started.set()
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                self.state = "FAILED"
                self.is_active = False
                raise
            return _FakeNativeResult()

        async def commit(self) -> None:
            self.state = "COMMITTED"
            self.is_active = False

        async def rollback(self) -> None:
            self.state = "ROLLED_BACK"
            self.is_active = False

    class CancellableClient:
        async def execute(self, statement: str, parameters: dict[str, Any] | None) -> Any:
            raise AssertionError("outside transaction should not be used")

        def execute_sync(self, statement: str, parameters: dict[str, Any] | None) -> Any:
            raise AssertionError("outside transaction should not be used")

        async def begin_transaction(self) -> CancellableTransaction:
            return CancellableTransaction()

        def close(self) -> None:
            pass

    session = AsyncSession(bridge=CancellableClient(), dialect="cypher", backend="native")
    tx = session.transaction()
    await tx.begin()

    task = asyncio.create_task(session.execute("CREATE (:TxUser)"))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert tx.state == "FAILED"
    _assert_no_active(session)


@pytest.mark.asyncio
async def test_async_falkordb_transaction_raises():
    class FakeGraph:
        def query(self, statement: str, parameters: dict[str, Any]) -> Any:
            return SimpleNamespace(header=[], result_set=[])

    session = AsyncSession(bridge=AsyncFalkorDBBridge(FakeGraph()), dialect="cypher")
    with pytest.raises(RuntimeError, match="FalkorDB does not support transactions"):
        async with session.transaction():
            pass


@pytest.mark.asyncio
async def test_async_native_transaction_route():
    class FakeAsyncNativeTransaction(_FakeNativeTransaction):
        async def execute(self, statement: str, parameters: dict[str, Any] | None) -> Any:
            self.queries.append((statement, parameters))
            return _FakeNativeResult()

        async def commit(self) -> None:
            self.state = "COMMITTED"
            self.is_active = False

        async def rollback(self) -> None:
            self.state = "ROLLED_BACK"
            self.is_active = False

    class FakeAsyncNativeClient:
        def __init__(self) -> None:
            self.transactions: list[FakeAsyncNativeTransaction] = []

        async def execute(self, statement: str, parameters: dict[str, Any] | None) -> Any:
            raise AssertionError("outside transaction should not be used")

        def execute_sync(self, statement: str, parameters: dict[str, Any] | None) -> Any:
            raise AssertionError("outside transaction should not be used")

        async def begin_transaction(self) -> FakeAsyncNativeTransaction:
            tx = FakeAsyncNativeTransaction()
            self.transactions.append(tx)
            return tx

        def close(self) -> None:
            pass

    native = FakeAsyncNativeClient()
    session = AsyncSession(bridge=native, dialect="cypher", backend="native")

    async with session.transaction() as tx:
        assert tx.backend == "native"
        await session.execute("CREATE (:TxUser)")
        await session.execute("MATCH (n) RETURN n")

    assert len(native.transactions) == 1
    assert [query for query, _ in native.transactions[0].queries] == [
        "CREATE (:TxUser)",
        "MATCH (n) RETURN n",
    ]
    assert tx.state == "COMMITTED"


class _ConcurrentNativeTransaction:
    """Fake native transaction that rejects overlapping calls and tracks concurrency."""

    def __init__(self, tx_id: int) -> None:
        self.tx_id = tx_id
        self.queries: list[tuple[str, dict[str, Any] | None]] = []
        self.state = "ACTIVE"
        self.is_active = True
        self.supports_savepoints = False
        self._in_flight = False
        self.max_concurrent = 0
        self._active_count = 0

    async def execute(self, statement: str, parameters: dict[str, Any] | None = None) -> Any:
        if self._in_flight:
            raise RuntimeError("Transaction is not active; overlapping execute detected")
        if self.state != "ACTIVE":
            raise RuntimeError("Transaction is not active")
        self._in_flight = True
        self._active_count += 1
        self.max_concurrent = max(self.max_concurrent, self._active_count)
        try:
            await asyncio.sleep(0.01)
            self.queries.append((statement, parameters))
            return _FakeNativeResult()
        finally:
            self._active_count -= 1
            self._in_flight = False

    async def commit(self) -> None:
        if self._in_flight:
            raise RuntimeError("Transaction is not active; overlapping commit detected")
        self.state = "COMMITTED"
        self.is_active = False

    async def rollback(self) -> None:
        self.state = "ROLLED_BACK"
        self.is_active = False


class _ConcurrentNativeClient:
    """Fake native client supporting independent concurrent transactions and autocommit."""

    def __init__(self) -> None:
        self.autocommit_queries: list[tuple[str, dict[str, Any] | None]] = []
        self.transactions: list[_ConcurrentNativeTransaction] = []

    async def execute(self, statement: str, parameters: dict[str, Any] | None = None) -> Any:
        self.autocommit_queries.append((statement, parameters))
        return _FakeNativeResult()

    def execute_sync(self, statement: str, parameters: dict[str, Any] | None = None) -> Any:
        self.autocommit_queries.append((statement, parameters))
        return _FakeNativeResult()

    async def begin_transaction(self) -> _ConcurrentNativeTransaction:
        tx = _ConcurrentNativeTransaction(len(self.transactions))
        self.transactions.append(tx)
        return tx

    def close(self) -> None:
        pass


@pytest.mark.asyncio
async def test_async_task2_execute_does_not_join_task1_transaction():
    native = _ConcurrentNativeClient()
    session = AsyncSession(bridge=native, dialect="cypher", backend="native")
    tx1_open = asyncio.Event()
    task2_done = asyncio.Event()

    async def task1() -> None:
        with pytest.raises(RuntimeError, match="rollback tx1"):
            async with session.transaction() as tx1:
                assert session._active_transaction is tx1
                await session.execute("IN_TX1")
                tx1_open.set()
                await task2_done.wait()
                raise RuntimeError("rollback tx1")

    async def task2() -> None:
        await tx1_open.wait()
        assert session._active_transaction is None
        await session.execute("OUTSIDE_TX")
        task2_done.set()

    await asyncio.gather(task1(), task2())

    assert len(native.transactions) == 1
    assert native.transactions[0].state == "ROLLED_BACK"
    assert [q for q, _ in native.transactions[0].queries] == ["IN_TX1"]
    assert [q for q, _ in native.autocommit_queries] == ["OUTSIDE_TX"]
    _assert_no_active(session)


@pytest.mark.asyncio
async def test_async_concurrent_tasks_each_commit_own_transaction():
    native = _ConcurrentNativeClient()
    session = AsyncSession(bridge=native, dialect="cypher", backend="native")
    task_count = 8
    barrier_count = 0
    all_open = asyncio.Event()

    async def worker(idx: int) -> str:
        nonlocal barrier_count
        async with session.transaction() as tx:
            assert session._active_transaction is tx
            barrier_count += 1
            if barrier_count == task_count:
                all_open.set()
            await all_open.wait()
            await session.execute(f"TASK_{idx}_STMT_1")
            await asyncio.sleep(0)
            await session.execute(f"TASK_{idx}_STMT_2")
        return tx.state

    states = await asyncio.gather(*(worker(i) for i in range(task_count)))

    assert all(state == "COMMITTED" for state in states)
    assert len(native.transactions) == task_count
    assert all(tx.state == "COMMITTED" for tx in native.transactions)
    recorded_pairs = {tuple(q for q, _ in tx.queries) for tx in native.transactions}
    assert recorded_pairs == {(f"TASK_{i}_STMT_1", f"TASK_{i}_STMT_2") for i in range(task_count)}
    assert native.autocommit_queries == []
    _assert_no_active(session)


@pytest.mark.asyncio
async def test_async_concurrent_statements_on_one_transaction_are_serialized():
    native = _ConcurrentNativeClient()
    session = AsyncSession(bridge=native, dialect="cypher", backend="native")

    async with session.transaction() as tx:
        await asyncio.gather(
            session.execute("Q1"),
            session.execute("Q2"),
            tx.execute("Q3"),
            tx.execute("Q4"),
            session.execute("Q5"),
            tx.execute("Q6"),
        )

    assert len(native.transactions) == 1
    driver_tx = native.transactions[0]
    assert driver_tx.state == "COMMITTED"
    assert driver_tx.max_concurrent == 1
    assert sorted(q for q, _ in driver_tx.queries) == ["Q1", "Q2", "Q3", "Q4", "Q5", "Q6"]
    _assert_no_active(session)


def test_sync_threads_do_not_hijack_shared_session_transaction():
    class _SyncThreadTx:
        def __init__(self) -> None:
            self.queries: list[str] = []
            self.state = "ACTIVE"
            self.is_active = True
            self.supports_savepoints = False

        def execute_sync(self, statement: str, parameters: dict[str, Any] | None = None) -> Any:
            self.queries.append(statement)
            return _FakeNativeResult()

        def commit_sync(self) -> None:
            self.state = "COMMITTED"
            self.is_active = False

        def rollback_sync(self) -> None:
            self.state = "ROLLED_BACK"
            self.is_active = False

    class _SyncThreadClient:
        def __init__(self) -> None:
            self.lock = threading.Lock()
            self.autocommit_queries: list[str] = []
            self.transactions: list[_SyncThreadTx] = []

        def execute_sync(self, statement: str, parameters: dict[str, Any] | None = None) -> Any:
            with self.lock:
                self.autocommit_queries.append(statement)
            return _FakeNativeResult()

        def begin_transaction_sync(self) -> _SyncThreadTx:
            tx = _SyncThreadTx()
            with self.lock:
                self.transactions.append(tx)
            return tx

        def close(self) -> None:
            pass

    native = _SyncThreadClient()
    session = Session(bridge=native, dialect="cypher", backend="native")
    tx1_open = threading.Event()
    t2_done = threading.Event()
    errors: list[BaseException] = []

    def thread1() -> None:
        try:
            with session.transaction():
                session.execute("T1_Q1")
                tx1_open.set()
                assert t2_done.wait(timeout=5)
                session.execute("T1_Q2")
        except BaseException as exc:
            errors.append(exc)

    def thread2() -> None:
        try:
            assert tx1_open.wait(timeout=5)
            assert session._active_transaction is None
            session.execute("T2_AUTOCOMMIT")
            with session.transaction():
                session.execute("T2_TX")
        except BaseException as exc:
            errors.append(exc)
        finally:
            t2_done.set()

    t1 = threading.Thread(target=thread1)
    t2 = threading.Thread(target=thread2)
    t1.start()
    t2.start()
    t1.join(timeout=5)
    t2.join(timeout=5)

    assert not errors
    assert native.autocommit_queries == ["T2_AUTOCOMMIT"]
    assert len(native.transactions) == 2
    assert all(tx.state == "COMMITTED" for tx in native.transactions)
    assert {tuple(tx.queries) for tx in native.transactions} == {
        ("T1_Q1", "T1_Q2"),
        ("T2_TX",),
    }
    _assert_no_active(session)


def test_sync_1000_sessions_do_not_leak_contextvars_or_transactions():
    from voyager_ogm.session import _ACTIVE_TRANSACTIONS

    before_ctx_len = len(contextvars.copy_context())
    session_refs: list[weakref.ref[Session]] = []
    tx_refs: list[weakref.ref[Any]] = []

    def run_once(idx: int) -> None:
        session = Session(bridge=MockBridge(), dialect="cypher")
        session_refs.append(weakref.ref(session))
        if idx % 3 == 0:
            try:
                with session.transaction() as tx:
                    tx_refs.append(weakref.ref(tx))
                    session.execute("RETURN 1")
                    raise RuntimeError("boom")
            except RuntimeError:
                pass
        else:
            with session.transaction() as tx:
                tx_refs.append(weakref.ref(tx))
                session.execute("RETURN 1")

    for i in range(1000):
        run_once(i)

    gc.collect()

    assert all(ref() is None for ref in session_refs)
    assert all(ref() is None for ref in tx_refs)
    assert _ACTIVE_TRANSACTIONS.get() == {}
    assert len(contextvars.copy_context()) <= before_ctx_len + 1


@pytest.mark.asyncio
async def test_async_1000_sessions_do_not_leak_contextvars_or_transactions():
    from voyager_ogm.session import _ACTIVE_TRANSACTIONS

    before_ctx_len = len(contextvars.copy_context())
    session_refs: list[weakref.ref[AsyncSession]] = []
    tx_refs: list[weakref.ref[Any]] = []

    async def run_once(idx: int) -> None:
        session = AsyncSession(bridge=AsyncMockBridge(), dialect="cypher")
        session_refs.append(weakref.ref(session))
        if idx % 3 == 0:
            try:
                async with session.transaction() as tx:
                    tx_refs.append(weakref.ref(tx))
                    await session.execute("RETURN 1")
                    raise RuntimeError("boom")
            except RuntimeError:
                pass
        else:
            async with session.transaction() as tx:
                tx_refs.append(weakref.ref(tx))
                await session.execute("RETURN 1")

    for i in range(1000):
        await run_once(i)

    gc.collect()

    assert all(ref() is None for ref in session_refs)
    assert all(ref() is None for ref in tx_refs)
    assert _ACTIVE_TRANSACTIONS.get() == {}
    assert len(contextvars.copy_context()) <= before_ctx_len + 1


@pytest.mark.asyncio
async def test_active_transactions_copy_on_write_and_clear_on_cancel_and_error():
    from voyager_ogm.session import _ACTIVE_TRANSACTIONS

    session1 = AsyncSession(bridge=AsyncMockBridge(), dialect="cypher")
    session2 = AsyncSession(bridge=AsyncMockBridge(), dialect="cypher")

    async with session1.transaction() as tx1:
        parent_map = _ACTIVE_TRANSACTIONS.get()
        assert parent_map[session1._tx_token] is tx1
        with pytest.raises(TypeError):
            operator.setitem(parent_map, 999999, None)

        async def child_worker() -> None:
            async with session2.transaction() as tx2:
                child_map = _ACTIVE_TRANSACTIONS.get()
                assert child_map[session1._tx_token] is tx1
                assert child_map[session2._tx_token] is tx2
                assert session2._tx_token not in parent_map
            assert session2._tx_token not in _ACTIVE_TRANSACTIONS.get()

        await asyncio.create_task(child_worker())
        assert _ACTIVE_TRANSACTIONS.get() is parent_map

    assert _ACTIVE_TRANSACTIONS.get() == {}

    started = asyncio.Event()

    async def cancelled_owner() -> None:
        async with session1.transaction():
            assert session1._tx_token in _ACTIVE_TRANSACTIONS.get()
            started.set()
            await asyncio.sleep(10)

    owner_task = asyncio.create_task(cancelled_owner())
    await started.wait()
    owner_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await owner_task

    assert _ACTIVE_TRANSACTIONS.get() == {}
    _assert_no_active(session1)
    _assert_no_active(session2)
