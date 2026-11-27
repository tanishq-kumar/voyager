"""Shared bridge protocols, result containers, and validation helpers."""

from __future__ import annotations

import operator
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

import polars as pl

from voyager_ogm.ingestion import BulkIngestionPlan

_PAGINATION_PARAMETER_PATTERN = re.compile(
    r"\b(?:SKIP|OFFSET|LIMIT)\s+\$([A-Za-z_][A-Za-z0-9_]*)\b", re.IGNORECASE
)


def validate_pagination_parameters(statement: str, parameters: Mapping[str, Any] | None) -> None:
    """Rejects negative or non-integer values bound to pagination parameters.

    Engines disagree on a bad ``SKIP``/``OFFSET``/``LIMIT`` binding: Neo4j and
    Memgraph raise, while Apache AGE, PostgreSQL and DuckDB silently truncate a
    fractional value. Validating before dispatch gives every dialect the same
    clear failure and keeps the error independent of the target database.

    Args:
        statement: Compiled statement, before dialect-specific parameter rewriting.
        parameters: Bound parameter values.

    Raises:
        ValueError: If a parameter bound to ``SKIP``, ``OFFSET`` or ``LIMIT`` is
            not a non-negative integer.
    """
    if not parameters:
        return
    for name in sorted(set(_PAGINATION_PARAMETER_PATTERN.findall(statement))):
        if name not in parameters:
            continue
        value = parameters[name]
        try:
            # operator.index accepts every true integer type (int, NumPy and
            # Polars integers) and rejects floats, strings and Decimal.
            count = None if isinstance(value, bool) else operator.index(value)
        except TypeError:
            count = None
        if count is None or count < 0:
            raise ValueError(
                f"Pagination parameter '${name}' must be a non-negative integer, "
                f"got {type(value).__name__} {value!r}"
            )


@dataclass
class BulkExecutionResult:
    """Result summary of a bulk ingestion execution.

    Attributes:
        total_batches: Number of chunks dispatched to the database.
        total_records: Total count of records inserted or merged.
        duration_seconds: Total wall-clock execution time in seconds.
        statement: Parameterized query statement executed.
    """

    total_batches: int
    total_records: int
    duration_seconds: float
    statement: str


def _infer_record_columns(records: Sequence[Any]) -> tuple[str, ...]:
    """Extracts ordered column names from the first mapping record, if present."""
    if records and isinstance(records[0], Mapping):
        return tuple(str(key) for key in records[0].keys())
    return ()


class BridgeResult(list[dict[str, Any]]):
    """List of record dictionaries carrying driver-reported projection column names.

    Subclasses ``list[dict[str, Any]]`` so bridges preserve their existing list
    return contract while forwarding column metadata on zero-row results.
    """

    def __init__(
        self,
        records: Sequence[dict[str, Any]] = (),
        columns: Sequence[str] | None = None,
    ) -> None:
        super().__init__(records)
        if columns is not None:
            self._columns: tuple[str, ...] = tuple(str(col) for col in columns)
        else:
            self._columns = _infer_record_columns(records)

    @property
    def columns(self) -> tuple[str, ...]:
        """Ordered projection column names from the driver metadata."""
        return self._columns


@runtime_checkable
class DatabaseBridge(Protocol):
    """Synchronous Database Bridge Protocol."""

    def execute(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Executes a compiled statement and returns records as a list of dicts.

        Args:
            statement: Query statement string.
            parameters: Query parameters dictionary.

        Returns:
            List of result records.
        """
        ...

    def execute_to_polars(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> pl.DataFrame:
        """Executes a compiled statement and returns records as a Polars DataFrame.

        Args:
            statement: Query statement string.
            parameters: Query parameters dictionary.

        Returns:
            Polars DataFrame containing result records.
        """
        ...

    def execute_bulk(
        self,
        plan_or_statement: BulkIngestionPlan | str,
        batches: Sequence[dict[str, Any]] | None = None,
    ) -> BulkExecutionResult:
        """Executes a bulk ingestion plan across batches.

        Args:
            plan_or_statement: BulkIngestionPlan instance or query statement.
            batches: Batch parameter dictionaries if statement is raw string.

        Returns:
            BulkExecutionResult metrics.
        """
        ...

    def close(self) -> None:
        """Closes the underlying driver connection."""
        ...


@runtime_checkable
class AsyncDatabaseBridge(Protocol):
    """Asynchronous Database Bridge Protocol."""

    async def execute(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Executes a compiled statement and returns records as a list of dicts.

        Args:
            statement: Query statement string.
            parameters: Query parameters dictionary.

        Returns:
            List of result records.
        """
        ...

    async def execute_to_polars(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> pl.DataFrame:
        """Executes a compiled statement and returns records as a Polars DataFrame.

        Args:
            statement: Query statement string.
            parameters: Query parameters dictionary.

        Returns:
            Polars DataFrame containing result records.
        """
        ...

    async def execute_bulk(
        self,
        plan_or_statement: BulkIngestionPlan | str,
        batches: Sequence[dict[str, Any]] | None = None,
    ) -> BulkExecutionResult:
        """Executes a bulk ingestion plan across batches asynchronously.

        Args:
            plan_or_statement: BulkIngestionPlan instance or query statement.
            batches: Batch parameter dictionaries if statement is raw string.

        Returns:
            BulkExecutionResult metrics.
        """
        ...

    async def close(self) -> None:
        """Closes the underlying driver connection."""
        ...


def _validate_savepoint_name(name: str) -> str:
    """Validates an unquoted savepoint identifier before interpolating SQL.

    Args:
        name: Caller-supplied savepoint name.

    Returns:
        The validated savepoint name.

    Raises:
        ValueError: If the name is not a safe SQL identifier.
    """
    if not isinstance(name, str) or not name:
        raise ValueError("Savepoint name must be a non-empty string")
    if not (name[0].isalpha() or name[0] == "_"):
        raise ValueError(f"Invalid savepoint name {name!r}")
    if not all(ch.isalnum() or ch == "_" for ch in name):
        raise ValueError(f"Invalid savepoint name {name!r}")
    return name


_SAFE_GRAPH_NAME_REGEX = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")


def validate_graph_name(graph_name: str) -> str:
    """Validates that a graph name is a safe SQL/AGE identifier.

    Graph names in Apache AGE are interpolated into SQL queries
    (e.g. `cypher('<graph_name>', ...)`). To prevent SQL injection and syntax
    errors, graph names must be valid alphanumeric/underscore identifiers
    starting with an ASCII letter or underscore, up to 63 characters in length.

    Args:
        graph_name: The graph name to validate.

    Returns:
        The validated graph name.

    Raises:
        ValueError: If graph_name is not a valid identifier.
    """
    if not isinstance(graph_name, str) or not _SAFE_GRAPH_NAME_REGEX.match(graph_name):
        raise ValueError(
            f"Invalid graph name {graph_name!r}. Graph names must be valid alphanumeric/underscore "
            "identifiers (1-63 characters, starting with an ASCII letter or underscore)."
        )
    return graph_name


def mask_uri_password(uri: str) -> str:
    """Masks password credentials in a database URI for safe error messages and logging.

    Replaces password with '***' while retaining username, host, port, path, and query.
    For non-string or non-URI inputs, returns the input converted to string.
    """
    if not isinstance(uri, str) or "://" not in uri:
        return str(uri)
    try:
        scheme, rest = uri.split("://", 1)
        slash_idx = rest.find("/")
        query_idx = rest.find("?")
        limit = len(rest)
        if slash_idx != -1:
            limit = min(limit, slash_idx)
        if query_idx != -1:
            limit = min(limit, query_idx)

        authority = rest[:limit]
        if "@" in authority:
            userinfo, hostport = authority.rsplit("@", 1)
            if ":" in userinfo:
                user, _ = userinfo.split(":", 1)
                masked_authority = f"{user}:***@{hostport}"
            else:
                masked_authority = f"{userinfo}@{hostport}"
            return f"{scheme}://{masked_authority}{rest[limit:]}"
    except Exception:
        pass
    return uri
