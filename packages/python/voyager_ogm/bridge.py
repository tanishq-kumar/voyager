"""Compatibility shim over the modular :mod:`voyager_ogm.bridges` package.

The bridge layer used to be one large module. It now lives in
``voyager_ogm.bridges`` (one module per engine plus shared base classes); this
module re-exports every public name so existing imports keep working.
"""

from __future__ import annotations

from voyager_ogm.bridges import (
    _is_postgres_conn,
    _strip_uri_namespace_prefix,
    create_bridge,
    register_bridge,
)
from voyager_ogm.bridges._duckdb_arrow import _duckdb_timetz_text_iso
from voyager_ogm.bridges.base import (
    AsyncDatabaseBridge,
    BridgeResult,
    BulkExecutionResult,
    DatabaseBridge,
    _infer_record_columns,
    mask_uri_password,
    validate_graph_name,
    validate_pagination_parameters,
)
from voyager_ogm.bridges.duckdb import (
    AsyncDuckDbBridge,
    AsyncDuckDbBridgeTransaction,
    DuckDbBridge,
    DuckDbBridgeTransaction,
    _encode_duckdb_literal,
)
from voyager_ogm.bridges.falkordb import (
    AsyncFalkorDBBridge,
    FalkorDBBridge,
    _adapt_falkordb_parameters,
    _adapt_falkordb_value,
    _strip_falkordb_utc,
)
from voyager_ogm.bridges.mock import (
    AsyncMockBridge,
    AsyncMockTransaction,
    MockBridge,
    MockTransaction,
)
from voyager_ogm.bridges.neo4j import (
    AsyncNeo4jBoltBridge,
    AsyncNeo4jBoltTransaction,
    Neo4jBoltBridge,
    Neo4jBoltTransaction,
    _adapt_bolt_parameters,
    _adapt_bolt_value,
)
from voyager_ogm.bridges.postgres import (
    AsyncPostgresBridge,
    AsyncPostgresBridgeTransaction,
    PostgresBridge,
    PostgresBridgeTransaction,
    _configure_pg_adapters,
    setup_postgres_age,
)
from voyager_ogm.bridges.relate import (
    AsyncRelateBridge,
    AsyncRelateBridgeTransaction,
    RelateBridge,
    RelateBridgeTransaction,
)
from voyager_ogm.codecs import records_to_polars as _records_to_polars

__all__ = [
    "AsyncDatabaseBridge",
    "AsyncDuckDbBridge",
    "AsyncDuckDbBridgeTransaction",
    "AsyncFalkorDBBridge",
    "AsyncMockBridge",
    "AsyncMockTransaction",
    "AsyncNeo4jBoltBridge",
    "AsyncNeo4jBoltTransaction",
    "AsyncPostgresBridge",
    "AsyncPostgresBridgeTransaction",
    "AsyncRelateBridge",
    "AsyncRelateBridgeTransaction",
    "BridgeResult",
    "BulkExecutionResult",
    "DatabaseBridge",
    "DuckDbBridge",
    "DuckDbBridgeTransaction",
    "FalkorDBBridge",
    "MockBridge",
    "MockTransaction",
    "Neo4jBoltBridge",
    "Neo4jBoltTransaction",
    "PostgresBridge",
    "PostgresBridgeTransaction",
    "RelateBridge",
    "RelateBridgeTransaction",
    "_adapt_bolt_parameters",
    "_adapt_bolt_value",
    "_adapt_falkordb_parameters",
    "_adapt_falkordb_value",
    "_configure_pg_adapters",
    "_duckdb_timetz_text_iso",
    "_encode_duckdb_literal",
    "_infer_record_columns",
    "_is_postgres_conn",
    "_records_to_polars",
    "_strip_falkordb_utc",
    "_strip_uri_namespace_prefix",
    "create_bridge",
    "mask_uri_password",
    "register_bridge",
    "setup_postgres_age",
    "validate_graph_name",
    "validate_pagination_parameters",
]
