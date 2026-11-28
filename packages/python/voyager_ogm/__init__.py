"""Voyager OGM: Multi-Dialect, Vendor-Neutral Object-Graph Mapper."""

from typing import TYPE_CHECKING, Any

from voyager_ogm import expressions, fn, types
from voyager_ogm._voyager_rs import (
    QUERY_SPEC_REVISION,
    QUERY_SPEC_VERSION,
    ArrowStream,
    AstExpr,
    NativeCircuitRouter,
    NativeClient,
    NativeQueryBuilder,
    NativeQueryResult,
    NativeSchemaRegistry,
    VoyagerError,
    compile_query_from_spec,
    extract_topology_from_query,
    generate_synthetic_stream,
    version,
)
from voyager_ogm.analytics import to_networkx
from voyager_ogm.bridge import (
    AsyncDatabaseBridge,
    AsyncDuckDbBridge,
    AsyncFalkorDBBridge,
    AsyncMockBridge,
    AsyncNeo4jBoltBridge,
    AsyncPostgresBridge,
    AsyncRelateBridge,
    BulkExecutionResult,
    DatabaseBridge,
    DuckDbBridge,
    FalkorDBBridge,
    MockBridge,
    Neo4jBoltBridge,
    PostgresBridge,
    RelateBridge,
    create_bridge,
    register_bridge,
    validate_graph_name,
)
from voyager_ogm.config import VoyagerConfig, configure, get_config, reset_config
from voyager_ogm.entities import GraphNode, GraphPath, GraphRelationship, json_default
from voyager_ogm.expressions import (
    AliasedExpr,
    BinaryExpr,
    CaseBuilder,
    CaseExpr,
    Expression,
    FunctionExpr,
    IdentExpr,
    LabelPredicateExpr,
    ListCompExpr,
    ListLiteralExpr,
    LiteralExpr,
    ParamExpr,
    PatternCompExpr,
    PropExpr,
    SubqueryExpr,
    case,
    col,
    has_label,
    ident,
    list_comprehension,
    lit,
    param,
    pattern_comprehension,
    prop,
    to_expression,
)
from voyager_ogm.hybrid import AsyncHybridSession, HybridQuery, HybridSession
from voyager_ogm.ingestion import (
    BulkIngestionBatch,
    BulkIngestionPlan,
    chunk_dataframe,
    chunk_records,
    create_bulk_create_plan,
    create_bulk_create_rel_plan,
    create_bulk_merge_plan,
    create_csv_ingest_plan,
    create_csv_rel_ingest_plan,
    create_parquet_ingest_plan,
    create_parquet_rel_ingest_plan,
    ingest_csv,
    ingest_csv_relationships,
    ingest_parquet,
    ingest_parquet_relationships,
)
from voyager_ogm.models import (
    BoundField,
    Field,
    FieldConstraintError,
    Node,
    PredicateExpr,
    Relationship,
    RelationshipFrom,
    RelationshipTo,
    VectorProperty,
    node,
    relationship,
    reset_alias_counters,
)
from voyager_ogm.query import (
    SPEC_VERSION,
    ColumnMeta,
    CompiledQuery,
    ConformanceDiagnostic,
    ConformanceReport,
    Path,
    Query,
    SchemaValidationError,
    explain,
    profile,
    validate_query,
)
from voyager_ogm.schema import (
    FullTextIndex,
    SchemaManager,
    SchemaRegistry,
    TextIndex,
    adapt_tantivy_query,
    canonical_fulltext_index_name,
    canonical_vector_index_name,
    fulltext_index,
    resolve_fulltext_search,
)
from voyager_ogm.schema_export import export_schema, introspect_schema
from voyager_ogm.session import (
    AsyncSession,
    ExecutionResult,
    MappingsResult,
    Result,
    ScalarsResult,
    Session,
    SessionHealth,
)
from voyager_ogm.stream import AsyncResultStream, ResultStream
from voyager_ogm.streaming import (
    QueryResult,
    to_arrow,
    to_polars,
)
from voyager_ogm.transaction import AsyncTransaction, Transaction, TransactionSavepoint
from voyager_ogm.types import Duration, Json, Point, parse_iso_duration, to_iso_duration
from voyager_ogm.unit_of_work import SavepointContext, UnitOfWork
from voyager_ogm.viewer import GraphViewer, explore, show, view_graph, visualize_query

if TYPE_CHECKING:
    from voyager_ogm.sqlalchemy import (
        GraphRelationshipProperty,
        GraphTableClause,
        PropertyGraph,
        as_cte,
        graph_relationship,
        graph_table,
    )

    _HAS_SQLALCHEMY = True
else:
    try:
        from voyager_ogm.sqlalchemy import (
            GraphRelationshipProperty,
            GraphTableClause,
            PropertyGraph,
            as_cte,
            graph_relationship,
            graph_table,
        )

        _HAS_SQLALCHEMY = True
    except ImportError:
        _HAS_SQLALCHEMY = False

_SQLALCHEMY_EXPORTS = frozenset(
    {
        "GraphRelationshipProperty",
        "GraphTableClause",
        "PropertyGraph",
        "as_cte",
        "graph_relationship",
        "graph_table",
    }
)
_SQLALCHEMY_INSTALL_HINT = 'pip install "voyager-ogm[sqlalchemy]"'


def __getattr__(name: str) -> Any:
    """Raises a clear ImportError for SQLAlchemy-only names when the extra is missing.

    Args:
        name: Attribute name requested from this package.

    Returns:
        The resolved attribute value when available.

    Raises:
        ImportError: If the attribute is SQLAlchemy-backed and SQLAlchemy is not installed.
        AttributeError: If the attribute does not exist.
    """
    if name in _SQLALCHEMY_EXPORTS and not _HAS_SQLALCHEMY:
        raise ImportError(
            f"{name} requires SQLAlchemy. Install the optional extra with: "
            f"{_SQLALCHEMY_INSTALL_HINT}"
        )
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__version__ = version()

# Ergonomic Aliases
func = fn
func_ = fn
query = Query
GraphQuery = Query

__all__ = [
    "AliasedExpr",
    "ArrowStream",
    "AstExpr",
    "AsyncDatabaseBridge",
    "AsyncDuckDbBridge",
    "AsyncFalkorDBBridge",
    "AsyncHybridSession",
    "AsyncMockBridge",
    "AsyncNeo4jBoltBridge",
    "AsyncPostgresBridge",
    "AsyncRelateBridge",
    "AsyncResultStream",
    "AsyncSession",
    "AsyncTransaction",
    "BinaryExpr",
    "BoundField",
    "BulkExecutionResult",
    "BulkIngestionBatch",
    "BulkIngestionPlan",
    "CaseBuilder",
    "CaseExpr",
    "ColumnMeta",
    "CompiledQuery",
    "compile_query_from_spec",
    "ConformanceDiagnostic",
    "ConformanceReport",
    "DatabaseBridge",
    "DuckDbBridge",
    "Duration",
    "ExecutionResult",
    "Expression",
    "FalkorDBBridge",
    "Field",
    "FieldConstraintError",
    "FullTextIndex",
    "FunctionExpr",
    "GraphNode",
    "GraphPath",
    "GraphQuery",
    "GraphRelationship",
    "GraphViewer",
    "HybridQuery",
    "HybridSession",
    "IdentExpr",
    "Json",
    "LabelPredicateExpr",
    "ListCompExpr",
    "ListLiteralExpr",
    "LiteralExpr",
    "MappingsResult",
    "MockBridge",
    "NativeCircuitRouter",
    "NativeClient",
    "NativeQueryBuilder",
    "NativeQueryResult",
    "NativeSchemaRegistry",
    "Neo4jBoltBridge",
    "Node",
    "ParamExpr",
    "Path",
    "Point",
    "parse_iso_duration",
    "to_iso_duration",
    "PatternCompExpr",
    "PostgresBridge",
    "PredicateExpr",
    "PropExpr",
    "QUERY_SPEC_REVISION",
    "QUERY_SPEC_VERSION",
    "Query",
    "QueryResult",
    "RelateBridge",
    "Relationship",
    "RelationshipFrom",
    "RelationshipTo",
    "Result",
    "ResultStream",
    "SavepointContext",
    "ScalarsResult",
    "SchemaManager",
    "SchemaRegistry",
    "SchemaValidationError",
    "Session",
    "SessionHealth",
    "SPEC_VERSION",
    "SubqueryExpr",
    "TextIndex",
    "Transaction",
    "TransactionSavepoint",
    "UnitOfWork",
    "VoyagerConfig",
    "adapt_tantivy_query",
    "canonical_fulltext_index_name",
    "canonical_vector_index_name",
    "case",
    "chunk_dataframe",
    "chunk_records",
    "col",
    "configure",
    "create_bridge",
    "create_bulk_create_plan",
    "create_bulk_create_rel_plan",
    "create_bulk_merge_plan",
    "create_csv_ingest_plan",
    "create_csv_rel_ingest_plan",
    "create_parquet_ingest_plan",
    "create_parquet_rel_ingest_plan",
    "ingest_csv",
    "ingest_csv_relationships",
    "ingest_parquet",
    "ingest_parquet_relationships",
    "json_default",
    "explain",
    "explore",
    "export_schema",
    "introspect_schema",
    "expressions",
    "extract_topology_from_query",
    "fn",
    "fulltext_index",
    "func",
    "func_",
    "generate_synthetic_stream",
    "get_config",
    "has_label",
    "ident",
    "list_comprehension",
    "lit",
    "node",
    "param",
    "pattern_comprehension",
    "profile",
    "prop",
    "query",
    "register_bridge",
    "relationship",
    "reset_alias_counters",
    "reset_config",
    "resolve_fulltext_search",
    "show",
    "to_arrow",
    "to_expression",
    "to_networkx",
    "to_polars",
    "types",
    "validate_graph_name",
    "validate_query",
    "VectorProperty",
    "version",
    "view_graph",
    "visualize_query",
    "VoyagerError",
]

if _HAS_SQLALCHEMY:
    __all__ += [
        "GraphRelationshipProperty",
        "GraphTableClause",
        "PropertyGraph",
        "as_cte",
        "graph_relationship",
        "graph_table",
    ]
