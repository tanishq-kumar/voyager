"""Voyager Python database bridge implementations.

The package groups every driver adapter behind the ``DatabaseBridge`` and
``AsyncDatabaseBridge`` protocols. :func:`create_bridge` detects a driver,
connection, or URI and returns the matching adapter; ``voyager_ogm.bridge``
remains a compatibility shim over these modules.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any, Literal, overload

from voyager_ogm.bridges.base import (
    AsyncDatabaseBridge,
    DatabaseBridge,
    mask_uri_password,
    validate_graph_name,
)
from voyager_ogm.bridges.duckdb import AsyncDuckDbBridge, DuckDbBridge
from voyager_ogm.bridges.falkordb import AsyncFalkorDBBridge, FalkorDBBridge
from voyager_ogm.bridges.mock import AsyncMockBridge, MockBridge
from voyager_ogm.bridges.neo4j import AsyncNeo4jBoltBridge, Neo4jBoltBridge
from voyager_ogm.bridges.postgres import (
    AsyncPostgresBridge,
    PostgresBridge,
    setup_postgres_age,
)
from voyager_ogm.bridges.relate import AsyncRelateBridge, RelateBridge

# Optional namespace prefixes accepted in front of a connection URI, matching the
# native URI parser (voyager-drivers `ParsedUri::parse`). They select the backend
# for a Session; the Python driver bridge ignores them and uses the URI after them.
_URI_NAMESPACE_PREFIXES = (
    "voyager+native:",
    "voyager+bridge:",
    "voyager-drivers:",
    "voyager:",
    "voy:",
    "vn:",
)


def _strip_uri_namespace_prefix(uri: str) -> str:
    """Removes an optional Voyager namespace prefix such as ``vn:`` or ``voy:``."""
    stripped = uri.strip()
    for prefix in _URI_NAMESPACE_PREFIXES:
        if stripped.startswith(prefix):
            return stripped[len(prefix) :].strip()
    return stripped


_BRIDGE_REGISTRY: list[tuple[Callable[[Any], bool], type, bool]] = []


def register_bridge(
    predicate_or_type: type | Callable[[Any], bool],
    bridge_class: type,
    is_async: bool = False,
) -> None:
    """Registers a new database driver adapter into the global bridge registry.

    Allows third-party and custom drivers to be auto-detected by Session.

    Args:
        predicate_or_type: Driver class type or matcher predicate function.
        bridge_class: Adapter class to instantiate.
        is_async: Flag indicating whether this adapter implements AsyncDatabaseBridge.
    """
    matcher: Callable[[Any], bool]
    if isinstance(predicate_or_type, type):
        target_cls = predicate_or_type

        def _matcher(obj: Any) -> bool:
            return isinstance(obj, target_cls)

        matcher = _matcher
    else:
        matcher = predicate_or_type

    _BRIDGE_REGISTRY.insert(0, (matcher, bridge_class, is_async))


def _is_neo4j_sync_driver(obj: Any) -> bool:
    type_name = f"{type(obj).__module__}.{type(obj).__qualname__}"
    return "neo4j" in type_name and "Async" not in type_name and hasattr(obj, "session")


def _is_neo4j_async_driver(obj: Any) -> bool:
    type_name = f"{type(obj).__module__}.{type(obj).__qualname__}"
    return "neo4j" in type_name and "Async" in type_name and hasattr(obj, "session")


def _is_duckdb_conn(obj: Any) -> bool:
    type_name = f"{type(obj).__module__}.{type(obj).__qualname__}"
    return "duckdb" in type_name and hasattr(obj, "execute")


def _is_postgres_conn(obj: Any) -> bool:
    type_name = f"{type(obj).__module__}.{type(obj).__qualname__}"
    return "psycopg" in type_name and hasattr(obj, "cursor")


def _is_falkordb_conn(obj: Any) -> bool:
    type_name = f"{type(obj).__module__}.{type(obj).__qualname__}"
    return "falkordb" in type_name and (hasattr(obj, "query") or hasattr(obj, "select_graph"))


def _is_relate_conn(obj: Any) -> bool:
    type_name = f"{type(obj).__module__}.{type(obj).__qualname__}"
    if "relate" in type_name and hasattr(obj, "execute"):
        return True
    if hasattr(obj, "execute_arrow") and hasattr(obj, "begin_transaction"):
        return True
    return False


register_bridge(_is_neo4j_sync_driver, Neo4jBoltBridge, is_async=False)
register_bridge(_is_neo4j_async_driver, AsyncNeo4jBoltBridge, is_async=True)
register_bridge(_is_duckdb_conn, DuckDbBridge, is_async=False)
register_bridge(_is_duckdb_conn, AsyncDuckDbBridge, is_async=True)
register_bridge(_is_postgres_conn, PostgresBridge, is_async=False)
register_bridge(_is_postgres_conn, AsyncPostgresBridge, is_async=True)
register_bridge(_is_falkordb_conn, FalkorDBBridge, is_async=False)
register_bridge(_is_falkordb_conn, AsyncFalkorDBBridge, is_async=True)
register_bridge(_is_relate_conn, RelateBridge, is_async=False)
register_bridge(_is_relate_conn, AsyncRelateBridge, is_async=True)


@overload
def create_bridge(
    driver_or_connection: Any = None,
    is_async: Literal[False] = False,
    fallback: str | None = None,
    *,
    auth: tuple[str, str] | Any | None = None,
    graph_name: str | None = None,
) -> DatabaseBridge: ...


@overload
def create_bridge(
    driver_or_connection: Any = None,
    is_async: Literal[True] = True,
    fallback: str | None = None,
    *,
    auth: tuple[str, str] | Any | None = None,
    graph_name: str | None = None,
) -> AsyncDatabaseBridge: ...


@overload
def create_bridge(
    driver_or_connection: Any = None,
    is_async: bool = False,
    fallback: str | None = None,
    *,
    auth: tuple[str, str] | Any | None = None,
    graph_name: str | None = None,
) -> DatabaseBridge | AsyncDatabaseBridge: ...


def create_bridge(
    driver_or_connection: Any = None,
    is_async: bool = False,
    fallback: str | None = None,
    *,
    auth: tuple[str, str] | Any | None = None,
    graph_name: str | None = None,
) -> DatabaseBridge | AsyncDatabaseBridge:
    """Creates or adapts a database bridge from a user-supplied driver, connection, or URI.

    Args:
        driver_or_connection: Database driver, connection instance, MockBridge, or custom bridge.
        is_async: Whether an async bridge is required.
        fallback: Fallback strategy when driver import fails or URI scheme is unrecognized.
            Pass ``fallback="mock"`` to explicitly opt into returning an in-memory MockBridge
            for testing, mocking, or demonstrations.
        auth: Explicit authentication credentials. For Bolt schemes, accepts a (username, password)
            tuple or a neo4j auth token (e.g. bearer_auth). For PostgreSQL/AGE schemes, accepts
            a (username, password) tuple passed as connect kwargs. For FalkorDB/Redis schemes,
            accepts a (username, password) tuple. Explicit auth overrides credentials present
            in the URI.
        graph_name: Optional graph name, validated as a safe identifier. For Apache AGE and
            PostgreSQL it selects the AGE graph. For FalkorDB it selects one of the
            server's named graphs; without it, the graph comes from the URI path
            (``redis://host:6379/<graph>``) or ``?graph=``, and otherwise defaults to
            ``voyager_graph``, the native FalkorDB client's default.

    Returns:
        Configured database bridge adapter.

    Raises:
        ValueError: If no driver/URI was provided, or if the URI scheme is unrecognized and
            fallback="mock" was not explicitly requested, or if graph_name is invalid.
        ImportError: If the official driver package required for the URI scheme is not installed
            and fallback="mock" was not explicitly requested.
        TypeError: If the driver_or_connection object type is unsupported, or if an invalid
            auth type was provided for the scheme.
    """
    if graph_name is not None:
        validate_graph_name(graph_name)

    if driver_or_connection is None:
        if fallback == "mock":
            return (
                AsyncMockBridge(graph_name=graph_name)
                if is_async
                else MockBridge(graph_name=graph_name)
            )
        raise ValueError(
            "No database driver, connection, or URI provided. "
            "Provide a valid driver instance, connection URI, or pass fallback='mock' (or use a 'mock://' URI) for offline testing."
        )

    if isinstance(driver_or_connection, MockBridge):
        if graph_name is not None:
            driver_or_connection.graph_name = graph_name
        return (
            AsyncMockBridge(driver_or_connection, graph_name=driver_or_connection.graph_name)
            if is_async
            else driver_or_connection
        )
    if isinstance(driver_or_connection, AsyncMockBridge):
        if graph_name is not None:
            driver_or_connection.graph_name = graph_name
        return driver_or_connection if is_async else driver_or_connection.sync_mock

    if (
        is_async
        and isinstance(driver_or_connection, AsyncDatabaseBridge)
        and inspect.iscoroutinefunction(getattr(driver_or_connection, "execute", None))
    ):
        if graph_name is not None and hasattr(driver_or_connection, "graph_name"):
            driver_or_connection.graph_name = graph_name
        return driver_or_connection
    if (
        not is_async
        and isinstance(driver_or_connection, DatabaseBridge)
        and not inspect.iscoroutinefunction(getattr(driver_or_connection, "execute", None))
    ):
        if graph_name is not None and hasattr(driver_or_connection, "graph_name"):
            driver_or_connection.graph_name = graph_name
        return driver_or_connection

    for matcher, bridge_cls, reg_is_async in _BRIDGE_REGISTRY:
        if reg_is_async == is_async and matcher(driver_or_connection):
            bridge_inst = bridge_cls(driver_or_connection)
            if graph_name is not None and hasattr(bridge_inst, "graph_name"):
                bridge_inst.graph_name = graph_name
            return bridge_inst

    if is_async:
        for matcher, bridge_cls, reg_is_async in _BRIDGE_REGISTRY:
            if not reg_is_async and matcher(driver_or_connection):
                sync_inst = bridge_cls(driver_or_connection)
                if isinstance(sync_inst, DuckDbBridge):
                    return AsyncDuckDbBridge(driver_or_connection)
                elif isinstance(sync_inst, FalkorDBBridge):
                    return AsyncFalkorDBBridge(driver_or_connection)
                elif isinstance(sync_inst, RelateBridge):
                    return AsyncRelateBridge(driver_or_connection)
                elif isinstance(sync_inst, PostgresBridge):
                    async_inst = AsyncPostgresBridge(driver_or_connection)
                    if graph_name is not None:
                        async_inst.graph_name = graph_name
                    return async_inst

    # If a database connection URI string was provided, attempt to auto-instantiate the official driver
    if isinstance(driver_or_connection, str):
        driver_or_connection = _strip_uri_namespace_prefix(driver_or_connection)
        if driver_or_connection.startswith("mock://"):
            from urllib.parse import parse_qs, urlsplit

            parsed_mock = urlsplit(driver_or_connection)
            qs_mock = parse_qs(parsed_mock.query)
            uri_mock_graph = qs_mock.get("graph", qs_mock.get("graph_name", [None]))[0]
            resolved_mock_graph = graph_name if graph_name is not None else uri_mock_graph
            if resolved_mock_graph is not None:
                validate_graph_name(resolved_mock_graph)
            return (
                AsyncMockBridge(graph_name=resolved_mock_graph)
                if is_async
                else MockBridge(graph_name=resolved_mock_graph)
            )

        if "://" not in driver_or_connection:
            if fallback == "mock":
                return AsyncMockBridge() if is_async else MockBridge()
            raise ValueError(
                f"Invalid connection URI: '{mask_uri_password(driver_or_connection)}'. Expected a valid URI scheme (e.g. 'bolt://', 'duckdb://', 'postgresql://', 'falkordb://', or 'mock://')."
            )

        scheme = driver_or_connection.split("://", 1)[0].lower()
        if scheme in (
            "bolt",
            "neo4j",
            "memgraph",
            "bolt+s",
            "neo4j+s",
            "memgraph+s",
            "bolt+ssc",
            "neo4j+ssc",
            "memgraph+ssc",
        ):
            try:
                import neo4j
            except ImportError as e:
                if fallback == "mock":
                    return AsyncMockBridge() if is_async else MockBridge()
                raise ImportError(
                    f"The 'neo4j' Python package is required to connect to URI '{mask_uri_password(driver_or_connection)}'. "
                    "Install the optional extra with: "
                    "pip install \"voyager-ogm[neo4j]\", or pass fallback='mock' for offline testing."
                ) from e
            from urllib.parse import parse_qs, unquote, urlsplit, urlunsplit

            parsed_bolt = urlsplit(driver_or_connection)
            netloc = parsed_bolt.netloc
            hostport = netloc.rsplit("@", 1)[-1]
            bolt_clean_uri = urlunsplit(
                (
                    parsed_bolt.scheme,
                    hostport,
                    parsed_bolt.path,
                    "",
                    "",
                )
            )
            bolt_kwargs: dict[str, Any] = {}
            if parsed_bolt.query:
                qs = parse_qs(parsed_bolt.query)
                if "connect_timeout" in qs:
                    try:
                        bolt_kwargs["connection_timeout"] = float(qs["connect_timeout"][0])
                    except ValueError:
                        pass
                if "connection_timeout" in qs:
                    try:
                        bolt_kwargs["connection_timeout"] = float(qs["connection_timeout"][0])
                    except ValueError:
                        pass

            effective_auth = None
            if auth is not None:
                effective_auth = auth
            elif "@" in netloc:
                userinfo = netloc.rsplit("@", 1)[0]
                if ":" in userinfo:
                    u_raw, p_raw = userinfo.split(":", 1)
                    effective_auth = (unquote(u_raw), unquote(p_raw))
                else:
                    effective_auth = (unquote(userinfo), "")

            if effective_auth is not None:
                bolt_kwargs["auth"] = effective_auth

            if is_async and hasattr(neo4j, "AsyncGraphDatabase"):
                async_drv = neo4j.AsyncGraphDatabase.driver(bolt_clean_uri, **bolt_kwargs)
                return AsyncNeo4jBoltBridge(async_drv)
            elif hasattr(neo4j, "GraphDatabase"):
                sync_drv = neo4j.GraphDatabase.driver(bolt_clean_uri, **bolt_kwargs)
                return Neo4jBoltBridge(sync_drv)
            else:
                if fallback == "mock":
                    return AsyncMockBridge() if is_async else MockBridge()
                raise RuntimeError(
                    f"Unable to instantiate neo4j driver from '{mask_uri_password(driver_or_connection)}'."
                )
        elif scheme == "duckdb":
            if auth is not None:
                raise TypeError("DuckDB does not accept auth credentials")
            try:
                import duckdb
            except ImportError as e:
                if fallback == "mock":
                    return AsyncMockBridge() if is_async else MockBridge()
                raise ImportError(
                    f"The 'duckdb' Python package is required to connect to URI '{mask_uri_password(driver_or_connection)}'. "
                    "Install the optional extra with: "
                    "pip install \"voyager-ogm[duckdb]\", or pass fallback='mock' for offline testing."
                ) from e
            path = driver_or_connection.replace("duckdb://", "") or ":memory:"
            duck_conn = duckdb.connect(path)
            duck_bridge = DuckDbBridge(duck_conn)
            return AsyncDuckDbBridge(duck_conn) if is_async else duck_bridge
        elif scheme in (
            "postgresql",
            "postgres",
            "age",
            "postgresql+s",
            "postgres+s",
            "age+s",
            "postgresql+ssc",
            "postgres+ssc",
            "age+ssc",
        ):
            try:
                import psycopg
            except ImportError as e:
                if fallback == "mock":
                    return AsyncMockBridge() if is_async else MockBridge()
                raise ImportError(
                    f"The 'psycopg' Python package is required to connect to URI '{mask_uri_password(driver_or_connection)}'. "
                    "Install the optional extra with: "
                    "pip install \"voyager-ogm[age]\", or pass fallback='mock' for offline testing."
                ) from e
            from urllib.parse import parse_qs, parse_qsl, urlencode, urlsplit, urlunsplit

            is_age_scheme = scheme in ("age", "age+s", "age+ssc")
            is_ssl_scheme = scheme in (
                "postgresql+s",
                "postgres+s",
                "age+s",
                "postgresql+ssc",
                "postgres+ssc",
                "age+ssc",
            )

            parsed_pg = urlsplit(driver_or_connection)
            target_scheme = "postgresql" if (is_age_scheme or is_ssl_scheme) else parsed_pg.scheme
            qs = parse_qs(parsed_pg.query)
            uri_graph = qs.get("graph", qs.get("graph_name", [None]))[0]
            resolved_graph = graph_name if graph_name is not None else uri_graph
            if resolved_graph is not None:
                validate_graph_name(resolved_graph)

            # psycopg rejects unknown libpq keywords, so the graph selectors are
            # consumed here and must not reach the connection string.
            pg_query_pairs = [
                (key, value)
                for key, value in parse_qsl(parsed_pg.query, keep_blank_values=True)
                if key not in ("graph", "graph_name")
            ]
            if is_ssl_scheme and "sslmode" not in qs and "ssl" not in qs:
                pg_query_pairs.append(("sslmode", "require"))
            new_query = urlencode(pg_query_pairs)

            clean_pg_uri = urlunsplit(
                (target_scheme, parsed_pg.netloc, parsed_pg.path, new_query, parsed_pg.fragment)
            )

            pg_kwargs: dict[str, Any] = {"autocommit": True}
            if is_ssl_scheme:
                pg_kwargs["sslmode"] = "require"

            if auth is not None:
                if not isinstance(auth, tuple) or len(auth) != 2:
                    raise TypeError(
                        f"Postgres/AGE auth must be a (username, password) tuple, got {type(auth).__name__}"
                    )
                pg_kwargs["user"] = auth[0]
                pg_kwargs["password"] = auth[1]

            pg_conn = psycopg.connect(clean_pg_uri, **pg_kwargs)
            setup_postgres_age(pg_conn, force=is_age_scheme)
            pg_bridge = PostgresBridge(pg_conn, graph_name=resolved_graph)
            return (
                AsyncPostgresBridge(pg_conn, graph_name=resolved_graph) if is_async else pg_bridge
            )
        elif scheme in (
            "falkordb",
            "falkordbs",
            "falkor",
            "redis",
            "rediss",
            "valkey",
            "valkeys",
        ):
            try:
                from falkordb import FalkorDB
            except ImportError as e:
                if fallback == "mock":
                    return AsyncMockBridge() if is_async else MockBridge()
                raise ImportError(
                    f"The 'falkordb' Python package is required to connect to URI '{mask_uri_password(driver_or_connection)}'. "
                    f"Install it with 'pip install falkordb', or pass fallback='mock' for offline testing."
                ) from e
            from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

            parsed_url = urlsplit(driver_or_connection)
            redis_scheme = "rediss" if scheme in ("rediss", "falkordbs", "valkeys") else "redis"
            qs = parse_qs(parsed_url.query)
            redis_qs = {}
            for k, vals in qs.items():
                if k == "connect_timeout":
                    redis_qs["socket_connect_timeout"] = vals[0]
                elif k in (
                    "socket_timeout",
                    "socket_connect_timeout",
                    "password",
                    "db",
                    "username",
                    "decode_responses",
                    "ssl",
                    "ssl_cert_reqs",
                ):
                    redis_qs[k] = vals[0]

            falkor_kwargs: dict[str, Any] = {}
            if auth is not None:
                if not isinstance(auth, tuple) or len(auth) != 2:
                    raise TypeError(
                        f"FalkorDB/Redis auth must be a (username, password) tuple, got {type(auth).__name__}"
                    )
                falkor_kwargs["username"] = auth[0]
                falkor_kwargs["password"] = auth[1]

            clean_url = urlunsplit(
                (
                    redis_scheme,
                    parsed_url.netloc,
                    parsed_url.path,
                    urlencode(redis_qs),
                    parsed_url.fragment,
                )
            )
            uri_graph = qs.get("graph", qs.get("graph_name", [None]))[0]
            if not uri_graph and parsed_url.path and parsed_url.path != "/":
                uri_graph = parsed_url.path.lstrip("/")
            resolved_graph = (
                graph_name
                if graph_name is not None
                else (uri_graph if uri_graph else "voyager_graph")
            )
            if resolved_graph is not None:
                validate_graph_name(resolved_graph)
            parsed = FalkorDB.from_url(clean_url, **falkor_kwargs)
            f_bridge = FalkorDBBridge(parsed, graph_name=resolved_graph)
            return AsyncFalkorDBBridge(parsed, graph_name=resolved_graph) if is_async else f_bridge
        elif scheme in ("relate", "relatedb"):
            if auth is not None:
                raise TypeError("Relate does not accept auth credentials")
            try:
                import relate
            except ImportError as e:
                if fallback == "mock":
                    return AsyncMockBridge() if is_async else MockBridge()
                raise ImportError(
                    f"The 'relate-db' Python package is required to connect to URI '{mask_uri_password(driver_or_connection)}'. "
                    "Install the optional extra with: "
                    "pip install \"voyager-ogm[relate]\", or pass fallback='mock' for offline testing."
                ) from e
            path = driver_or_connection.split("://", 1)[1]
            target_path = ":memory:" if (not path or path == ":memory:") else path
            relate_conn = relate.connect(target_path)
            r_bridge = RelateBridge(relate_conn)
            return AsyncRelateBridge(relate_conn) if is_async else r_bridge
        else:
            if fallback == "mock":
                return AsyncMockBridge() if is_async else MockBridge()
            raise ValueError(
                f"No bridge available for URI '{mask_uri_password(driver_or_connection)}' (unrecognized scheme: '{scheme}'). "
                f"Tried adapters: bolt, duckdb, postgres, falkordb, relate. "
                f"Install required driver or check the URI, or pass fallback='mock' (or use a 'mock://' URI) for offline testing."
            )

    if fallback == "mock":
        return AsyncMockBridge() if is_async else MockBridge()
    raise TypeError(
        f"Unsupported database driver or connection object of type {type(driver_or_connection).__name__}. "
        f"Expected a recognized driver connection instance, DatabaseBridge, or URI string."
    )
