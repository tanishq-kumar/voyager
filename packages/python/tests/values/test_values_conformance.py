"""Differential live Hypothesis value conformance pack (#299).

Compares the engine's official client (reference), Voyager's bridge Session,
Voyager's native Session, and native/bridge Arrow & Polars exports across:
- Neo4j, Memgraph, FalkorDB, Apache AGE, PostgreSQL 19 (SQL), DuckDB
- Top-level parameter round trips, stored property/column round trips,
  nested lists/maps, node & relationship properties, and transactions.
"""

from __future__ import annotations

import datetime
import json
import os
import uuid
from collections.abc import Iterator
from decimal import Decimal
from typing import Any

import pytest
from hypothesis import HealthCheck, given, seed, settings

from .engines import BaseValueAdapter, EngineUnavailableError, open_adapter
from .equality import compare_value_and_type
from .known_differences import (
    KnownDifferenceEntry,
    find_matching_entry,
    find_stale_entries,
    load_known_differences,
)
from .matrix_report import MatrixRecorder
from .restrictions import (
    ENGINES,
    VALUE_KINDS,
    get_restriction,
    is_supported,
    strategy_for,
)
from .tracer import build_trace

pytestmark = pytest.mark.live


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    return int(raw) if raw else default


PROFILE_NAME = os.environ.get("VOYAGER_VALUES_PROFILE", "ci").strip().lower() or "ci"
SEED = _env_int("VOYAGER_VALUES_SEED", 299)
DEFAULT_EXAMPLES = 50 if PROFILE_NAME == "exploratory" else 5
MAX_EXAMPLES = _env_int("VOYAGER_VALUES_EXAMPLES", DEFAULT_EXAMPLES)
SELECTED_PATHS = {
    p.strip()
    for p in os.environ.get("VOYAGER_VALUES_PATHS", "all").split(",")
    if p.strip() and p.strip() != "all"
}


def _path_enabled(path: str) -> bool:
    return not SELECTED_PATHS or path in SELECTED_PATHS


settings.register_profile(
    "values_ci",
    max_examples=MAX_EXAMPLES,
    derandomize=True,
    database=None,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
settings.register_profile(
    "values_exploratory",
    max_examples=MAX_EXAMPLES,
    derandomize=False,
    database=None,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
settings.load_profile("values_exploratory" if PROFILE_NAME == "exploratory" else "values_ci")


KNOWN_DIFFERENCES = load_known_differences()
RECORDER = MatrixRecorder(seed=SEED, examples=MAX_EXAMPLES, profile=PROFILE_NAME)
_ADAPTERS: dict[str, BaseValueAdapter | None] = {}
_UNAVAILABLE_REASONS: dict[str, str] = {}


def _get_adapter(engine: str) -> BaseValueAdapter:
    if engine in _ADAPTERS:
        adapter = _ADAPTERS[engine]
        if adapter is None:
            pytest.skip(_UNAVAILABLE_REASONS[engine])
        return adapter
    try:
        adapter = open_adapter(engine)
    except EngineUnavailableError as exc:
        _ADAPTERS[engine] = None
        _UNAVAILABLE_REASONS[engine] = str(exc)
        pytest.skip(str(exc))
    _ADAPTERS[engine] = adapter
    return adapter


@pytest.fixture(scope="module", autouse=True)
def _finalize_conformance_run() -> Iterator[None]:
    """Closes adapters, checks stale known-differences, and emits matrix artifacts."""
    yield
    for adapter in _ADAPTERS.values():
        if adapter is not None:
            adapter.close()
    _ADAPTERS.clear()

    stale = find_stale_entries(
        KNOWN_DIFFERENCES,
        RECORDER.executed_cells,
        RECORDER.matched_known_diffs,
    )
    stale_msgs = [
        (
            f"Stale known_differences entry #{e.index}: engine={e.engine!r}, "
            f"backend={e.backend!r}, path={e.path!r}, value_kind={e.value_kind!r}, "
            f"reason={e.reason!r}"
        )
        for e in stale
    ]
    json_path, md_path = RECORDER.write_artifacts(stale_descriptions=stale_msgs)
    print(RECORDER.format_terminal_summary())
    print(f"Matrix artifacts: {json_path} | {md_path}")
    if stale_msgs:
        pytest.fail("Stale known-difference entries no longer reproduce:\n" + "\n".join(stale_msgs))


def _parse_arrow_meta(type_desc: str) -> dict[str, str]:
    """Extracts ``key=value`` pairs from ``_extract_arrow_cell`` type descriptions."""
    if "[" not in type_desc or not type_desc.endswith("]"):
        return {}
    inner = type_desc.rsplit("[", 1)[1][:-1]
    meta: dict[str, str] = {}
    for part in inner.split(", "):
        if "=" in part:
            k, v = part.split("=", 1)
            meta[k.strip()] = v.strip()
    return meta


def _decode_columnar_value(
    raw: Any,
    reference: Any,
    type_desc: str,
    value_kind: str,
    *,
    is_polars: bool = False,
) -> Any:
    """Decodes documented Arrow/Polars transport shapes back to comparable Python objects."""
    from voyager_ogm import Duration, Point
    from voyager_ogm.types import _hydrate_date, _hydrate_datetime, _hydrate_time

    if raw is None:
        return None
    meta = _parse_arrow_meta(type_desc)
    v_enc = meta.get("voyager.encoding")
    v_type = meta.get("voyager.type")

    if isinstance(reference, uuid.UUID):
        if isinstance(raw, bytes) and len(raw) == 16:
            return uuid.UUID(bytes=raw)
        if isinstance(raw, str):
            try:
                return uuid.UUID(raw)
            except ValueError:
                pass
    if v_type == "hugeint" and isinstance(raw, Decimal):
        return int(raw)
    if isinstance(raw, Decimal) and isinstance(reference, int) and raw == raw.to_integral_value():
        return int(raw)
    if is_polars and isinstance(raw, str) and isinstance(reference, datetime.time):
        return _hydrate_time(raw)

    if v_enc == "json" or (
        is_polars
        and isinstance(raw, str)
        and (value_kind in ("json_doc", "map") or isinstance(reference, (dict, list)))
    ):
        if isinstance(raw, str):
            try:
                return json.loads(raw)
            except Exception:
                return raw
    if isinstance(raw, datetime.timedelta) and isinstance(reference, Duration):
        return Duration.from_timedelta(raw)
    if (
        isinstance(raw, dict)
        and set(raw.keys()) == {"months", "days", "seconds", "nanoseconds"}
        and isinstance(reference, Duration)
    ):
        return Duration(
            months=int(raw["months"]),
            days=int(raw["days"]),
            seconds=int(raw["seconds"]),
            nanoseconds=int(raw["nanoseconds"]),
        )
    if (
        isinstance(raw, dict)
        and set(raw.keys()) == {"srid", "x", "y", "z"}
        and (
            v_type == "point"
            or (is_polars and (value_kind == "point" or isinstance(reference, Point)))
        )
    ):
        return Point.from_dict(raw)
    if isinstance(raw, list) and isinstance(reference, list):
        if is_polars and value_kind == "list" and reference and isinstance(reference[0], uuid.UUID):
            return [uuid.UUID(bytes=x) if isinstance(x, bytes) and len(x) == 16 else x for x in raw]
        return [
            _decode_columnar_value(
                r_item,
                ref_item if idx < len(reference) else None,
                type_desc,
                value_kind,
                is_polars=is_polars,
            )
            for idx, (r_item, ref_item) in enumerate(zip(raw, reference, strict=False))
        ]
    if is_polars and value_kind == "uuid" and isinstance(raw, bytes) and len(raw) == 16:
        return uuid.UUID(bytes=raw)
    if v_type == "date" and isinstance(raw, str):
        return _hydrate_date(raw)
    if v_type == "time_tz" and isinstance(raw, str):
        return _hydrate_time(raw)
    if v_type in ("datetime_tz", "local_datetime") and isinstance(raw, str):
        return _hydrate_datetime(raw)
    if v_type == "decimal" and isinstance(raw, str):
        try:
            return Decimal(raw)
        except Exception:
            return raw
    if v_type == "duration" and isinstance(raw, str):
        try:
            return Duration.from_iso(raw)
        except Exception:
            return raw
    return raw


def _handle_difference(
    *,
    engine: str,
    backend: str,
    path: str,
    value_kind: str,
    value: Any,
    message: str,
    statement: str,
    server_type: str = "n/a",
    arrow_type: str = "n/a",
    result: Any = None,
    result_error: str | None = None,
    py_type: str = "",
) -> str | None:
    """Checks ``KNOWN_DIFFERENCES`` and either marks known_diff or returns failure text."""
    entry: KnownDifferenceEntry | None = find_matching_entry(
        KNOWN_DIFFERENCES,
        engine=engine,
        backend=backend,
        path=path,
        value_kind=value_kind,
        message=message,
    )
    if entry is not None:
        RECORDER.mark_known_diff_matched(entry.index)
        RECORDER.record(
            engine=engine,
            backend=backend,
            path=path,
            value_kind=value_kind,
            status="known_diff",
            python_type=py_type,
            arrow_type=arrow_type if arrow_type != "n/a" else "",
            detail=f"[{entry.category}] {entry.reason}",
        )
        return None

    RECORDER.record(
        engine=engine,
        backend=backend,
        path=path,
        value_kind=value_kind,
        status="fail",
        python_type=py_type,
        arrow_type=arrow_type if arrow_type != "n/a" else "",
        detail=message,
    )
    trace = build_trace(
        value=value,
        backend=backend,
        engine=engine,
        statement=statement,
        server_type=server_type,
        arrow_type=arrow_type,
        result=result,
        result_error=result_error,
    )
    lines = [
        f"Conformance failure: engine={engine} backend={backend} path={path} value_kind={value_kind}",
        f"  Shrunk value: {type(value).__name__}: {value!r}"[:240],
        f"  Difference  : {message}",
        *trace.format_lines(),
    ]
    return "\n".join(lines)


def _run_path_check(
    adapter: BaseValueAdapter,
    *,
    engine: str,
    value_kind: str,
    path: str,
    value: Any,
) -> None:
    """Runs one round-trip path across all backends for ``engine`` and compares against reference."""

    def invoke(backend: str) -> tuple[Any, str, str]:
        if path == "param":
            return adapter.run_param(backend, value, value_kind)
        if path == "stored":
            return adapter.run_stored(backend, value, value_kind)
        if path == "nested_list":
            return adapter.run_nested_list(backend, value, value_kind)
        if path == "nested_map":
            return adapter.run_nested_map(backend, value, value_kind)
        if path == "node_prop":
            return adapter.run_entity_prop(backend, value, value_kind, entity_kind="node")
        if path == "rel_prop":
            return adapter.run_entity_prop(backend, value, value_kind, entity_kind="rel")
        if path == "transaction":
            return adapter.run_stored(backend, value, value_kind, in_tx=True)
        raise ValueError(f"Unknown path: {path!r}")

    # 1. Official client reference
    ref_val, _, ref_stype = invoke("official")
    RECORDER.record(
        engine=engine,
        backend="official",
        path=path,
        value_kind=value_kind,
        status="pass",
        python_type=type(ref_val).__name__,
    )

    failures: list[str] = []

    # 2. Voyager backends (bridge, native, arrow, polars)
    for backend in adapter.backends:
        if backend == "official":
            continue
        is_columnar = backend.endswith(("_arrow", "_polars"))
        is_polars = backend.endswith("_polars")
        try:
            actual_raw, col_type_desc, stype = invoke(backend)
        except Exception as exc:
            err = _handle_difference(
                engine=engine,
                backend=backend,
                path=path,
                value_kind=value_kind,
                value=value,
                message=f"execution raised {type(exc).__name__}: {exc}",
                statement=f"<{path}>",
                server_type=ref_stype or "n/a",
                result_error=f"{type(exc).__name__}: {exc}",
            )
            if err is not None:
                failures.append(err)
            continue

        actual = (
            _decode_columnar_value(
                actual_raw, ref_val, col_type_desc, value_kind, is_polars=is_polars
            )
            if is_columnar
            else actual_raw
        )
        diffs = compare_value_and_type(actual, ref_val, context=f"{backend}_vs_official")
        py_type = type(actual_raw).__name__
        if diffs:
            err = _handle_difference(
                engine=engine,
                backend=backend,
                path=path,
                value_kind=value_kind,
                value=value,
                message="; ".join(diffs),
                statement=f"<{path}>",
                server_type=stype or ref_stype or "n/a",
                arrow_type=col_type_desc or "n/a",
                result=actual_raw,
                py_type=py_type,
            )
            if err is not None:
                failures.append(err)
        else:
            RECORDER.record(
                engine=engine,
                backend=backend,
                path=path,
                value_kind=value_kind,
                status="pass",
                python_type=py_type,
                arrow_type=col_type_desc,
            )

    # 3. Also verify official client against input value for top-level param/stored
    if path in ("param", "stored"):
        in_diffs = compare_value_and_type(ref_val, value, context="official_vs_input")
        if in_diffs:
            err = _handle_difference(
                engine=engine,
                backend="official",
                path=path,
                value_kind=value_kind,
                value=value,
                message="; ".join(in_diffs),
                statement=f"<{path}>",
                server_type=ref_stype or "n/a",
                result=ref_val,
                py_type=type(ref_val).__name__,
            )
            if err is not None:
                failures.append(err)

    if failures:
        pytest.fail("\n\n".join(failures), pytrace=False)


def _core_cases() -> list[Any]:
    cases: list[Any] = []
    for engine in ENGINES:
        for kind in VALUE_KINDS:
            cases.append(pytest.param(engine, kind, id=f"{engine}-{kind}"))
    return cases


def _can_store_property_value(engine: str, value_kind: str, value: Any) -> bool:
    """Returns whether ``value`` obeys engine property-storage constraints (e.g. Neo4j/FalkorDB list rules)."""
    if value_kind != "list" or not isinstance(value, list):
        return True
    if engine == "neo4j":
        if any(x is None or isinstance(x, (list, dict)) for x in value):
            return False
        elem_types = {type(x) for x in value}
        if len(elem_types) > 1:
            return False
    elif engine == "falkordb":
        if any(x is None or isinstance(x, dict) for x in value):
            return False
    return True


@pytest.mark.parametrize(("engine", "value_kind"), _core_cases())
def test_value_conformance_core(engine: str, value_kind: str) -> None:
    """Differential Hypothesis round-trips for param, stored, nested_list, and nested_map."""
    if not is_supported(engine, value_kind):
        restriction = get_restriction(engine, value_kind)
        reason = restriction.reason if restriction else "Not supported by engine"
        RECORDER.record(
            engine=engine,
            backend="*",
            path="*",
            value_kind=value_kind,
            status="restricted",
            detail=reason,
            executed=False,
        )
        pytest.skip(f"{engine}/{value_kind} restricted: {reason}")

    adapter = _get_adapter(engine)
    strat = strategy_for(engine, value_kind, path="param")

    @seed(SEED)
    @given(value=strat)
    def _check(value: Any) -> None:
        if _path_enabled("param"):
            _run_path_check(
                adapter, engine=engine, value_kind=value_kind, path="param", value=value
            )
        if (
            _path_enabled("stored")
            and is_supported(engine, value_kind, path="stored")
            and _can_store_property_value(engine, value_kind, value)
        ):
            _run_path_check(
                adapter, engine=engine, value_kind=value_kind, path="stored", value=value
            )
        if _path_enabled("nested_list") and is_supported(engine, value_kind, path="nested_list"):
            _run_path_check(
                adapter, engine=engine, value_kind=value_kind, path="nested_list", value=value
            )
        if _path_enabled("nested_map") and is_supported(engine, value_kind, path="nested_map"):
            _run_path_check(
                adapter, engine=engine, value_kind=value_kind, path="nested_map", value=value
            )

    _check()


@pytest.mark.parametrize(("engine", "value_kind"), _core_cases())
def test_value_conformance_entities_and_tx(engine: str, value_kind: str) -> None:
    """Differential Hypothesis round-trips for node/rel properties and transactions."""
    if not is_supported(engine, value_kind):
        pytest.skip(f"{engine}/{value_kind} restricted")
    if SELECTED_PATHS and not (SELECTED_PATHS & {"node_prop", "rel_prop", "transaction"}):
        pytest.skip("entity/tx paths deselected via VOYAGER_VALUES_PATHS")

    adapter = _get_adapter(engine)
    strat = strategy_for(engine, value_kind, path="node_prop")
    tx_examples = max(2, MAX_EXAMPLES // 2)

    @seed(SEED)
    @settings(max_examples=tx_examples)
    @given(value=strat)
    def _check(value: Any) -> None:
        if _path_enabled("node_prop") and is_supported(engine, value_kind, path="node_prop"):
            _run_path_check(
                adapter, engine=engine, value_kind=value_kind, path="node_prop", value=value
            )
        if _path_enabled("rel_prop") and is_supported(engine, value_kind, path="rel_prop"):
            _run_path_check(
                adapter, engine=engine, value_kind=value_kind, path="rel_prop", value=value
            )
        if (
            _path_enabled("transaction")
            and adapter.supports_transactions
            and is_supported(engine, value_kind, path="transaction")
            and _can_store_property_value(engine, value_kind, value)
        ):
            _run_path_check(
                adapter, engine=engine, value_kind=value_kind, path="transaction", value=value
            )

    _check()
