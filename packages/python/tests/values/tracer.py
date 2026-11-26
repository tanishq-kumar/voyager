"""Per-stage trace profiler for failing value conformance cases (#299)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from voyager_ogm._voyager_rs import inspect_native_params, inspect_native_wire
from voyager_ogm.types import serialize_param_value


def _preview(value: Any, limit: int = 160) -> str:
    """Formats a bounded repr of ``value`` so 70KB payloads stay readable in traces."""
    text = repr(value)
    if len(text) > limit:
        return f"{text[:limit]}... (len={len(text)})"
    return text


@dataclass(frozen=True, slots=True)
class ValueTrace:
    """Six-stage diagnostic trace showing where a value or its type changed."""

    python_value: str
    voyager_param: str
    wire_encoding: str
    server_type: str
    arrow_type: str
    python_result: str

    def format_lines(self) -> list[str]:
        """Renders the trace as indented human-readable lines."""
        return [
            "  Stage trace:",
            f"    1. Python value       : {self.python_value}",
            f"    2. Voyager param      : {self.voyager_param}",
            f"    3. Wire encoding      : {self.wire_encoding}",
            f"    4. Server-side type   : {self.server_type}",
            f"    5. Arrow/Polars type  : {self.arrow_type}",
            f"    6. Python result      : {self.python_result}",
        ]


def inspect_voyager_param(value: Any, backend: str, engine: str) -> str:
    """Captures stage 2: Voyager's parameter representation before wire encoding."""
    if backend.startswith("native"):
        try:
            serialized = serialize_param_value(value)
            raw = inspect_native_params({"v": serialized})
            parsed = json.loads(raw).get("v")
            return _preview(parsed)
        except Exception as exc:
            return f"<ParamValue error: {type(exc).__name__}: {exc}>"
    if engine in ("neo4j", "memgraph"):
        try:
            from voyager_ogm.bridge import _adapt_bolt_value

            adapted = _adapt_bolt_value(serialize_param_value(value))
            return f"{type(adapted).__name__}: {_preview(adapted)}"
        except Exception as exc:
            return f"<Bolt adapt error: {type(exc).__name__}: {exc}>"
    if engine == "falkordb":
        try:
            from voyager_ogm.bridge import _adapt_falkordb_value

            adapted = _adapt_falkordb_value(serialize_param_value(value))
            return f"{type(adapted).__name__}: {_preview(adapted)}"
        except Exception as exc:
            return f"<FalkorDB adapt error: {type(exc).__name__}: {exc}>"
    return f"{type(value).__name__}: {_preview(value)}"


def inspect_wire(value: Any, backend: str, engine: str, statement: str) -> str:
    """Captures stage 3: protocol wire encoding sent to the server."""
    if backend.startswith("native"):
        bolt_major = 4 if engine == "memgraph" else 5
        try:
            serialized = serialize_param_value(value)
            raw = inspect_native_wire(engine, statement, {"v": serialized}, bolt_major=bolt_major)
            return _preview(json.loads(raw), limit=220)
        except Exception as exc:
            return f"<wire error: {type(exc).__name__}: {exc}>"
    return f"<bridge driver ({engine}) param={type(value).__name__}>"


def build_trace(
    *,
    value: Any,
    backend: str,
    engine: str,
    statement: str,
    server_type: str = "n/a",
    arrow_type: str = "n/a",
    result: Any = None,
    result_error: str | None = None,
) -> ValueTrace:
    """Constructs a full :class:`ValueTrace` for a failing conformance case."""
    py_val = f"{type(value).__name__}: {_preview(value)}"
    v_param = inspect_voyager_param(value, backend, engine)
    wire = inspect_wire(value, backend, engine, statement)
    if result_error is not None:
        py_res = f"<error: {result_error}>"
    else:
        py_res = f"{type(result).__name__}: {_preview(result)}"
    return ValueTrace(
        python_value=py_val,
        voyager_param=v_param,
        wire_encoding=wire,
        server_type=server_type,
        arrow_type=arrow_type,
        python_result=py_res,
    )
