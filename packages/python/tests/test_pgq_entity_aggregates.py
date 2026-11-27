"""Aggregates over whole nodes on SQL/PGQ and DuckPGQ.

COUNT(n) has a row-counting equivalent and compiles to COUNT(*); the other
aggregates over a whole element are rejected at compile time.
"""

from __future__ import annotations

from typing import Any

import pytest
from voyager_ogm import Session, VoyagerError
from voyager_ogm._voyager_rs import compile_query_from_spec

DIALECTS = ("sql_pgq", "duckpgq")


def _entity_aggregate_spec(
    func: str = "count", property_name: str = "*", alias: str = "cnt"
) -> dict[str, Any]:
    return {
        "spec_version": 1,
        "matches": [{"paths": [[["node", "n", ["Person"]]]]}],
        "projections": [["agg", "n", property_name, func, alias]],
    }


@pytest.mark.parametrize("dialect", DIALECTS)
def test_count_nodes_compiles_to_count_star(dialect: str) -> None:
    compiled = compile_query_from_spec(_entity_aggregate_spec(), dialect, graph_name="g")
    assert compiled["statement"] == (
        'SELECT COUNT(*) AS "cnt" FROM GRAPH_TABLE (g MATCH (n IS "Person") COLUMNS (1 AS "_c0"))'
    )


@pytest.mark.parametrize("dialect", DIALECTS)
@pytest.mark.parametrize(
    ("func", "expected_call"),
    [
        ("count_distinct", "COUNT(DISTINCT n)"),
        ("collect", "COLLECT(n)"),
        ("sum", "SUM(n)"),
        ("avg", "AVG(n)"),
        ("min", "MIN(n)"),
        ("max", "MAX(n)"),
    ],
)
def test_other_entity_aggregates_are_rejected(dialect: str, func: str, expected_call: str) -> None:
    with pytest.raises(VoyagerError) as exc_info:
        compile_query_from_spec(_entity_aggregate_spec(func), dialect, graph_name="g")
    assert exc_info.value.code == "UNSUPPORTED_FEATURE"
    message = str(exc_info.value)
    assert expected_call in message
    assert "whole node or relationship" in message


def _duckpgq_available() -> bool:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(
        ":memory:",
        config={
            "autoinstall_known_extensions": "false",
            "autoload_known_extensions": "false",
        },
    )
    try:
        con.execute("LOAD duckpgq;")
        return True
    except Exception:
        return False
    finally:
        con.close()


def test_count_nodes_runs_on_embedded_duckpgq() -> None:
    if not _duckpgq_available():
        pytest.skip(
            "duckpgq extension is not installed locally; run INSTALL duckpgq FROM community;"
        )
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(":memory:")
    try:
        con.execute(
            """
            CREATE TABLE person (
                id BIGINT PRIMARY KEY,
                name VARCHAR
            );
            INSERT INTO person VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol');
            """
        )
        session = Session(bridge=con, dialect="duckpgq", graph_name="zz_entity_agg_pg")
        session.execute(
            'CREATE PROPERTY GRAPH zz_entity_agg_pg VERTEX TABLES (person LABEL "Person")'
        )
        compiled = compile_query_from_spec(
            _entity_aggregate_spec(), "duckpgq", graph_name="zz_entity_agg_pg"
        )
        records = session.execute(compiled["statement"]).all()
        assert records == [{"cnt": 3}]
    finally:
        con.close()
