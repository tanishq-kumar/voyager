"""Graph entity and path topology reconstruction engine for Voyager GraphViewer."""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any

from voyager_ogm.entities import GraphNode, GraphPath, GraphRelationship


def extract_graph_pattern_from_cypher(
    stmt: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Extracts node and edge topology directly from query path patterns using native Rust AST engine."""
    if not stmt or not isinstance(stmt, str):
        return [], []

    try:
        from voyager_ogm._voyager_rs import extract_topology_from_query

        topo = extract_topology_from_query(stmt)
        return topo.get("nodes", []), topo.get("edges", [])
    except Exception:
        return [], []


_extract_graph_pattern_from_cypher = extract_graph_pattern_from_cypher


def extract_path_topology_from_query(
    query: Any,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Extracts graph path patterns directly from a Voyager Query, AST arena, or raw query statement."""
    if hasattr(query, "extract_topology"):
        try:
            topo = query.extract_topology()
            return topo.get("nodes", []), topo.get("edges", [])
        except Exception:
            pass

    cypher_stmt = ""
    if hasattr(query, "compile"):
        try:
            cypher_stmt = query.compile("cypher").statement
        except Exception:
            cypher_stmt = str(query)
    elif hasattr(query, "statement"):
        cypher_stmt = str(query.statement)
    else:
        cypher_stmt = str(query)

    return extract_graph_pattern_from_cypher(cypher_stmt)


def extract_graph_entities_from_records(
    records: Sequence[dict[str, Any]],
    statement: str = "",
    query: Any = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Extracts live graph nodes, relationships, and reconstructed paths from database session records.

    Inspects returned records for direct entity objects (Node, Rel, Path) or aligns
    query path patterns (MATCH / CREATE / MERGE) to row column values.
    """
    if not records:
        return [], []

    nodes_dict: dict[str, dict[str, Any]] = {}
    edges_list: list[dict[str, Any]] = []
    seen_edge_ids: set[str] = set()

    def _add_graph_node(node: GraphNode, fallback_key: str) -> None:
        n_id = str(
            node.element_id
            if node.element_id is not None
            else (node.id if node.id is not None else fallback_key)
        )
        lbl = sorted(node.labels)[0] if node.labels else "Node"
        props = dict(node.properties)
        if "id" not in props and node.id is not None:
            props["id"] = node.id
        nodes_dict[n_id] = {
            "id": n_id,
            "label": lbl,
            "group": lbl,
            "size": 11,
            "data": props,
        }

    def _add_graph_rel(rel: GraphRelationship) -> None:
        src = str(
            rel.start_element_id
            if rel.start_element_id is not None
            else (rel.start_id if rel.start_id is not None else "")
        )
        tgt = str(
            rel.end_element_id
            if rel.end_element_id is not None
            else (rel.end_id if rel.end_id is not None else "")
        )
        if not (src and tgt):
            return
        r_id = str(
            rel.element_id
            if rel.element_id is not None
            else (rel.id if rel.id is not None else f"rel_{len(edges_list)}")
        )
        if r_id in seen_edge_ids:
            return
        seen_edge_ids.add(r_id)
        edges_list.append(
            {
                "id": r_id,
                "source": src,
                "target": tgt,
                "label": rel.type or "CONNECTED_TO",
                "color": "#64748b",
                "data": dict(rel.properties),
            }
        )

    def _collect_entities(val: Any, fallback_key: str) -> bool:
        if isinstance(val, GraphNode):
            _add_graph_node(val, fallback_key)
            return True
        if isinstance(val, GraphRelationship):
            _add_graph_rel(val)
            return True
        if isinstance(val, GraphPath):
            for n in val.nodes:
                _add_graph_node(n, fallback_key)
            for rel in val.relationships:
                _add_graph_rel(rel)
            return True
        if isinstance(val, list):
            found = False
            for idx, item in enumerate(val):
                if _collect_entities(item, f"{fallback_key}_{idx}"):
                    found = True
            return found
        if isinstance(val, dict):
            if "labels" in val or "_labels" in val or "label" in val:
                n_id = str(val.get("id", val.get("_id", fallback_key)))
                lbls = val.get("labels", val.get("_labels", [val.get("label", "Node")]))
                lbl = lbls[0] if isinstance(lbls, list) and lbls else str(lbls)
                props = dict(
                    val.get(
                        "properties",
                        {
                            pk: pv
                            for pk, pv in val.items()
                            if pk not in ("labels", "_labels", "label", "_id")
                        },
                    )
                )
                if "id" not in props and val.get("id") is not None:
                    props["id"] = val.get("id")
                nodes_dict[n_id] = {
                    "id": n_id,
                    "label": lbl,
                    "group": lbl,
                    "size": 11,
                    "data": props,
                }
                return True
            if "type" in val or "_type" in val:
                e_type = str(val.get("type", val.get("_type", "CONNECTED_TO")))
                src = str(
                    val.get(
                        "start",
                        val.get("source", val.get("start_node_id", val.get("start_id", ""))),
                    )
                )
                tgt = str(
                    val.get(
                        "end",
                        val.get("target", val.get("end_node_id", val.get("end_id", ""))),
                    )
                )
                if src and tgt:
                    props = dict(
                        val.get(
                            "properties",
                            {
                                pk: pv
                                for pk, pv in val.items()
                                if pk
                                not in (
                                    "type",
                                    "_type",
                                    "start",
                                    "source",
                                    "end",
                                    "target",
                                    "start_id",
                                    "end_id",
                                )
                            },
                        )
                    )
                    edges_list.append(
                        {
                            "id": f"rel_{len(edges_list)}",
                            "source": src,
                            "target": tgt,
                            "label": e_type,
                            "color": "#64748b",
                            "data": props,
                        }
                    )
                    return True
            found = False
            for sub_k, sub_v in val.items():
                if _collect_entities(sub_v, f"{fallback_key}_{sub_k}"):
                    found = True
            return found
        return False

    has_entity_objects = False
    for r in records:
        if not isinstance(r, dict):
            continue
        for k, v in r.items():
            if _collect_entities(v, k):
                has_entity_objects = True

    if has_entity_objects and (nodes_dict or edges_list):
        return list(nodes_dict.values()), edges_list

    cypher_stmt = ""
    if statement:
        cypher_stmt = statement
    elif query is not None:
        if hasattr(query, "compile"):
            try:
                cypher_stmt = query.compile("cypher").statement
            except Exception:
                cypher_stmt = str(query)
        else:
            cypher_stmt = str(query)

    topo_nodes: list[dict[str, Any]] = []
    topo_edges: list[dict[str, Any]] = []

    if hasattr(query, "extract_topology"):
        try:
            t = query.extract_topology()
            topo_nodes = t.get("nodes", [])
            topo_edges = t.get("edges", [])
        except Exception:
            pass

    if not topo_nodes and not topo_edges and cypher_stmt:
        try:
            from voyager_ogm._voyager_rs import extract_topology_from_query

            t = extract_topology_from_query(cypher_stmt)
            topo_nodes = t.get("nodes", [])
            topo_edges = t.get("edges", [])
        except Exception:
            pass

    path_links = []
    if topo_edges:
        node_by_id = {str(n.get("id")): n for n in topo_nodes}
        for edge in topo_edges:
            src_node = node_by_id.get(
                str(edge.get("source")),
                {"id": str(edge.get("source")), "label": str(edge.get("source")), "data": {}},
            )
            tgt_node = node_by_id.get(
                str(edge.get("target")),
                {"id": str(edge.get("target")), "label": str(edge.get("target")), "data": {}},
            )
            src_var = src_node.get("data", {}).get("variable") or src_node.get("id")
            tgt_var = tgt_node.get("data", {}).get("variable") or tgt_node.get("id")
            path_links.append(
                {
                    "src": {
                        "var": str(src_var or ""),
                        "label": str(src_node.get("label") or "Entity"),
                    },
                    "tgt": {
                        "var": str(tgt_var or ""),
                        "label": str(tgt_node.get("label") or "Entity"),
                    },
                    "rel_var": str(edge.get("data", {}).get("variable") or edge.get("id") or ""),
                    "rel_type": str(edge.get("label") or "CONNECTED_TO"),
                }
            )

        if path_links:
            first_row = records[0]
            col_names = list(first_row.keys())

            var_to_col: dict[str, str] = {}
            for m in re.finditer(
                r"\b([a-zA-Z0-9_]+)(?:\.[a-zA-Z0-9_]+)?\s+AS\s+([a-zA-Z0-9_]+)\b",
                cypher_stmt,
                re.IGNORECASE,
            ):
                v_name, col_name = m.groups()
                var_to_col[v_name.lower()] = col_name

            for m in re.finditer(
                r"\b([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\b",
                cypher_stmt,
            ):
                v_name, prop_name = m.groups()
                if v_name.lower() not in var_to_col:
                    for c in col_names:
                        if (
                            c.lower() == f"{v_name.lower()}.{prop_name.lower()}"
                            or c.lower() == prop_name.lower()
                        ):
                            var_to_col[v_name.lower()] = c
                            break

            def find_col_for_entity(entity_info: dict[str, Any]) -> str | None:
                var = str(entity_info.get("var") or "").lower()
                if var and var in var_to_col and var_to_col[var] in col_names:
                    return var_to_col[var]

                lbl = str(entity_info.get("label") or "").lower()
                for c in col_names:
                    clow = c.lower()
                    if var and (clow == var or clow == f"{var}_id" or clow == f"id_{var}"):
                        return c
                    if lbl and (clow == lbl or clow == f"{lbl}_id" or clow == f"id_{lbl}"):
                        return c
                for c in col_names:
                    clow = c.lower()
                    if var and (
                        clow.startswith(f"{var}_")
                        or clow.endswith(f"_{var}")
                        or (len(var) > 2 and var in clow)
                    ):
                        return c
                    if lbl and (
                        clow.startswith(f"{lbl}_")
                        or clow.endswith(f"_{lbl}")
                        or (len(lbl) > 2 and lbl in clow)
                    ):
                        return c
                return None

            edge_idx = 0
            for r in records:
                for link in path_links:
                    src_info = link.get("src")
                    tgt_info = link.get("tgt")
                    if not isinstance(src_info, dict) or not isinstance(tgt_info, dict):
                        continue

                    src_col = find_col_for_entity(src_info)
                    tgt_col = find_col_for_entity(tgt_info)

                    if src_col and tgt_col:
                        src_val = r.get(src_col)
                        tgt_val = r.get(tgt_col)

                        if src_val is not None and tgt_val is not None:
                            s_id = str(src_val)
                            t_id = str(tgt_val)

                            if s_id not in nodes_dict:
                                nodes_dict[s_id] = {
                                    "id": s_id,
                                    "label": s_id,
                                    "group": str(src_info.get("label") or "Node"),
                                    "size": 11,
                                    "data": {src_col: src_val},
                                }

                            if t_id not in nodes_dict:
                                nodes_dict[t_id] = {
                                    "id": t_id,
                                    "label": t_id,
                                    "group": str(tgt_info.get("label") or "Node"),
                                    "size": 11,
                                    "data": {tgt_col: tgt_val},
                                }

                            rel_props = {}
                            rel_var_str = str(link.get("rel_var") or "")
                            if rel_var_str:
                                rv = rel_var_str.lower()
                                for k, v in r.items():
                                    if k not in (src_col, tgt_col) and (
                                        rv in k.lower()
                                        or "amount" in k.lower()
                                        or "weight" in k.lower()
                                        or "since" in k.lower()
                                    ):
                                        rel_props[k] = v

                            edge_idx += 1
                            edges_list.append(
                                {
                                    "id": f"edge_{edge_idx}_{s_id}_{t_id}",
                                    "source": s_id,
                                    "target": t_id,
                                    "label": link["rel_type"],
                                    "color": "#64748b",
                                    "data": rel_props,
                                }
                            )

            if nodes_dict:
                return list(nodes_dict.values()), edges_list

    first_row = records[0]
    col_names = list(first_row.keys())
    src_col = next(
        (
            c
            for c in ["source", "from", "src", "start", "u", "from_id", "source_id"]
            if c in col_names
        ),
        None,
    )
    tgt_col = next(
        (c for c in ["target", "to", "dst", "end", "v", "to_id", "target_id"] if c in col_names),
        None,
    )
    rel_col = next(
        (c for c in ["rel", "relationship", "type", "label", "edge_type", "r"] if c in col_names),
        None,
    )

    if src_col and tgt_col:
        for idx, r in enumerate(records):
            s_val = r.get(src_col)
            t_val = r.get(tgt_col)
            if s_val is not None and t_val is not None:
                s_id = str(s_val)
                t_id = str(t_val)
                r_lbl = str(r.get(rel_col, "CONNECTED_TO")) if rel_col else "CONNECTED_TO"
                if s_id not in nodes_dict:
                    nodes_dict[s_id] = {
                        "id": s_id,
                        "label": s_id,
                        "group": "Node",
                        "size": 11,
                        "data": {k: v for k, v in r.items() if k not in (tgt_col, rel_col)},
                    }
                if t_id not in nodes_dict:
                    nodes_dict[t_id] = {
                        "id": t_id,
                        "label": t_id,
                        "group": "Node",
                        "size": 11,
                        "data": {k: v for k, v in r.items() if k not in (src_col, rel_col)},
                    }
                edges_list.append(
                    {
                        "id": f"e_{idx}_{s_id}_{t_id}",
                        "source": s_id,
                        "target": t_id,
                        "label": r_lbl,
                        "color": "#64748b",
                        "data": {
                            k: v for k, v in r.items() if k not in (src_col, tgt_col, rel_col)
                        },
                    }
                )
        return list(nodes_dict.values()), edges_list

    id_col = next((c for c in ["id", "node_id", "name", "key"] if c in col_names), None)
    if id_col:
        for r in records:
            val = r.get(id_col)
            if val is not None:
                val_id = str(val)
                if val_id not in nodes_dict:
                    nodes_dict[val_id] = {
                        "id": val_id,
                        "label": val_id,
                        "group": "Node",
                        "size": 11,
                        "data": r,
                    }
        return list(nodes_dict.values()), []

    return [], []
