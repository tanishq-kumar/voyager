SELECT * FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") WHERE NOT (n.city = $p0) COLUMNS (n.name))
