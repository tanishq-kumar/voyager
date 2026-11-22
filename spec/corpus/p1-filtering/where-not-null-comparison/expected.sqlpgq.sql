SELECT * FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") WHERE NOT (n.nickname = $p0) COLUMNS (n.name))
