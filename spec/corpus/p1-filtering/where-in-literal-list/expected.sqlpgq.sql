SELECT * FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") WHERE n.name IN ($p0, $p1, $p2) COLUMNS (n.name))
