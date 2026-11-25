SELECT * FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") WHERE n.nickname = $p0 COLUMNS (n.name))
