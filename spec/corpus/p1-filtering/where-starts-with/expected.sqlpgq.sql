SELECT * FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") WHERE starts_with(n.name, $p0) COLUMNS (n.name))
