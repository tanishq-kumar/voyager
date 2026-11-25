SELECT * FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") WHERE strpos(n.name, $p0) > 0 COLUMNS (n.name))
