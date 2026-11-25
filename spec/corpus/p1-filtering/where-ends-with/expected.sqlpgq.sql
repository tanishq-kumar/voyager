SELECT * FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") WHERE right(n.name, length($p0)) = $p0 COLUMNS (n.name))
