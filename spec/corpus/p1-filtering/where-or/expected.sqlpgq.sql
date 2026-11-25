SELECT * FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") WHERE (n.city = $p0) OR (n.age > $p1) COLUMNS (n.name))
