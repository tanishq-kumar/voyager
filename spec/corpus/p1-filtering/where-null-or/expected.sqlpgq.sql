SELECT * FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") WHERE (n.nickname = $p0) OR (n.age > $p1) COLUMNS (n.name))
