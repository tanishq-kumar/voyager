SELECT * FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") WHERE ((n.age >= $p0) OR (n.city = $p1)) AND NOT ((n.age >= $p0) AND (n.city = $p1)) COLUMNS (n.name))
