SELECT * FROM GRAPH_TABLE (graph_table MATCH (a IS "Person") -[r IS "KNOWS" WHERE r.weight = $p0]->{1,2} (b IS "Person") WHERE a.name = $p1 COLUMNS (b.name AS "b_name"))
