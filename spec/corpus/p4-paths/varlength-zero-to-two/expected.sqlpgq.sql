SELECT * FROM GRAPH_TABLE (graph_table MATCH (a IS "Person") -[r IS "KNOWS"]->{0,2} (b IS "Person") WHERE a.name = $p0 COLUMNS (b.name AS b_name))
