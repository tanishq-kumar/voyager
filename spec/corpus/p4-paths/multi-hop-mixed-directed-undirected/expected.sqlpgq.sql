SELECT * FROM GRAPH_TABLE (graph_table MATCH (a IS "Person") -[r1 IS "KNOWS"]- (b IS "Person") -[r2 IS "KNOWS"]-> (c IS "Person") WHERE a.name = $p0 COLUMNS (b.name AS b_name, c.name AS c_name))
