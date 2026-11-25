SELECT * FROM GRAPH_TABLE (graph_table MATCH (x IS "Person") -[r1 IS "KNOWS"]- (y IS "Person") -[r2 IS "KNOWS"]- (z IS "Person") COLUMNS (x.name AS "x_name", z.name AS "z_name"))
