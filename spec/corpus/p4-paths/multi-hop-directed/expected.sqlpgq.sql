SELECT * FROM GRAPH_TABLE (graph_table MATCH (a IS "Person") -[r1 IS "KNOWS"]-> (b IS "Person") -[r2 IS "KNOWS"]-> (c IS "Person") COLUMNS (a.name AS "a_name", c.name AS "c_name"))
