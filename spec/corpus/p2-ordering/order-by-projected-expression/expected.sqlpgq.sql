SELECT * FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") COLUMNS (n.name, n.age + $p0 AS age_plus_one)) ORDER BY age_plus_one ASC NULLS LAST
