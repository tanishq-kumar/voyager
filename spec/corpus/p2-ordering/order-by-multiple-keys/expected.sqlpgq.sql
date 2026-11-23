SELECT * FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") COLUMNS (n.active, n.age)) ORDER BY active ASC NULLS LAST, age ASC NULLS LAST
