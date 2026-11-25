SELECT ARRAY_AGG(n) AS persons FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") COLUMNS (n AS n))
