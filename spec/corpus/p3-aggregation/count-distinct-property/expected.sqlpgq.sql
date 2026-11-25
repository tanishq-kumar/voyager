SELECT COUNT(DISTINCT city) AS cnt FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") COLUMNS (n.city AS city))
