SELECT COUNT(*) AS cnt FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") COLUMNS (1 AS _c0))
