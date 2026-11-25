SELECT DISTINCT * FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") COLUMNS (n.city, n.active))
