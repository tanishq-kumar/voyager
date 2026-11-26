SELECT * FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") COLUMNS (n.name)) ORDER BY "name" DESC NULLS FIRST
