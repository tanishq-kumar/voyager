SELECT * FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") COLUMNS (n.active, n.name)) ORDER BY "active" DESC NULLS FIRST, "name" ASC NULLS LAST
