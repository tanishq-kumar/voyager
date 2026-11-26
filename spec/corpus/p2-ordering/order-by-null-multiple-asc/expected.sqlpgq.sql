SELECT * FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") COLUMNS (n.name, n.nickname)) ORDER BY "nickname" ASC NULLS LAST, "name" ASC NULLS LAST
