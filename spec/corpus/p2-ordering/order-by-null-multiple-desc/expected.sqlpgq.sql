SELECT * FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") COLUMNS (n.name, n.nickname)) ORDER BY "nickname" DESC NULLS FIRST, "name" ASC NULLS LAST
