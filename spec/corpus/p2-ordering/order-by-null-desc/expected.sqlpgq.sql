SELECT * FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") COLUMNS (n.name, n.city)) ORDER BY city DESC NULLS FIRST, name ASC NULLS LAST
