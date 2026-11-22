SELECT * FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") WHERE n.nickname IS NOT NULL COLUMNS (n.name))
