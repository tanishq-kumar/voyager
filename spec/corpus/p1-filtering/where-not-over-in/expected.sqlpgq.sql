SELECT * FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") WHERE NOT (n.name = ANY($names)) COLUMNS (n.name))
