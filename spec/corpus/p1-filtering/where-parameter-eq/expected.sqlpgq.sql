SELECT * FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") WHERE n.name = $name COLUMNS (n.name))
