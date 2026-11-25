SELECT MAX(name) AS max_name FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") COLUMNS (n.name AS name))
