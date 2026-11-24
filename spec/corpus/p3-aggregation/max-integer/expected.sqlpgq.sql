SELECT MAX(age) AS max_age FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") COLUMNS (n.age AS age))
