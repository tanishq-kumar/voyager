SELECT AVG(age) AS avg_age FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") COLUMNS (n.age AS age))
