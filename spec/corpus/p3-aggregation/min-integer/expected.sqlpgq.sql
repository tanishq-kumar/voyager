SELECT MIN(age) AS min_age FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") COLUMNS (n.age AS age))
