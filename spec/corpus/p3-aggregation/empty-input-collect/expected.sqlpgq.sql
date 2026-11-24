SELECT ARRAY_AGG(age) AS ages FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") WHERE $p0 COLUMNS (n.age AS age))
