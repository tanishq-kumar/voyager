SELECT COALESCE(ARRAY_AGG(age) FILTER (WHERE age IS NOT NULL), '{}') AS ages FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") WHERE $p0 COLUMNS (n.age AS age))
