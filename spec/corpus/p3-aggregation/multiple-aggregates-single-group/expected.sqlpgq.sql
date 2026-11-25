SELECT COUNT(age) AS cnt, COALESCE(SUM(age), 0) AS total, AVG(age) AS avg_age, MIN(age) AS min_age, MAX(age) AS max_age FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") COLUMNS (n.age AS age))
