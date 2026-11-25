SELECT city, AVG(age) AS avg_age FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") COLUMNS (n.city AS city, n.age AS age)) GROUP BY city ORDER BY city ASC NULLS LAST
