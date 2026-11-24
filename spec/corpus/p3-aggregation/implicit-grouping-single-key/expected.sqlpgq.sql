SELECT city, COUNT(age) AS cnt FROM GRAPH_TABLE (graph_table MATCH (n IS "Person") COLUMNS (n.city AS city, n.age AS age)) GROUP BY city
