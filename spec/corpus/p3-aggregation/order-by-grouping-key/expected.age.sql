SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) RETURN n.city AS city, AVG(n.age) AS avg_age ORDER BY city ASC $$) AS ("city" agtype, "avg_age" agtype)
