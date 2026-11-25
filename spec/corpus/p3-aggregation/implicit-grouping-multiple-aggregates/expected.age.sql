SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) RETURN n.city AS city, COUNT(n.age) AS cnt, AVG(n.age) AS avg_age $$) AS (city agtype, cnt agtype, avg_age agtype)
