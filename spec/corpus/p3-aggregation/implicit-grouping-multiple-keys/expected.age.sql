SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) RETURN n.city AS city, n.active AS active, COUNT(n.age) AS cnt $$) AS (city agtype, active agtype, cnt agtype)
