SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) RETURN COUNT(n.age) AS cnt $$) AS (cnt agtype)
