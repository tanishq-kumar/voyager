SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) RETURN COUNT(n) AS cnt $$) AS (cnt agtype)
