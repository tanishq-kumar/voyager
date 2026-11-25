SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) RETURN COUNT(DISTINCT n.city) AS cnt $$) AS (cnt agtype)
