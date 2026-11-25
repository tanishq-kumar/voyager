SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) RETURN COUNT(DISTINCT n) AS cnt $$) AS (cnt agtype)
