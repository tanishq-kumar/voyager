SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) RETURN COLLECT(n) AS persons $$) AS (persons agtype)
