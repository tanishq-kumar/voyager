SELECT * FROM cypher('age_graph', $$ MATCH (a:A) RETURN a $$) AS (a agtype)
