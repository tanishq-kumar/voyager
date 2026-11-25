SELECT * FROM cypher('age_graph', $$ MATCH (a) RETURN a.num $$) AS (num agtype)
