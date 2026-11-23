SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) RETURN DISTINCT n.city ORDER BY n.city ASC $$) AS (city agtype)
