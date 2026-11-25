SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) RETURN DISTINCT n.city, n.active $$) AS (city agtype, active agtype)
