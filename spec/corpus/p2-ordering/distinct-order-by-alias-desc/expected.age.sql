SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) RETURN DISTINCT n.city AS town ORDER BY town DESC $$) AS (town agtype)
