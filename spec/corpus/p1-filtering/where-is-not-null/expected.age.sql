SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) WHERE n.nickname IS NOT NULL RETURN n.name $$) AS (name agtype)
