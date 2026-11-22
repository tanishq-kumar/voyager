SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) WHERE n.nickname IS NULL RETURN n.name $$) AS (name agtype)
