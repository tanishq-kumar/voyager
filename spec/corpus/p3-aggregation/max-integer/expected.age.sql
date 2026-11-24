SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) RETURN MAX(n.age) AS max_age $$) AS (max_age agtype)
