SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) RETURN AVG(n.age) AS avg_age $$) AS (avg_age agtype)
