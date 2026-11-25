SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) RETURN SUM(n.age) AS total_age $$) AS (total_age agtype)
