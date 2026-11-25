SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) RETURN coalesce(sum(n.age), 0) AS total_age $$) AS ("total_age" agtype)
