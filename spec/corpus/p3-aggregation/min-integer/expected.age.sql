SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) RETURN MIN(n.age) AS min_age $$) AS ("min_age" agtype)
