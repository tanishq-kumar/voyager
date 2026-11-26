SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) RETURN DISTINCT n.city $$) AS ("city" agtype)
