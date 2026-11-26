SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) RETURN COLLECT(n.name) AS names $$) AS ("names" agtype)
