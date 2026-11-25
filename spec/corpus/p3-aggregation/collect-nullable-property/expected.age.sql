SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) RETURN COLLECT(n.city) AS cities $$) AS ("cities" agtype)
