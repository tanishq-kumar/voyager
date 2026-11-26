SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) RETURN MAX(n.name) AS max_name $$) AS ("max_name" agtype)
