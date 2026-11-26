SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) RETURN MIN(n.name) AS min_name $$) AS ("min_name" agtype)
