SELECT * FROM cypher('age_graph', $$ MATCH (a) RETURN a.id AS a, a.id $$) AS ("a" agtype, "id" agtype)
