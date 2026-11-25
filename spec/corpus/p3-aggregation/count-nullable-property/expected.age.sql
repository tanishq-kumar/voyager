SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) RETURN COUNT(n.nickname) AS cnt $$) AS ("cnt" agtype)
