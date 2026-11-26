SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) RETURN n.name ORDER BY n.name ASC $$) AS ("name" agtype)
