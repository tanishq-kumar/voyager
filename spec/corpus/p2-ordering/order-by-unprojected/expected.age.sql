SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) RETURN n.name ORDER BY n.age ASC $$) AS ("name" agtype)
