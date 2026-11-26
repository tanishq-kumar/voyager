SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) RETURN n.city AS city, COUNT(n.age) AS cnt $$) AS ("city" agtype, "cnt" agtype)
