SELECT * FROM cypher('age_graph', $$ MATCH (n:Person) RETURN n.city AS city, COUNT(n.age) AS cnt ORDER BY cnt DESC, city ASC LIMIT 2 $$) AS ("city" agtype, "cnt" agtype)
